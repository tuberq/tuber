//! The primary-side shipping loop: serve one replica connection from its
//! handshake position to disconnect.
//!
//! The handoff between catch-up and the live tee is the delicate part.
//! The design rule is *subscribe first, then scan*: on connect we stop
//! discarding tee frames and start buffering them, and only then open a
//! [`WalReader`] at the replica's position. Every record then reaches the
//! replica through at least one of the two paths —
//!
//! - appended before the connection and flushed: on disk, the scan finds it;
//! - appended after the connection: teed into our buffer;
//! - appended before the connection but still sitting in the writer's
//!   buffer (and its tee frame possibly discarded pre-connect): this is
//!   the hole the **durable gate** closes. We hold the scan until the tee
//!   delivers a `Durable` frame. The channel is FIFO, so any `Durable` we
//!   observe was enqueued *after* every frame we discarded — meaning its
//!   fsync flushed those records to disk, where the scan will find them.
//!   If the channel stays silent for `flush_wait` instead, the engine has
//!   been idle past a sync interval and the disk is already complete.
//!
//! Overlap between the two paths is expected and harmless: the replica
//! applies only records above its cursor ([`super::follower::Follower`]).
//! Order within each path is ascending, which is all the dedup needs.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tuber::body_store::BodyStore;
use tuber::wal::{Position, WalReader, WalRecord};

use super::sender::{StreamLost, StreamReceiver, TeeFrame};
use super::wire::{self, BodyUnreadable, Frame};

/// How a single replica connection ended. The listener goes back to
/// accepting in every case.
#[derive(Debug)]
pub enum ServeEnd {
    /// The replica hung up (or its socket errored). It can reconnect and
    /// resume from its position — nothing is lost.
    ReplicaDropped(std::io::Error),
    /// The replica read too slowly: the tee buffer passed `buffer_cap`
    /// while we were still scanning. Dropped rather than allowed to run
    /// the primary out of memory; on reconnect the backlog is served
    /// from disk, where it costs nothing to hold.
    ReplicaTooSlow,
}

/// Fatal ends: the stream (not just one replica) is done.
#[derive(Debug)]
pub enum ServeError {
    /// The replica's handshake was absent or malformed.
    Handshake(std::io::Error),
    /// The tee overflowed or the WAL closed; see [`StreamLost`].
    Stream(StreamLost),
    /// The catch-up scan hit an error the WAL layer calls real corruption
    /// (a sealed segment failed to parse). A replica retry would hit the
    /// same bytes — this needs an operator.
    Scan(String),
    /// A live job's body failed its check on the primary. Shipping it as
    /// missing would give the replica an empty body, and every reconnect
    /// would hit the same body — this needs an operator too.
    Body(BodyUnreadable),
}

/// Why shipping one record failed.
enum ShipFailure {
    /// The replica's socket; it can reconnect and resume.
    Conn(std::io::Error),
    Body(BodyUnreadable),
}

impl ShipFailure {
    /// Where a failed ship leaves `serve`.
    fn end(self) -> Result<ServeEnd, ServeError> {
        match self {
            ShipFailure::Conn(e) => Ok(ServeEnd::ReplicaDropped(e)),
            ShipFailure::Body(e) => Err(ServeError::Body(e)),
        }
    }
}

pub struct Shipper {
    rx: StreamReceiver,
    body_store: Option<Arc<BodyStore>>,
    wal_dir: PathBuf,
    /// Durable-gate fallback: how long the tee must stay silent before we
    /// conclude the engine is idle and the disk complete. Must comfortably
    /// exceed the server's `--sync-interval`.
    pub flush_wait: Duration,
    /// Buffered-frame ceiling during catch-up; see [`ServeEnd::ReplicaTooSlow`].
    pub buffer_cap: usize,
    /// How many scanned records to ship between tee drains. Only pacing —
    /// correctness doesn't depend on it.
    pub drain_every: usize,
}

impl Shipper {
    /// Consume the tee's opening `Attached` frame (the observer sends it
    /// before any record — server startup order guarantees that) and
    /// build a shipper around what it carries.
    pub fn from_tee(rx: StreamReceiver) -> Result<Shipper, StreamLost> {
        loop {
            match rx.recv()? {
                TeeFrame::Attached {
                    body_store, dir, ..
                } => {
                    return Ok(Shipper {
                        rx,
                        body_store,
                        wal_dir: dir,
                        flush_wait: Duration::from_millis(250),
                        buffer_cap: 262_144,
                        drain_every: 256,
                    });
                }
                _ => continue,
            }
        }
    }

    /// Discard everything pending on the tee. For the accept loop: while
    /// no replica is connected, unshipped frames must not pile up into an
    /// overflow — the disk retains them for the eventual catch-up scan.
    pub fn discard_pending(&self) -> Result<(), StreamLost> {
        while self.rx.try_next()?.is_some() {}
        Ok(())
    }

    /// Serve one replica to completion: handshake, catch-up from its
    /// position, live tee until it drops.
    pub fn serve<C: Read + Write>(&self, conn: &mut C) -> Result<ServeEnd, ServeError> {
        // Handshake: the replica leads with where it is.
        let mut rbuf = Vec::new();
        let from = match wire::read_frame(conn, &mut rbuf).map_err(ServeError::Handshake)? {
            Frame::Hello { last_applied } => last_applied,
            other => {
                return Err(ServeError::Handshake(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("expected hello, got {other:?}"),
                )));
            }
        };

        // Subscribe: buffer the tee from here on. The durable gate (see
        // module docs) decides when the disk is complete enough to scan.
        let mut buffered: Vec<TeeFrame> = Vec::new();
        let idle_since = Instant::now();
        loop {
            match self.rx.try_next().map_err(ServeError::Stream)? {
                Some(frame) => {
                    let gate_open = matches!(frame, TeeFrame::Durable { .. });
                    buffered.push(frame);
                    if gate_open {
                        break;
                    }
                }
                None => {
                    if idle_since.elapsed() >= self.flush_wait {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }

        // Scan: everything durable from the replica's position up. The
        // reader stops at the durable frontier (a truncated tail is the
        // writer's buffer, not corruption) — the remainder is in `buffered`.
        let mut reader =
            WalReader::open(&self.wal_dir, from).map_err(|e| ServeError::Scan(e.to_string()))?;
        let mut since_drain = 0usize;
        while let Some((pos, record)) = reader
            .next_record()
            .map_err(|e| ServeError::Scan(format!("{e:?}")))?
        {
            if let Err(failure) = self.ship_record(conn, pos, &record) {
                return failure.end();
            }
            since_drain += 1;
            if since_drain >= self.drain_every {
                since_drain = 0;
                if !self.drain_into(&mut buffered).map_err(ServeError::Stream)? {
                    return Ok(ServeEnd::ReplicaTooSlow);
                }
            }
        }
        if !self.drain_into(&mut buffered).map_err(ServeError::Stream)? {
            return Ok(ServeEnd::ReplicaTooSlow);
        }

        // Replay the buffer (overlap with the scan dedupes replica-side),
        // then follow the tee live.
        for frame in buffered.drain(..) {
            if let Err(failure) = self.ship_tee(conn, frame) {
                return failure.end();
            }
        }
        loop {
            let frame = self.rx.recv().map_err(ServeError::Stream)?;
            if let Err(failure) = self.ship_tee(conn, frame) {
                return failure.end();
            }
        }
    }

    /// Pull everything currently queued on the tee into `buffered`.
    /// `false` means the buffer cap was blown.
    fn drain_into(&self, buffered: &mut Vec<TeeFrame>) -> Result<bool, StreamLost> {
        while let Some(frame) = self.rx.try_next()? {
            buffered.push(frame);
            if buffered.len() > self.buffer_cap {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn ship_tee<C: Write>(&self, conn: &mut C, frame: TeeFrame) -> Result<(), ShipFailure> {
        match frame {
            TeeFrame::Record { pos, record } => self.ship_record(conn, pos, &record),
            TeeFrame::Durable { end } => conn
                .write_all(&wire::encode_durable(end))
                .map_err(ShipFailure::Conn),
            TeeFrame::Attached { .. } => Ok(()),
        }
    }

    fn ship_record<C: Write>(
        &self,
        conn: &mut C,
        pos: Position,
        record: &WalRecord,
    ) -> Result<(), ShipFailure> {
        let (bytes, flags) = wire::serialize_for_wire(record, self.body_store.as_deref())
            .map_err(ShipFailure::Body)?;
        conn.write_all(&wire::encode_record(pos, &bytes, flags))
            .map_err(ShipFailure::Conn)
    }
}
