//! The replica-side ingest: connect, say where we are, and iterate new
//! records exactly once.
//!
//! The primary's stream deliberately overlaps — catch-up scan and live
//! tee can both carry a record near the handoff — and the two phases are
//! each internally ascending. `Follower` is where that contract is
//! cashed in: it tracks the applied cursor and yields only records
//! strictly above it, so the caller applies each record exactly once
//! without ever knowing which phase it arrived through.

use std::io::{Read, Write};

use tuber::wal::{Position, WalRecord};

use super::wire::{self, Frame};

#[derive(Debug)]
pub enum FollowerEvent {
    /// A record the caller has not seen before. Apply it, then persist
    /// `Follower::last_applied` as the resume cursor.
    Record {
        pos: Position,
        record: WalRecord,
        flags: u8,
    },
    /// Every record strictly below `end` is durable on the primary.
    Durable { end: Position },
}

pub struct Follower<C> {
    conn: C,
    buf: Vec<u8>,
    last_applied: Position,
}

impl<C: Read + Write> Follower<C> {
    /// Handshake over an established connection: tell the primary the
    /// last position we applied (`Position::ZERO` to bootstrap — the WAL
    /// is its own base backup) and start reading the stream.
    pub fn start(mut conn: C, last_applied: Position) -> std::io::Result<Follower<C>> {
        conn.write_all(&wire::encode_hello(last_applied))?;
        Ok(Follower {
            conn,
            buf: Vec::new(),
            last_applied,
        })
    }

    /// The resume cursor: the position of the newest record yielded.
    /// Persist this (via its `Display` form) to survive restarts.
    pub fn last_applied(&self) -> Position {
        self.last_applied
    }

    /// Next event. Records at or below the cursor — the catch-up/tee
    /// overlap — are skipped silently; every yielded record is new.
    pub fn next_event(&mut self) -> std::io::Result<FollowerEvent> {
        loop {
            match wire::read_frame(&mut self.conn, &mut self.buf)? {
                Frame::Record { pos, record, flags } => {
                    if pos <= self.last_applied {
                        continue;
                    }
                    self.last_applied = pos;
                    return Ok(FollowerEvent::Record { pos, record, flags });
                }
                Frame::Durable { end } => return Ok(FollowerEvent::Durable { end }),
                Frame::Hello { .. } => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "primary sent a hello frame",
                    ));
                }
            }
        }
    }
}
