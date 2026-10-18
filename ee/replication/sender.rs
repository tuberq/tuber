//! The primary-side tee: a [`tuber::wal::WalObserver`] that forwards WAL
//! records into a bounded channel for a shipping thread to drain.
//!
//! Division of labour, dictated by the observer contract ("fast and
//! infallible, no IO"): [`StreamSender`] runs on the engine's write path
//! and does nothing but clone the record and `try_send`. Body resolution
//! (disk IO) and serialization happen on the *consumer* side — the
//! shipping thread calls [`super::wire::serialize_for_wire`] with the
//! body store it received in [`TeeFrame::Attached`].
//!
//! Overflow kills the stream rather than blocking the engine: on a full
//! channel the sender sets a poison flag and permanently stops
//! forwarding (a stream with a gap is worse than no stream — records are
//! not idempotent to reapply). The receiver drains what was buffered,
//! then observes [`StreamLost::Lagged`]; replicas reconnect and catch up
//! from their last applied position via `WalReader`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TryRecvError, sync_channel};
use std::time::Duration;

use tuber::body_store::BodyStore;
use tuber::wal::{ObserverContext, Position, ScheduleRecord, WalEvent, WalObserver, WalRecord};

/// What flows through the tee channel.
pub enum TeeFrame {
    /// Always the first frame: the attach context, forwarded from
    /// [`WalObserver::on_attach`]. The shipping side keeps the body store
    /// for inlining and the dir + attach position for catch-up scans.
    Attached {
        body_store: Option<Arc<BodyStore>>,
        dir: PathBuf,
        attach_position: Position,
    },
    /// A record appended to the WAL (not yet durable — see `Durable`).
    Record { pos: Position, record: WalRecord },
    /// Every record strictly below `end` is now fsynced on the primary.
    Durable { end: Position },
}

// Manual impl: `BodyStore` has no `Debug`, so show only its presence.
impl std::fmt::Debug for TeeFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TeeFrame::Attached {
                body_store,
                dir,
                attach_position,
            } => f
                .debug_struct("Attached")
                .field("body_store", &body_store.is_some())
                .field("dir", dir)
                .field("attach_position", attach_position)
                .finish(),
            TeeFrame::Record { pos, record } => f
                .debug_struct("Record")
                .field("pos", pos)
                .field("record", record)
                .finish(),
            TeeFrame::Durable { end } => f.debug_struct("Durable").field("end", end).finish(),
        }
    }
}

/// Why the stream ended.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamLost {
    /// The tee channel overflowed; the buffered prefix was delivered but
    /// everything after it is gone. Consumers must resync.
    Lagged,
    /// The sender was dropped (WAL closed / server shut down).
    Disconnected,
}

pub struct StreamSender {
    tx: SyncSender<TeeFrame>,
    lagged: Arc<AtomicBool>,
}

pub struct StreamReceiver {
    rx: Receiver<TeeFrame>,
    lagged: Arc<AtomicBool>,
}

/// Create a tee channel holding at most `capacity` frames. Size it for
/// the shipping thread's worst hiccup — each buffered Put costs a boxed
/// `Job` clone (body bytes are *not* in the frame; they inline at ship
/// time).
pub fn stream_channel(capacity: usize) -> (StreamSender, StreamReceiver) {
    let (tx, rx) = sync_channel(capacity);
    let lagged = Arc::new(AtomicBool::new(false));
    (
        StreamSender {
            tx,
            lagged: Arc::clone(&lagged),
        },
        StreamReceiver { rx, lagged },
    )
}

impl StreamSender {
    /// True once the channel has overflowed (or the receiver vanished) —
    /// the stream is dead and the sender forwards nothing further.
    pub fn is_lagged(&self) -> bool {
        self.lagged.load(Ordering::Relaxed)
    }

    fn forward(&self, frame: TeeFrame) {
        if self.is_lagged() {
            return;
        }
        if self.tx.try_send(frame).is_err() {
            // Full or disconnected — either way the stream can no longer
            // be gapless, so poison it. One log line, not one per record.
            self.lagged.store(true, Ordering::Relaxed);
            tracing::warn!("replication tee overflowed; stream poisoned, replicas must resync");
        }
    }
}

impl WalObserver for StreamSender {
    fn on_attach(&mut self, ctx: ObserverContext) {
        self.forward(TeeFrame::Attached {
            body_store: ctx.body_store,
            dir: ctx.dir,
            attach_position: ctx.attach_position,
        });
    }

    fn on_record(&mut self, pos: Position, event: WalEvent<'_>) {
        if self.is_lagged() {
            return; // skip the clone, not just the send
        }
        let record = match event {
            WalEvent::Put { job } => WalRecord::FullJob(Box::new(job.clone())),
            WalEvent::StateChange {
                job_id,
                new_state,
                new_priority,
                new_delay_nanos,
                expiry_epoch_secs,
                reason,
                change_epoch_secs,
            } => WalRecord::StateChange {
                job_id,
                new_state,
                new_priority,
                new_delay_nanos,
                expiry_epoch_secs,
                reason,
                change_epoch_secs,
            },
            WalEvent::Tombstone {
                job_id,
                tube,
                key,
                expires_epoch_secs,
            } => WalRecord::Tombstone {
                job_id,
                tube: tube.to_owned(),
                key: key.to_owned(),
                expires_epoch_secs,
            },
            WalEvent::IdMark { high_water } => WalRecord::IdMark { high_water },
            WalEvent::ToastMark(point) => WalRecord::ToastMark(point),
            // Bounded: tuber caps an owner's stored set (8 MiB).
            WalEvent::ScheduleSet { owner, entries } => WalRecord::ScheduleSet {
                owner: owner.to_owned(),
                entries: entries
                    .iter()
                    .map(|e| ScheduleRecord {
                        line: e.line.to_owned(),
                        body: e.body.to_vec(),
                        synced_at: e.synced_at,
                        last_fire: e.last_fire,
                    })
                    .collect(),
            },
            WalEvent::ScheduleFired {
                owner,
                name,
                last_fire,
            } => WalRecord::ScheduleFired {
                owner: owner.to_owned(),
                name: name.to_owned(),
                last_fire,
            },
        };
        self.forward(TeeFrame::Record { pos, record });
    }

    fn on_sync(&mut self, end: Position) {
        self.forward(TeeFrame::Durable { end });
    }
}

impl StreamReceiver {
    /// Non-blocking variant of [`recv`](Self::recv): `Ok(None)` when the
    /// channel is empty and the stream is still alive.
    pub fn try_next(&self) -> Result<Option<TeeFrame>, StreamLost> {
        match self.rx.try_recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(TryRecvError::Empty) => {
                if self.lagged.load(Ordering::Relaxed) {
                    Err(StreamLost::Lagged)
                } else {
                    Ok(None)
                }
            }
            Err(TryRecvError::Disconnected) => Err(StreamLost::Disconnected),
        }
    }

    /// Next frame, blocking. Buffered frames drain out even after the
    /// sender lags — the delivered prefix is still gapless and usable —
    /// then the poison surfaces as `Err(Lagged)`.
    pub fn recv(&self) -> Result<TeeFrame, StreamLost> {
        loop {
            match self.rx.try_recv() {
                Ok(frame) => return Ok(frame),
                Err(TryRecvError::Empty) => {
                    if self.lagged.load(Ordering::Relaxed) {
                        return Err(StreamLost::Lagged);
                    }
                    // Block, but wake periodically to notice a lag flag
                    // set while we were parked on an empty channel.
                    match self.rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(frame) => return Ok(frame),
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(StreamLost::Disconnected);
                        }
                    }
                }
                Err(TryRecvError::Disconnected) => return Err(StreamLost::Disconnected),
            }
        }
    }
}
