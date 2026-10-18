//! WAL streaming replication for tuber. Licensed under BUSL-1.1, not MIT:
//! see `ee/LICENSE`.
//!
//! Written as if it were a crate of its own: it reaches tuber only through
//! `tuber::` paths to items tuber makes `pub` (the WAL seam, `cli`,
//! `server::ServerConfig`), never `crate::` or `pub(crate)` internals, and
//! its own modules through `super::`. That keeps the seam the only
//! boundary between the MIT and BUSL code, and a move to a separate crate
//! mechanical. `ee/tests/boundary.rs` holds it: the build fails otherwise.
//!
//! Design: `ee/docs/replication.md`. The load-bearing
//! decisions, briefly:
//!
//! - **Stream = WAL, verbatim.** Every frame carries a WAL record at its
//!   WAL position; catch-up (a [`tuber::wal::WalReader`] scan) and the
//!   live tee produce the same frames, so the replica can't tell the
//!   phases apart and dedupes purely by position. Records are *not*
//!   idempotent to reapply (a duplicate Reserve double-increments
//!   `reserve_ct`) — position dedup is what makes the overlap between
//!   phases harmless.
//! - **Bodies cross the wire inline, exactly once.** The sender resolves
//!   `BodyRef::External` through the primary's live body store and
//!   re-serializes the record with the body inline — the record shape the
//!   OSS replayer already migrates into TOAST. Compaction stays local to
//!   each side.
//! - **The tee never blocks the engine.** [`StreamSender`] pushes into a
//!   bounded channel with `try_send`; overflow kills the stream (the
//!   replica reconnects and catches up from its last position) rather
//!   than back-pressuring puts.

pub mod follower;
pub mod listener;
pub mod sender;
pub mod ship;
pub mod wire;

pub use follower::{Follower, FollowerEvent};
pub use listener::{configure, with_flags};
pub use sender::{StreamLost, StreamReceiver, StreamSender, TeeFrame, stream_channel};
pub use ship::{ServeEnd, ServeError, Shipper};
