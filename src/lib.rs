// Lets `ee/` name this crate `tuber::`, as a crate depending on it would.
extern crate self as tuber;

pub mod body_store;
pub mod cli;
pub mod cli_output;
pub mod client;
pub mod cmd_put;
pub mod cmd_schedules;
pub mod cmd_stats;
pub mod cmd_tubes;
pub mod cmd_work;
pub mod conn;
// BUSL-1.1, not MIT: see ee/LICENSE.
#[path = "../ee/mod.rs"]
pub mod ee;
pub mod heap;
pub mod job;
pub mod metrics;
pub mod protocol;
pub mod schedule;
pub mod server;
pub mod shrink;
pub mod telemetry;
pub mod tube;
pub mod wal;
