//! The primary's replication listener, and the `server` flag that starts
//! it.
//!
//! With `--replication-listen ADDR`, a replica connects, sends the position
//! it last applied (`0:0` to bootstrap), receives a catch-up scan from the
//! primary's WAL and then the live tee — one seamless stream, overlap
//! deduped replica-side by position. One replica at a time; a dropped
//! replica reconnects and resumes from its cursor.
//!
//! Not here yet: replica mode (`--replica-of` with the apply loop),
//! promotion.

use std::net::TcpListener;
use std::time::Duration;

use clap::{Arg, ArgMatches, Command};
use tuber::cli::HookError;
use tuber::server::ServerConfig;

use super::sender::{StreamReceiver, stream_channel};
use super::ship::{ServeEnd, ServeError, Shipper};

/// Frames buffered between the engine's tee and the shipping thread.
/// Overflow poisons the stream (replicas must resync), so this is sized
/// for a shipping hiccup, not sustained slowness.
const TEE_CAPACITY: usize = 4096;

/// `server` with the replication flags added.
pub fn with_flags(server: Command) -> Command {
    server.arg(
        Arg::new("replication_listen")
            .long("replication-listen")
            .env("TUBER_REPLICATION_LISTEN")
            .value_name("ADDR")
            .help("Serve WAL replication to a replica on ADDR (host:port); needs --binlog-dir"),
    )
}

/// The [`tuber::cli::run`] hook: when `--replication-listen` is given,
/// attach the tee to the server's WAL and start the listener.
pub fn configure(server: &ArgMatches, config: &mut ServerConfig) -> Result<(), HookError> {
    let Some(addr) = server.get_one::<String>("replication_listen") else {
        return Ok(());
    };
    if config.wal_dir.is_none() {
        return Err("--replication-listen needs --binlog-dir: replication ships the WAL".into());
    }
    // Bind before the server starts, so a bad address stops startup
    // rather than leaving a primary that nothing can replicate from.
    let listener = TcpListener::bind(addr)
        .map_err(|e| format!("replication listener can't bind {addr}: {e}"))?;
    listener.set_nonblocking(true)?;
    tracing::info!("replication listener on {addr}");

    let (sender, receiver) = stream_channel(TEE_CAPACITY);
    config.wal_observer = Some(Box::new(sender));
    std::thread::Builder::new()
        .name("replication-ship".into())
        .spawn(move || serve_replicas(listener, receiver))?;
    Ok(())
}

fn serve_replicas(listener: TcpListener, rx: StreamReceiver) {
    let shipper = match Shipper::from_tee(rx) {
        Ok(s) => s,
        Err(lost) => {
            tracing::error!("tee ended before attach: {lost:?}");
            return;
        }
    };

    loop {
        // Wait for a replica, discarding tee frames meanwhile — the disk
        // retains everything for the catch-up scan, and letting frames
        // sit would overflow the channel.
        let mut conn = loop {
            match listener.accept() {
                Ok((conn, peer)) => {
                    tracing::info!(%peer, "replica connected");
                    // BSD/macOS accepted sockets inherit the listener's
                    // non-blocking flag; the serve loop wants blocking IO.
                    conn.set_nonblocking(false)
                        .expect("blocking replica socket");
                    conn.set_nodelay(true).ok();
                    break conn;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    match shipper.discard_pending() {
                        Ok(()) => std::thread::sleep(Duration::from_millis(5)),
                        Err(lost) => {
                            tracing::error!("replication stream ended: {lost:?}");
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("replication accept failed: {e}");
                    return;
                }
            }
        };

        match shipper.serve(&mut conn) {
            Ok(ServeEnd::ReplicaDropped(e)) => tracing::warn!("replica dropped: {e}"),
            Ok(ServeEnd::ReplicaTooSlow) => {
                tracing::warn!(
                    "replica too slow for live catch-up; dropped, will resync from disk"
                );
            }
            Err(ServeError::Handshake(e)) => tracing::warn!("bad replica handshake: {e}"),
            Err(ServeError::Stream(lost)) => {
                tracing::error!("replication stream ended: {lost:?}");
                return;
            }
            Err(ServeError::Scan(e)) => {
                tracing::error!("catch-up scan failed — WAL needs attention: {e}");
                return;
            }
            Err(ServeError::Body(e)) => {
                tracing::error!("replication stopped — TOAST needs attention: {e}");
                return;
            }
        }
    }
}
