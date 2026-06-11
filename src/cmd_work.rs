use crate::client::{ReserveResult, TuberClient};
use std::io;
use std::time::Duration;
use tokio::process::Command;
use tokio::signal;
use tokio::sync::watch;

/// Fallback time-to-run (seconds) used when the server's `stats-job` doesn't
/// report a usable `ttr` for a reserved job. Matches beanstalkd's default.
const DEFAULT_TTR_SECS: u32 = 60;
/// Give up a worker after this many consecutive connect/session failures.
const MAX_CONSECUTIVE_FAILURES: u32 = 10;
/// Reconnect backoff bounds.
const BACKOFF_BASE: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

pub async fn run(addr: &str, tube: &str, parallel: usize) -> io::Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        wait_for_signal().await;
        eprintln!("\nShutting down gracefully...");
        let _ = shutdown_tx.send(true);
    });

    let mut handles = Vec::new();
    for worker_id in 0..parallel {
        let addr = addr.to_string();
        let tube = tube.to_string();
        let shutdown_rx = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            supervise_worker(worker_id, &addr, &tube, shutdown_rx).await
        }));
    }

    // A worker returns Ok(()) when it stopped because of a graceful shutdown,
    // and Err(..) when it gave up after exhausting reconnect attempts. Exit
    // non-zero only on total failure (every worker gave up).
    let mut total = 0usize;
    let mut failed = 0usize;
    for handle in handles {
        total += 1;
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                failed += 1;
                eprintln!("worker error: {e}");
            }
            Err(e) => {
                // Join error (panic / cancellation) counts as a failure.
                failed += 1;
                eprintln!("worker task failed: {e}");
            }
        }
    }

    if total > 0 && failed == total {
        return Err(io::Error::other("all workers exited with errors"));
    }
    Ok(())
}

/// Block until SIGINT (Ctrl-C) or, on unix, SIGTERM is received.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                signal::ctrl_c().await.ok();
            }
        }
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await.ok();
    }
}

/// Supervise a single worker: (re)connect with exponential backoff and run its
/// job loop. Returns Ok(()) on graceful shutdown, Err once reconnect attempts
/// are exhausted.
async fn supervise_worker(
    worker_id: usize,
    addr: &str,
    tube: &str,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut failures = 0u32;
    let mut backoff = BACKOFF_BASE;

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        match connect_session(addr, tube).await {
            Ok(mut client) => {
                // A working session resets the failure budget.
                failures = 0;
                backoff = BACKOFF_BASE;
                match worker_loop(worker_id, &mut client, &mut shutdown_rx).await {
                    Ok(()) => return Ok(()), // graceful shutdown
                    Err(e) => {
                        if *shutdown_rx.borrow() {
                            return Ok(());
                        }
                        eprintln!("worker {worker_id}: session ended: {e}");
                    }
                }
            }
            Err(e) => {
                if *shutdown_rx.borrow() {
                    return Ok(());
                }
                eprintln!("worker {worker_id}: connect failed: {e}");
                failures += 1;
                if failures >= MAX_CONSECUTIVE_FAILURES {
                    return Err(io::Error::other(format!(
                        "worker {worker_id}: giving up after {failures} consecutive failures"
                    )));
                }
            }
        }

        // Wait out the backoff, but wake immediately on shutdown.
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown_rx.changed() => return Ok(()),
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// Connect and establish the tube subscription, validating server responses.
async fn connect_session(addr: &str, tube: &str) -> io::Result<TuberClient> {
    let mut client = TuberClient::connect(addr).await?;
    if tube != "default" {
        let resp = client.watch(tube).await?;
        if !resp.starts_with("WATCHING") {
            return Err(io::Error::other(format!("watch {tube} failed: {resp:?}")));
        }
        let resp = client.ignore("default").await?;
        // `ignore` returns `WATCHING <n>` on success; anything else (e.g.
        // NOT_IGNORED) means we may still pull from `default` — refuse to run.
        if !resp.starts_with("WATCHING") {
            return Err(io::Error::other(format!("ignore default failed: {resp:?}")));
        }
    }
    Ok(client)
}

enum JobOutcome {
    Finished(std::process::ExitStatus),
    Interrupted,
}

async fn worker_loop(
    worker_id: usize,
    client: &mut TuberClient,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        match client.reserve_with_timeout(5).await? {
            ReserveResult::Reserved { id, body } => {
                let body_str = String::from_utf8_lossy(&body).into_owned();
                eprintln!("worker {worker_id}: executing job {id}: {body_str}");

                let ttr = job_ttr(client, id).await;
                let outcome = run_job(client, worker_id, id, &body_str, ttr, shutdown_rx).await?;
                match outcome {
                    JobOutcome::Finished(status) => {
                        if status.success() {
                            let resp = client.delete(id).await?;
                            if resp.trim() == "DELETED" {
                                eprintln!("worker {worker_id}: job {id} completed");
                            } else {
                                eprintln!(
                                    "worker {worker_id}: job {id} finished OK but delete returned {resp:?} \
                                     — job may have timed out (TTR) and been reassigned"
                                );
                            }
                        } else {
                            let code = status.code().unwrap_or(-1);
                            let resp = client.bury(id, 0).await?;
                            if resp.trim() == "BURIED" {
                                eprintln!(
                                    "worker {worker_id}: job {id} failed (exit {code}), buried"
                                );
                            } else {
                                eprintln!(
                                    "worker {worker_id}: job {id} failed (exit {code}) but bury returned {resp:?} \
                                     — job may have timed out (TTR) and been reassigned"
                                );
                            }
                        }
                    }
                    JobOutcome::Interrupted => {
                        // Shutdown while the job was running: hand it back for
                        // another worker rather than burying it.
                        let resp = client.release(id, 0, 0).await?;
                        eprintln!(
                            "worker {worker_id}: shutdown — released job {id} ({})",
                            resp.trim()
                        );
                        break;
                    }
                }
            }
            ReserveResult::TimedOut => continue,
            ReserveResult::Error(msg) => {
                // Protocol-level error (e.g. DEADLINE_SOON). Pause briefly
                // rather than hot-looping; honour shutdown while waiting.
                eprintln!("worker {worker_id}: reserve error: {msg}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown_rx.changed() => break,
                }
            }
        }
    }

    eprintln!("worker {worker_id}: stopped");
    Ok(())
}

/// Run a job's body as a shell command, touching it every ~ttr/2 to keep the
/// reservation alive, and aborting if shutdown is requested mid-run.
async fn run_job(
    client: &mut TuberClient,
    worker_id: usize,
    id: u64,
    body: &str,
    ttr: u32,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> io::Result<JobOutcome> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(body)
        .kill_on_drop(true)
        .spawn()?;

    let touch_period = Duration::from_secs((ttr / 2).max(1) as u64);
    let mut touch_interval = tokio::time::interval(touch_period);
    touch_interval.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            status = child.wait() => {
                return Ok(JobOutcome::Finished(status?));
            }
            _ = touch_interval.tick() => {
                let resp = client.touch(id).await?;
                if resp.trim() != "TOUCHED" {
                    eprintln!(
                        "worker {worker_id}: touch on job {id} returned {resp:?} \
                         — reservation may be lost (job could be reassigned)"
                    );
                }
            }
            _ = shutdown_rx.changed() => {
                let _ = child.kill().await;
                return Ok(JobOutcome::Interrupted);
            }
        }
    }
}

/// Best-effort lookup of a job's TTR via `stats-job`; falls back to the default
/// if the job can't be inspected (e.g. NOT_FOUND or a parse miss).
async fn job_ttr(client: &mut TuberClient, id: u64) -> u32 {
    match client.stats_job(id).await {
        Ok(body) => parse_ttr(&body).unwrap_or(DEFAULT_TTR_SECS),
        Err(_) => DEFAULT_TTR_SECS,
    }
}

/// Parse the `ttr:` field out of a `stats-job` YAML body.
fn parse_ttr(body: &str) -> Option<u32> {
    for line in body.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("ttr:")
            && let Ok(v) = rest.trim().parse::<u32>()
        {
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ttr_reads_value() {
        let body = "id: 1\ntube: default\nstate: reserved\nttr: 120\npri: 0\n";
        assert_eq!(parse_ttr(body), Some(120));
    }

    #[test]
    fn parse_ttr_missing_field() {
        let body = "id: 1\ntube: default\nstate: reserved\n";
        assert_eq!(parse_ttr(body), None);
    }
}
