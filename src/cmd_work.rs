use crate::client::{ReserveResult, TuberClient};
use std::io;
use tokio::process::Command;
use tokio::signal;
use tokio::sync::watch;

pub async fn run(addr: &str, tube: &str, parallel: usize) -> io::Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        eprintln!("\nShutting down gracefully...");
        let _ = shutdown_tx.send(true);
    });

    let mut handles = Vec::new();
    for worker_id in 0..parallel {
        let addr = addr.to_string();
        let tube = tube.to_string();
        let shutdown_rx = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = worker_loop(worker_id, &addr, &tube, shutdown_rx).await {
                eprintln!("worker {worker_id}: error: {e}");
            }
        }));
    }

    for handle in handles {
        handle.await.ok();
    }

    Ok(())
}

async fn worker_loop(
    worker_id: usize,
    addr: &str,
    tube: &str,
    shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut client = TuberClient::connect(addr).await?;

    if tube != "default" {
        client.watch(tube).await?;
        client.ignore("default").await?;
    }

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        match client.reserve_with_timeout(5).await? {
            ReserveResult::Reserved { id, body } => {
                let body_str = String::from_utf8_lossy(&body);
                eprintln!("worker {worker_id}: executing job {id}: {body_str}");

                let status = Command::new("sh")
                    .arg("-c")
                    .arg(body_str.as_ref())
                    .status()
                    .await?;

                if status.success() {
                    client.delete(id).await?;
                    eprintln!("worker {worker_id}: job {id} completed");
                } else {
                    let code = status.code().unwrap_or(-1);
                    eprintln!("worker {worker_id}: job {id} failed (exit {code}), burying");
                    client.bury(id, 0).await?;
                }
            }
            ReserveResult::TimedOut => continue,
            ReserveResult::Error(msg) => {
                eprintln!("worker {worker_id}: reserve error: {msg}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    eprintln!("worker {worker_id}: stopped");
    Ok(())
}
