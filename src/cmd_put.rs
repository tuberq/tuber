use crate::client::TuberClient;
use std::io;
use tokio::io::AsyncBufReadExt;

/// Puts sent, and how many the server turned down. A rejection doesn't stop a
/// stdin run — the remaining lines are still put — but it fails the exit.
#[derive(Default)]
struct Tally {
    sent: usize,
    rejected: usize,
}

impl Tally {
    fn record(&mut self, resp: &str) -> io::Result<()> {
        self.sent += 1;
        // Anything but INSERTED is the server refusing the job: JOB_TOO_BIG,
        // DRAINING, OUT_OF_MEMORY, BAD_FORMAT, ... A dedup hit replies
        // `INSERTED <id> <state>` and is the idempotency key working.
        if !resp.starts_with("INSERTED ") {
            self.rejected += 1;
        }
        crate::outln!("{resp}")
    }
}

// One parameter per CLI flag of `tuber put`; clap already owns the grouping.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    addr: &str,
    tube: &str,
    priority: u32,
    delay: u32,
    ttr: u32,
    body: Option<String>,
    idempotency_key: Option<String>,
    group: Option<String>,
    after_group: Option<String>,
    concurrency_key: Option<String>,
) -> io::Result<()> {
    let mut client = TuberClient::connect(addr).await?;

    if tube != "default" {
        let resp = client.use_tube(tube).await?;
        if !resp.starts_with("USING") {
            return Err(io::Error::other(format!("use tube failed: {resp}")));
        }
    }

    let idp = idempotency_key.as_deref();
    let grp = group.as_deref();
    let aft = after_group.as_deref();
    let con = concurrency_key.as_deref();

    let mut tally = Tally::default();
    if let Some(body) = body {
        let resp = client
            .put(priority, delay, ttr, body.as_bytes(), idp, grp, aft, con)
            .await?;
        tally.record(&resp)?;
    } else {
        let stdin = tokio::io::BufReader::new(tokio::io::stdin());
        let mut lines = stdin.lines();
        while let Some(line) = lines.next_line().await? {
            if line.is_empty() {
                continue;
            }
            let resp = client
                .put(priority, delay, ttr, line.as_bytes(), idp, grp, aft, con)
                .await?;
            tally.record(&resp)?;
        }
    }

    if tally.rejected > 0 {
        return Err(io::Error::other(format!(
            "{} of {} puts rejected",
            tally.rejected, tally.sent
        )));
    }
    Ok(())
}
