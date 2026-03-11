use crate::client::TuberClient;
use std::io;
use tokio::io::AsyncBufReadExt;

pub async fn run(
    addr: &str,
    tube: &str,
    priority: u32,
    delay: u32,
    ttr: u32,
    body: Option<String>,
    idempotency_key: Option<String>,
) -> io::Result<()> {
    let mut client = TuberClient::connect(addr).await?;

    if tube != "default" {
        let resp = client.use_tube(tube).await?;
        if !resp.starts_with("USING") {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("use tube failed: {resp}"),
            ));
        }
    }

    let idp = idempotency_key.as_deref();

    if let Some(body) = body {
        let resp = client
            .put(priority, delay, ttr, body.as_bytes(), idp)
            .await?;
        println!("{resp}");
    } else {
        let stdin = tokio::io::BufReader::new(tokio::io::stdin());
        let mut lines = stdin.lines();
        while let Some(line) = lines.next_line().await? {
            if line.is_empty() {
                continue;
            }
            let resp = client
                .put(priority, delay, ttr, line.as_bytes(), idp)
                .await?;
            println!("{resp}");
        }
    }

    Ok(())
}
