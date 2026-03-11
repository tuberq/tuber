use crate::client::TuberClient;
use std::io;

pub async fn run(addr: &str, tube: Option<String>) -> io::Result<()> {
    let mut client = TuberClient::connect(addr).await?;

    let body = if let Some(tube) = tube {
        client.stats_tube(&tube).await?
    } else {
        client.stats().await?
    };

    print!("{body}");
    Ok(())
}
