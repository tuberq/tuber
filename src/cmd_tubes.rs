use crate::client::TuberClient;
use std::io;

pub async fn run(addr: &str) -> io::Result<()> {
    let mut client = TuberClient::connect(addr).await?;

    let body = client.list_tubes().await?;

    // Parse tube names from YAML list format (- name\n)
    let tubes: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix("- "))
        .collect();

    if tubes.is_empty() {
        println!("No tubes.");
        return Ok(());
    }

    // Fetch stats for each tube and display a summary
    for tube in &tubes {
        match client.stats_tube(tube).await {
            Ok(stats) => {
                let get = |key: &str| -> String {
                    stats
                        .lines()
                        .find(|l| l.starts_with(&format!("{key}:")))
                        .and_then(|l| l.split_once(':'))
                        .map(|(_, v)| v.trim().to_string())
                        .unwrap_or_else(|| "?".to_string())
                };

                let ready = get("current-jobs-ready");
                let reserved = get("current-jobs-reserved");
                let delayed = get("current-jobs-delayed");
                let buried = get("current-jobs-buried");

                println!(
                    "{tube}: ready={ready} reserved={reserved} delayed={delayed} buried={buried}"
                );
            }
            Err(e) => {
                println!("{tube}: error fetching stats: {e}");
            }
        }
    }

    Ok(())
}
