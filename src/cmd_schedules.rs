use crate::client::TuberClient;
use std::io;

pub async fn run(addr: &str, owner: Option<String>) -> io::Result<()> {
    let mut client = TuberClient::connect(addr).await?;

    let body = client.list_schedules(owner.as_deref()).await?;
    let entries: Vec<(&str, &str)> = body
        .lines()
        .filter_map(|line| line.strip_prefix("- ")?.split_once(' '))
        .collect();

    if entries.is_empty() {
        return crate::outln!("No schedules.");
    }

    for (owner, name) in entries {
        match client.stats_schedule(owner, name).await {
            Ok(stats) => {
                // Values hold colons (times, `con:1`), so split on ": ".
                let get = |key: &str| -> String {
                    stats
                        .lines()
                        .find_map(|l| l.strip_prefix(key)?.strip_prefix(": "))
                        .map(|v| v.trim().trim_matches('"').to_string())
                        .filter(|v| !v.is_empty())
                        .unwrap_or_else(|| "-".to_string())
                };
                let streak = get("buried-streak");
                let warning = if streak != "0" && streak != "-" {
                    format!(" (deduped against a buried job {streak} times running)")
                } else {
                    String::new()
                };
                crate::outln!(
                    "{owner} {name}: {} -> {} next={} last={}{warning}",
                    get("spec"),
                    get("tube"),
                    get("next-fire"),
                    get("last-result"),
                )?;
            }
            Err(e) => {
                crate::outln!("{owner} {name}: error fetching stats: {e}")?;
            }
        }
    }

    Ok(())
}
