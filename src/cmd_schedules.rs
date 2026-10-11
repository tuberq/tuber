use crate::client::{TuberClient, parse_yaml_map};
use std::io;

pub async fn run(addr: &str, owner: Option<String>) -> io::Result<()> {
    let mut client = TuberClient::connect(addr).await?;

    let entries = client.list_schedules(owner.as_deref()).await?;

    if entries.is_empty() {
        return crate::outln!("No schedules.");
    }

    for (owner, name) in &entries {
        match client.stats_schedule(owner, name).await {
            Ok(stats) => {
                let map = parse_yaml_map(&stats);
                let get = |key: &str| {
                    map.get(key)
                        .copied()
                        .filter(|v| !v.is_empty())
                        .unwrap_or("-")
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
