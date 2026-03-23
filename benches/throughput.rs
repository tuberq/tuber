use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tuber::client::{ReserveResult, TuberClient};

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

struct BenchConfig {
    connections: Vec<usize>,
    jobs_per_conn: usize,
    body_size: usize,
    scenarios: Vec<String>,
    json: bool,
}

impl BenchConfig {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut connections = vec![1, 10, 50];
        let mut jobs_per_conn = 1000;
        let mut body_size = 256;
        let mut scenarios = vec![
            "put".to_string(),
            "reserve+delete".to_string(),
            "mixed".to_string(),
        ];
        let mut json = false;

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--connections" => {
                    i += 1;
                    connections = args[i]
                        .split(',')
                        .map(|s| s.parse().expect("invalid connection count"))
                        .collect();
                }
                "--jobs-per-conn" => {
                    i += 1;
                    jobs_per_conn = args[i].parse().expect("invalid jobs-per-conn");
                }
                "--body-size" => {
                    i += 1;
                    body_size = args[i].parse().expect("invalid body-size");
                }
                "--scenarios" => {
                    i += 1;
                    scenarios = args[i].split(',').map(|s| s.to_string()).collect();
                }
                "--json" => {
                    json = true;
                }
                _ => {}
            }
            i += 1;
        }

        BenchConfig {
            connections,
            jobs_per_conn,
            body_size,
            scenarios,
            json,
        }
    }
}

// ---------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------

struct BenchServer {
    port: u16,
    handle: tokio::task::JoinHandle<()>,
}

impl BenchServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener(listener, 65535, None, None)
                .await
                .ok();
        });
        // Give the server a moment to start accepting
        tokio::time::sleep(Duration::from_millis(10)).await;
        BenchServer { port, handle }
    }

    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for BenchServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

// ---------------------------------------------------------------------------
// Latency stats
// ---------------------------------------------------------------------------

struct LatencyStats {
    ops: usize,
    elapsed: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

fn compute_stats(mut latencies: Vec<Duration>, elapsed: Duration) -> LatencyStats {
    let ops = latencies.len();
    if ops == 0 {
        return LatencyStats {
            ops: 0,
            elapsed,
            p50: Duration::ZERO,
            p95: Duration::ZERO,
            p99: Duration::ZERO,
        };
    }
    latencies.sort();
    let p = |pct: f64| -> Duration {
        let idx = ((pct / 100.0) * (ops as f64 - 1.0)).round() as usize;
        latencies[idx.min(ops - 1)]
    };
    LatencyStats {
        ops,
        elapsed,
        p50: p(50.0),
        p95: p(95.0),
        p99: p(99.0),
    }
}

fn ops_per_sec(stats: &LatencyStats) -> f64 {
    if stats.elapsed.as_secs_f64() == 0.0 {
        return 0.0;
    }
    stats.ops as f64 / stats.elapsed.as_secs_f64()
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1000 {
        format!("{us}us")
    } else {
        format!("{:.2}ms", us as f64 / 1000.0)
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

async fn scenario_put(
    addr: &str,
    num_conns: usize,
    jobs_per_conn: usize,
    body: &[u8],
) -> LatencyStats {
    let body = Arc::new(body.to_vec());
    let addr = addr.to_string();

    let start = Instant::now();
    let mut handles = Vec::new();

    for _ in 0..num_conns {
        let addr = addr.clone();
        let body = body.clone();
        handles.push(tokio::spawn(async move {
            let mut client = TuberClient::connect(&addr).await.unwrap();
            let mut latencies = Vec::with_capacity(jobs_per_conn);
            for _ in 0..jobs_per_conn {
                let t = Instant::now();
                let resp = client
                    .put(0, 0, 60, &body, None, None, None, None)
                    .await
                    .unwrap();
                latencies.push(t.elapsed());
                assert!(resp.starts_with("INSERTED"), "unexpected: {resp}");
            }
            latencies
        }));
    }

    let mut all_latencies = Vec::with_capacity(num_conns * jobs_per_conn);
    for h in handles {
        all_latencies.extend(h.await.unwrap());
    }
    let elapsed = start.elapsed();
    compute_stats(all_latencies, elapsed)
}

async fn scenario_reserve_delete(
    addr: &str,
    num_conns: usize,
    total_jobs: usize,
    body: &[u8],
) -> LatencyStats {
    // Pre-load jobs using a single connection
    {
        let mut loader = TuberClient::connect(addr).await.unwrap();
        for _ in 0..total_jobs {
            let resp = loader
                .put(0, 0, 60, body, None, None, None, None)
                .await
                .unwrap();
            assert!(resp.starts_with("INSERTED"), "preload failed: {resp}");
        }
    }

    let addr = addr.to_string();
    let jobs_per_conn = total_jobs / num_conns;

    let start = Instant::now();
    let mut handles = Vec::new();

    for _ in 0..num_conns {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut client = TuberClient::connect(&addr).await.unwrap();
            let mut latencies = Vec::with_capacity(jobs_per_conn);
            for _ in 0..jobs_per_conn {
                let t = Instant::now();
                match client.reserve_with_timeout(1).await.unwrap() {
                    ReserveResult::Reserved { id, .. } => {
                        let resp = client.delete(id).await.unwrap();
                        latencies.push(t.elapsed());
                        assert!(resp.starts_with("DELETED"), "unexpected: {resp}");
                    }
                    ReserveResult::TimedOut => break,
                    ReserveResult::Error(e) => panic!("reserve error: {e}"),
                }
            }
            latencies
        }));
    }

    let mut all_latencies = Vec::new();
    for h in handles {
        all_latencies.extend(h.await.unwrap());
    }
    let elapsed = start.elapsed();
    compute_stats(all_latencies, elapsed)
}

struct MixedResult {
    put_stats: LatencyStats,
    reserve_delete_stats: LatencyStats,
}

async fn scenario_mixed(
    addr: &str,
    num_conns: usize,
    jobs_per_conn: usize,
    body: &[u8],
) -> MixedResult {
    let producers = num_conns.max(2) / 2;
    let consumers = num_conns.max(2) - producers;
    let body = Arc::new(body.to_vec());
    let addr_str = addr.to_string();

    // Pre-load some jobs so consumers don't starve at startup
    {
        let mut loader = TuberClient::connect(addr).await.unwrap();
        let preload = consumers * jobs_per_conn / 4;
        for _ in 0..preload {
            loader
                .put(0, 0, 60, &body, None, None, None, None)
                .await
                .unwrap();
        }
    }

    let start = Instant::now();
    let mut producer_handles = Vec::new();
    let mut consumer_handles = Vec::new();

    for _ in 0..producers {
        let addr = addr_str.clone();
        let body = body.clone();
        producer_handles.push(tokio::spawn(async move {
            let mut client = TuberClient::connect(&addr).await.unwrap();
            let mut latencies = Vec::with_capacity(jobs_per_conn);
            for _ in 0..jobs_per_conn {
                let t = Instant::now();
                let resp = client
                    .put(0, 0, 60, &body, None, None, None, None)
                    .await
                    .unwrap();
                latencies.push(t.elapsed());
                assert!(resp.starts_with("INSERTED"), "unexpected: {resp}");
            }
            latencies
        }));
    }

    for _ in 0..consumers {
        let addr = addr_str.clone();
        consumer_handles.push(tokio::spawn(async move {
            let mut client = TuberClient::connect(&addr).await.unwrap();
            let mut latencies = Vec::with_capacity(jobs_per_conn);
            for _ in 0..jobs_per_conn {
                let t = Instant::now();
                match client.reserve_with_timeout(2).await.unwrap() {
                    ReserveResult::Reserved { id, .. } => {
                        let resp = client.delete(id).await.unwrap();
                        latencies.push(t.elapsed());
                        assert!(resp.starts_with("DELETED"), "unexpected: {resp}");
                    }
                    ReserveResult::TimedOut => break,
                    ReserveResult::Error(e) => panic!("reserve error: {e}"),
                }
            }
            latencies
        }));
    }

    let mut put_latencies = Vec::new();
    for h in producer_handles {
        put_latencies.extend(h.await.unwrap());
    }
    let mut rd_latencies = Vec::new();
    for h in consumer_handles {
        rd_latencies.extend(h.await.unwrap());
    }
    let elapsed = start.elapsed();

    MixedResult {
        put_stats: compute_stats(put_latencies, elapsed),
        reserve_delete_stats: compute_stats(rd_latencies, elapsed),
    }
}

// ---------------------------------------------------------------------------
// Result collection
// ---------------------------------------------------------------------------

struct ScenarioResult {
    key: String,
    #[allow(dead_code)]
    label: String,
    #[allow(dead_code)]
    conns: String,
    stats: LatencyStats,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn print_header() {
    println!("\nTuber Benchmark Results");
    println!("=======================");
    println!(
        "{:<26} {:>5} {:>8} {:>12} {:>10} {:>10} {:>10}",
        "Scenario", "Conns", "Ops", "Ops/sec", "p50", "p95", "p99"
    );
}

fn print_row(label: &str, conns: &str, stats: &LatencyStats) {
    println!(
        "{:<26} {:>5} {:>8} {:>12.0} {:>10} {:>10} {:>10}",
        label,
        conns,
        stats.ops,
        ops_per_sec(stats),
        fmt_dur(stats.p50),
        fmt_dur(stats.p95),
        fmt_dur(stats.p99),
    );
}

fn print_json(results: &[ScenarioResult]) {
    print!("{{");
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!(
            "\"{}\":{{\"ops_sec\":{:.0},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{}}}",
            r.key,
            ops_per_sec(&r.stats),
            r.stats.p50.as_micros(),
            r.stats.p95.as_micros(),
            r.stats.p99.as_micros(),
        );
    }
    println!("}}");
}

#[tokio::main]
async fn main() {
    let config = BenchConfig::from_args();
    let body = vec![b'x'; config.body_size];
    let mut results: Vec<ScenarioResult> = Vec::new();

    if !config.json {
        print_header();
    }

    for &num_conns in &config.connections {
        for scenario in &config.scenarios {
            match scenario.as_str() {
                "put" => {
                    let server = BenchServer::start().await;
                    let stats =
                        scenario_put(&server.addr(), num_conns, config.jobs_per_conn, &body).await;
                    if !config.json {
                        print_row("put", &num_conns.to_string(), &stats);
                    }
                    results.push(ScenarioResult {
                        key: format!("put_{num_conns}"),
                        label: "put".to_string(),
                        conns: num_conns.to_string(),
                        stats,
                    });
                }
                "reserve+delete" => {
                    let server = BenchServer::start().await;
                    let total_jobs = num_conns * config.jobs_per_conn;
                    let stats =
                        scenario_reserve_delete(&server.addr(), num_conns, total_jobs, &body).await;
                    if !config.json {
                        print_row("reserve+delete", &num_conns.to_string(), &stats);
                    }
                    results.push(ScenarioResult {
                        key: format!("reserve_delete_{num_conns}"),
                        label: "reserve+delete".to_string(),
                        conns: num_conns.to_string(),
                        stats,
                    });
                }
                "mixed" => {
                    let server = BenchServer::start().await;
                    let result =
                        scenario_mixed(&server.addr(), num_conns, config.jobs_per_conn, &body)
                            .await;
                    let effective = num_conns.max(2);
                    let conns_label = format!("{}+{}", effective / 2, effective - effective / 2);
                    if !config.json {
                        print_row("mixed (put)", &conns_label, &result.put_stats);
                        print_row(
                            "mixed (reserve+delete)",
                            &conns_label,
                            &result.reserve_delete_stats,
                        );
                    }
                    results.push(ScenarioResult {
                        key: format!("mixed_put_{num_conns}"),
                        label: "mixed (put)".to_string(),
                        conns: conns_label.clone(),
                        stats: result.put_stats,
                    });
                    results.push(ScenarioResult {
                        key: format!("mixed_reserve_delete_{num_conns}"),
                        label: "mixed (reserve+delete)".to_string(),
                        conns: conns_label,
                        stats: result.reserve_delete_stats,
                    });
                }
                other => {
                    eprintln!("Unknown scenario: {other}");
                }
            }
        }
    }

    if config.json {
        print_json(&results);
    } else {
        println!();
    }
}
