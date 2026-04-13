#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand};

/// Parse a human-readable byte count for `--max-job-size`, clamping to u32.
fn parse_max_job_size(s: &str) -> Result<u32, String> {
    let n = tuber::server::parse_bytes(s)?;
    u32::try_from(n).map_err(|_| format!("max-job-size {s:?} does not fit in u32"))
}

#[derive(Parser, Debug)]
#[command(name = "tuber", about = "A simple, fast work queue", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the tuber server
    Server {
        /// Listen address
        #[arg(short = 'l', long, default_value = "0.0.0.0", env = "TUBER_LISTEN")]
        listen: String,

        /// Listen port
        #[arg(short = 'p', long, default_value_t = 11300, env = "TUBER_PORT")]
        port: u16,

        /// WAL directory (enables persistence)
        #[arg(short = 'b', long, env = "TUBER_BINLOG_DIR")]
        binlog_dir: Option<String>,

        /// Maximum size of a single job's body.
        /// Accepts suffixes: k, m, g, t (e.g. 64k, 1m). Default: 65535.
        #[arg(short = 'z', long, default_value = "65535", value_parser = parse_max_job_size, env = "TUBER_MAX_JOB_SIZE")]
        max_job_size: u32,

        /// Maximum total in-memory size of all jobs (bodies + per-job overhead
        /// + idempotency tombstones). PUT returns OUT_OF_MEMORY once exceeded;
        /// reserve/release/bury/kick/delete always succeed. Accepts suffixes:
        /// k, m, g, t (e.g. 2g, 500M, 100k). Default: unlimited.
        #[arg(long, value_parser = tuber::server::parse_bytes, env = "TUBER_MAX_JOBS_SIZE")]
        max_jobs_size: Option<u64>,

        /// Increase verbosity (-V for info, -VV for debug)
        #[arg(short = 'V', action = clap::ArgAction::Count, env = "TUBER_VERBOSE")]
        verbose: u8,

        /// Enable Prometheus metrics endpoint on this port
        #[arg(long, env = "TUBER_METRICS_PORT")]
        metrics_port: Option<u16>,

        /// Instance name
        #[arg(long, env = "TUBER_NAME")]
        name: Option<String>,
    },

    /// Put a job onto a tube
    Put {
        /// Job body (reads from stdin if omitted, one job per line)
        body: Option<String>,

        /// Tube name
        #[arg(short = 't', long, default_value = "default")]
        tube: String,

        /// Job priority (0 is most urgent)
        #[arg(short = 'p', long = "pri", default_value_t = 0)]
        priority: u32,

        /// Delay in seconds before job becomes ready
        #[arg(short = 'd', long, default_value_t = 0)]
        delay: u32,

        /// Time-to-run in seconds
        #[arg(long, default_value_t = 60)]
        ttr: u32,

        /// Idempotency key (prevents duplicate jobs)
        #[arg(short = 'i', long)]
        idp: Option<String>,

        /// Group name (for job grouping)
        #[arg(short = 'g', long)]
        grp: Option<String>,

        /// After-group dependency (wait for this group to complete)
        #[arg(long)]
        aft: Option<String>,

        /// Concurrency key (only one job with this key runs at a time)
        #[arg(short = 'c', long)]
        con: Option<String>,

        /// Server address (host:port)
        #[arg(short = 'a', long, default_value = "localhost:11300")]
        addr: String,
    },

    /// Show server or tube statistics
    Stats {
        /// Tube name (omit for global stats)
        #[arg(short = 't', long)]
        tube: Option<String>,

        /// Server address (host:port)
        #[arg(short = 'a', long, default_value = "localhost:11300")]
        addr: String,
    },

    /// List all tubes with job counts
    Tubes {
        /// Server address (host:port)
        #[arg(short = 'a', long, default_value = "localhost:11300")]
        addr: String,
    },

    /// Worker mode - reserve and execute jobs as shell commands
    Work {
        /// Tube to watch
        #[arg(short = 't', long, default_value = "default")]
        tube: String,

        /// Number of parallel workers
        #[arg(short = 'j', long, default_value_t = 1)]
        parallel: usize,

        /// Server address (host:port)
        #[arg(short = 'a', long, default_value = "localhost:11300")]
        addr: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            listen,
            port,
            binlog_dir,
            max_job_size,
            max_jobs_size,
            verbose,
            metrics_port,
            name,
        } => {
            let level = match verbose {
                0 => tracing::Level::WARN,
                1 => tracing::Level::INFO,
                _ => tracing::Level::DEBUG,
            };
            tracing_subscriber::fmt().with_max_level(level).init();
            if let Err(e) = tuber::server::run(
                &listen,
                port,
                max_job_size,
                max_jobs_size,
                binlog_dir.as_deref(),
                metrics_port,
                name,
            )
            .await
            {
                tracing::error!("server error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Put {
            body,
            tube,
            priority,
            delay,
            ttr,
            idp,
            grp,
            aft,
            con,
            addr,
        } => {
            if let Err(e) =
                tuber::cmd_put::run(&addr, &tube, priority, delay, ttr, body, idp, grp, aft, con)
                    .await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Stats { tube, addr } => {
            if let Err(e) = tuber::cmd_stats::run(&addr, tube).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Tubes { addr } => {
            if let Err(e) = tuber::cmd_tubes::run(&addr).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Work {
            tube,
            parallel,
            addr,
        } => {
            if let Err(e) = tuber::cmd_work::run(&addr, &tube, parallel).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
    }
}
