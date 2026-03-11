use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "tuber", about = "A simple, fast work queue")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the tuber server
    Server {
        /// Listen address
        #[arg(short = 'l', long, default_value = "0.0.0.0")]
        listen: String,

        /// Listen port
        #[arg(short = 'p', long, default_value_t = 11300)]
        port: u16,

        /// WAL directory (enables persistence)
        #[arg(short = 'b', long)]
        binlog_dir: Option<String>,

        /// Max job size in bytes
        #[arg(short = 'z', long, default_value_t = 65535)]
        max_job_size: u32,

        /// Increase verbosity (-V for info, -VV for debug)
        #[arg(short = 'V', action = clap::ArgAction::Count)]
        verbose: u8,

        /// Enable Prometheus metrics endpoint on this port
        #[arg(long)]
        metrics_port: Option<u16>,
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
            verbose,
            metrics_port,
        } => {
            let level = match verbose {
                0 => tracing::Level::WARN,
                1 => tracing::Level::INFO,
                _ => tracing::Level::DEBUG,
            };
            tracing_subscriber::fmt().with_max_level(level).init();
            tracing::info!("tuber listening on {listen}:{port}");

            if let Err(e) = tuber::server::run(
                &listen,
                port,
                max_job_size,
                binlog_dir.as_deref(),
                metrics_port,
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
            addr,
        } => {
            if let Err(e) = tuber::cmd_put::run(&addr, &tube, priority, delay, ttr, body, idp).await
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
