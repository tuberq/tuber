mod conn;
mod heap;
mod job;
mod protocol;
mod server;
mod tube;
mod wal;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "beanstalkd", about = "A simple, fast work queue")]
struct Args {
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
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    tracing::info!(
        "beanstalkd-rs listening on {}:{}",
        args.listen,
        args.port
    );

    if let Err(e) = server::run(&args.listen, args.port, args.max_job_size).await {
        tracing::error!("server error: {}", e);
        std::process::exit(1);
    }
}
