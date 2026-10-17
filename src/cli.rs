//! tuber's command line, in the library so a binary that embeds tuber can run
//! it: `tuber`'s own `main` is [`run`] with [`command`] and a hook that changes
//! nothing. An embedder (tuber-pro) adds its own `server` flags to the
//! command, sets its name and version, and uses the hook to adjust the
//! [`ServerConfig`] before the server starts. The `#[global_allocator]` stays
//! in each binary.

use std::time::Duration;

use clap::parser::ValueSource;
use clap::{ArgMatches, Args, Command, CommandFactory, FromArgMatches, Parser, Subcommand};

use crate::server::ServerConfig;

/// Parse a human-readable byte count for `--max-job-size`. Rejects anything
/// above `JOB_DATA_SIZE_LIMIT_MAX` (1 GiB): the protocol can't carry a larger
/// body, and a value beyond it (or beyond u32) is a configuration mistake, not
/// something to silently honour.
fn parse_max_job_size(s: &str) -> Result<u32, String> {
    let n = crate::server::parse_bytes(s)?;
    let n = u32::try_from(n).map_err(|_| format!("max-job-size {s:?} does not fit in u32"))?;
    if n > crate::job::JOB_DATA_SIZE_LIMIT_MAX {
        return Err(format!(
            "max-job-size {s:?} exceeds the 1 GiB limit ({} bytes)",
            crate::job::JOB_DATA_SIZE_LIMIT_MAX
        ));
    }
    Ok(n)
}

/// Parse a human-readable duration (e.g. `0`, `50ms`, `1s`, `2m`) into a `Duration`.
/// `0` (with or without a unit) means "fsync on every write".
/// Minimal ms/s/m/h parser so we don't pull in a new dep.
fn parse_sync_interval(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num_str, unit) = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .map(|i| (&s[..i], &s[i..]))
        .unwrap_or((s, "ms"));
    let n: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration number in {s:?}"))?;
    let nanos = match unit.trim() {
        "ns" => n,
        "us" | "µs" => n * 1_000.0,
        "ms" | "" => n * 1_000_000.0,
        "s" => n * 1_000_000_000.0,
        "m" => n * 60.0 * 1_000_000_000.0,
        "h" => n * 3_600.0 * 1_000_000_000.0,
        other => return Err(format!("unknown duration unit {other:?} in {s:?}")),
    };
    if nanos < 0.0 {
        return Err(format!("negative duration {s:?}"));
    }
    Ok(Duration::from_nanos(nanos as u64))
}

#[derive(Parser, Debug)]
#[command(name = "tuber", about = "A simple, fast work queue", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// The `server` subcommand's flags. An embedder adds its own beside them
/// with [`command`] and `mut_subcommand("server", ..)`, and reads them back
/// from the matches [`run`] hands its hook.
#[derive(Args, Debug)]
pub struct ServerArgs {
    /// Listen address
    #[arg(
        short = 'l',
        long,
        default_value = crate::server::DEFAULT_LISTEN_ADDR,
        env = "TUBER_LISTEN"
    )]
    pub listen: String,

    /// Listen port
    #[arg(
        short = 'p',
        long,
        default_value_t = crate::server::DEFAULT_PORT,
        env = "TUBER_PORT"
    )]
    pub port: u16,

    /// WAL directory (enables persistence)
    #[arg(short = 'b', long, env = "TUBER_BINLOG_DIR")]
    pub binlog_dir: Option<String>,

    /// Minimum interval between fsyncs of the WAL and body store
    /// (e.g. 0, 50ms, 1s). "0" means fsync on every write (strongest
    /// durability; ack follows durability). Positive values bound how
    /// much committed state can be lost on crash. Only meaningful when
    /// --binlog-dir is set.
    ///
    /// `--wal-sync-interval` is accepted as a hidden alias for
    /// backwards compatibility; new deployments should use
    /// `--sync-interval`. The env var `TUBER_WAL_SYNC_INTERVAL` is
    /// also still honoured.
    #[arg(
        short = 'i',
        long,
        alias = "wal-sync-interval",
        default_value = "100ms",
        value_parser = parse_sync_interval,
        env = "TUBER_SYNC_INTERVAL"
    )]
    pub sync_interval: Duration,

    /// Maximum size of a single job's body.
    ///
    /// Note that a body passes through RAM in full on both put and
    /// reserve, so this bounds transient memory per in-flight command,
    /// not just stored size. Accepts suffixes: k, m, g, t (e.g. 64k, 1m).
    #[arg(
        short = 'z',
        long,
        default_value_t = crate::job::JOB_DATA_SIZE_LIMIT_DEFAULT,
        value_parser = parse_max_job_size,
        env = "TUBER_MAX_JOB_SIZE"
    )]
    pub max_job_size: u32,

    /// Maximum total in-memory size of all jobs (bodies + per-job overhead
    /// + idempotency tombstones).
    ///
    /// PUT returns OUT_OF_MEMORY once exceeded;
    /// reserve/release/bury/kick/delete always succeed. Also enforced at
    /// startup: replay aborts if the binlog would exceed the budget in
    /// memory. Accepts suffixes: k, m, g, t (e.g. 2g, 500M, 100k).
    /// Pass 0 for unlimited.
    #[arg(
        long,
        default_value_t = crate::job::JOBS_SIZE_LIMIT_DEFAULT,
        value_parser = crate::server::parse_bytes,
        env = "TUBER_MAX_JOBS_SIZE"
    )]
    pub max_jobs_size: u64,

    /// Maximum concurrent client connections.
    ///
    /// Defaults to a value derived from the file-descriptor soft limit
    /// (`ulimit -n`) minus the descriptors TOAST and the WAL are currently
    /// holding, so storage always keeps the fds it needs and connections
    /// yield to it — a refused connection is a client retry, whereas a
    /// segment that cannot be opened is failed puts. Recomputed as the body
    /// store grows. Pass 0 for unlimited.
    #[arg(long, env = "TUBER_MAX_CONNECTIONS")]
    pub max_connections: Option<usize>,

    /// Close connections idle for this many seconds. 0 (the default)
    /// disables pruning.
    ///
    /// "Idle" means the connection has neither completed a command nor sent
    /// a byte for the whole period, *and* the server has confirmed it is
    /// holding nothing: a worker parked in `reserve` or still running jobs
    /// it reserved is never closed, however long it stays silent. Without
    /// this, a client that opens connections and leaves them open holds
    /// slots against the connection ceiling until it disconnects. Pick a
    /// period comfortably longer than your slowest legitimate client gap —
    /// a producer that puts a job once an hour on a long-lived connection
    /// looks exactly like an abandoned socket.
    #[arg(long, env = "TUBER_CONN_IDLE_TIMEOUT", default_value_t = 0)]
    pub conn_idle_timeout: u64,

    /// Seconds of silence before TCP keepalive probes start on a client
    /// connection. 0 turns keepalive off.
    ///
    /// Probes then go out every sixth of this period, and the
    /// connection is closed after 6 go unanswered, so a peer that vanished
    /// without closing (host crash, network partition, a NAT or load
    /// balancer dropping the flow) is noticed within about twice this
    /// period. Its reserved jobs then go straight back to ready. The
    /// probes are answered by the client's kernel, so a worker busy on a
    /// long job is never mistaken for a dead one, and they keep idle
    /// connections alive through middleboxes that drop quiet flows.
    #[arg(
        long,
        env = "TUBER_TCP_KEEPALIVE",
        default_value_t = crate::server::DEFAULT_TCP_KEEPALIVE_SECS,
        value_parser = clap::value_parser!(u64).range(0..=crate::server::MAX_TCP_KEEPALIVE_SECS)
    )]
    pub tcp_keepalive: u64,

    /// Maximum total on-disk size for the WAL + body store combined.
    /// PUT returns OUT_OF_STORAGE once exceeded; reserve / release /
    /// bury / kick / delete always succeed (they only ever shrink the
    /// budget once compaction reclaims). One WAL segment's worth of
    /// headroom is reserved so deletes can always be journalled.
    /// Accepts suffixes: k, m, g, t (e.g. 100g, 50G).
    /// Required when --binlog-dir is set: the server refuses to start
    /// with persistence enabled and no disk budget. A common starting
    /// point is 80% of the volume. Otherwise (in-memory mode) ignored.
    #[arg(
        short = 's',
        long,
        value_parser = crate::server::parse_bytes,
        env = "TUBER_MAX_STORAGE_BYTES"
    )]
    pub max_storage_bytes: Option<u64>,

    /// Opt in to migrating a legacy (pre-v5) WAL into the v5+TOAST
    /// format on startup. Without this flag the server refuses to
    /// start when it detects pre-v5 records, so operators get an
    /// explicit decision point before in-place format conversion.
    /// Set once on the upgrade run; once the WAL has been migrated
    /// the flag becomes a no-op.
    #[arg(long, env = "TUBER_MIGRATE_WAL")]
    pub migrate_wal: bool,

    /// Start even when an older WAL or TOAST segment is damaged (a
    /// corrupt record or header) or a WAL segment can't be read, using
    /// what remains and keeping the damaged segment as
    /// `<segment>.corrupt`. Without this the server refuses to start,
    /// since replaying past a lost delete brings its job back, and a lost
    /// body drops its job. A write cut short at the end of the newest
    /// segment is what a crash leaves, and is truncated either way.
    #[arg(long, env = "TUBER_WAL_SKIP_CORRUPT")]
    pub wal_skip_corrupt: bool,

    /// Increase verbosity (-V for info, -VV for debug)
    #[arg(short = 'V', action = clap::ArgAction::Count, env = "TUBER_VERBOSE")]
    pub verbose: u8,

    /// Enable Prometheus metrics endpoint on this port
    #[arg(long, env = "TUBER_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// Address the metrics endpoint listens on [default: the -l address].
    /// Set it to keep metrics private while the queue port is public, e.g.
    /// `-l 0.0.0.0 --metrics-listen 127.0.0.1`.
    #[arg(long, env = "TUBER_METRICS_LISTEN")]
    pub metrics_listen: Option<String>,

    /// Instance name
    #[arg(long, env = "TUBER_NAME")]
    pub name: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the tuber server
    Server(ServerArgs),

    /// Put a job onto a tube
    Put {
        /// Job body (reads from stdin if omitted, one job per line)
        body: Option<String>,

        /// Tube name
        #[arg(short = 't', long, default_value = "default")]
        tube: String,

        /// Job priority (0 is most urgent)
        #[arg(short = 'p', long = "pri", default_value_t = 65536)]
        priority: u32,

        /// Delay in seconds before job becomes ready
        #[arg(short = 'd', long, default_value_t = 0)]
        delay: u32,

        /// Time-to-run in seconds
        #[arg(long, default_value_t = 120)]
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

        /// Server address [host][:port] [default: $TUBER_URL, $TUBER_ADDR,
        /// $BEANSTALKD_URL, then localhost:11300]
        #[arg(short = 'a', long)]
        addr: Option<String>,
    },

    /// Show server or tube statistics
    Stats {
        /// Tube name (omit for global stats)
        #[arg(short = 't', long)]
        tube: Option<String>,

        /// Server address [host][:port] [default: $TUBER_URL, $TUBER_ADDR,
        /// $BEANSTALKD_URL, then localhost:11300]
        #[arg(short = 'a', long)]
        addr: Option<String>,
    },

    /// List all tubes with job counts
    Tubes {
        /// Server address [host][:port] [default: $TUBER_URL, $TUBER_ADDR,
        /// $BEANSTALKD_URL, then localhost:11300]
        #[arg(short = 'a', long)]
        addr: Option<String>,
    },

    /// List recurring schedule entries with their next fire and last result
    Schedules {
        /// Only this owner's entries
        #[arg(short = 'o', long)]
        owner: Option<String>,

        /// Server address [host][:port] [default: $TUBER_URL, $TUBER_ADDR,
        /// $BEANSTALKD_URL, then localhost:11300]
        #[arg(short = 'a', long)]
        addr: Option<String>,
    },

    /// Worker mode - reserve and execute jobs as shell commands
    Work {
        /// Tube to watch
        #[arg(short = 't', long, default_value = "default")]
        tube: String,

        /// Number of parallel workers
        #[arg(short = 'j', long, default_value_t = 1)]
        parallel: usize,

        /// Server address [host][:port] [default: $TUBER_URL, $TUBER_ADDR,
        /// $BEANSTALKD_URL, then localhost:11300]
        #[arg(short = 'a', long)]
        addr: Option<String>,
    },
}

/// What a [`run`] hook returns when the server shouldn't start. It is
/// logged, and the process exits 1, as for any other startup failure.
pub type HookError = Box<dyn std::error::Error + Send + Sync>;

/// tuber's command line. An embedding binary can add flags to `server` with
/// `mut_subcommand("server", ..)`, and set its own `name` and `version`, before
/// handing it to [`run`].
pub fn command() -> Command {
    Cli::command()
}

/// Parse the process arguments with `cmd`, [`command`] or one an embedder
/// built from it, and do what they say, as the `tuber` binary does.
///
/// For `server`, `configure` gets the subcommand's matches, which hold any
/// flags the embedder added, and the [`ServerConfig`] tuber's own flags
/// describe, and can change it before the server starts. It runs inside the
/// tokio runtime, so it can spawn tasks. An error from it stops startup.
///
/// Exits the process on a usage error (2) or a failure (1), as `tuber` does.
/// It creates the tokio runtime itself, after applying the deprecated
/// `TUBER_WAL_SYNC_INTERVAL` alias to the environment, which is only sound
/// while the process has one thread: call it before starting any of yours.
pub fn run<F>(cmd: Command, configure: F)
where
    F: FnOnce(&ArgMatches, &mut ServerConfig) -> Result<(), HookError>,
{
    alias_deprecated_env();
    let matches = cmd.get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("start the tokio runtime")
        .block_on(dispatch(cli, &matches, configure));
}

/// Honour the old `TUBER_WAL_SYNC_INTERVAL` env var when the new
/// `TUBER_SYNC_INTERVAL` is unset, so existing deployments keep working
/// unchanged, with a one-shot warning.
///
/// This MUST run before the tokio runtime starts: the runtime spawns worker
/// threads, and `set_var` is only sound while the process is single-threaded.
fn alias_deprecated_env() {
    if std::env::var_os("TUBER_SYNC_INTERVAL").is_none()
        && let Some(old) = std::env::var_os("TUBER_WAL_SYNC_INTERVAL")
    {
        crate::errln!("tuber: TUBER_WAL_SYNC_INTERVAL is deprecated; use TUBER_SYNC_INTERVAL");
        // SAFETY: no runtime and no other threads exist yet — single-threaded.
        unsafe { std::env::set_var("TUBER_SYNC_INTERVAL", old) };
    }
}

/// The config `server`'s flags describe, as `configure` then changes it.
/// `server` is the subcommand's matches, which `args` was parsed from.
fn start_config<F>(
    args: ServerArgs,
    server: &ArgMatches,
    configure: F,
) -> Result<ServerConfig, HookError>
where
    F: FnOnce(&ArgMatches, &mut ServerConfig) -> Result<(), HookError>,
{
    let mut config = server_config(args, server);
    configure(server, &mut config)?;
    Ok(config)
}

/// The config `server`'s flags describe, warning about any persistence flag
/// set without `--binlog-dir`.
fn server_config(args: ServerArgs, server: &ArgMatches) -> ServerConfig {
    let ServerArgs {
        listen,
        port,
        binlog_dir,
        sync_interval,
        max_job_size,
        max_jobs_size,
        max_connections,
        conn_idle_timeout,
        tcp_keepalive,
        max_storage_bytes,
        migrate_wal,
        wal_skip_corrupt,
        verbose: _,
        metrics_port,
        metrics_listen,
        name,
    } = args;

    // Persistence-only flags are harmlessly ignored in-memory. Warn —
    // don't refuse — when one was set without --binlog-dir, so a
    // globally-exported env var (TUBER_SYNC_INTERVAL etc.) can't block
    // in-memory startup. The genuine "persistence requires a disk
    // budget" constraint is still enforced in server::run.
    if binlog_dir.is_none() {
        // Set explicitly (CLI or env), not just the default. We no longer use
        // clap `requires` for this: an env-provided value counts as "present"
        // and would otherwise refuse in-memory startup whenever
        // TUBER_SYNC_INTERVAL is exported.
        if server
            .value_source("sync_interval")
            .is_some_and(|src| src != ValueSource::DefaultValue)
        {
            tracing::warn!(
                "--sync-interval / TUBER_SYNC_INTERVAL is ignored without --binlog-dir (running in-memory)"
            );
        }
        if migrate_wal {
            tracing::warn!(
                "--migrate-wal / TUBER_MIGRATE_WAL is ignored without --binlog-dir (running in-memory)"
            );
        }
        if wal_skip_corrupt {
            tracing::warn!(
                "--wal-skip-corrupt / TUBER_WAL_SKIP_CORRUPT is ignored without --binlog-dir (running in-memory)"
            );
        }
        if max_storage_bytes.is_some() {
            tracing::warn!(
                "--max-storage-bytes / TUBER_MAX_STORAGE_BYTES is ignored without --binlog-dir (running in-memory)"
            );
        }
    }

    ServerConfig {
        addr: listen,
        port,
        max_job_size,
        // 0 means unlimited, matching the `max-jobs-size: 0` sentinel
        // that `stats` has always reported for an unset budget.
        max_job_bytes: (max_jobs_size != 0).then_some(max_jobs_size),
        max_storage_bytes,
        wal_dir: binlog_dir.map(std::path::PathBuf::from),
        sync_interval,
        migrate_wal,
        wal_skip_corrupt,
        metrics_port,
        metrics_listen,
        name,
        max_connections,
        // 0 means disabled, matching the other 0-is-no-limit knobs.
        conn_idle_timeout: (conn_idle_timeout != 0).then(|| Duration::from_secs(conn_idle_timeout)),
        tcp_keepalive: (tcp_keepalive != 0).then(|| Duration::from_secs(tcp_keepalive)),
        wal_observer: None,
    }
}

async fn dispatch<F>(cli: Cli, matches: &ArgMatches, configure: F)
where
    F: FnOnce(&ArgMatches, &mut ServerConfig) -> Result<(), HookError>,
{
    match cli.command {
        Commands::Server(args) => {
            let server = matches
                .subcommand_matches("server")
                .expect("parsed as the server subcommand");
            let level = match args.verbose {
                0 => tracing::Level::WARN,
                1 => tracing::Level::INFO,
                _ => tracing::Level::DEBUG,
            };
            crate::telemetry::init(level);
            let config = match start_config(args, server, configure) {
                Ok(config) => config,
                Err(e) => {
                    tracing::error!("server error: {e}");
                    std::process::exit(1);
                }
            };
            if let Err(e) = crate::server::run(config).await {
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
            let addr = server_addr(addr);
            if let Err(e) =
                crate::cmd_put::run(&addr, &tube, priority, delay, ttr, body, idp, grp, aft, con)
                    .await
            {
                crate::errln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Stats { tube, addr } => {
            if let Err(e) = crate::cmd_stats::run(&server_addr(addr), tube).await {
                crate::errln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Tubes { addr } => {
            if let Err(e) = crate::cmd_tubes::run(&server_addr(addr)).await {
                crate::errln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Schedules { owner, addr } => {
            if let Err(e) = crate::cmd_schedules::run(&server_addr(addr), owner).await {
                crate::errln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Work {
            tube,
            parallel,
            addr,
        } => {
            if let Err(e) = crate::cmd_work::run(&server_addr(addr), &tube, parallel).await {
                crate::errln!("error: {e}");
                std::process::exit(1);
            }
        }
    }
}

/// Where a client subcommand connects: `-a`, else the address env vars.
fn server_addr(flag: Option<String>) -> String {
    crate::client::resolve_addr(flag.as_deref(), |var| std::env::var(var).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Arg;
    use std::path::Path;

    /// tuber's command as tuber-pro builds it: renamed, with a flag of its
    /// own on `server`.
    fn embedder_command() -> Command {
        command()
            .name("tuber-pro")
            .version("0.17.0 (pro)")
            .mut_subcommand("server", |s| {
                s.arg(Arg::new("replication_listen").long("replication-listen"))
            })
    }

    /// Parse `args` with [`embedder_command`] as [`run`] does, and return
    /// the `server` subcommand's typed flags and matches.
    fn parse_server(args: &[&str]) -> (ServerArgs, ArgMatches) {
        let matches = embedder_command().try_get_matches_from(args).unwrap();
        let Commands::Server(server_args) = Cli::from_arg_matches(&matches).unwrap().command else {
            panic!("not parsed as server")
        };
        let server = matches.subcommand_matches("server").unwrap().clone();
        (server_args, server)
    }

    #[test]
    fn an_embedders_server_flag_reaches_its_hook_with_tubers_config() {
        let (args, server) = parse_server(&[
            "tuber-pro",
            "server",
            "-b",
            "/var/lib/tuber",
            "-s",
            "1g",
            "-p",
            "11301",
            "--replication-listen",
            "0.0.0.0:11400",
        ]);
        let mut seen = None;
        let config = start_config(args, &server, |m, config| {
            seen = m.get_one::<String>("replication_listen").cloned();
            config.name = Some("replica".into());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen.as_deref(), Some("0.0.0.0:11400"));
        assert_eq!(config.port, 11301);
        assert_eq!(config.wal_dir.as_deref(), Some(Path::new("/var/lib/tuber")));
        assert_eq!(config.max_storage_bytes, Some(1 << 30));
        assert_eq!(
            config.name.as_deref(),
            Some("replica"),
            "the hook's change kept"
        );
    }

    #[test]
    fn a_hook_error_stops_the_server_starting() {
        let (args, server) = parse_server(&["tuber-pro", "server"]);
        let result = start_config(args, &server, |_, _| Err("no replication listener".into()));
        assert_eq!(result.err().unwrap().to_string(), "no replication listener");
    }

    #[test]
    fn an_embedder_names_itself_in_version() {
        assert_eq!(
            embedder_command().render_version(),
            "tuber-pro 0.17.0 (pro)\n"
        );
    }
}
