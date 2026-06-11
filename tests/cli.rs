//! CLI startup tests for the `server` subcommand, spawning the real binary.
//!
//! Regression coverage for review item #3: a persistence-only flag provided
//! via env var (e.g. `TUBER_SYNC_INTERVAL`) used to trip clap's `requires =
//! "binlog_dir"` and refuse in-memory startup. Those flags are now validated
//! after parsing: ignored (with a warning) in-memory, while the genuine
//! "persistence needs a disk budget" constraint is still enforced.

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_tuber")
}

/// Startup-influencing env vars, cleared so each test is hermetic regardless
/// of the environment `cargo test` inherits.
const PERSIST_ENVS: &[&str] = &[
    "TUBER_SYNC_INTERVAL",
    "TUBER_WAL_SYNC_INTERVAL",
    "TUBER_BINLOG_DIR",
    "TUBER_MAX_STORAGE_BYTES",
    "TUBER_MIGRATE_WAL",
];

/// Spawn `tuber server <extra_args>` with a hermetic env plus `envs`, piping
/// stdout (tracing's writer) and discarding stderr.
fn spawn_server(envs: &[(&str, &str)], extra_args: &[&str]) -> Child {
    let mut cmd = Command::new(bin());
    cmd.args(["server", "-l", "127.0.0.1", "-p", "0"])
        .args(extra_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for e in PERSIST_ENVS {
        cmd.env_remove(e);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn tuber binary")
}

/// Let the process run briefly, then capture whether it had already exited and
/// drain its stdout. Always kills/reaps the child.
fn settle(mut child: Child) -> (Option<std::process::ExitStatus>, String) {
    std::thread::sleep(Duration::from_millis(700));
    let early = child.try_wait().expect("try_wait");
    let _ = child.kill();
    let mut out = String::new();
    if let Some(mut so) = child.stdout.take() {
        let _ = so.read_to_string(&mut out);
    }
    let _ = child.wait();
    (early, out)
}

#[test]
fn sync_interval_env_does_not_block_in_memory_start() {
    // The bug: this exited immediately with code 2 ("--binlog-dir required").
    let child = spawn_server(&[("TUBER_SYNC_INTERVAL", "50ms")], &[]);
    let (early, out) = settle(child);

    assert!(
        early.is_none(),
        "server exited early ({early:?}); TUBER_SYNC_INTERVAL must not block in-memory startup.\nstdout:\n{out}"
    );
    assert!(
        out.contains("--sync-interval") && out.contains("ignored"),
        "expected an in-memory ignore warning; stdout:\n{out}"
    );
}

#[test]
fn legacy_wal_sync_interval_env_does_not_block_in_memory_start() {
    // The deprecation shim copies this into TUBER_SYNC_INTERVAL before parse;
    // it must not resurrect the refuse-to-start behaviour either.
    let child = spawn_server(&[("TUBER_WAL_SYNC_INTERVAL", "50ms")], &[]);
    let (early, out) = settle(child);

    assert!(
        early.is_none(),
        "server exited early ({early:?}); legacy TUBER_WAL_SYNC_INTERVAL must not block in-memory startup.\nstdout:\n{out}"
    );
}

#[test]
fn plain_in_memory_start_emits_no_ignore_warning() {
    // No persistence flags set: clean startup, no spurious warning.
    let child = spawn_server(&[], &[]);
    let (early, out) = settle(child);

    assert!(early.is_none(), "plain in-memory server exited early ({early:?})\nstdout:\n{out}");
    assert!(
        !out.contains("ignored"),
        "unexpected ignore warning on a plain start:\n{out}"
    );
}

#[test]
fn binlog_without_storage_budget_is_rejected() {
    // The real constraint (persistence requires a disk budget) must still be
    // enforced — moving it out of clap `requires` didn't drop it.
    let dir = std::env::temp_dir().join("tuber_cli_no_budget_test");
    let _ = std::fs::remove_dir_all(&dir);
    let child = spawn_server(&[], &["-b", dir.to_str().unwrap()]);
    let (early, out) = settle(child);
    let _ = std::fs::remove_dir_all(&dir);

    let status = early.expect("server must refuse to start with -b and no budget, but it is still running");
    assert!(!status.success(), "expected non-zero exit, got {status:?}\nstdout:\n{out}");
    assert!(
        out.contains("max-storage-bytes"),
        "expected the storage-budget error message; stdout:\n{out}"
    );
}
