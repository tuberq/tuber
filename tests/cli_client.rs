//! Client subcommands (`put`, `stats`, `tubes`, `work`) run as the real binary
//! against a real server: exit status on rejected puts, where the server
//! address comes from (`-a`, then `TUBER_URL`, `TUBER_ADDR`, `BEANSTALKD_URL`),
//! and output into a pipe whose reader has gone (`tuber … | head`).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_tuber")
}

/// Address env vars, cleared from every client run so tests are hermetic
/// regardless of the environment `cargo test` inherits.
const ADDR_ENVS: &[&str] = &["TUBER_URL", "TUBER_ADDR", "BEANSTALKD_URL"];

/// Nothing listens here: a client that dials it fails to connect.
const DEAD_ADDR: &str = "127.0.0.1:1";

/// A `tuber server` on a free loopback port, killed on drop.
struct Server {
    child: Child,
    port: u16,
}

impl Server {
    fn start(extra_args: &[&str]) -> Server {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("bind ephemeral port")
            .port();
        let child = Command::new(bin())
            .args(["server", "-l", "127.0.0.1", "-p", &port.to_string()])
            .args(extra_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn tuber server");
        let server = Server { child, port };
        for _ in 0..100 {
            if TcpStream::connect(server.addr()).is_ok() {
                return server;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "tuber server never accepted connections on {}",
            server.addr()
        );
    }

    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// `current-jobs-ready` for `tube`.
    fn ready_count(&self, tube: &str) -> u64 {
        self.tube_stat(tube, "current-jobs-ready")
    }

    /// One field of `stats-tube <tube>` (0 if there is no such tube), read
    /// over the protocol directly so the check doesn't depend on the client
    /// code under test.
    fn tube_stat(&self, tube: &str, field: &str) -> u64 {
        let mut conn = TcpStream::connect(self.addr()).unwrap();
        conn.write_all(format!("stats-tube {tube}\r\n").as_bytes())
            .unwrap();
        let mut reader = BufReader::new(conn);
        let mut header = String::new();
        reader.read_line(&mut header).unwrap();
        if header.starts_with("NOT_FOUND") {
            return 0;
        }
        let len: usize = header.trim().strip_prefix("OK ").unwrap().parse().unwrap();
        let mut body = vec![0u8; len + 2];
        reader.read_exact(&mut body).unwrap();
        String::from_utf8(body)
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix(field)?.strip_prefix(": "))
            .unwrap()
            .parse()
            .unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn `tuber <args>` with `envs` as the only address env vars, feeding
/// `stdin` if given.
fn spawn(
    args: &[&str],
    envs: &[(&str, &str)],
    stdin: Option<&str>,
    stdout: Stdio,
    stderr: Stdio,
) -> Child {
    let mut cmd = Command::new(bin());
    cmd.args(args)
        .stdin(Stdio::piped())
        .stdout(stdout)
        .stderr(stderr);
    for e in ADDR_ENVS {
        cmd.env_remove(e);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn tuber client");
    let mut pipe = child.stdin.take().unwrap();
    if let Some(input) = stdin {
        pipe.write_all(input.as_bytes()).unwrap();
    }
    drop(pipe);
    child
}

/// Run `tuber <args>` to completion, capturing its output.
fn run(args: &[&str], envs: &[(&str, &str)], stdin: Option<&str>) -> Output {
    spawn(args, envs, stdin, Stdio::piped(), Stdio::piped())
        .wait_with_output()
        .unwrap()
}

/// A pipe whose read end is already closed, so the first write to it fails
/// with EPIPE -- what `| head` does, minus the race.
fn closed_pipe() -> Stdio {
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    writer.into()
}

/// Run `tuber <args>` with stdout -- and stderr too, if `close_stderr` --
/// writing into a closed pipe. Otherwise stderr is captured.
fn run_into_closed_pipe(args: &[&str], stdin: Option<&str>, close_stderr: bool) -> Output {
    let stderr = if close_stderr {
        closed_pipe()
    } else {
        Stdio::piped()
    };
    spawn(args, &[], stdin, closed_pipe(), stderr)
        .wait_with_output()
        .unwrap()
}

/// Exited with `code`, not a panic (which exits 101 and says "panicked").
fn assert_exit(out: &Output, code: i32) {
    assert!(
        !stderr(out).contains("panicked"),
        "tuber panicked: {}",
        stderr(out)
    );
    assert_eq!(out.status.code(), Some(code), "stderr: {}", stderr(out));
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

// --- `tuber put` exit status -------------------------------------------------

#[test]
fn put_rejected_by_the_server_exits_nonzero() {
    let server = Server::start(&["-z", "10"]);
    let out = run(
        &["put", "-a", &server.addr(), "longer than ten bytes"],
        &[],
        None,
    );
    assert_eq!(stdout(&out), "JOB_TOO_BIG\n");
    assert!(
        !out.status.success(),
        "a rejected put must not exit 0; stderr: {}",
        stderr(&out)
    );
}

#[test]
fn put_from_stdin_reports_every_line_then_exits_nonzero_if_any_was_rejected() {
    let server = Server::start(&["-z", "10"]);
    let out = run(
        &["put", "-a", &server.addr()],
        &[],
        Some("short\nthis line is longer than ten bytes\nok\n"),
    );
    // The rejection doesn't stop the run: the line after it is still put.
    assert_eq!(stdout(&out), "INSERTED 1\nJOB_TOO_BIG\nINSERTED 2\n");
    assert_eq!(server.ready_count("default"), 2);
    assert_eq!(out.status.code(), Some(1), "stderr: {}", stderr(&out));
    assert!(
        stderr(&out).contains("1 of 3"),
        "stderr should count the rejections: {}",
        stderr(&out)
    );
}

#[test]
fn put_that_is_accepted_or_deduplicated_exits_zero() {
    let server = Server::start(&[]);
    let first = run(&["put", "-a", &server.addr(), "-i", "k", "x"], &[], None);
    let dup = run(&["put", "-a", &server.addr(), "-i", "k", "x"], &[], None);
    assert_eq!(stdout(&first), "INSERTED 1\n");
    assert!(first.status.success(), "stderr: {}", stderr(&first));
    // A dedup hit is the idempotency key working, not a failure.
    assert_eq!(stdout(&dup), "INSERTED 1 READY\n");
    assert!(dup.status.success(), "stderr: {}", stderr(&dup));
}

// --- server address ----------------------------------------------------------

#[test]
fn put_reads_tuber_url() {
    let server = Server::start(&[]);
    let out = run(
        &["put", "-t", "env", "x"],
        &[("TUBER_URL", &server.addr())],
        None,
    );
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    assert_eq!(server.ready_count("env"), 1);
}

#[test]
fn tuber_url_beats_tuber_addr_which_beats_beanstalkd_url() {
    let server = Server::start(&[]);
    let good = server.addr();
    for envs in [
        vec![("TUBER_URL", good.as_str()), ("TUBER_ADDR", DEAD_ADDR)],
        vec![("TUBER_URL", good.as_str()), ("BEANSTALKD_URL", DEAD_ADDR)],
        vec![("TUBER_ADDR", good.as_str()), ("BEANSTALKD_URL", DEAD_ADDR)],
        vec![("BEANSTALKD_URL", good.as_str())],
    ] {
        let out = run(&["put", "-t", "env", "x"], &envs, None);
        assert!(out.status.success(), "{envs:?}: stderr: {}", stderr(&out));
    }
    assert_eq!(server.ready_count("env"), 4);
}

#[test]
fn an_empty_env_var_falls_through_to_the_next() {
    let server = Server::start(&[]);
    let out = run(
        &["put", "-t", "env", "x"],
        &[("TUBER_URL", ""), ("TUBER_ADDR", &server.addr())],
        None,
    );
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    assert_eq!(server.ready_count("env"), 1);
}

#[test]
fn addr_flag_beats_env() {
    let server = Server::start(&[]);
    let out = run(
        &["put", "-a", &server.addr(), "-t", "env", "x"],
        &[("TUBER_URL", DEAD_ADDR)],
        None,
    );
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    assert_eq!(server.ready_count("env"), 1);
}

#[test]
fn port_only_address_means_localhost() {
    let server = Server::start(&[]);
    let port_only = format!(":{}", server.port);
    let flag = run(&["put", "-a", &port_only, "-t", "env", "x"], &[], None);
    let env = run(
        &["put", "-t", "env", "x"],
        &[("TUBER_URL", &port_only)],
        None,
    );
    assert!(flag.status.success(), "-a :port: {}", stderr(&flag));
    assert!(env.status.success(), "TUBER_URL=:port: {}", stderr(&env));
    assert_eq!(server.ready_count("env"), 2);
}

#[test]
fn stats_and_tubes_read_tuber_url() {
    let server = Server::start(&[]);
    let envs = [("TUBER_URL", server.addr())];
    let envs: Vec<(&str, &str)> = envs.iter().map(|(k, v)| (*k, v.as_str())).collect();

    let put = run(&["put", "-t", "env", "x"], &envs, None);
    assert!(put.status.success(), "put: {}", stderr(&put));

    let tubes = run(&["tubes"], &envs, None);
    assert!(tubes.status.success(), "tubes: {}", stderr(&tubes));
    assert!(
        stdout(&tubes).contains("env: ready=1"),
        "{}",
        stdout(&tubes)
    );

    let stats = run(&["stats", "-t", "env"], &envs, None);
    assert!(stats.status.success(), "stats: {}", stderr(&stats));
    assert!(
        stdout(&stats).contains("current-jobs-ready: 1"),
        "{}",
        stdout(&stats)
    );
}

// --- output into a closed pipe (`tuber … | head`) ----------------------------

#[test]
fn tubes_into_a_closed_pipe_exits_cleanly() {
    let server = Server::start(&[]);
    for tube in ["a", "b", "c"] {
        run(&["put", "-a", &server.addr(), "-t", tube, "x"], &[], None);
    }
    let out = run_into_closed_pipe(&["tubes", "-a", &server.addr()], None, false);
    assert_exit(&out, 0);
}

#[test]
fn stats_into_a_closed_pipe_exits_cleanly() {
    let server = Server::start(&[]);
    let out = run_into_closed_pipe(&["stats", "-a", &server.addr()], None, false);
    assert_exit(&out, 0);
}

#[test]
fn put_into_a_closed_pipe_still_puts_every_line() {
    // Losing the reader of the replies must not lose the jobs: before, the
    // first reply's println! panicked and only one of these was ever put.
    let server = Server::start(&[]);
    let out = run_into_closed_pipe(&["put", "-a", &server.addr()], Some("a\nb\nc\n"), false);
    assert_exit(&out, 0);
    assert_eq!(server.ready_count("default"), 3);
}

#[test]
fn rejected_put_with_both_pipes_closed_still_exits_1() {
    // The rejection count can't be reported anywhere, but the exit status
    // still has to say the run failed.
    let server = Server::start(&["-z", "10"]);
    let out = run_into_closed_pipe(
        &["put", "-a", &server.addr()],
        Some("short\nlonger than ten bytes\n"),
        true,
    );
    assert_eq!(out.status.code(), Some(1), "101 means it panicked");
    assert_eq!(server.ready_count("default"), 1);
}

#[test]
fn work_keeps_working_when_its_log_pipe_closes() {
    // `tuber work 2>&1 | grep -m1 completed`: once grep exits, every worker
    // log line hits a dead pipe. That must not take the worker down.
    let server = Server::start(&[]);
    run(&["put", "-a", &server.addr(), "-t", "w", "true"], &[], None);
    let mut worker = spawn(
        &["work", "-a", &server.addr(), "-t", "w"],
        &[],
        None,
        Stdio::null(),
        closed_pipe(),
    );

    let mut deleted = 0;
    for _ in 0..100 {
        deleted = server.tube_stat("w", "cmd-delete");
        if deleted == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Shut down the way `timeout` or Ctrl-C would.
    Command::new("kill")
        .args(["-TERM", &worker.id().to_string()])
        .status()
        .unwrap();
    let status = worker.wait().unwrap();

    assert_eq!(deleted, 1, "the worker never finished the job");
    assert_eq!(status.code(), Some(0), "101 means it panicked");
}
