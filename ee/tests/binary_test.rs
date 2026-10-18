//! The `tuber` binary end to end: tuber's command line with the hidden
//! replication flag added, and a replica fed by a real server.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tuber::ee::replication::{Follower, FollowerEvent};
use tuber::wal::{Position, WalRecord};

const BIN: &str = env!("CARGO_BIN_EXE_tuber");

/// A loopback port nothing is listening on (as of now).
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Connect, retrying while the server starts.
fn connect(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(conn) => {
                conn.set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                return conn;
            }
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("port {port} never came up: {e}"),
        }
    }
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A server on `dir` with replication, returned once it has bound both its
/// ports. `free_port` only finds ports free *now*: another process (other
/// test suites run alongside this one) can take one before the server
/// binds it, and the test would then talk to that process instead. So wait
/// for the server's "listening on" line, which it logs only after both
/// binds, and retry with fresh ports if it exits first.
fn start_replicating_server(dir: &Path) -> (Server, u16, u16) {
    for _ in 0..5 {
        let (port, repl_port) = (free_port(), free_port());
        let mut child = Command::new(BIN)
            .args(["server", "-V", "-b"])
            .arg(dir)
            .args(["-s", "1g", "-p", &port.to_string()])
            .args(["--replication-listen", &format!("127.0.0.1:{repl_port}")])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        if lines
            .by_ref()
            .map_while(Result::ok)
            .any(|line| line.contains("listening on"))
        {
            // Keep the pipe drained so the server never blocks on a log line.
            std::thread::spawn(move || lines.for_each(drop));
            return (Server(child), port, repl_port);
        }
        // Its output ended before the line: it exited, a port taken.
        let _ = child.wait();
    }
    panic!("the server never bound its ports");
}

#[test]
fn replication_without_a_binlog_dir_is_refused() {
    let out = Command::new(BIN)
        .args(["server", "-p", &free_port().to_string()])
        .args(["--replication-listen", "127.0.0.1:0"])
        .env_remove("TUBER_BINLOG_DIR")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    // tuber logs to stdout; look in both rather than depend on which.
    let output = String::from_utf8_lossy(&out.stdout) + String::from_utf8_lossy(&out.stderr);
    assert!(
        output.contains("--replication-listen needs --binlog-dir"),
        "output: {output}"
    );
}

#[test]
fn replication_is_hidden_from_server_help() {
    let out = Command::new(BIN)
        .args(["server", "--help"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("--binlog-dir"), "not server's help: {help}");
    assert!(
        !help.contains("replication"),
        "replication shows in help: {help}"
    );
}

#[test]
fn replication_listen_is_read_from_the_environment() {
    let out = Command::new(BIN)
        .args(["server", "-p", &free_port().to_string()])
        .env("TUBER_REPLICATION_LISTEN", "127.0.0.1:0")
        .env_remove("TUBER_BINLOG_DIR")
        .output()
        .unwrap();
    // Refused for want of --binlog-dir: the hidden flag was still read.
    assert_eq!(out.status.code(), Some(1));
    let output = String::from_utf8_lossy(&out.stdout) + String::from_utf8_lossy(&out.stderr);
    assert!(
        output.contains("--replication-listen needs --binlog-dir"),
        "output: {output}"
    );
}

#[test]
fn a_replica_bootstraps_from_the_running_server() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, port, repl_port) = start_replicating_server(dir.path());

    // A client put on tuber's own port...
    let mut client = connect(port);
    client.write_all(b"put 0 0 30 5\r\nhello\r\n").unwrap();
    let mut reply = [0u8; 64];
    let n = client.read(&mut reply).unwrap();
    assert!(
        reply[..n].starts_with(b"INSERTED "),
        "{:?}",
        String::from_utf8_lossy(&reply[..n])
    );

    // ...reaches a replica bootstrapping from ZERO, body inline.
    let mut follower = Follower::start(connect(repl_port), Position::ZERO).unwrap();
    loop {
        match follower.next_event().unwrap() {
            FollowerEvent::Record {
                record: WalRecord::FullJob(job),
                ..
            } => {
                assert_eq!(job.body.as_inline_bytes(), Some(&b"hello"[..]));
                break;
            }
            FollowerEvent::Record { .. } | FollowerEvent::Durable { .. } => continue,
        }
    }
}
