//! The `tuber-pro` binary end to end: tuber's command line with the
//! replication flag added, and a replica fed by a real server.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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
fn a_replica_bootstraps_from_the_running_server() {
    let dir = tempfile::tempdir().unwrap();
    let (port, repl_port) = (free_port(), free_port());
    let _server = Server(
        Command::new(BIN)
            .args(["server", "-b"])
            .arg(dir.path())
            .args(["-s", "1g", "-p", &port.to_string()])
            .args(["--replication-listen", &format!("127.0.0.1:{repl_port}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );

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
