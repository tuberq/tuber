use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

struct TestServer {
    port: u16,
    handle: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with_max_job_size(65535).await
    }

    async fn start_with_max_job_size(max_job_size: u32) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener(listener, max_job_size, None)
                .await
                .ok();
        });

        TestServer { port, handle }
    }

    async fn connect(&self) -> TestConn {
        let stream = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        TestConn {
            reader: BufReader::new(reader),
            writer,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct TestConn {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestConn {
    async fn mustsend(&mut self, s: &str) {
        self.writer.write_all(s.as_bytes()).await.unwrap();
    }

    /// Read a full line (ending with \r\n) and assert it equals `expected`.
    async fn ckresp(&mut self, expected: &str) {
        let line = self.readline().await;
        assert_eq!(expected, line, "expected {:?}, got {:?}", expected, line);
    }

    /// Read a full line and assert it contains `sub`.
    async fn ckrespsub(&mut self, sub: &str) {
        let line = self.readline().await;
        assert!(line.contains(sub), "{:?} not found in {:?}", sub, line);
    }

    /// Read a line terminated by \r\n, with a 5-second timeout.
    async fn readline(&mut self) -> String {
        let mut buf = String::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.reader.read_line(&mut buf).await.unwrap();
                if buf.ends_with("\r\n") {
                    break;
                }
            }
        })
        .await
        .expect("readline timed out after 5s");
        buf
    }

    /// Read an OK <len>\r\n<body>\r\n response and return the full response
    /// (both lines concatenated).
    async fn read_ok_body(&mut self) -> String {
        let header = self.readline().await;
        assert!(header.starts_with("OK "), "expected OK, got {:?}", header);
        let len: usize = header
            .trim_end()
            .strip_prefix("OK ")
            .unwrap()
            .parse()
            .unwrap();
        let mut body_buf = vec![0u8; len + 2]; // +2 for trailing \r\n
        tokio::time::timeout(
            Duration::from_secs(5),
            self.reader.read_exact(&mut body_buf),
        )
        .await
        .expect("read_ok_body timed out")
        .unwrap();
        String::from_utf8_lossy(&body_buf[..len]).to_string()
    }

    /// Convenience: put a job and return the inserted ID.
    async fn put_job(&mut self, pri: u32, delay: u32, ttr: u32, body: &str) -> u64 {
        let cmd = format!("put {} {} {} {}\r\n", pri, delay, ttr, body.len());
        self.mustsend(&cmd).await;
        self.mustsend(&format!("{}\r\n", body)).await;
        let line = self.readline().await;
        let id_str = line
            .strip_prefix("INSERTED ")
            .unwrap_or_else(|| panic!("expected INSERTED, got {:?}", line))
            .trim();
        id_str.parse().unwrap()
    }
}

// ---------------------------------------------------------------------------
// Batch 1: Basic protocol tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_unknown_command() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("nont10knowncommand\r\n").await;
    c.ckresp("UNKNOWN_COMMAND\r\n").await;
}

#[tokio::test]
async fn test_peek_ok() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("peek 1\r\n").await;
    c.ckresp("FOUND 1 1\r\n").await;
    c.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_peek_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("peek 2\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("peek 18446744073709551615\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("peek 18446744073709551616\r\n").await; // UINT64_MAX+1
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("peek 184467440737095516160000000000000000000000000000\r\n")
        .await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("peek foo111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("peek 111foo\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_touch_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("touch a111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("touch 111a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("touch !@#!@#\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_touch_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("touch 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("touch 100000000000000\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_bury_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("bury 111abc 2\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("bury 111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("bury 111 222abc\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_kickjob_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("kick-job a111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("kick-job 111a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("kick-job !@#!@#\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_delete_ready_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;
}

#[tokio::test]
async fn test_delete_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("delete 18446744073709551616\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("delete 184467440737095516160000000000000000000000000000\r\n")
        .await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("delete foo111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("delete 111foo\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_release_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // bad id
    c.mustsend("release 18446744073709551616 1 1\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("release 184467440737095516160000000000000000000000000000 1 1\r\n")
        .await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("release foo111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("release 111foo\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    // bad priority
    c.mustsend("release 18446744073709551615 abc 1\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    // bad duration
    c.mustsend("release 18446744073709551615 1 abc\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_release_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("release 1 1 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_underscore_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("use x_y\r\n").await;
    c.ckresp("USING x_y\r\n").await;
}

#[tokio::test]
async fn test_two_commands_one_packet() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("use a\r\nuse b\r\n").await;
    c.ckresp("USING a\r\n").await;
    c.ckresp("USING b\r\n").await;
}

#[tokio::test]
async fn test_too_big_job() {
    let srv = TestServer::start_with_max_job_size(10).await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 11\r\n").await;
    c.mustsend("delete 9999\r\n").await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("JOB_TOO_BIG\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_job_size_invalid() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 4294967296\r\n").await;
    c.mustsend("put 0 0 0 10b\r\n").await;
    c.mustsend("put 0 0 0 --!@#$%^&&**()0b\r\n").await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_negative_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 512 -1 100 0\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_garbage_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put -1kkdj9djjkd9 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_negative_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put -1 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_max_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 4294967295 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_too_big_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 4294967296 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_zero_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_list_tubes() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("watch w\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    c.mustsend("use u\r\n").await;
    c.ckresp("USING u\r\n").await;

    c.mustsend("list-tubes\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("default"), "expected 'default' in list-tubes");
    assert!(body.contains("w"), "expected 'w' in list-tubes");
    assert!(body.contains("u"), "expected 'u' in list-tubes");

    c.mustsend("list-tube-used\r\n").await;
    c.ckresp("USING u\r\n").await;

    c.mustsend("list-tubes-watched\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("default"));
    assert!(body.contains("w"));

    c.mustsend("ignore default\r\n").await;
    c.ckresp("WATCHING 1\r\n").await;

    c.mustsend("list-tubes-watched\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("w"));
    assert!(!body.contains("default"));

    c.mustsend("ignore w\r\n").await;
    c.ckresp("NOT_IGNORED\r\n").await;
}

#[tokio::test]
async fn test_use_tube_long_name() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    let name200 = "0".repeat(200);
    // 200 chars is okay
    c.mustsend(&format!("use {}\r\n", name200)).await;
    c.ckresp(&format!("USING {}\r\n", name200)).await;
    // 201 chars is too much
    c.mustsend(&format!("use {}Z\r\n", name200)).await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_reserve_mode_fifo() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode fifo\r\n").await;
    c.ckresp("USING fifo\r\n").await;
}

#[tokio::test]
async fn test_reserve_mode_weighted() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode weighted\r\n").await;
    c.ckresp("USING weighted\r\n").await;
}

#[tokio::test]
async fn test_reserve_mode_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode blah\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("reserve-mode \r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_watch_with_weight() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch foo 4\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    // Re-watch updates weight (no new tube)
    c.mustsend("watch foo 2\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
}

#[tokio::test]
async fn test_watch_weight_default() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch bar\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
}

#[tokio::test]
async fn test_watch_weight_zero() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch foo 0\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_watch_weight_too_large() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch foo 10000\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

// ---------------------------------------------------------------------------
// Batch 2: Job lifecycle tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_peek_delayed() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 99 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("put 0 1 1 1\r\n").await;
    c.mustsend("C\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("FOUND 3 1\r\n").await;
    c.ckresp("C\r\n").await;

    c.mustsend("delete 3\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("FOUND 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_buried_kick() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // cannot bury unreserved job
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("peek-buried\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // now we can bury
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;
    c.mustsend("peek-buried\r\n").await;
    c.ckresp("FOUND 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // kick and verify the job is ready
    c.mustsend("kick 1\r\n").await;
    c.ckresp("KICKED 1\r\n").await;
    c.mustsend("peek-buried\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("peek-ready\r\n").await;
    c.ckresp("FOUND 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // nothing is left to kick
    c.mustsend("kick 1\r\n").await;
    c.ckresp("KICKED 0\r\n").await;
}

#[tokio::test]
async fn test_kickjob_buried() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    c.mustsend("kick-job 100\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("KICKED\r\n").await;
    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_kickjob_delayed() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 10 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("kick-job 2\r\n").await;
    c.ckresp("KICKED\r\n").await;
    c.mustsend("kick-job 2\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_stats_job_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("stats-job 111ABC\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("stats-job 111 222\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("stats-job 111\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_stats_tube_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("use tubea\r\n").await;
    c.ckresp("USING tubea\r\n").await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube tubea\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("name: \"tubea\""),
        "name missing from stats-tube"
    );
    assert!(
        body.contains("current-jobs-ready: 0"),
        "current-jobs-ready missing"
    );
    assert!(body.contains("total-jobs: 1"), "total-jobs missing");
}

#[tokio::test]
async fn test_ttr_large() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 120 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 0 5000 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body1 = c.read_ok_body().await;
    assert!(body1.contains("ttr: 120"));

    c.mustsend("stats-job 2\r\n").await;
    let body2 = c.read_ok_body().await;
    assert!(body2.contains("ttr: 5000"));
}

#[tokio::test]
async fn test_ttr_small() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("ttr: 1"), "ttr should be 1, got: {}", body);
}

#[tokio::test]
async fn test_reserve_job_ready() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    // Non-existing job
    c.mustsend("reserve-job 3\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    // id=1 was not reserved
    c.mustsend("release 1 1 0\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    c.mustsend("release 2 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
}

#[tokio::test]
async fn test_reserve_job_delayed() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 100 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 100 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("put 0 100 1 1\r\n").await;
    c.mustsend("C\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    c.mustsend("release 2 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    // verify that job was released in ready state
    c.mustsend("stats-job 2\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: ready"));
}

#[tokio::test]
async fn test_reserve_job_buried() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // put, reserve and bury
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;
    c.mustsend("bury 1 1\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // put, reserve and bury
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;
    c.mustsend("bury 2 1\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // reserve by ids
    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;
    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // release back and check if jobs are ready
    c.mustsend("release 1 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
    c.mustsend("release 2 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
    c.mustsend("stats-job 1\r\n").await;
    let body1 = c.read_ok_body().await;
    assert!(body1.contains("state: ready"));
    c.mustsend("stats-job 2\r\n").await;
    let body2 = c.read_ok_body().await;
    assert!(body2.contains("state: ready"));
}

#[tokio::test]
async fn test_reserve_job_already_reserved() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // Job should not be reserved twice
    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// ---------------------------------------------------------------------------
// Batch 3: Multi-connection & timing tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multi_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("use abc\r\n").await;
    c.ckresp("USING abc\r\n").await;
    c.mustsend("put 999999 0 0 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("use def\r\n").await;
    c.ckresp("USING def\r\n").await;
    c.mustsend("put 99 0 0 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("watch abc\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("watch def\r\n").await;
    c.ckresp("WATCHING 3\r\n").await;
    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 2 0\r\n").await;
}

#[tokio::test]
async fn test_reserve_timeout_two_conns() {
    let srv = TestServer::start().await;
    let mut c0 = srv.connect().await;
    let mut c1 = srv.connect().await;
    c0.mustsend("watch foo\r\n").await;
    c0.ckresp("WATCHING 2\r\n").await;
    c0.mustsend("reserve-with-timeout 1\r\n").await;
    c1.mustsend("watch foo\r\n").await;
    c1.ckresp("WATCHING 2\r\n").await;
    // c0 should time out after ~1s
    c0.ckresp("TIMED_OUT\r\n").await;
}

#[tokio::test]
async fn test_close_releases_job() {
    let srv = TestServer::start().await;
    let mut cons = srv.connect().await;
    let mut prod = srv.connect().await;
    cons.mustsend("reserve-with-timeout 1\r\n").await;

    prod.mustsend("put 0 0 100 1\r\n").await;
    prod.mustsend("a\r\n").await;
    prod.ckresp("INSERTED 1\r\n").await;

    cons.ckresp("RESERVED 1 1\r\n").await;
    cons.ckresp("a\r\n").await;

    prod.mustsend("stats-job 1\r\n").await;
    let body = prod.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Drop the consumer connection - should release the job
    drop(cons);

    // Job should be released quickly (< 1s)
    prod.mustsend("reserve-with-timeout 1\r\n").await;
    prod.ckresp("RESERVED 1 1\r\n").await;
    prod.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_quit_releases_job() {
    let srv = TestServer::start().await;
    let mut cons = srv.connect().await;
    let mut prod = srv.connect().await;
    cons.mustsend("reserve-with-timeout 1\r\n").await;

    prod.mustsend("put 0 0 100 1\r\n").await;
    prod.mustsend("a\r\n").await;
    prod.ckresp("INSERTED 1\r\n").await;

    cons.ckresp("RESERVED 1 1\r\n").await;
    cons.ckresp("a\r\n").await;

    prod.mustsend("stats-job 1\r\n").await;
    let body = prod.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Quit consumer - should release the job
    cons.mustsend("quit\r\n").await;

    // Job should be released quickly (< 1s)
    prod.mustsend("reserve-with-timeout 1\r\n").await;
    prod.ckresp("RESERVED 1 1\r\n").await;
    prod.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_delete_reserved_by_other() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    let mut o = srv.connect().await;
    o.mustsend("reserve\r\n").await;
    o.ckresp("RESERVED 1 1\r\n").await;
    o.ckresp("a\r\n").await;

    c.mustsend("delete 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_small_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 1 1 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_delayed_to_ready() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 1 1 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"));
    assert!(body.contains("current-jobs-delayed: 1"));
    assert!(body.contains("total-jobs: 1"));

    // Wait for the delay to expire
    tokio::time::sleep(Duration::from_millis(1100)).await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("current-jobs-ready: 1"),
        "delayed job should be ready now: {}",
        body
    );
    assert!(body.contains("current-jobs-delayed: 0"));
}

#[tokio::test]
async fn test_pause_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    let start = std::time::Instant::now();
    c.mustsend("pause-tube default 1\r\n").await;
    c.ckresp("PAUSED\r\n").await;
    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("x\r\n").await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_secs(1),
        "pause-tube should delay reserve by 1s, elapsed {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_unpause_tube() {
    let srv = TestServer::start().await;
    let mut c0 = srv.connect().await;
    let mut c1 = srv.connect().await;

    c0.mustsend("put 0 0 0 0\r\n").await;
    c0.mustsend("\r\n").await;
    c0.ckresp("INSERTED 1\r\n").await;

    c0.mustsend("pause-tube default 86400\r\n").await;
    c0.ckresp("PAUSED\r\n").await;

    c1.mustsend("reserve\r\n").await;

    c0.mustsend("pause-tube default 0\r\n").await;
    c0.ckresp("PAUSED\r\n").await;

    // c1 should get the job quickly after unpause
    c1.ckresp("RESERVED 1 0\r\n").await;
    c1.ckresp("\r\n").await;
}

#[tokio::test]
async fn test_reserve_ttr_deadline_soon() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // After 0.2s the job should still be reserved
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("DEADLINE_SOON\r\n").await;

    // Job should still be reserved
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Release and check it's ready
    c.mustsend("release 1 0 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: ready"));
}

#[tokio::test]
async fn test_reserve_job_ttr_deadline_soon() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 5 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: delayed"));

    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // After 0.1s the job should still be reserved
    tokio::time::sleep(Duration::from_millis(100)).await;
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("DEADLINE_SOON\r\n").await;

    // Job should still be reserved
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Wait for TTR to expire and check auto-release
    tokio::time::sleep(Duration::from_millis(1000)).await;
    // put a dummy job to trigger tick processing
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    // check that ID=1 gets released
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: ready"));
}

#[tokio::test]
async fn test_weighted_reserve_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode weighted\r\n").await;
    c.ckresp("USING weighted\r\n").await;

    c.mustsend("watch empty 1\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    // Put a job in default tube
    c.mustsend("put 0 0 120 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve should come from default (only tube with jobs)
    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_weighted_reserve_distribution() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode weighted\r\n").await;
    c.ckresp("USING weighted\r\n").await;

    c.mustsend("use high\r\n").await;
    c.ckresp("USING high\r\n").await;

    // Put many jobs in "high" tube
    for i in 0..100 {
        c.mustsend("put 0 0 120 1\r\n").await;
        c.mustsend("h\r\n").await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }

    c.mustsend("use low\r\n").await;
    c.ckresp("USING low\r\n").await;

    for i in 0..100 {
        c.mustsend("put 0 0 120 1\r\n").await;
        c.mustsend("l\r\n").await;
        c.ckresp(&format!("INSERTED {}\r\n", 100 + i + 1)).await;
    }

    // Watch high:4 low:1, then ignore default
    c.mustsend("watch high 4\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("watch low 1\r\n").await;
    c.ckresp("WATCHING 3\r\n").await;
    c.mustsend("ignore default\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    let mut high_ct = 0;
    let mut low_ct = 0;
    for _ in 0..100 {
        c.mustsend("reserve\r\n").await;
        let _header = c.readline().await; // "RESERVED <id> 1\r\n"
        let body = c.readline().await; // "h\r\n" or "l\r\n"
        if body.starts_with('h') {
            high_ct += 1;
        } else if body.starts_with('l') {
            low_ct += 1;
        }
    }
    // With 4:1 ratio, expect ~80 high, ~20 low. Use wide bounds.
    assert!(high_ct > 50, "high_ct={} should be > 50", high_ct);
    assert!(low_ct > 2, "low_ct={} should be > 2", low_ct);
}

// ---------------------------------------------------------------------------
// Batch 4: Put extension fields (idp, grp, aft, con)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_put_with_idempotency_key() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 idp:abc123\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: abc123"), "body: {}", body);
    assert!(
        body.contains("group: \n"),
        "group should be empty, body: {}",
        body
    );
}

#[tokio::test]
async fn test_put_with_all_extensions() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 idp:k1 grp:g1 aft:g0 con:c1\r\n")
        .await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: k1"), "body: {}", body);
    assert!(body.contains("group: g1"), "body: {}", body);
    assert!(body.contains("after-group: g0"), "body: {}", body);
    assert!(body.contains("concurrency-key: c1"), "body: {}", body);
}

#[tokio::test]
async fn test_put_with_extensions_order_independent() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 con:c1 idp:k1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: k1"), "body: {}", body);
    assert!(body.contains("concurrency-key: c1"), "body: {}", body);
}

#[tokio::test]
async fn test_put_with_unknown_tag_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 foo:bar\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_put_with_invalid_key_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 idp:-bad\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_put_without_extensions_still_works() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: \n"), "body: {}", body);
}

// ---------------------------------------------------------------------------
// Phase 5: Stats format, SIGUSR1, verbose flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stats_format_complete() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;

    // All C-expected fields must be present
    let expected_fields = [
        "current-jobs-urgent:",
        "current-jobs-ready:",
        "current-jobs-reserved:",
        "current-jobs-delayed:",
        "current-jobs-buried:",
        "cmd-put:",
        "cmd-peek:",
        "cmd-peek-ready:",
        "cmd-peek-delayed:",
        "cmd-peek-buried:",
        "cmd-reserve:",
        "cmd-reserve-with-timeout:",
        "cmd-delete:",
        "cmd-release:",
        "cmd-use:",
        "cmd-watch:",
        "cmd-ignore:",
        "cmd-bury:",
        "cmd-kick:",
        "cmd-touch:",
        "cmd-stats:",
        "cmd-stats-job:",
        "cmd-stats-tube:",
        "cmd-list-tubes:",
        "cmd-list-tube-used:",
        "cmd-list-tubes-watched:",
        "cmd-pause-tube:",
        "job-timeouts:",
        "total-jobs:",
        "max-job-size:",
        "current-tubes:",
        "current-connections:",
        "current-producers:",
        "current-workers:",
        "current-waiting:",
        "total-connections:",
        "pid:",
        "version:",
        "rusage-utime:",
        "rusage-stime:",
        "uptime:",
        "binlog-oldest-index:",
        "binlog-current-index:",
        "binlog-records-migrated:",
        "binlog-records-written:",
        "binlog-max-size:",
        "draining:",
        "id:",
        "hostname:",
        "os:",
        "platform:",
    ];

    for field in &expected_fields {
        assert!(
            body.contains(field),
            "missing field '{}' in stats:\n{}",
            field,
            body
        );
    }

    // rusage-utime/stime should match N.NNNNNN pattern
    for line in body.lines() {
        if line.starts_with("rusage-utime:") || line.starts_with("rusage-stime:") {
            let val = line.split(':').nth(1).unwrap().trim();
            let parts: Vec<&str> = val.split('.').collect();
            assert_eq!(
                parts.len(),
                2,
                "rusage value should be N.NNNNNN, got: {}",
                val
            );
            assert_eq!(
                parts[1].len(),
                6,
                "rusage fractional should be 6 digits, got: {}",
                val
            );
        }
    }

    // id should be 16-char hex
    for line in body.lines() {
        if line.starts_with("id:") {
            let val = line.split(':').nth(1).unwrap().trim();
            assert_eq!(val.len(), 16, "id should be 16 chars, got: '{}'", val);
            assert!(
                val.chars().all(|c| c.is_ascii_hexdigit()),
                "id should be hex, got: '{}'",
                val
            );
        }
    }

    // hostname, os, platform should be non-empty
    for field in &["hostname:", "os:", "platform:"] {
        for line in body.lines() {
            if line.starts_with(field) {
                let val = line.split(':').nth(1).unwrap().trim();
                assert!(!val.is_empty(), "{} should be non-empty", field);
            }
        }
    }

    // binlog-* fields should be 0 when WAL disabled
    assert!(
        body.contains("binlog-oldest-index: 0"),
        "binlog-oldest-index should be 0"
    );
    assert!(
        body.contains("binlog-current-index: 0"),
        "binlog-current-index should be 0"
    );
    assert!(
        body.contains("binlog-max-size: 0"),
        "binlog-max-size should be 0"
    );
}

#[tokio::test]
async fn test_stats_field_ordering() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;

    // Verify key fields appear in C-compatible order by checking line positions
    let ordered_fields = [
        "\ncurrent-jobs-urgent:",
        "\npid:",
        "\nversion:",
        "\nrusage-utime:",
        "\nrusage-stime:",
        "\nuptime:",
        "\nbinlog-oldest-index:",
        "\ndraining:",
        "\nid:",
        "\nhostname:",
        "\nos:",
        "\nplatform:",
    ];

    let mut last_pos = 0;
    for field in &ordered_fields {
        let pos = body
            .find(field)
            .unwrap_or_else(|| panic!("field '{}' not found", field));
        assert!(
            pos >= last_pos,
            "field '{}' (pos {}) should come after previous field (pos {})",
            field,
            pos,
            last_pos
        );
        last_pos = pos;
    }
}

/// SIGUSR1 drain mode test. This test sends SIGUSR1 to the current process,
/// which affects all engine tasks running in the same process. Run in isolation.
#[tokio::test]
#[ignore] // Run with: cargo test test_sigusr1_drain_mode -- --ignored
async fn test_sigusr1_drain_mode() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Get the server PID from stats
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    let pid: i32 = body
        .lines()
        .find(|l| l.starts_with("pid:"))
        .unwrap()
        .split(':')
        .nth(1)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // Verify not draining initially
    assert!(
        body.contains("draining: false"),
        "should not be draining initially"
    );

    // Send SIGUSR1
    unsafe { libc::kill(pid, libc::SIGUSR1) };

    // Give signal time to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // put should return DRAINING
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("DRAINING\r\n").await;

    // stats should show draining: true
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("draining: true"),
        "should be draining after SIGUSR1"
    );
}

#[tokio::test]
async fn test_stats_job_file_field() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    // Without WAL, file should be 0
    assert!(
        body.contains("file: 0"),
        "file should be 0 without WAL, got: {}",
        body
    );
}

// ---------------------------------------------------------------------------
// flush-tube tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_flush_tube_basic() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put 3 ready jobs
    c.put_job(0, 0, 1, "a").await;
    c.put_job(0, 0, 1, "b").await;
    c.put_job(0, 0, 1, "c").await;

    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 3\r\n").await;

    // Verify tube is empty
    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"), "ready should be 0");
}

#[tokio::test]
async fn test_flush_tube_mixed_states() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // 1 buried job: put, reserve, bury
    c.put_job(0, 0, 120, "bury_me").await;
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 7\r\n").await;
    c.ckresp("bury_me\r\n").await;
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // 1 ready job
    c.put_job(0, 0, 1, "ready").await;

    // 1 delayed job
    c.put_job(0, 3600, 1, "delayed").await;

    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 3\r\n").await;

    // Verify all queues empty
    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"), "ready should be 0");
    assert!(
        body.contains("current-jobs-delayed: 0"),
        "delayed should be 0"
    );
    assert!(
        body.contains("current-jobs-buried: 0"),
        "buried should be 0"
    );
}

#[tokio::test]
async fn test_flush_tube_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("flush-tube nonexistent\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_flush_tube_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Use the tube to create it
    c.mustsend("use default\r\n").await;
    c.ckresp("USING default\r\n").await;

    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 0\r\n").await;
}

#[tokio::test]
async fn test_flush_tube_with_reserved() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put 2 jobs, reserve 1
    c.put_job(0, 0, 120, "job1").await;
    c.put_job(0, 0, 120, "job2").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.readline().await; // RESERVED line
    c.readline().await; // body line

    // Now we have 1 reserved + 1 ready
    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 2\r\n").await;

    // Verify tube is empty
    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"), "ready should be 0");
    assert!(
        body.contains("current-jobs-reserved: 0"),
        "reserved should be 0"
    );
}

// ---------------------------------------------------------------------------
// Idempotency key tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_idempotency_basic() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // First put with idempotency key
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Second put with same key on same tube → same ID
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_idempotency_different_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 5 idp:key2\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_different_tubes() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put on default tube
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Switch tube and put with same key → different ID
    c.mustsend("use other\r\n").await;
    c.ckresp("USING other\r\n").await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_after_delete() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After delete, same key should create new job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_buried_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve and bury
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Dedup still works for buried job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_idempotency_delayed_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 3600 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Dedup works for delayed job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_idempotency_reserved_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve the job
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    // Dedup still works for reserved job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_idempotency_flush_clears_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Flush the tube
    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 1\r\n").await;

    // Key should be freed, new job gets new ID
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}
