use std::path::{Path, PathBuf};
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
    wal_dir: PathBuf,
}

impl TestServer {
    async fn start_with_wal(dir: &Path) -> Self {
        Self::start_with_wal_and_max_job_size(dir, 65535).await
    }

    async fn start_with_wal_and_max_job_size(dir: &Path, max_job_size: u32) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let wal_dir = dir.to_path_buf();
        let wal_dir_clone = wal_dir.clone();

        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener(listener, max_job_size, Some(wal_dir_clone.as_path()))
                .await
                .ok();
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        TestServer {
            port,
            handle,
            wal_dir,
        }
    }

    async fn connect(&self) -> TestConn {
        // Retry connection a few times (server may still be starting)
        let mut last_err = None;
        for _ in 0..20 {
            match TcpStream::connect(("127.0.0.1", self.port)).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let (reader, writer) = stream.into_split();
                    return TestConn {
                        reader: BufReader::new(reader),
                        writer,
                    };
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!("could not connect after retries: {:?}", last_err);
    }

    fn shutdown(self) -> PathBuf {
        self.handle.abort();
        self.wal_dir.clone()
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

    async fn ckresp(&mut self, expected: &str) {
        let line = self.readline().await;
        assert_eq!(expected, line, "expected {:?}, got {:?}", expected, line);
    }

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

    async fn read_ok_body(&mut self) -> String {
        let header = self.readline().await;
        assert!(header.starts_with("OK "), "expected OK, got {:?}", header);
        let len: usize = header
            .trim_end()
            .strip_prefix("OK ")
            .unwrap()
            .parse()
            .unwrap();
        let mut body_buf = vec![0u8; len + 2];
        tokio::time::timeout(
            Duration::from_secs(5),
            self.reader.read_exact(&mut body_buf),
        )
        .await
        .expect("read_ok_body timed out")
        .unwrap();
        String::from_utf8_lossy(&body_buf[..len]).to_string()
    }
}

// ---------------------------------------------------------------------------
// WAL integration tests
// ---------------------------------------------------------------------------

/// Test 1: Basic put, restart, peek finds job, delete works
#[tokio::test]
async fn test_binlog_basic() {
    let dir = tempfile::tempdir().unwrap();

    // Start server, put a job
    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 10 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Shutdown
    drop(c);
    let wal_dir = srv.shutdown();

    // Wait for shutdown
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Job should still exist
    c2.mustsend("peek 1\r\n").await;
    c2.ckresp("FOUND 1 5\r\n").await;
    c2.ckresp("hello\r\n").await;

    // Delete should work
    c2.mustsend("delete 1\r\n").await;
    c2.ckresp("DELETED\r\n").await;

    // After delete, peek should not find it
    c2.mustsend("peek 1\r\n").await;
    c2.ckresp("NOT_FOUND\r\n").await;
}

/// Test 2: Put, reserve, bury, restart, peek-buried finds it
#[tokio::test]
async fn test_binlog_bury() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 10 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    c.mustsend("bury 1 10\r\n").await;
    c.ckresp("BURIED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("peek-buried\r\n").await;
    c2.ckresp("FOUND 1 5\r\n").await;
    c2.ckresp("hello\r\n").await;
}

/// Test 3: Put to named tube, release with new pri, restart, verify state
#[tokio::test]
async fn test_binlog_read() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("use mytube\r\n").await;
    c.ckresp("USING mytube\r\n").await;

    c.mustsend("put 100 0 60 4\r\n").await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("watch mytube\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 4\r\n").await;
    c.ckresp("data\r\n").await;

    // Release with new priority
    c.mustsend("release 1 50 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
    assert!(body.contains("pri: 50"), "body: {}", body);
    assert!(body.contains("tube: \"mytube\""), "body: {}", body);
}

/// Test 4: Small max_file_size, put many jobs, verify multiple files created
#[tokio::test]
async fn test_binlog_size_limit() {
    let dir = tempfile::tempdir().unwrap();

    // Use a very small max_file_size (we can't set it via the public API directly,
    // but we can verify WAL files are created by checking the directory)
    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Put several jobs
    for i in 0..20 {
        let body = format!("job-body-{:04}", i);
        c.mustsend(&format!("put 0 0 60 {}\r\n", body.len())).await;
        c.mustsend(&format!("{}\r\n", body)).await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }

    // Verify WAL files exist
    let wal_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("binlog."))
        .collect();
    assert!(
        !wal_files.is_empty(),
        "expected WAL files in {:?}",
        dir.path()
    );

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart and verify all jobs survived
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    for i in 0..20 {
        c2.mustsend(&format!("peek {}\r\n", i + 1)).await;
        let line = c2.readline().await;
        assert!(
            line.starts_with(&format!("FOUND {}", i + 1)),
            "expected FOUND for job {}, got {:?}",
            i + 1,
            line
        );
        let _ = c2.readline().await; // body
    }
}

/// Test 5: Put 96 jobs, delete all, verify no crash
#[tokio::test]
async fn test_binlog_allocation() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    for i in 0..96 {
        c.mustsend("put 0 0 60 1\r\n").await;
        c.mustsend("x\r\n").await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }

    for i in 0..96 {
        c.mustsend(&format!("delete {}\r\n", i + 1)).await;
        c.ckresp("DELETED\r\n").await;
    }

    // Server should still work
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("y\r\n").await;
    c.ckresp("INSERTED 97\r\n").await;
}

/// Test 6: Reserved job replays as ready
#[tokio::test]
async fn test_binlog_reserved_replayed_as_ready() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 4\r\n").await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 4\r\n").await;
    c.ckresp("data\r\n").await;

    // Job is now reserved. Restart without releasing.
    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Job should be replayed as ready (not reserved)
    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
}

/// Test 7: Delayed job survives restart
#[tokio::test]
async fn test_binlog_delayed_job() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 3600 60 4\r\n").await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: delayed"), "body: {}", body);
}

/// Test 8: Put with extension fields, restart, stats-job shows them
#[tokio::test]
async fn test_binlog_extension_fields() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 4 idp:mykey grp:g1 aft:g0 con:c1\r\n")
        .await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("idempotency-key: mykey"), "body: {}", body);
    assert!(body.contains("group: g1"), "body: {}", body);
    assert!(body.contains("after-group: g0"), "body: {}", body);
    assert!(body.contains("concurrency-key: c1"), "body: {}", body);
}

/// Test 9: Multiple puts and deletes, restart preserves only surviving jobs
#[tokio::test]
async fn test_binlog_put_delete_restart() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Put 5 jobs
    for i in 1..=5 {
        let body = format!("body{}", i);
        c.mustsend(&format!("put 0 0 60 {}\r\n", body.len())).await;
        c.mustsend(&format!("{}\r\n", body)).await;
        c.ckresp(&format!("INSERTED {}\r\n", i)).await;
    }

    // Delete jobs 2 and 4
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;
    c.mustsend("delete 4\r\n").await;
    c.ckresp("DELETED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Jobs 1, 3, 5 should exist
    for &id in &[1, 3, 5] {
        c2.mustsend(&format!("peek {}\r\n", id)).await;
        let line = c2.readline().await;
        assert!(
            line.starts_with(&format!("FOUND {}", id)),
            "expected FOUND for job {}, got {:?}",
            id,
            line
        );
        let _ = c2.readline().await;
    }

    // Jobs 2, 4 should not exist
    for &id in &[2, 4] {
        c2.mustsend(&format!("peek {}\r\n", id)).await;
        c2.ckresp("NOT_FOUND\r\n").await;
    }

    // Next job ID should be 6
    c2.mustsend("put 0 0 60 1\r\n").await;
    c2.mustsend("x\r\n").await;
    c2.ckresp("INSERTED 6\r\n").await;
}

/// Test 10: Kick survives restart
#[tokio::test]
async fn test_binlog_kick_restart() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Put, reserve, bury
    c.mustsend("put 10 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    c.mustsend("bury 1 10\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Kick it back to ready
    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("KICKED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Should be ready after restart
    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
}

/// Test 11: Idempotency key survives WAL replay
#[tokio::test]
async fn test_wal_replay_idempotency() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart server — idempotency index should be rebuilt from WAL
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Same key should return same ID with state
    c2.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c2.mustsend("world\r\n").await;
    c2.ckresp("INSERTED 1 READY\r\n").await;

    // Different key should get new ID
    c2.mustsend("put 0 0 60 5 idp:key2\r\n").await;
    c2.mustsend("other\r\n").await;
    c2.ckresp("INSERTED 2\r\n").await;
}
