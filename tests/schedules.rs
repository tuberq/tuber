//! Recurring schedules: the sync protocol, firing, and persistence.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

struct TestServer {
    port: u16,
    handle: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        Self::spawn(|listener| async move {
            tuber::server::run_with_listener(listener, 65535, None, None)
                .await
                .ok();
        })
        .await
    }

    async fn start_with_max_jobs_size(max_jobs_size: u64) -> Self {
        Self::spawn(move |listener| async move {
            tuber::server::run_with_listener_limited(
                listener,
                65535,
                Some(max_jobs_size),
                None,
                None,
                false,
                None,
            )
            .await
            .ok();
        })
        .await
    }

    async fn start_with_binlog(dir: &std::path::Path) -> Self {
        let dir = dir.to_path_buf();
        Self::spawn(move |listener| async move {
            tuber::server::run_with_listener(listener, 65535, Some(&dir), None)
                .await
                .ok();
        })
        .await
    }

    async fn start_with_binlog_and_storage_budget(dir: &std::path::Path, bytes: u64) -> Self {
        let dir = dir.to_path_buf();
        Self::spawn(move |listener| async move {
            tuber::server::run_with_listener_limited(
                listener,
                65535,
                None,
                Some(bytes),
                Some(&dir),
                true,
                None,
            )
            .await
            .ok();
        })
        .await
    }

    /// Stop the server the way a crash would, as far as schedules go:
    /// nothing gets a chance to write on the way down.
    async fn kill(self) {
        self.handle.abort();
        // Let the engine task notice its channel close before a new server
        // opens the same directory.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    async fn spawn<F, Fut>(run: F) -> Self
    where
        F: FnOnce(tokio::net::TcpListener) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(run(listener));
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
    async fn send(&mut self, s: &str) {
        self.writer.write_all(s.as_bytes()).await.unwrap();
    }

    async fn send_bytes(&mut self, b: &[u8]) {
        self.writer.write_all(b).await.unwrap();
    }

    async fn readline(&mut self) -> String {
        let mut buf = String::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.reader.read_line(&mut buf).await.unwrap();
                if buf.ends_with("\r\n") || buf.is_empty() {
                    break;
                }
            }
        })
        .await
        .expect("readline timed out after 5s");
        buf.trim_end().to_string()
    }

    /// Send `line` and assert the reply.
    async fn ck(&mut self, line: &str, expected: &str) {
        self.send(&format!("{line}\r\n")).await;
        let got = self.readline().await;
        assert_eq!(got, expected, "reply to {line:?}");
    }

    /// `schedule-put` with `body`, returning the reply.
    async fn schedule_put(&mut self, name: &str, tube: &str, rest: &str, body: &str) -> String {
        self.send(&format!(
            "schedule-put {name} {tube} 1024 0 120 {} {rest}\r\n{body}\r\n",
            body.len()
        ))
        .await;
        self.readline().await
    }

    async fn read_ok_body(&mut self) -> String {
        let header = self.readline().await;
        let len: usize = header
            .strip_prefix("OK ")
            .unwrap_or_else(|| panic!("expected OK, got {header:?}"))
            .parse()
            .unwrap();
        let mut body = vec![0u8; len + 2];
        self.reader.read_exact(&mut body).await.unwrap();
        String::from_utf8(body[..len].to_vec()).unwrap()
    }

    async fn list(&mut self, cmd: &str) -> Vec<String> {
        self.send(&format!("{cmd}\r\n")).await;
        let body = self.read_ok_body().await;
        body.lines()
            .filter_map(|l| l.strip_prefix("- ").map(str::to_string))
            .collect()
    }

    async fn stats_schedule(&mut self, owner: &str, name: &str) -> String {
        self.send(&format!("stats-schedule {owner} {name}\r\n"))
            .await;
        self.read_ok_body().await
    }
}

fn stat<'a>(yaml: &'a str, key: &str) -> &'a str {
    yaml.lines()
        .find_map(|l| l.strip_prefix(&format!("{key}: ")))
        .unwrap_or_else(|| panic!("no {key} in {yaml}"))
}

// ---------------------------------------------------------------------------
// Sync protocol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_adds_then_changes_and_removes() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.ck("schedule-begin splat", "SYNCING splat").await;
    assert_eq!(
        c.schedule_put("rollup", "maint", "idp:rollup con:1 5 * * * *", "{}")
            .await,
        "STAGED"
    );
    assert_eq!(
        c.schedule_put("checkpoint", "checkins", "every 10m", "{}")
            .await,
        "STAGED"
    );
    assert_eq!(
        c.schedule_put("stats", "maint", "7 * * * *", "{}").await,
        "STAGED"
    );
    c.ck("schedule-commit", "SYNCED 3 0 0 0").await;
    assert_eq!(
        c.list("list-schedules").await,
        ["splat checkpoint", "splat rollup", "splat stats"]
    );

    // Another replica sends the same set.
    let mut replica = srv.connect().await;
    replica.ck("schedule-begin splat", "SYNCING splat").await;
    replica
        .schedule_put("rollup", "maint", "idp:rollup con:1 5 * * * *", "{}")
        .await;
    replica
        .schedule_put("checkpoint", "checkins", "every 10m", "{}")
        .await;
    replica
        .schedule_put("stats", "maint", "7 * * * *", "{}")
        .await;
    replica.ck("schedule-commit", "SYNCED 0 0 0 3").await;

    // The next release edits one, drops one and renames one.
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put("rollup", "maint", "idp:rollup con:1 15 * * * *", "{}")
        .await;
    c.schedule_put("stats_hourly", "maint", "7 * * * *", "{}")
        .await;
    c.ck("schedule-commit", "SYNCED 1 1 2 0").await;
    assert_eq!(
        c.list("list-schedules").await,
        ["splat rollup", "splat stats_hourly"]
    );
}

#[tokio::test]
async fn a_refused_entry_aborts_the_sync_and_keeps_the_old_set() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put("retention", "maint", "0 2 * * * Australia/Melbourne", "{}")
        .await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;

    for (rest, reply) in [
        ("0 2 * * * Australia/Melborne", "BAD_SCHEDULE"),
        ("0 0 30 2 *", "BAD_SCHEDULE"),
        ("every 1h offset 1h", "BAD_SCHEDULE"),
        ("bogus:tag 0 2 * * *", "BAD_FORMAT"),
        ("idp:rollup", "BAD_FORMAT"),
        ("catchup:soon 0 2 * * *", "BAD_FORMAT"),
    ] {
        c.ck("schedule-begin splat", "SYNCING splat").await;
        assert_eq!(
            c.schedule_put("other", "maint", "@hourly", "{}").await,
            "STAGED"
        );
        assert_eq!(
            c.schedule_put("retention", "maint", rest, "{}").await,
            reply,
            "{rest:?}"
        );
        // The body was consumed: the connection is still in step.
        c.ck("list-tube-used", "USING default").await;
        c.ck("schedule-commit", "ABORTED").await;
        assert_eq!(
            c.list("list-schedules splat").await,
            ["splat retention"],
            "{rest:?}"
        );
    }
}

#[tokio::test]
async fn a_malformed_line_aborts_the_sync() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Bad priority: the body length is still known, so the body is drained.
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.send("schedule-put a maint x 0 120 2 @hourly\r\n{}\r\n")
        .await;
    assert_eq!(c.readline().await, "BAD_FORMAT");
    c.ck("list-tube-used", "USING default").await;
    c.ck("schedule-commit", "ABORTED").await;

    // Too short to carry a body length at all.
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.ck("schedule-put a maint", "BAD_FORMAT").await;
    c.ck("schedule-commit", "ABORTED").await;

    // No arguments at all.
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.ck("schedule-put", "BAD_FORMAT").await;
    c.ck("schedule-commit", "ABORTED").await;

    // A line that isn't UTF-8 (a Latin-1 name), and one with a NUL: refused
    // like any other bad line, body consumed, sync aborted.
    for line in [
        &b"schedule-put r\xe9port maint 0 0 120 2 @daily\r\n{}\r\n"[..],
        &b"schedule-put re\x00port maint 0 0 120 2 @daily\r\n{}\r\n"[..],
    ] {
        c.ck("schedule-begin splat", "SYNCING splat").await;
        assert_eq!(
            c.schedule_put("ok", "maint", "@daily", "{}").await,
            "STAGED"
        );
        c.send_bytes(line).await;
        assert_eq!(c.readline().await, "BAD_FORMAT", "{line:?}");
        c.ck("list-tube-used", "USING default").await;
        c.ck("schedule-commit", "ABORTED").await;
    }

    // Same name twice in one sync.
    c.ck("schedule-begin splat", "SYNCING splat").await;
    assert_eq!(
        c.schedule_put("a", "maint", "@hourly", "{}").await,
        "STAGED"
    );
    assert_eq!(
        c.schedule_put("a", "maint", "@daily", "{}").await,
        "BAD_FORMAT"
    );
    c.ck("schedule-commit", "ABORTED").await;

    // Beginning twice.
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.ck("schedule-begin splat", "BAD_FORMAT").await;
    c.ck("schedule-commit", "ABORTED").await;

    assert!(c.list("list-schedules").await.is_empty());
}

#[tokio::test]
async fn an_oversize_body_aborts_the_sync() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    let body = "x".repeat(65536);
    assert_eq!(
        c.schedule_put("big", "maint", "@hourly", &body).await,
        "JOB_TOO_BIG"
    );
    c.ck("list-tube-used", "USING default").await;
    c.ck("schedule-commit", "ABORTED").await;
}

#[tokio::test]
async fn commands_outside_a_sync() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    assert_eq!(
        c.schedule_put("a", "maint", "@hourly", "{}").await,
        "NOT_SYNCING"
    );
    c.ck("list-tube-used", "USING default").await;
    c.ck("schedule-commit", "NOT_SYNCING").await;
    c.ck("schedule-abort", "NOT_SYNCING").await;
    c.ck("schedule-begin bad owner", "BAD_FORMAT").await;
    c.ck("schedule-begin -splat", "BAD_FORMAT").await;
}

#[tokio::test]
async fn abort_and_disconnect_discard_the_staged_set() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put("a", "maint", "@hourly", "{}").await;
    c.ck("schedule-abort", "ABORTED").await;
    assert!(c.list("list-schedules").await.is_empty());

    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put("a", "maint", "@hourly", "{}").await;
    drop(c);

    let mut c = srv.connect().await;
    // The engine handles the disconnect before this connection's commands.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(c.list("list-schedules").await.is_empty());
}

#[tokio::test]
async fn owners_are_namespaces_and_an_empty_sync_removes_all() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put("a", "maint", "@hourly", "{}").await;
    c.schedule_put("b", "maint", "@daily", "{}").await;
    c.ck("schedule-commit", "SYNCED 2 0 0 0").await;

    c.ck("schedule-begin booko", "SYNCING booko").await;
    c.schedule_put("a", "jobs", "@hourly", "{}").await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;
    assert_eq!(
        c.list("list-schedules").await,
        ["booko a", "splat a", "splat b"]
    );
    assert_eq!(c.list("list-schedules booko").await, ["booko a"]);

    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.ck("schedule-commit", "SYNCED 0 0 2 0").await;
    assert_eq!(c.list("list-schedules").await, ["booko a"]);
    assert!(c.list("list-schedules nobody").await.is_empty());
}

#[tokio::test]
async fn stats_schedule_reports_the_entry_in_its_zone() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put(
        "retention",
        "splat.maintenance",
        "con:1 catchup:300 0 2 * * * Australia/Melbourne",
        "{}",
    )
    .await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;

    let s = c.stats_schedule("splat", "retention").await;
    assert_eq!(stat(&s, "owner"), "\"splat\"");
    assert_eq!(stat(&s, "name"), "\"retention\"");
    assert_eq!(stat(&s, "spec"), "\"0 2 * * * Australia/Melbourne\"");
    assert_eq!(stat(&s, "tube"), "\"splat.maintenance\"");
    assert_eq!(stat(&s, "pri"), "1024");
    assert_eq!(stat(&s, "ttr"), "120");
    assert_eq!(stat(&s, "tags"), "\"con:1\"");
    assert_eq!(stat(&s, "catchup"), "\"once within 300s\"");
    assert_eq!(stat(&s, "last-fire"), "\"\"");
    assert_eq!(stat(&s, "total-fires"), "0");
    for key in ["next-fire", "next-fire-2", "next-fire-3"] {
        let t = stat(&s, key);
        // 02:00 local, at +10:00 or +11:00 depending on the season.
        assert!(t.contains("T02:00:00+1"), "{key}: {t}");
    }
    assert!(stat(&s, "next-fire") < stat(&s, "next-fire-2"));

    c.send("stats-schedule splat nothing\r\n").await;
    assert_eq!(c.readline().await, "NOT_FOUND");
    c.ck("stats-schedule splat", "BAD_FORMAT").await;
}

#[tokio::test]
async fn entries_share_the_memory_budget() {
    // An entry costs its body plus ~530 bytes, as a job costs its body plus
    // 512: one 2000-byte body fits in 4096, two don't.
    let srv = TestServer::start_with_max_jobs_size(4096).await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    let body = "x".repeat(2000);
    assert_eq!(
        c.schedule_put("a", "maint", "@hourly", &body).await,
        "STAGED"
    );
    assert_eq!(
        c.schedule_put("b", "maint", "@hourly", &body).await,
        "OUT_OF_MEMORY"
    );
    c.ck("schedule-commit", "ABORTED").await;

    c.ck("schedule-begin splat", "SYNCING splat").await;
    assert_eq!(
        c.schedule_put("a", "maint", "@hourly", &body).await,
        "STAGED"
    );
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;
    // The committed entry holds its share: a put that fit before no longer does.
    c.send(&format!("put 0 0 10 {}\r\n{body}\r\n", body.len()))
        .await;
    assert_eq!(c.readline().await, "OUT_OF_MEMORY");
}

#[tokio::test]
async fn staging_on_every_connection_shares_one_budget() {
    // Each 2000-byte entry costs ~2530: two connections staging two each
    // need ~10 KB, more than the 8 KB budget.
    let srv = TestServer::start_with_max_jobs_size(8192).await;
    let body = "x".repeat(2000);
    let mut a = srv.connect().await;
    a.ck("schedule-begin a", "SYNCING a").await;
    assert_eq!(
        a.schedule_put("1", "maint", "@hourly", &body).await,
        "STAGED"
    );
    assert_eq!(
        a.schedule_put("2", "maint", "@hourly", &body).await,
        "STAGED"
    );

    let mut b = srv.connect().await;
    b.ck("schedule-begin b", "SYNCING b").await;
    assert_eq!(
        b.schedule_put("1", "maint", "@hourly", &body).await,
        "STAGED"
    );
    assert_eq!(
        b.schedule_put("2", "maint", "@hourly", &body).await,
        "OUT_OF_MEMORY"
    );

    // Aborting gives the staged bytes back.
    a.ck("schedule-abort", "ABORTED").await;
    b.ck("schedule-abort", "ABORTED").await;
    b.ck("schedule-begin b", "SYNCING b").await;
    for name in ["1", "2", "3"] {
        assert_eq!(
            b.schedule_put(name, "maint", "@hourly", &body).await,
            "STAGED"
        );
    }
    // As does a connection closing mid-sync.
    drop(b);
    tokio::time::sleep(Duration::from_millis(50)).await;
    a.ck("schedule-begin a", "SYNCING a").await;
    for name in ["1", "2", "3"] {
        assert_eq!(
            a.schedule_put(name, "maint", "@hourly", &body).await,
            "STAGED"
        );
    }
}

#[tokio::test]
async fn a_resync_is_not_charged_twice() {
    let srv = TestServer::start_with_max_jobs_size(8192).await;
    let body = "x".repeat(2000);
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put("a", "maint", "@hourly", &body).await;
    c.schedule_put("b", "maint", "@hourly", &body).await;
    c.ck("schedule-commit", "SYNCED 2 0 0 0").await;
    // ~5 KB of entries plus a ~2 KB job: no room for another copy of the
    // entries, but a resync replaces them rather than adding to them.
    let job = "j".repeat(1500);
    c.send(&format!("put 0 0 10 {}\r\n{job}\r\n", job.len()))
        .await;
    assert!(c.readline().await.starts_with("INSERTED"));

    c.ck("schedule-begin splat", "SYNCING splat").await;
    assert_eq!(
        c.schedule_put("a", "maint", "@hourly", &body).await,
        "STAGED"
    );
    assert_eq!(
        c.schedule_put("b", "maint", "@hourly", &body).await,
        "STAGED"
    );
    c.ck("schedule-commit", "SYNCED 0 0 0 2").await;
}

#[tokio::test]
async fn a_commit_that_frees_space_works_on_a_full_disk() {
    let dir = tempfile::tempdir().unwrap();
    // One WAL segment (10 MiB) is held in reserve; leave ~1 MiB above it.
    let srv = TestServer::start_with_binlog_and_storage_budget(dir.path(), 11 * 1024 * 1024).await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin app", "SYNCING app").await;
    c.schedule_put("runaway", "jobs", "@yearly", "{}").await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;

    // Fill the disk until even a one-byte put is refused.
    for body in ["x".repeat(60_000), "x".to_string()] {
        let mut refused = false;
        for _ in 0..5_000 {
            c.send(&format!("put 0 0 10 {}\r\n{body}\r\n", body.len()))
                .await;
            if c.readline().await == "OUT_OF_STORAGE" {
                refused = true;
                break;
            }
        }
        assert!(refused, "the budget never filled");
    }

    // Removing the entry adds nothing to the disk: it must go through.
    c.ck("schedule-begin app", "SYNCING app").await;
    c.ck("schedule-commit", "SYNCED 0 0 1 0").await;
}

#[tokio::test]
async fn schedules_count_toward_the_memory_budget_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    let srv = TestServer::start_with_binlog(dir.path()).await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin app", "SYNCING app").await;
    c.schedule_put("big", "jobs", "@yearly", &"x".repeat(20_000))
        .await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;
    drop(c);
    srv.kill().await;

    // 20 KB of schedules don't fit a 10 KB budget: refuse to start, as
    // replayed jobs that don't fit do.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let started = tokio::time::timeout(
        Duration::from_secs(2),
        tuber::server::run_with_listener_limited(
            listener,
            65535,
            Some(10_000),
            Some(1024 * 1024 * 1024),
            Some(dir.path()),
            true,
            None,
        ),
    )
    .await;
    match started {
        Ok(Err(e)) => assert_eq!(e.kind(), std::io::ErrorKind::OutOfMemory, "{e}"),
        Ok(Ok(())) => panic!("server exited cleanly"),
        Err(_) => panic!("server started over its memory budget"),
    }
}

// ---------------------------------------------------------------------------
// Firing
// ---------------------------------------------------------------------------

/// `reserve-with-timeout`, returning (id, body) or panicking on TIMED_OUT.
async fn reserve(c: &mut TestConn, timeout: u32) -> (u64, String) {
    c.send(&format!("reserve-with-timeout {timeout}\r\n")).await;
    let line = c.readline().await;
    let id = line
        .strip_prefix("RESERVED ")
        .unwrap_or_else(|| panic!("expected RESERVED, got {line:?}"))
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    (id, c.readline().await)
}

#[tokio::test]
async fn an_entry_puts_its_body_into_its_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin app", "SYNCING app").await;
    assert_eq!(
        c.schedule_put("tick", "ticks", "every 1s", r#"{"class":"TickJob"}"#)
            .await,
        "STAGED"
    );
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;

    c.ck("watch ticks", "WATCHING 2").await;
    c.ck("ignore default", "WATCHING 1").await;
    let (id, body) = reserve(&mut c, 3).await;
    assert_eq!(body, r#"{"class":"TickJob"}"#);

    c.send(&format!("stats-job {id}\r\n")).await;
    let job = c.read_ok_body().await;
    assert_eq!(stat(&job, "tube"), "\"ticks\"");
    assert_eq!(stat(&job, "pri"), "1024");
    assert_eq!(stat(&job, "ttr"), "120");

    let s = c.stats_schedule("app", "tick").await;
    assert!(
        stat(&s, "total-inserted").parse::<u64>().unwrap() >= 1,
        "{s}"
    );
    assert!(stat(&s, "last-result").starts_with("\"INSERTED "), "{s}");
    assert_ne!(stat(&s, "last-fire"), "\"\"");
}

#[tokio::test]
async fn a_fire_deduped_against_a_buried_job_is_visible() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin app", "SYNCING app").await;
    c.schedule_put("rollup", "maint", "idp:rollup every 1s", "{}")
        .await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;

    c.ck("watch maint", "WATCHING 2").await;
    let (id, _) = reserve(&mut c, 3).await;
    c.ck(&format!("bury {id} 0"), "BURIED").await;

    // Let at least one more fire land on the buried job.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let s = c.stats_schedule("app", "rollup").await;
    assert!(
        stat(&s, "total-deduped-buried").parse::<u64>().unwrap() >= 1,
        "{s}"
    );
    assert!(
        stat(&s, "buried-streak").parse::<u64>().unwrap() >= 1,
        "{s}"
    );
    assert_eq!(stat(&s, "last-result"), format!("\"INSERTED {id} BURIED\""));
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

/// A spec whose only tick in the next hour is `secs` from now:
/// `every 1h offset <n>s`, with `n` the second of the hour then.
fn tick_in(secs: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("every 1h offset {}s", (now + secs) % 3600)
}

async fn ready_jobs(c: &mut TestConn, tube: &str) -> u64 {
    c.send(&format!("stats-tube {tube}\r\n")).await;
    let line = c.readline().await;
    if line == "NOT_FOUND" {
        return 0;
    }
    let len: usize = line.strip_prefix("OK ").unwrap().parse().unwrap();
    let mut body = vec![0u8; len + 2];
    c.reader.read_exact(&mut body).await.unwrap();
    let yaml = String::from_utf8(body).unwrap();
    stat(&yaml, "current-jobs-ready").parse().unwrap()
}

#[tokio::test]
async fn entries_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let srv = TestServer::start_with_binlog(dir.path()).await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put(
        "retention",
        "maint",
        "con:1 catchup:600 0 2 * * * Australia/Melbourne",
        r#"{"class":"DataRetentionJob"}"#,
    )
    .await;
    c.schedule_put("checkpoint", "checkins", "idp:checkpoint every 10m", "{}")
        .await;
    c.ck("schedule-commit", "SYNCED 2 0 0 0").await;
    let before = c.stats_schedule("splat", "retention").await;
    drop(c);
    srv.kill().await;

    let srv = TestServer::start_with_binlog(dir.path()).await;
    let mut c = srv.connect().await;
    assert_eq!(
        c.list("list-schedules").await,
        ["splat checkpoint", "splat retention"]
    );
    let after = c.stats_schedule("splat", "retention").await;
    for key in [
        "spec",
        "tube",
        "pri",
        "delay",
        "ttr",
        "tags",
        "catchup",
        "synced-at",
        "next-fire",
    ] {
        assert_eq!(stat(&after, key), stat(&before, key), "{key}");
    }

    // The restored set is the one a sync compares against.
    c.ck("schedule-begin splat", "SYNCING splat").await;
    c.schedule_put(
        "retention",
        "maint",
        "con:1 catchup:600 CRON_TZ=Australia/Melbourne 0 2 * * *",
        r#"{"class":"DataRetentionJob"}"#,
    )
    .await;
    c.ck("schedule-commit", "SYNCED 0 0 1 1").await;
    drop(c);
    srv.kill().await;

    let srv = TestServer::start_with_binlog(dir.path()).await;
    let mut c = srv.connect().await;
    assert_eq!(c.list("list-schedules").await, ["splat retention"]);
}

#[tokio::test]
async fn a_tick_missed_while_down_fires_once_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let srv = TestServer::start_with_binlog(dir.path()).await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin app", "SYNCING app").await;
    c.schedule_put("hourly", "jobs", &tick_in(2), "{}").await;
    c.schedule_put(
        "strict",
        "strict",
        &format!("catchup:0 {}", tick_in(2)),
        "{}",
    )
    .await;
    c.ck("schedule-commit", "SYNCED 2 0 0 0").await;
    drop(c);
    srv.kill().await;

    // Down across the tick, and for longer than the 5 s within which a late
    // tick still counts as on time.
    tokio::time::sleep(Duration::from_millis(8000)).await;
    let srv = TestServer::start_with_binlog(dir.path()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut c = srv.connect().await;
    assert_eq!(ready_jobs(&mut c, "jobs").await, 1, "one catch-up fire");
    assert_eq!(ready_jobs(&mut c, "strict").await, 0, "catchup:0 skips it");
    let s = c.stats_schedule("app", "strict").await;
    assert_eq!(stat(&s, "total-fires"), "0");
}

#[tokio::test]
async fn a_fire_is_stamped_before_it_is_put() {
    let dir = tempfile::tempdir().unwrap();
    let srv = TestServer::start_with_binlog(dir.path()).await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin app", "SYNCING app").await;
    c.schedule_put("hourly", "jobs", &tick_in(1), "{}").await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;

    // Wait for the fire, then crash.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let s = c.stats_schedule("app", "hourly").await;
    assert_eq!(stat(&s, "total-fires"), "1", "{s}");
    drop(c);
    srv.kill().await;

    // The restart knows the tick was fired: it doesn't fire it again.
    let srv = TestServer::start_with_binlog(dir.path()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut c = srv.connect().await;
    let after = c.stats_schedule("app", "hourly").await;
    assert_eq!(stat(&after, "total-fires"), "0", "{after}");
    assert_eq!(stat(&after, "last-fire"), stat(&s, "last-fire"));
}

#[tokio::test]
async fn a_damaged_schedules_file_is_set_aside_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("schedules"), b"TSCH not really").unwrap();

    let srv = TestServer::start_with_binlog(dir.path()).await;
    let mut c = srv.connect().await;
    assert!(c.list("list-schedules").await.is_empty());
    assert!(dir.path().join("schedules.corrupt").exists());

    // And schedules work again from the next sync.
    c.ck("schedule-begin app", "SYNCING app").await;
    c.schedule_put("a", "jobs", "@daily", "{}").await;
    c.ck("schedule-commit", "SYNCED 1 0 0 0").await;
}

#[tokio::test]
async fn a_second_damaged_file_does_not_overwrite_the_first() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("schedules"), b"TSCH first damage").unwrap();
    let srv = TestServer::start_with_binlog(dir.path()).await;
    srv.connect().await;
    srv.kill().await;

    std::fs::write(dir.path().join("schedules"), b"TSCH second damage").unwrap();
    let srv = TestServer::start_with_binlog(dir.path()).await;
    srv.connect().await;

    let mut kept: Vec<Vec<u8>> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("schedules.corrupt")
        })
        .map(|p| std::fs::read(p).unwrap())
        .collect();
    kept.sort();
    assert_eq!(
        kept,
        [
            b"TSCH first damage".to_vec(),
            b"TSCH second damage".to_vec()
        ]
    );
}

#[tokio::test]
async fn a_schedules_file_from_a_newer_tuber_stops_startup() {
    let dir = tempfile::tempdir().unwrap();
    // A well-formed, checksummed file of format version 2. Not damage: good
    // schedules this binary can't read, which it mustn't throw away.
    let mut newer = b"TSCH".to_vec();
    newer.extend_from_slice(&2u32.to_le_bytes());
    newer.extend_from_slice(&[0; 4]);
    let crc = crc32fast::hash(&newer);
    newer.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(dir.path().join("schedules"), &newer).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let started = tokio::time::timeout(
        Duration::from_secs(2),
        tuber::server::run_with_listener(listener, 65535, Some(dir.path()), None),
    )
    .await;
    match started {
        Ok(Err(e)) => assert!(e.to_string().contains("version 2"), "{e}"),
        Ok(Ok(())) => panic!("server exited cleanly"),
        Err(_) => panic!("server started without the newer schedules"),
    }
    assert_eq!(std::fs::read(dir.path().join("schedules")).unwrap(), newer);
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

async fn scrape(port: u16) -> String {
    let ml = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mport = ml.local_addr().unwrap().port();
    tokio::spawn(tuber::metrics::serve_with_listener(
        ml,
        format!("127.0.0.1:{port}"),
    ));
    let mut s = TcpStream::connect(("127.0.0.1", mport)).await.unwrap();
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n")
        .await
        .unwrap();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await.unwrap();
    let resp = String::from_utf8(resp).unwrap();
    resp.split_once("\r\n\r\n").unwrap().1.to_string()
}

#[tokio::test]
async fn metrics_show_fires_by_result() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.ck("schedule-begin app", "SYNCING app").await;
    c.schedule_put("rollup", "maint", "idp:rollup every 1s", "{}")
        .await;
    c.schedule_put("nightly", "maint", "0 2 * * * Australia/Melbourne", "{}")
        .await;
    c.ck("schedule-commit", "SYNCED 2 0 0 0").await;
    c.ck("watch maint", "WATCHING 2").await;
    let (id, _) = reserve(&mut c, 3).await;
    c.ck(&format!("bury {id} 0"), "BURIED").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let body = scrape(srv.port).await;
    let sample = |prefix: &str| -> Option<f64> {
        body.lines()
            .find_map(|l| l.strip_prefix(prefix)?.strip_prefix(' '))
            .map(|v| v.parse().unwrap())
    };
    assert_eq!(sample("tuber_schedules"), Some(2.0), "{body}");
    let rollup = "owner=\"app\",name=\"rollup\"";
    assert_eq!(
        sample(&format!(
            "tuber_schedule_fires_total{{{rollup},result=\"inserted\"}}"
        )),
        Some(1.0),
        "{body}"
    );
    assert!(
        sample(&format!(
            "tuber_schedule_fires_total{{{rollup},result=\"deduped_buried\"}}"
        ))
        .unwrap()
            >= 1.0,
        "{body}"
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as f64;
    let last = sample(&format!(
        "tuber_schedule_last_fire_timestamp_seconds{{{rollup}}}"
    ))
    .unwrap();
    assert!((now - 3.0..=now).contains(&last), "{last} vs {now}");
    let nightly = "owner=\"app\",name=\"nightly\"";
    let next = sample(&format!(
        "tuber_schedule_next_fire_timestamp_seconds{{{nightly}}}"
    ))
    .unwrap();
    assert!((now..now + 86_400.0 + 3_600.0).contains(&next), "{next}");
    assert_eq!(
        sample(&format!(
            "tuber_schedule_last_fire_timestamp_seconds{{{nightly}}}"
        )),
        None,
        "never fired: no sample"
    );

    // Each family's samples sit together under its TYPE line.
    for family in [
        "tuber_schedule_fires_total",
        "tuber_schedule_last_fire_timestamp_seconds",
        "tuber_schedule_next_fire_timestamp_seconds",
    ] {
        let lines: Vec<usize> = body
            .lines()
            .enumerate()
            .filter(|(_, l)| l.starts_with(&format!("{family}{{")))
            .map(|(i, _)| i)
            .collect();
        assert!(
            lines.windows(2).all(|w| w[1] == w[0] + 1),
            "{family} samples not contiguous:\n{body}"
        );
    }
}
