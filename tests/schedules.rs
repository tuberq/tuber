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
