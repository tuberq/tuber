//! Catch-up + handoff, end to end over real TCP: a `Shipper` serving a
//! WAL on disk, a `Follower` connecting with a position, records crossing
//! exactly once in order regardless of which phase carried them.

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use tuber::body_store::{BodyStore, DEFAULT_SEGMENT_SIZE};
use tuber::ee::replication::{Follower, FollowerEvent, ServeEnd, Shipper, stream_channel};
use tuber::job::{BodyRef, Job};
use tuber::wal::{Position, Wal, WalConfig, WalRecord};

/// A WAL + body store on disk with the tee attached, plus a serving loop
/// on a loopback listener that keeps accepting replicas (like the real
/// binary's listener thread).
struct Primary {
    wal: Wal,
    body_store: Arc<BodyStore>,
    port: u16,
    _dir: tempfile::TempDir,
}

fn start_primary() -> Primary {
    let dir = tempfile::tempdir().unwrap();
    let toast = dir.path().join("toast");
    std::fs::create_dir_all(&toast).unwrap();
    let body_store = Arc::new(BodyStore::open(&toast, DEFAULT_SEGMENT_SIZE).unwrap());

    let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
    wal.set_body_store(Arc::clone(&body_store));
    let (sender, receiver) = stream_channel(1024);
    wal.set_observer(Box::new(sender));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let shipper = Shipper::from_tee(receiver).unwrap();
        loop {
            let (mut conn, _) = match listener.accept() {
                Ok(c) => c,
                Err(_) => return,
            };
            match shipper.serve(&mut conn) {
                Ok(ServeEnd::ReplicaDropped(_)) => continue,
                other => panic!("serve ended unexpectedly: {other:?}"),
            }
        }
    });

    Primary {
        wal,
        body_store,
        port,
        _dir: dir,
    }
}

fn put(primary: &mut Primary, id: u64, body: &[u8]) -> Position {
    let (body_id, crc) = primary.body_store.write_body_with_crc(body).unwrap();
    let mut job = Job::new(
        id,
        100,
        Duration::ZERO,
        Duration::from_secs(30),
        Vec::new(),
        "catchup-tube",
    );
    job.body = BodyRef::External(body_id, Some(crc));
    let pos = primary.wal.end_position();
    primary.wal.write_put(&mut job).unwrap();
    pos
}

fn connect(primary: &Primary, from: Position) -> Follower<TcpStream> {
    let conn = TcpStream::connect(("127.0.0.1", primary.port)).unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    Follower::start(conn, from).unwrap()
}

/// Collect the next `n` records (skipping Durable frames), returning
/// `(pos, job_id, inline_body)` triples.
fn take_records(follower: &mut Follower<TcpStream>, n: usize) -> Vec<(Position, u64, Vec<u8>)> {
    let mut out = Vec::new();
    while out.len() < n {
        match follower.next_event().unwrap() {
            FollowerEvent::Record {
                pos,
                record: WalRecord::FullJob(job),
                ..
            } => {
                let body = job
                    .body
                    .as_inline_bytes()
                    .expect("wire records carry bodies inline")
                    .to_vec();
                out.push((pos, job.id, body));
            }
            // Segment-opening and TOAST marks occupy positions like any
            // record; these tests only count jobs.
            FollowerEvent::Record {
                record: WalRecord::IdMark { .. } | WalRecord::ToastMark(_),
                ..
            } => continue,
            FollowerEvent::Record { record, .. } => panic!("unexpected record {record:?}"),
            FollowerEvent::Durable { .. } => continue,
        }
    }
    out
}

#[test]
fn bootstrap_streams_history_then_live() {
    let mut primary = start_primary();

    // History: two durable records before any replica exists.
    put(&mut primary, 1, b"body-one");
    put(&mut primary, 2, b"body-two");
    primary.wal.sync().unwrap();

    // Bootstrap from ZERO: the catch-up scan must deliver the history...
    let mut follower = connect(&primary, Position::ZERO);
    let records = take_records(&mut follower, 2);
    assert_eq!(records[0].1, 1);
    assert_eq!(records[0].2, b"body-one");
    assert_eq!(records[1].1, 2);
    assert_eq!(records[1].2, b"body-two");
    assert!(records[0].0 < records[1].0, "stream must ascend");

    // ...and the same connection then carries live writes.
    put(&mut primary, 3, b"body-three");
    primary.wal.sync().unwrap();
    let live = take_records(&mut follower, 1);
    assert_eq!(live[0].1, 3);
    assert_eq!(live[0].2, b"body-three");
    assert!(live[0].0 > records[1].0);
}

#[test]
fn reconnect_resumes_from_cursor() {
    let mut primary = start_primary();

    put(&mut primary, 1, b"one");
    put(&mut primary, 2, b"two");
    primary.wal.sync().unwrap();

    // First connection bootstraps, learns its cursor, then drops.
    let cursor = {
        let mut follower = connect(&primary, Position::ZERO);
        let records = take_records(&mut follower, 2);
        assert_eq!(records[1].1, 2);
        follower.last_applied()
    };

    // Unstick the serve loop (it notices the drop on its next write).
    put(&mut primary, 3, b"three");
    primary.wal.sync().unwrap();

    // Reconnect with the cursor: only records above it may arrive.
    let mut follower = connect(&primary, cursor);
    put(&mut primary, 4, b"four");
    primary.wal.sync().unwrap();
    let records = take_records(&mut follower, 2);
    assert_eq!(
        records.iter().map(|r| r.1).collect::<Vec<_>>(),
        vec![3, 4],
        "already-applied records must not be re-delivered"
    );
    assert!(records.iter().all(|r| r.0 > cursor));
}

#[test]
fn unflushed_tail_arrives_via_tee_not_scan() {
    let mut primary = start_primary();

    // One durable record, then one that exists only in the writer's
    // buffer and the tee — invisible to any file scan.
    put(&mut primary, 1, b"flushed");
    primary.wal.sync().unwrap();
    put(&mut primary, 2, b"buffered-tail");

    // A bootstrapping replica must still see both, exactly once, in
    // order: record 1 via the scan, record 2 via the buffered tee.
    let mut follower = connect(&primary, Position::ZERO);
    let records = take_records(&mut follower, 2);
    assert_eq!(
        records.iter().map(|r| r.1).collect::<Vec<_>>(),
        vec![1, 2],
        "no record may be lost or duplicated across the scan/tee handoff"
    );
    assert_eq!(records[1].2, b"buffered-tail");
}
