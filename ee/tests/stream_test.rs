//! End-to-end: a real `Wal` + `BodyStore` on disk, `StreamSender` attached
//! as the observer, frames drained, serialized for the wire, decoded back
//! — the full primary-side path a replica's bytes take.

use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp;
use tuber::body_store::{BodyStore, BodyStoreError, DEFAULT_SEGMENT_SIZE};
use tuber::ee::replication::wire::{self, FLAG_BODY_MISSING, Frame};
use tuber::ee::replication::{StreamLost, StreamReceiver, TeeFrame, stream_channel};
use tuber::job::{BodyId, BodyRef, Job, JobState};
use tuber::wal::{Position, ScheduleRecord, StateChangeReason, Wal, WalConfig, WalRecord};

fn make_job(id: u64, body_id: BodyId) -> Job {
    let mut job = Job::new(
        id,
        100,
        Duration::ZERO,
        Duration::from_secs(30),
        Vec::new(),
        "repl-tube",
    );
    job.body = BodyRef::External(body_id, None);
    job
}

#[test]
fn tee_to_wire_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let toast = dir.path().join("toast");
    std::fs::create_dir_all(&toast).unwrap();
    let body_store = Arc::new(BodyStore::open(&toast, DEFAULT_SEGMENT_SIZE).unwrap());

    let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
    wal.set_body_store(Arc::clone(&body_store));
    let (sender, receiver) = stream_channel(64);
    wal.set_observer(Box::new(sender));

    // Attach context arrives first, with the body store and the boundary.
    let (shipping_store, attach_position) = match receiver.recv().unwrap() {
        TeeFrame::Attached {
            body_store,
            dir: ctx_dir,
            attach_position,
        } => {
            assert_eq!(ctx_dir, dir.path());
            (
                body_store.expect("body store must ride the attach context"),
                attach_position,
            )
        }
        _ => panic!("first frame must be Attached"),
    };
    assert_eq!(attach_position, wal.end_position());

    // One put (external body) and one state change, then a sync.
    let body_bytes = b"replicate me".to_vec();
    let (id, crc) = body_store.write_body_with_crc(&body_bytes).unwrap();
    let mut job = make_job(1, id);
    job.body = BodyRef::External(id, Some(crc));
    wal.write_put(&mut job).unwrap();
    wal.write_state_change(
        &mut job,
        Some(JobState::Buried),
        50,
        Duration::ZERO,
        0,
        StateChangeReason::Bury,
    )
    .unwrap();
    wal.sync().unwrap();

    // Put: ship it — the wire record must carry the body inline.
    let (pos, record) = match next_past_marks(&receiver) {
        TeeFrame::Record { pos, record } => (pos, record),
        other => panic!("expected the put record, got {other:?}"),
    };
    assert!(pos >= attach_position);
    let (bytes, flags) = wire::serialize_for_wire(&record, Some(&shipping_store)).unwrap();
    assert_eq!(flags, 0);
    let encoded = wire::encode_record(pos, &bytes, flags);
    let (frame, consumed) = wire::decode(&encoded).unwrap().expect("complete frame");
    assert_eq!(consumed, encoded.len());
    match frame {
        Frame::Record {
            pos: decoded_pos,
            record: WalRecord::FullJob(decoded),
            flags: 0,
        } => {
            assert_eq!(decoded_pos, pos);
            assert_eq!(decoded.id, 1);
            assert_eq!(decoded.tube_name.as_ref() as &str, "repl-tube");
            assert_eq!(
                decoded.body.as_inline_bytes(),
                Some(body_bytes.as_slice()),
                "external body must cross the wire inline"
            );
        }
        other => panic!("expected an inline-body FullJob frame, got {other:?}"),
    }

    // State change: fields survive the roundtrip verbatim.
    let (sc_pos, sc_record) = match next_past_marks(&receiver) {
        TeeFrame::Record { pos, record } => (pos, record),
        other => panic!("expected the state-change record, got {other:?}"),
    };
    let (bytes, flags) = wire::serialize_for_wire(&sc_record, Some(&shipping_store)).unwrap();
    let encoded = wire::encode_record(sc_pos, &bytes, flags);
    let (frame, _) = wire::decode(&encoded).unwrap().unwrap();
    match frame {
        Frame::Record {
            record:
                WalRecord::StateChange {
                    job_id,
                    new_state,
                    new_priority,
                    reason,
                    ..
                },
            ..
        } => {
            assert_eq!(job_id, 1);
            assert_eq!(new_state, Some(JobState::Buried));
            assert_eq!(new_priority, 50);
            assert_eq!(reason, StateChangeReason::Bury);
        }
        other => panic!("expected a StateChange frame, got {other:?}"),
    }

    // The sync's durable frontier covers both records.
    match next_past_marks(&receiver) {
        TeeFrame::Durable { end } => {
            assert!(end > sc_pos);
            assert_eq!(end, wal.end_position());
        }
        other => panic!("expected the durable frontier, got {other:?}"),
    }
}

/// The next tee frame that isn't an IdMark or ToastMark. A segment opens
/// with both, and a sync that follows a body write adds a ToastMark.
fn next_past_marks(receiver: &StreamReceiver) -> TeeFrame {
    loop {
        match receiver.recv().unwrap() {
            TeeFrame::Record {
                record: WalRecord::IdMark { .. } | WalRecord::ToastMark(_),
                ..
            } => continue,
            frame => return frame,
        }
    }
}

#[test]
fn missing_body_ships_flagged_placeholder() {
    let dir = tempfile::tempdir().unwrap();
    let body_store = Arc::new(BodyStore::open(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
    let id = body_store.write_body(b"soon gone").unwrap();
    let record = WalRecord::FullJob(Box::new(make_job(7, id)));

    // Job deleted between tee and ship: its body is dereferenced.
    body_store.delete(id);

    let (bytes, flags) = wire::serialize_for_wire(&record, Some(&body_store)).unwrap();
    assert_eq!(flags, FLAG_BODY_MISSING);
    let encoded = wire::encode_record("1:12".parse().unwrap(), &bytes, flags);
    match wire::decode(&encoded).unwrap().unwrap().0 {
        Frame::Record {
            record: WalRecord::FullJob(job),
            flags,
            ..
        } => {
            assert_eq!(flags, FLAG_BODY_MISSING);
            assert_eq!(job.body.as_inline_bytes(), Some(&[][..]));
        }
        other => panic!("expected flagged FullJob, got {other:?}"),
    }
}

/// Ship one teed record through the wire and decode it back.
fn through_the_wire(frame: TeeFrame) -> WalRecord {
    let TeeFrame::Record { pos, record } = frame else {
        panic!("expected a record, got {frame:?}");
    };
    let (bytes, flags) = wire::serialize_for_wire(&record, None).unwrap();
    match wire::decode(&wire::encode_record(pos, &bytes, flags))
        .unwrap()
        .unwrap()
        .0
    {
        Frame::Record { record, .. } => record,
        other => panic!("expected a record frame, got {other:?}"),
    }
}

#[test]
fn schedule_records_and_tombstones_cross_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
    let (sender, receiver) = stream_channel(64);
    wal.set_observer(Box::new(sender));
    assert!(matches!(
        receiver.recv().unwrap(),
        TeeFrame::Attached { .. }
    ));

    let synced_at = Timestamp::from_second(1_760_000_000).unwrap();
    let fired = Timestamp::from_second(1_760_000_060).unwrap();
    let entry = ScheduleRecord {
        line: "nightly 0 0 30 * * * * *".to_owned(),
        body: b"run it".to_vec(),
        synced_at,
        last_fire: None,
    };
    wal.write_schedule_set("app", std::slice::from_ref(&entry))
        .unwrap();
    wal.write_schedule_fired("app", "nightly", Some(fired))
        .unwrap();
    wal.write_schedule_set::<Vec<u8>>("app", &[]).unwrap();
    wal.write_tombstone("tube", "key-1", 9, 1_760_000_100)
        .unwrap();

    match through_the_wire(next_past_marks(&receiver)) {
        WalRecord::ScheduleSet { owner, entries } => {
            assert_eq!(owner, "app");
            assert_eq!(entries, vec![entry]);
        }
        other => panic!("expected the set, got {other:?}"),
    }
    match through_the_wire(next_past_marks(&receiver)) {
        WalRecord::ScheduleFired {
            owner,
            name,
            last_fire,
        } => assert_eq!(
            (owner.as_str(), name.as_str(), last_fire),
            ("app", "nightly", Some(fired))
        ),
        other => panic!("expected the fire, got {other:?}"),
    }
    // An owner's removal is an empty set, and must reach the replica too.
    match through_the_wire(next_past_marks(&receiver)) {
        WalRecord::ScheduleSet { owner, entries } => {
            assert_eq!(owner, "app");
            assert!(entries.is_empty());
        }
        other => panic!("expected the empty set, got {other:?}"),
    }
    match through_the_wire(next_past_marks(&receiver)) {
        WalRecord::Tombstone {
            job_id,
            tube,
            key,
            expires_epoch_secs,
        } => assert_eq!(
            (job_id, tube.as_str(), key.as_str(), expires_epoch_secs),
            (9, "tube", "key-1", 1_760_000_100)
        ),
        other => panic!("expected the tombstone, got {other:?}"),
    }
}

#[test]
fn a_body_that_fails_its_check_is_not_shipped_as_missing() {
    let dir = tempfile::tempdir().unwrap();
    let body_store = Arc::new(BodyStore::open(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
    let (id, crc) = body_store
        .write_body_with_crc(b"the primary's bytes")
        .unwrap();
    // The WAL recorded another CRC: TOAST holds other bytes under this id.
    let mut job = make_job(7, id);
    job.body = BodyRef::External(id, Some(crc ^ 1));
    let record = WalRecord::FullJob(Box::new(job));

    // Missing means the job was deleted and a delete follows. A damaged
    // body is still live on the primary; shipping it as missing hands the
    // replica an empty body it would serve after promotion.
    let err = wire::serialize_for_wire(&record, Some(&body_store))
        .expect_err("damaged body shipped as missing");
    assert_eq!(err.job_id, 7);
    assert!(matches!(
        err.error,
        Some(BodyStoreError::NotTheWalsBody { .. })
    ));
}

#[test]
fn overflow_poisons_stream_after_buffered_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
    // Capacity 1: the Attached frame fills the channel; the first record
    // overflows it and poisons the stream.
    let (sender, receiver) = stream_channel(1);
    wal.set_observer(Box::new(sender));

    let mut job = make_job(1, BodyId(1));
    wal.write_put(&mut job).unwrap();
    let mut job2 = make_job(2, BodyId(2));
    wal.write_put(&mut job2).unwrap();

    // The buffered prefix still drains...
    assert!(matches!(
        receiver.recv().unwrap(),
        TeeFrame::Attached { .. }
    ));
    // ...then the poison surfaces, and stays.
    assert_eq!(receiver.recv().unwrap_err(), StreamLost::Lagged);
    assert_eq!(receiver.try_next().unwrap_err(), StreamLost::Lagged);
}

#[test]
fn decode_partial_frame_asks_for_more() {
    let pos: Position = "3:99".parse().unwrap();
    let encoded = wire::encode_durable(pos);
    for cut in 0..encoded.len() {
        assert!(
            wire::decode(&encoded[..cut]).unwrap().is_none(),
            "truncated frame at {cut} bytes must read as incomplete"
        );
    }
    match wire::decode(&encoded).unwrap().unwrap().0 {
        Frame::Durable { end } => assert_eq!(end, pos),
        other => panic!("expected Durable, got {other:?}"),
    }
}
