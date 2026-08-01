// Write-ahead log for persistence.
//
// When enabled via `-b <dir>`, all job mutations are logged to append-only files.
// On restart, the WAL is replayed to restore state.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jiff::Timestamp;

use crate::body_store::BodyStore;
use crate::body_store::DurablePoint;
use crate::job::{BodyId, BodyRef, Job, JobState};

// --- Constants ---

const WAL_MAGIC: &[u8; 4] = b"TWAL";
/// Current WAL format version.
///
/// - v3: original format.
/// - v4: state-change records gained a `reason` byte.
/// - v5: `FullJob` records carry a `BodyId` instead of inline body bytes.
///   Bodies live in the external body store ("TOAST").
/// - v6: `FullJob` body field is a per-record `body_kind` discriminant
///   followed by either inline bytes (kind=0) or a `BodyId` (kind=1).
///   Small bodies stay inline in the WAL record; only larger ones
///   are pushed to the body store. The discriminant decouples the
///   on-disk format from the runtime threshold choice — changing
///   `HEAP_INLINE_MAX` doesn't require a format bump.
/// - v7: state-change records gained a `change_epoch_secs` (wall-clock
///   second the change was applied). A release/kick-to-delayed job
///   replays with its remaining delay instead of resetting to the
///   full delay on every restart. Pre-v7 records replay full-delay.
/// - v8: two new record types. A tombstone record carries an `idp:`
///   cooldown whole (tube, key, job id, expiry), so it no longer depends
///   on the job's put record surviving, and holds a reference on its
///   segment until it expires; compaction carries it forward. An id mark
///   opens every segment with the highest job id assigned so far, so ids
///   never go backwards when older segments are collected.
/// - v9: a TOAST mark record names how far the body store was durable at
///   each fsync (its recovery point), and a `FullJob` referencing a TOAST
///   body also carries the body's CRC32 (body kind 2). Startup uses the
///   first to tell a torn TOAST tail from corruption, and the second to
///   check TOAST holds the body the WAL wrote. See
///   `docs/toast-recovery-point.md`. Recurring schedules moved into the
///   WAL too, from two files beside it: a schedule set record holds one
///   owner's entries, and a schedule fired record one entry's last fire.
pub const WAL_VERSION: u32 = 9;
const WAL_VERSION_MIN: u32 = 3; // Oldest version we can still read

// v6 body_kind discriminant values
const BODY_KIND_INLINE: u8 = 0x00;
const BODY_KIND_EXTERNAL: u8 = 0x01;
const BODY_KIND_EXTERNAL_CRC: u8 = 0x02; // v9
const HEADER_SIZE: usize = 12; // magic(4) + version(4) + flags(4)
const RECORD_TYPE_FULL_JOB: u8 = 0x01;
const RECORD_TYPE_STATE_CHANGE: u8 = 0x02;
const RECORD_TYPE_TOMBSTONE: u8 = 0x03; // v8
const RECORD_TYPE_ID_MARK: u8 = 0x04; // v8
const RECORD_TYPE_TOAST_MARK: u8 = 0x05; // v9
const RECORD_TYPE_SCHEDULE_SET: u8 = 0x06; // v9
const RECORD_TYPE_SCHEDULE_FIRED: u8 = 0x07; // v9
/// First version with tombstone and id-mark records.
const WAL_VERSION_TOMBSTONES: u32 = 8;
/// First version with TOAST marks and body CRCs.
const WAL_VERSION_TOAST_MARKS: u32 = 9;
/// First version with schedule records.
const WAL_VERSION_SCHEDULES: u32 = 9;
/// Size of an id-mark record: type(1) + high_water(8) + payload_len(4) + crc(4).
#[cfg(test)]
const ID_MARK_RECORD_SIZE: usize = 1 + 8 + 4 + 4;
/// Size of a TOAST mark record: type(1) + seq(8) + payload_len(4) +
/// offset(8) + crc(4).
#[cfg(test)]
const TOAST_MARK_RECORD_SIZE: usize = 1 + 8 + 4 + 8 + 4;
const STATE_CHANGE_PAYLOAD_LEN_V3: u32 = 21;
const STATE_CHANGE_PAYLOAD_LEN_V4: u32 = 22; // v4: added reason byte
const STATE_CHANGE_PAYLOAD_LEN_V7: u32 = 30; // v7: added change_epoch_secs (u64)
/// Payload length written by the current serializer (v7).
const STATE_CHANGE_PAYLOAD_LEN: u32 = STATE_CHANGE_PAYLOAD_LEN_V7;
/// Size of a state change record: type(1) + job_id(8) + payload_len(4) + payload + crc(4)
const STATE_CHANGE_RECORD_SIZE: usize = 1 + 8 + 4 + STATE_CHANGE_PAYLOAD_LEN as usize + 4;
/// Default cap on a single WAL segment before rotation (10 MiB).
pub const DEFAULT_MAX_FILE_SIZE: usize = 10 * 1024 * 1024;
const FILE_PREFIX: &str = "binlog.";
/// Userland write buffer capacity per WAL file. Amortises syscall overhead.
/// Durability is not affected: every path that calls `sync_all` first calls `flush`.
const BUF_CAPACITY: usize = 64 * 1024;
/// Default interval between fsyncs. `Duration::ZERO` means fsync on every write.
pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(100);

// --- State encoding ---

fn state_to_u8(state: JobState) -> u8 {
    match state {
        JobState::Ready => 0,
        JobState::Reserved => 1,
        JobState::Delayed => 2,
        JobState::Buried => 3,
    }
}

const STATE_DELETED: u8 = 0xFF;

// --- State change reason encoding ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateChangeReason {
    None = 0,
    Reserve = 1,
    Release = 2,
    Bury = 3,
    Kick = 4,
    Timeout = 5,
}

fn reason_to_u8(r: StateChangeReason) -> u8 {
    r as u8
}

fn u8_to_reason(v: u8) -> StateChangeReason {
    match v {
        1 => StateChangeReason::Reserve,
        2 => StateChangeReason::Release,
        3 => StateChangeReason::Bury,
        4 => StateChangeReason::Kick,
        5 => StateChangeReason::Timeout,
        _ => StateChangeReason::None,
    }
}

fn u8_to_state(v: u8) -> Option<JobState> {
    match v {
        0 => Some(JobState::Ready),
        1 => Some(JobState::Reserved),
        2 => Some(JobState::Delayed),
        3 => Some(JobState::Buried),
        _ => None,
    }
}

// --- Serialization helpers ---

fn write_option_string(buf: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => buf.extend_from_slice(&0u16.to_le_bytes()),
        Some(s) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
}

fn read_option_string(data: &[u8], offset: &mut usize) -> Result<Option<String>, WalError> {
    if *offset + 2 > data.len() {
        return Err(WalError::Truncated);
    }
    let len = u16::from_le_bytes([data[*offset], data[*offset + 1]]) as usize;
    *offset += 2;
    if len == 0 {
        return Ok(None);
    }
    if *offset + len > data.len() {
        return Err(WalError::Truncated);
    }
    let s = String::from_utf8(data[*offset..*offset + len].to_vec())
        .map_err(|_| WalError::InvalidData)?;
    *offset += len;
    Ok(Some(s))
}

// --- Error type ---

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    BadMagic,
    BadVersion(u32),
    BadCrc,
    Truncated,
    InvalidData,
    UnknownRecordType(u8),
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {}", e),
            WalError::BadMagic => write!(f, "WAL bad magic"),
            WalError::BadVersion(v) => write!(f, "WAL bad version: {}", v),
            WalError::BadCrc => write!(f, "WAL CRC mismatch"),
            WalError::Truncated => write!(f, "WAL record truncated"),
            WalError::InvalidData => write!(f, "WAL invalid data"),
            WalError::UnknownRecordType(t) => write!(f, "WAL unknown record type: {}", t),
        }
    }
}

// --- WAL record types ---

/// The format versions of a WAL directory's segments (`Wal::format_versions`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatVersions {
    /// The lowest version of any segment.
    pub min: u32,
    /// The newest segment's version: what the last server to start wrote.
    pub newest: u32,
}

/// A record that holds a reference on its segment until it is released:
/// the segment's seq and the record's size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRef {
    pub seq: u64,
    pub len: u32,
}

/// An idempotency tombstone recovered from WAL replay.
#[derive(Debug)]
pub struct IdpTombstone {
    pub tube_name: std::sync::Arc<str>,
    pub key: String,
    pub job_id: u64,
    pub expires_at: SystemTime,
    /// Its v8 tombstone record. `None` for a cooldown rebuilt from a pre-v8
    /// delete record, which holds no reference and must be rewritten.
    pub record: Option<RecordRef>,
}

/// A recurring schedule entry as the WAL holds it. `line` is its
/// `schedule-put` arguments, parsed back through the protocol parser on
/// restore, so a restored entry passes exactly the checks a synced one did.
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleRecord<B = Vec<u8>> {
    pub line: String,
    /// Owned when replayed; borrowed from the entry when written.
    pub body: B,
    /// When its definition took effect.
    pub synced_at: Timestamp,
    pub last_fire: Option<Timestamp>,
}

/// The schedules [`Wal::replay`] found ([`Wal::take_replayed_schedules`]).
#[derive(Debug, Default)]
pub struct ReplayedSchedules {
    /// Each owner's latest set, and the record holding it.
    pub sets: HashMap<String, (Vec<ScheduleRecord>, RecordRef)>,
    /// Last fires written since their owner's set, by (owner, name), and
    /// the records holding them.
    pub stamps: HashMap<(String, String), (Option<Timestamp>, RecordRef)>,
    /// Whether any schedule record was read, an owner's removal included:
    /// the WAL holds the schedules, not files an earlier tuber kept.
    pub seen: bool,
}

/// What [`Wal::replay`] returns: `(jobs, next_job_id, idempotency
/// tombstones, orphaned body ids, buried order)`.
pub type ReplayOutcome = (
    HashMap<u64, Box<Job>>,
    u64,
    Vec<IdpTombstone>,
    Vec<BodyId>,
    Vec<u64>,
);

#[derive(Debug)]
pub enum WalRecord {
    FullJob(Box<Job>),
    StateChange {
        job_id: u64,
        new_state: Option<JobState>, // None = Deleted
        new_priority: u32,
        new_delay_nanos: u64,
        expiry_epoch_secs: u64, // For idempotency tombstones (0 = no tombstone)
        reason: StateChangeReason,
        // Wall-clock second at which this change was applied (v7+). Lets a
        // release/kick-to-delayed job replay with its *remaining* delay rather
        // than resetting to the full delay on every restart. 0 = absent
        // (pre-v7 record) → fall back to full delay.
        change_epoch_secs: u64,
    },
    /// An `idp:` cooldown (v8+): `key` in `tube` stays taken by `job_id`
    /// until `expires_epoch_secs`. Supersedes any earlier record for the
    /// same tube and key; `expires_epoch_secs == 0` clears the cooldown.
    Tombstone {
        job_id: u64,
        tube: String,
        key: String,
        expires_epoch_secs: u64,
    },
    /// The highest job id assigned when its segment was created (v8+).
    IdMark {
        high_water: u64,
    },
    /// How far the body store was durable when this record was written
    /// (v9+): the TOAST recovery point.
    ToastMark(DurablePoint),
    /// One owner's recurring schedules (v9+), replacing its earlier set
    /// and every last fire written since. Empty once the owner has none.
    ScheduleSet {
        owner: String,
        entries: Vec<ScheduleRecord>,
    },
    /// An entry's last fire (v9+), superseding the one in its owner's set
    /// and any earlier record. `None` when a refused fire is put back on
    /// an entry that hadn't fired.
    ScheduleFired {
        owner: String,
        name: String,
        last_fire: Option<Timestamp>,
    },
}

// --- Observation ---

/// Byte-exact location of a record in the WAL: the segment's sequence
/// number and the offset of the record's first byte within that segment
/// (header included, so the first record of a segment sits at
/// `HEADER_SIZE`). Lexicographic order is log order: `a < b` means `a` was
/// appended before `b`. Positions are stable across restarts — they name
/// bytes in files, not anything in memory — which makes them usable as a
/// resume cursor by external readers (backups, replication, audit tails).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub seg: u64,
    pub offset: u64,
}

impl Position {
    /// Sorts below every real position (segments start at 1, records at
    /// `HEADER_SIZE`), so a reader opened here yields the whole log. This
    /// is the bootstrap cursor: the WAL is its own base backup.
    pub const ZERO: Position = Position { seg: 0, offset: 0 };
}

/// A WAL record at the moment of its append, in logical (pre-serialization)
/// form. Field-for-field identical to what `deserialize_record` yields for
/// the same bytes, so a consumer sees one record shape whether it taps the
/// live write path or scans segment files.
#[derive(Debug)]
pub enum WalEvent<'a> {
    /// A FullJob record was appended. Covers both client puts and
    /// compaction migrations (a migration is a rewrite of a live job into
    /// the tail; replay semantics treat it as a replacement, and observers
    /// must too). For `BodyRef::External` bodies only the reference is in
    /// the job — the bytes live in the body store.
    Put { job: &'a Job },
    /// A StateChange record was appended. Fields are exactly the record's
    /// payload; `new_state: None` is a delete.
    StateChange {
        job_id: u64,
        new_state: Option<JobState>,
        new_priority: u32,
        new_delay_nanos: u64,
        expiry_epoch_secs: u64,
        reason: StateChangeReason,
        change_epoch_secs: u64,
    },
    /// A Tombstone record was appended: an `idp:` cooldown set, or cleared
    /// when `expires_epoch_secs == 0`.
    Tombstone {
        job_id: u64,
        tube: &'a str,
        key: &'a str,
        expires_epoch_secs: u64,
    },
    /// An IdMark record was appended — the first record of every segment.
    IdMark { high_water: u64 },
    /// A ToastMark record was appended: how far this server's body store
    /// was durable. Names local TOAST bytes, so it means nothing to a peer;
    /// reported because it occupies a position like any other record.
    ToastMark(DurablePoint),
    /// A ScheduleSet record was appended: `owner`'s recurring schedules,
    /// replacing its earlier set and every last fire written since. No
    /// entries once the owner has none.
    ScheduleSet {
        owner: &'a str,
        entries: ScheduleEntries<'a>,
    },
    /// A ScheduleFired record was appended: the last fire of `owner`'s
    /// entry `name`, superseding the one in its set and any earlier record.
    ScheduleFired {
        owner: &'a str,
        name: &'a str,
        last_fire: Option<Timestamp>,
    },
}

/// The entries of a [`WalEvent::ScheduleSet`], in record order. Borrowed
/// either way: from the set being written, whose bodies are borrowed from
/// the server's entries, or from a [`WalRecord::ScheduleSet`] read back,
/// which owns them. The two hold their bodies differently, hence the view.
#[derive(Clone, Copy)]
pub struct ScheduleEntries<'a>(&'a dyn ScheduleEntryList);

impl<'a> ScheduleEntries<'a> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = ScheduleEntry<'a>> + 'a {
        let list = self.0;
        (0..list.len()).map(move |i| list.entry(i))
    }
}

impl std::fmt::Debug for ScheduleEntries<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// One entry of a [`WalEvent::ScheduleSet`]: a [`ScheduleRecord`],
/// borrowed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScheduleEntry<'a> {
    pub line: &'a str,
    pub body: &'a [u8],
    pub synced_at: Timestamp,
    pub last_fire: Option<Timestamp>,
}

impl<'a, B: AsRef<[u8]>> From<&'a ScheduleRecord<B>> for ScheduleEntry<'a> {
    fn from(r: &'a ScheduleRecord<B>) -> Self {
        ScheduleEntry {
            line: &r.line,
            body: r.body.as_ref(),
            synced_at: r.synced_at,
            last_fire: r.last_fire,
        }
    }
}

/// What [`ScheduleEntries`] reads through: a list of schedule records,
/// whatever holds their bodies. On the sized holders, since a slice can't
/// be made a trait object itself.
trait ScheduleEntryList {
    fn len(&self) -> usize;
    fn entry(&self, i: usize) -> ScheduleEntry<'_>;
}

impl<B: AsRef<[u8]>> ScheduleEntryList for &[ScheduleRecord<B>] {
    fn len(&self) -> usize {
        <[_]>::len(self)
    }

    fn entry(&self, i: usize) -> ScheduleEntry<'_> {
        (&self[i]).into()
    }
}

impl<B: AsRef<[u8]>> ScheduleEntryList for Vec<ScheduleRecord<B>> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn entry(&self, i: usize) -> ScheduleEntry<'_> {
        (&self[i]).into()
    }
}

/// Tap into the WAL write path. `on_record` fires synchronously for every
/// record appended — puts, state changes, tombstones, and the marks a
/// segment opens with or a sync appends — after the record's bytes have
/// been appended to the segment buffer and its position assigned — but
/// before any fsync, so an observed record is *not yet durable*. A consumer that
/// must not run ahead of durability should buffer until `on_sync` reports
/// a covering position.
///
/// Implementations must be fast and infallible: this runs on the engine's
/// hot path, and there is no way to fail the write from here. Anything
/// slow (IO, network) belongs behind a channel — and if that channel is
/// bounded, the observer must drop or disconnect its consumer rather than
/// block.
pub trait WalObserver: Send {
    fn on_record(&mut self, pos: Position, event: WalEvent<'_>);

    /// Every record at a position strictly below `end` is durable on disk.
    /// `end` is the position one-past the last synced byte (i.e. where the
    /// *next* record will land), not the position of a record. Default:
    /// ignore.
    fn on_sync(&mut self, end: Position) {
        let _ = end;
    }
}

// --- Serialization ---

pub fn serialize_full_job(job: &Job) -> Vec<u8> {
    let inline_len = job.body.as_inline_bytes().map_or(0, <[u8]>::len);
    let mut p = Payload::with_capacity(128 + job.tube_name.len() + inline_len);
    let payload = &mut p.0;

    // priority, delay_nanos, ttr_nanos, created_at_epoch_secs
    payload.extend_from_slice(&job.priority.to_le_bytes());
    payload.extend_from_slice(&job.delay.as_nanos().min(u64::MAX as u128).to_le_bytes()[..8]);
    payload.extend_from_slice(&job.ttr.as_nanos().min(u64::MAX as u128).to_le_bytes()[..8]);
    payload.extend_from_slice(&job.created_at_epoch.to_le_bytes());

    // state
    payload.push(state_to_u8(job.state));

    // counters
    payload.extend_from_slice(&job.reserve_ct.to_le_bytes());
    payload.extend_from_slice(&job.timeout_ct.to_le_bytes());
    payload.extend_from_slice(&job.release_ct.to_le_bytes());
    payload.extend_from_slice(&job.bury_ct.to_le_bytes());
    payload.extend_from_slice(&job.kick_ct.to_le_bytes());

    // tube_name
    let tn = job.tube_name.as_bytes();
    payload.extend_from_slice(&(tn.len() as u16).to_le_bytes());
    payload.extend_from_slice(tn);

    // extension fields (grouped: key + its associated value)
    // idempotency_key + ttl
    write_option_string(payload, job.idempotency_key().map(|(k, _)| k.as_str()));
    payload.extend_from_slice(
        &job.idempotency_key()
            .map_or(0u32, |(_, ttl)| *ttl)
            .to_le_bytes(),
    );

    // group, after_group
    write_option_string(payload, job.group().map(String::as_str));
    write_option_string(payload, job.after_group().map(String::as_str));

    // concurrency_key + limit
    write_option_string(payload, job.concurrency_key().map(|(k, _)| k.as_str()));
    payload.extend_from_slice(
        &job.concurrency_key()
            .map_or(0u32, |(_, l)| *l)
            .to_le_bytes(),
    );

    // body — v6: `body_kind` discriminant + variant payload.
    //   inline       → kind=0x00, then `body_len (u32 LE)` then `len` bytes.
    //   external     → kind=0x01, then `body_id (u64 LE)`.
    //   external+crc → kind=0x02, then `body_id (u64 LE)`, `crc32 (u32 LE)` (v9).
    if let Some(bytes) = job.body.as_inline_bytes() {
        payload.push(BODY_KIND_INLINE);
        payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(bytes);
    } else {
        let BodyRef::External(id, crc) = &job.body else {
            unreachable!("as_inline_bytes returned None ⇒ External");
        };
        match crc {
            Some(crc) => {
                payload.push(BODY_KIND_EXTERNAL_CRC);
                payload.extend_from_slice(&id.0.to_le_bytes());
                payload.extend_from_slice(&crc.to_le_bytes());
            }
            None => {
                payload.push(BODY_KIND_EXTERNAL);
                payload.extend_from_slice(&id.0.to_le_bytes());
            }
        }
    }

    p.frame(RECORD_TYPE_FULL_JOB, job.id)
}

pub fn serialize_state_change(
    job_id: u64,
    state: Option<JobState>,
    priority: u32,
    delay_nanos: u64,
    expiry_epoch_secs: u64,
    reason: StateChangeReason,
    change_epoch_secs: u64,
) -> Vec<u8> {
    let mut record = Vec::with_capacity(STATE_CHANGE_RECORD_SIZE);
    record.push(RECORD_TYPE_STATE_CHANGE);
    record.extend_from_slice(&job_id.to_le_bytes());
    record.extend_from_slice(&STATE_CHANGE_PAYLOAD_LEN.to_le_bytes());

    // payload (v7): state + priority + delay_nanos + expiry_epoch_secs + reason
    //             + change_epoch_secs
    let state_byte = match state {
        Some(s) => state_to_u8(s),
        None => STATE_DELETED,
    };
    record.push(state_byte);
    record.extend_from_slice(&priority.to_le_bytes());
    record.extend_from_slice(&delay_nanos.to_le_bytes());
    record.extend_from_slice(&expiry_epoch_secs.to_le_bytes());
    record.push(reason_to_u8(reason));
    record.extend_from_slice(&change_epoch_secs.to_le_bytes());

    let crc = crc32fast::hash(&record);
    record.extend_from_slice(&crc.to_le_bytes());

    record
}

/// A v8 tombstone record. `expires_epoch_secs == 0` clears the cooldown.
pub fn serialize_tombstone(job_id: u64, tube: &str, key: &str, expires_epoch_secs: u64) -> Vec<u8> {
    let mut p = Payload::with_capacity(8 + 2 + tube.len() + 2 + key.len());
    p.0.extend_from_slice(&expires_epoch_secs.to_le_bytes());
    p.str16(tube);
    p.str16(key);
    p.frame(RECORD_TYPE_TOMBSTONE, job_id)
}

/// A v8 id mark, carrying `high_water` in the job-id field.
pub fn serialize_id_mark(high_water: u64) -> Vec<u8> {
    Payload::with_capacity(0).frame(RECORD_TYPE_ID_MARK, high_water)
}

/// A v9 TOAST mark, carrying the segment seq in the job-id field and the
/// offset as its payload.
pub fn serialize_toast_mark(point: DurablePoint) -> Vec<u8> {
    let mut p = Payload::with_capacity(8);
    p.0.extend_from_slice(&point.offset.to_le_bytes());
    p.frame(RECORD_TYPE_TOAST_MARK, point.seq)
}

/// A v9 schedule set record: `owner`'s entries, replacing its earlier set.
pub fn serialize_schedule_set<B: AsRef<[u8]>>(owner: &str, entries: &[ScheduleRecord<B>]) -> Vec<u8> {
    let mut p = Payload::with_capacity(schedule_set_size(owner, entries));
    p.bytes(owner.as_bytes());
    p.0.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        p.bytes(e.line.as_bytes());
        p.bytes(e.body.as_ref());
        p.time(e.synced_at);
        p.opt_time(e.last_fire);
    }
    p.frame(RECORD_TYPE_SCHEDULE_SET, 0)
}

/// The payload size of a schedule set record, at most.
fn schedule_set_size<B: AsRef<[u8]>>(owner: &str, entries: &[ScheduleRecord<B>]) -> usize {
    let entries: usize = entries
        .iter()
        .map(|e| schedule_entry_size(&e.line, e.body.as_ref().len()))
        .sum();
    4 + owner.len() + 4 + entries
}

/// What one entry adds to a schedule set record, at most: its line, its
/// body, its sync time and its last fire.
pub fn schedule_entry_size(line: &str, body_len: usize) -> usize {
    4 + line.len() + 4 + body_len + Payload::TIME + Payload::OPT_TIME
}

/// A v9 schedule fired record: the last fire of `owner`'s entry `name`.
pub fn serialize_schedule_fired(owner: &str, name: &str, last_fire: Option<Timestamp>) -> Vec<u8> {
    let mut p = Payload::with_capacity(4 + owner.len() + 4 + name.len() + Payload::OPT_TIME);
    p.bytes(owner.as_bytes());
    p.bytes(name.as_bytes());
    p.opt_time(last_fire);
    p.frame(RECORD_TYPE_SCHEDULE_FIRED, 0)
}

/// Builds a record: type(1) + job_id(8) + payload_len(4) + payload +
/// crc(4), the CRC covering everything before it. The payload is written
/// after room for the header [`Payload::frame`] fills in, so it is copied
/// once. Little-endian; bytes are `len:u32 + bytes` (`len:u16` for
/// [`Payload::str16`]), a time `seconds:i64 nanos:i32`, an optional time a
/// flag byte and then the time if the flag is 1.
struct Payload(Vec<u8>);

impl Payload {
    const HEADER: usize = 1 + 8 + 4;
    const TIME: usize = 8 + 4;
    const OPT_TIME: usize = 1 + Self::TIME;

    fn with_capacity(payload_len: usize) -> Payload {
        let mut record = Vec::with_capacity(Self::HEADER + payload_len + 4);
        record.resize(Self::HEADER, 0);
        Payload(record)
    }

    /// The framed record.
    fn frame(mut self, record_type: u8, job_id: u64) -> Vec<u8> {
        let payload_len = (self.0.len() - Self::HEADER) as u32;
        self.0[0] = record_type;
        self.0[1..9].copy_from_slice(&job_id.to_le_bytes());
        self.0[9..13].copy_from_slice(&payload_len.to_le_bytes());
        let crc = crc32fast::hash(&self.0);
        self.0.extend_from_slice(&crc.to_le_bytes());
        self.0
    }

    fn bytes(&mut self, b: &[u8]) {
        self.0.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.0.extend_from_slice(b);
    }

    fn str16(&mut self, s: &str) {
        self.0.extend_from_slice(&(s.len() as u16).to_le_bytes());
        self.0.extend_from_slice(s.as_bytes());
    }

    fn time(&mut self, t: Timestamp) {
        self.0.extend_from_slice(&t.as_second().to_le_bytes());
        self.0.extend_from_slice(&t.subsec_nanosecond().to_le_bytes());
    }

    fn opt_time(&mut self, t: Option<Timestamp>) {
        match t {
            None => self.0.push(0),
            Some(t) => {
                self.0.push(1);
                self.time(t);
            }
        }
    }
}

/// Reads a payload [`Payload`] built.
struct PayloadReader<'a>(&'a [u8]);

impl<'a> PayloadReader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], WalError> {
        if self.0.len() < n {
            return Err(WalError::InvalidData);
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn u32(&mut self) -> Result<u32, WalError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, WalError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn str16(&mut self) -> Result<String, WalError> {
        let len = u16::from_le_bytes(self.take(2)?.try_into().unwrap());
        String::from_utf8(self.take(len as usize)?.to_vec()).map_err(|_| WalError::InvalidData)
    }

    fn bytes(&mut self) -> Result<&'a [u8], WalError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, WalError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| WalError::InvalidData)
    }

    fn time(&mut self) -> Result<Timestamp, WalError> {
        let secs = i64::from_le_bytes(self.take(8)?.try_into().unwrap());
        let nanos = i32::from_le_bytes(self.take(4)?.try_into().unwrap());
        Timestamp::new(secs, nanos).map_err(|_| WalError::InvalidData)
    }

    fn opt_time(&mut self) -> Result<Option<Timestamp>, WalError> {
        match self.take(1)?[0] {
            0 => Ok(None),
            1 => self.time().map(Some),
            _ => Err(WalError::InvalidData),
        }
    }

    /// The payload must be used up: anything left over is damage.
    fn finish(self) -> Result<(), WalError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(WalError::InvalidData)
        }
    }
}

/// On-wire size of the v6 body section for a FullJob record.
/// `body_external == true` selects the BodyId encoding; otherwise the
/// body rides inline as `len + bytes`.
fn body_section_bytes(body_len: usize, body_external: bool) -> usize {
    if body_external {
        1 + 8 + 4 // body_kind + BodyId + crc32
    } else {
        1 + 4 + body_len // body_kind + body_len + bytes
    }
}

pub fn estimate_full_job_size(job: &Job) -> usize {
    let body_external = matches!(job.body, BodyRef::External(..));
    estimate_full_job_size_raw(
        &job.tube_name,
        job.idempotency_key().map(|(k, _)| k.as_str()),
        job.group().map(|s| s.as_str()),
        job.after_group().map(|s| s.as_str()),
        job.concurrency_key().map(|(k, _)| k.as_str()),
        job.body.len(),
        body_external,
    )
}

/// Estimate full job record size without needing a Job struct.
///
/// `body_len` is the body byte count; `body_external` indicates the
/// body lives in the body store (BodyId reference) rather than inline.
pub fn estimate_full_job_size_raw(
    tube_name: &str,
    idempotency_key: Option<&str>,
    group: Option<&str>,
    after_group: Option<&str>,
    concurrency_key: Option<&str>,
    body_len: usize,
    body_external: bool,
) -> usize {
    // v6 layout (excluding the body section, which body_section_bytes covers):
    //   type(1) + job_id(8) + payload_len(4) + crc(4) = 17 overhead
    //   payload: pri(4) + delay(8) + ttr(8) + epoch(8) + state(1)
    //          + 5 counters * 4 = 20 + tube_name_len(2) + tube_name
    //          + 4 option_strings (2 bytes each min) + idp_ttl(4)
    //          + concurrency_limit(4)
    let fixed = 17 + 4 + 8 + 8 + 8 + 1 + 20 + 2 + 8 + 4 + 4;
    let variable = tube_name.len()
        + idempotency_key.map_or(0, str::len)
        + group.map_or(0, str::len)
        + after_group.map_or(0, str::len)
        + concurrency_key.map_or(0, str::len);
    fixed + variable + body_section_bytes(body_len, body_external)
}

// --- Deserialization ---

/// Deserialize a single record from `data`. Returns (record, bytes_consumed).
pub fn deserialize_record(data: &[u8], version: u32) -> Result<(WalRecord, usize), WalError> {
    if data.is_empty() {
        return Err(WalError::Truncated);
    }

    let record_type = data[0];
    match record_type {
        RECORD_TYPE_FULL_JOB => deserialize_full_job(data, version),
        RECORD_TYPE_STATE_CHANGE => deserialize_state_change(data),
        RECORD_TYPE_TOMBSTONE if version >= WAL_VERSION_TOMBSTONES => deserialize_tombstone(data),
        RECORD_TYPE_ID_MARK if version >= WAL_VERSION_TOMBSTONES => deserialize_id_mark(data),
        RECORD_TYPE_TOAST_MARK if version >= WAL_VERSION_TOAST_MARKS => {
            deserialize_toast_mark(data)
        }
        RECORD_TYPE_SCHEDULE_SET if version >= WAL_VERSION_SCHEDULES => {
            deserialize_schedule_set(data)
        }
        RECORD_TYPE_SCHEDULE_FIRED if version >= WAL_VERSION_SCHEDULES => {
            deserialize_schedule_fired(data)
        }
        _ => Err(WalError::UnknownRecordType(record_type)),
    }
}

/// Check a framed record's length and CRC. Returns its job-id field, its
/// payload and its total size.
fn unframe_record(data: &[u8]) -> Result<(u64, &[u8], usize), WalError> {
    if data.len() < 13 {
        return Err(WalError::Truncated);
    }
    let job_id = u64::from_le_bytes(data[1..9].try_into().map_err(|_| WalError::Truncated)?);
    let payload_len =
        u32::from_le_bytes(data[9..13].try_into().map_err(|_| WalError::Truncated)?) as usize;
    let total_len = 13 + payload_len + 4;
    if data.len() < total_len {
        return Err(WalError::Truncated);
    }
    let stored_crc = u32::from_le_bytes(
        data[total_len - 4..total_len]
            .try_into()
            .map_err(|_| WalError::Truncated)?,
    );
    if crc32fast::hash(&data[..total_len - 4]) != stored_crc {
        return Err(WalError::BadCrc);
    }
    Ok((job_id, &data[13..13 + payload_len], total_len))
}

fn deserialize_tombstone(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    let (job_id, payload, total_len) = unframe_record(data)?;
    let mut r = PayloadReader(payload);
    let expires_epoch_secs = r.u64()?;
    let tube = r.str16()?;
    let key = r.str16()?;
    Ok((
        WalRecord::Tombstone {
            job_id,
            tube,
            key,
            expires_epoch_secs,
        },
        total_len,
    ))
}

fn deserialize_id_mark(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    let (high_water, _, total_len) = unframe_record(data)?;
    Ok((WalRecord::IdMark { high_water }, total_len))
}

fn deserialize_toast_mark(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    let (seq, payload, total_len) = unframe_record(data)?;
    let offset = u64::from_le_bytes(payload.try_into().map_err(|_| WalError::InvalidData)?);
    Ok((
        WalRecord::ToastMark(DurablePoint { seq, offset }),
        total_len,
    ))
}

fn deserialize_schedule_set(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    let (_, payload, total_len) = unframe_record(data)?;
    let mut r = PayloadReader(payload);
    let owner = r.string()?;
    let count = r.u32()?;
    let mut entries = Vec::new();
    for _ in 0..count {
        entries.push(ScheduleRecord {
            line: r.string()?,
            body: r.bytes()?.to_vec(),
            synced_at: r.time()?,
            last_fire: r.opt_time()?,
        });
    }
    r.finish()?;
    Ok((WalRecord::ScheduleSet { owner, entries }, total_len))
}

fn deserialize_schedule_fired(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    let (_, payload, total_len) = unframe_record(data)?;
    let mut r = PayloadReader(payload);
    let owner = r.string()?;
    let name = r.string()?;
    let last_fire = r.opt_time()?;
    r.finish()?;
    Ok((
        WalRecord::ScheduleFired {
            owner,
            name,
            last_fire,
        },
        total_len,
    ))
}

fn deserialize_full_job(data: &[u8], version: u32) -> Result<(WalRecord, usize), WalError> {
    let (job_id, payload, total_len) = unframe_record(data)?;
    let mut off = 0;

    macro_rules! read_u32 {
        () => {{
            if off + 4 > payload.len() {
                return Err(WalError::Truncated);
            }
            let v = u32::from_le_bytes(
                payload[off..off + 4]
                    .try_into()
                    .map_err(|_| WalError::Truncated)?,
            );
            off += 4;
            v
        }};
    }
    macro_rules! read_u64 {
        () => {{
            if off + 8 > payload.len() {
                return Err(WalError::Truncated);
            }
            let v = u64::from_le_bytes(
                payload[off..off + 8]
                    .try_into()
                    .map_err(|_| WalError::Truncated)?,
            );
            off += 8;
            v
        }};
    }
    macro_rules! read_u16 {
        () => {{
            if off + 2 > payload.len() {
                return Err(WalError::Truncated);
            }
            let v = u16::from_le_bytes(
                payload[off..off + 2]
                    .try_into()
                    .map_err(|_| WalError::Truncated)?,
            );
            off += 2;
            v
        }};
    }

    let priority = read_u32!();
    let delay_nanos = read_u64!();
    let ttr_nanos = read_u64!();
    let created_at_epoch = read_u64!();

    if off >= payload.len() {
        return Err(WalError::Truncated);
    }
    let state_byte = payload[off];
    off += 1;
    let state = u8_to_state(state_byte).ok_or(WalError::InvalidData)?;

    let reserve_ct = read_u32!();
    let timeout_ct = read_u32!();
    let release_ct = read_u32!();
    let bury_ct = read_u32!();
    let kick_ct = read_u32!();

    let tube_name_len = read_u16!() as usize;
    if off + tube_name_len > payload.len() {
        return Err(WalError::Truncated);
    }
    let tube_name = String::from_utf8(payload[off..off + tube_name_len].to_vec())
        .map_err(|_| WalError::InvalidData)?;
    off += tube_name_len;

    // Extension fields (grouped: key + associated value)
    // idempotency_key + ttl
    let idempotency_key_str = read_option_string(payload, &mut off)?;
    let idempotency_ttl = read_u32!();
    let idempotency_key = idempotency_key_str.map(|k| (k, idempotency_ttl));

    // group, after_group
    let group = read_option_string(payload, &mut off)?;
    let after_group = read_option_string(payload, &mut off)?;

    // concurrency_key + limit
    let concurrency_key_str = read_option_string(payload, &mut off)?;
    let concurrency_limit = read_u32!();
    let concurrency_key = concurrency_key_str.map(|k| (k, concurrency_limit.max(1)));

    // body — v6 records carry a body_kind discriminant + variant payload.
    // v5 records always reference a BodyId (external). v3/v4 records carry
    // raw inline bytes that the legacy migration step promotes into the
    // appropriate tier.
    let body_ref = if version >= 6 {
        if off >= payload.len() {
            return Err(WalError::Truncated);
        }
        let kind = payload[off];
        off += 1;
        match kind {
            BODY_KIND_INLINE => {
                let body_len = read_u32!() as usize;
                if off + body_len > payload.len() {
                    return Err(WalError::Truncated);
                }
                let body = payload[off..off + body_len].to_vec();
                off += body_len;
                BodyRef::new_inline(body)
            }
            BODY_KIND_EXTERNAL => {
                let body_id = read_u64!();
                BodyRef::External(crate::job::BodyId(body_id), None)
            }
            BODY_KIND_EXTERNAL_CRC if version >= WAL_VERSION_TOAST_MARKS => {
                let body_id = read_u64!();
                let crc = read_u32!();
                BodyRef::External(crate::job::BodyId(body_id), Some(crc))
            }
            _ => return Err(WalError::InvalidData),
        }
    } else if version == 5 {
        let body_id = read_u64!();
        BodyRef::External(crate::job::BodyId(body_id), None)
    } else {
        let body_len = read_u32!() as usize;
        if off + body_len > payload.len() {
            return Err(WalError::Truncated);
        }
        let body = payload[off..off + body_len].to_vec();
        off += body_len;
        BodyRef::new_inline(body)
    };
    let _ = off;

    let delay = Duration::from_nanos(delay_nanos);
    let ttr = Duration::from_nanos(ttr_nanos);
    let now = Instant::now();

    let (replay_state, deadline_at) = match state {
        // Reserved jobs replay as Ready
        JobState::Reserved => (JobState::Ready, None),
        JobState::Delayed => {
            // Replay with the *remaining* delay, not the full original delay,
            // so a delayed job doesn't reset its countdown on every restart
            // (a 1h-delay job restarted at t=59m would otherwise wait another
            // full hour). For an initial delayed put, created_at_epoch is when
            // the delay started; subtract the elapsed wall-clock. A later
            // release/kick-to-delayed is superseded on replay by its v7
            // StateChange record, which carries its own change_epoch_secs — see
            // the StateChange replay arm.
            let now_epoch = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(created_at_epoch);
            let elapsed = Duration::from_secs(now_epoch.saturating_sub(created_at_epoch));
            (JobState::Delayed, Some(now + delay.saturating_sub(elapsed)))
        }
        _ => (state, None),
    };

    // Construct the Job with an empty inline body, then overwrite with the
    // version-specific BodyRef. This keeps `Job::new`'s public signature
    // stable while supporting both v5 (External) and pre-v5 (Inline) replay.
    let mut job = Job::new(job_id, priority, Duration::ZERO, ttr, Vec::new(), tube_name);
    job.body = body_ref;
    job.delay = delay;
    job.state = replay_state;
    job.deadline_at = deadline_at;
    job.created_at_epoch = created_at_epoch;
    job.reserve_ct = reserve_ct;
    job.timeout_ct = timeout_ct;
    job.release_ct = release_ct;
    job.bury_ct = bury_ct;
    job.kick_ct = kick_ct;
    // set_ext skips the allocation when every key is None — the common
    // case, and the one that matters at replay scale.
    job.set_ext(crate::job::JobExt {
        idempotency_key,
        group,
        after_group,
        concurrency_key,
    });
    job.reserver_id = None;

    Ok((WalRecord::FullJob(Box::new(job)), total_len))
}

fn deserialize_state_change(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    // Determine record size from payload_len to support v3 (21), v4-v6 (22),
    // and v7 (30). The record is self-describing via payload_len, so a newer
    // reader can still decode older, shorter records.
    if data.len() < 13 {
        return Err(WalError::Truncated);
    }
    let payload_len = u32::from_le_bytes(data[9..13].try_into().map_err(|_| WalError::Truncated)?);
    let record_size = (1 + 8 + 4 + payload_len + 4) as usize; // type + id + len + payload + crc

    if data.len() < record_size {
        return Err(WalError::Truncated);
    }
    if payload_len != STATE_CHANGE_PAYLOAD_LEN_V3
        && payload_len != STATE_CHANGE_PAYLOAD_LEN_V4
        && payload_len != STATE_CHANGE_PAYLOAD_LEN_V7
    {
        return Err(WalError::InvalidData);
    }

    let job_id = u64::from_le_bytes(data[1..9].try_into().map_err(|_| WalError::Truncated)?);

    // Verify CRC
    let stored_crc = u32::from_le_bytes(
        data[record_size - 4..record_size]
            .try_into()
            .map_err(|_| WalError::Truncated)?,
    );
    let computed_crc = crc32fast::hash(&data[..record_size - 4]);
    if stored_crc != computed_crc {
        return Err(WalError::BadCrc);
    }

    let state_byte = data[13];
    let new_state = if state_byte == STATE_DELETED {
        None
    } else {
        Some(u8_to_state(state_byte).ok_or(WalError::InvalidData)?)
    };
    let new_priority =
        u32::from_le_bytes(data[14..18].try_into().map_err(|_| WalError::Truncated)?);
    let new_delay_nanos =
        u64::from_le_bytes(data[18..26].try_into().map_err(|_| WalError::Truncated)?);
    let expiry_epoch_secs =
        u64::from_le_bytes(data[26..34].try_into().map_err(|_| WalError::Truncated)?);

    let reason = if payload_len >= STATE_CHANGE_PAYLOAD_LEN_V4 {
        u8_to_reason(data[34])
    } else {
        StateChangeReason::None
    };
    let change_epoch_secs = if payload_len >= STATE_CHANGE_PAYLOAD_LEN_V7 {
        u64::from_le_bytes(data[35..43].try_into().map_err(|_| WalError::Truncated)?)
    } else {
        0
    };

    Ok((
        WalRecord::StateChange {
            job_id,
            new_state,
            new_priority,
            new_delay_nanos,
            expiry_epoch_secs,
            reason,
            change_epoch_secs,
        },
        record_size,
    ))
}

/// Path a corrupt segment is moved/copied aside to: `<path>.corrupt`.
pub(crate) fn corrupt_sidecar_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(".corrupt");
    PathBuf::from(os)
}

/// Rename a corrupt segment to `<path>.corrupt` so it is preserved for operator
/// recovery instead of being skipped-then-GC-unlinked (which turns a transient
/// read error into permanent loss of the segment's live jobs). Returns the
/// sidecar path on success.
fn quarantine_corrupt_segment(path: &Path) -> io::Result<PathBuf> {
    let dest = corrupt_sidecar_path(path);
    fs::rename(path, &dest)?;
    Ok(dest)
}

/// The error that stops a replay at a damaged segment. `damage` completes
/// "WAL segment <path> ...". `older` says a newer segment exists, which a
/// crash can't explain: rotation fsyncs a segment before creating the next.
fn damaged_segment_error(path: &Path, damage: &str, older: bool) -> io::Error {
    let why = if older {
        "It is not the newest segment, so this is not a write cut short by a crash: \
         its records were fsynced. "
    } else {
        ""
    };
    io::Error::other(format!(
        "WAL segment {} {damage}. {why}Replaying past the damage would lose the \
         changes recorded there: a lost delete brings its job back, and a lost put \
         loses its job. \
         Restore the WAL directory from a backup, or start with --wal-skip-corrupt \
         (env TUBER_WAL_SKIP_CORRUPT) to replay what remains, keeping the damaged \
         segment as {}.",
        path.display(),
        corrupt_sidecar_path(path).display(),
    ))
}

/// The error that stops a `--wal-skip-corrupt` replay at a damaged older
/// segment it can't keep a copy of. Cutting the segment without one would
/// destroy the records past the damage, which may still be recoverable.
fn uncopied_segment_error(path: &Path, damage: &str, err: &io::Error) -> io::Error {
    io::Error::other(format!(
        "WAL segment {} {damage}. --wal-skip-corrupt replays the records before the \
         damage and cuts the segment there, but couldn't keep a copy of it as {} first \
         ({err}), and cutting it without one would destroy the records past the damage. \
         Free some disk space, or fix whatever stopped the copy, and start again with \
         --wal-skip-corrupt, or restore the WAL directory from a backup.",
        path.display(),
        corrupt_sidecar_path(path).display(),
    ))
}

fn write_header(w: &mut impl Write) -> io::Result<()> {
    w.write_all(WAL_MAGIC)?;
    w.write_all(&WAL_VERSION.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved flags
    Ok(())
}

/// Returns `(version, flags)` from a WAL file header. Validates that the
/// version is within the supported range.
fn read_header(data: &[u8]) -> Result<(u32, u32), WalError> {
    if data.len() < HEADER_SIZE {
        return Err(WalError::Truncated);
    }
    if &data[0..4] != WAL_MAGIC {
        return Err(WalError::BadMagic);
    }
    let version = u32::from_le_bytes(data[4..8].try_into().map_err(|_| WalError::Truncated)?);
    if !(WAL_VERSION_MIN..=WAL_VERSION).contains(&version) {
        return Err(WalError::BadVersion(version));
    }
    let flags = u32::from_le_bytes(data[8..12].try_into().map_err(|_| WalError::Truncated)?);
    Ok((version, flags))
}

/// Human-readable byte size for log lines. Picks the largest binary
/// prefix where the value would render with a meaningful integer part.
pub(crate) fn format_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;
    if n >= TIB {
        format!("{:.2} TiB", n as f64 / TIB as f64)
    } else if n >= GIB {
        format!("{:.2} GiB", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.1} MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.1} KiB", n as f64 / KIB as f64)
    } else {
        format!("{} B", n)
    }
}

// --- WAL file management ---

/// Path of segment `seq` in `dir`. The zero-padding is cosmetic (keeps
/// `ls` sorted); parsing is lenient about width.
fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{}{:06}", FILE_PREFIX, seq))
}

/// Sequence numbers of every segment present in `dir`, ascending.
/// Non-segment names — including `.corrupt` quarantine sidecars, whose
/// suffix fails the numeric parse — are skipped.
fn list_segment_seqs(dir: &Path) -> io::Result<Vec<u64>> {
    let mut seqs: Vec<u64> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(seq_str) = name.strip_prefix(FILE_PREFIX)
            && let Ok(seq) = seq_str.parse::<u64>()
        {
            seqs.push(seq);
        }
    }
    seqs.sort();
    Ok(seqs)
}

struct WalFile {
    seq: u64,
    path: PathBuf,
    fd: Option<BufWriter<File>>,
    refs: u64,
    bytes_written: usize,
    /// Value of `Wal::ops_written` when `refs` last dropped to zero.
    /// GC only unlinks the file once `synced_ops` has caught up — the
    /// records that superseded this file's contents (migration FullJobs,
    /// delete StateChanges) must be durable before the old copy goes away.
    zero_refs_at_op: u64,
}

/// Configuration for [`Wal::open`].
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum size of a single segment file before rotation. `None` uses [`DEFAULT_MAX_FILE_SIZE`].
    pub max_file_size: Option<usize>,
    /// Minimum interval between fsyncs. `Duration::ZERO` fsyncs on every write
    /// (blocking the caller until durable). Positive values bound how much committed
    /// state can be lost on crash.
    pub sync_interval: Duration,
    /// Let replay carry on past a damaged segment, setting it aside as
    /// `.corrupt` (`--wal-skip-corrupt`). Without it, damage that a crash
    /// can't explain stops the replay with an error; see [`Wal::replay`].
    pub skip_corrupt: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            max_file_size: None,
            sync_interval: DEFAULT_SYNC_INTERVAL,
            skip_corrupt: false,
        }
    }
}

impl WalConfig {
    /// Convenience for callers that only need to override the file size.
    pub fn with_max_file_size(max: usize) -> Self {
        WalConfig {
            max_file_size: Some(max),
            ..WalConfig::default()
        }
    }
}

pub struct Wal {
    dir: PathBuf,
    max_file_size: usize,
    sync_interval: Duration,
    /// See [`WalConfig::skip_corrupt`].
    skip_corrupt: bool,
    /// Highest job id replayed or written, stamped into each new segment's
    /// id mark.
    max_job_id: u64,
    last_sync_at: Instant,
    files: VecDeque<WalFile>,
    next_seq: u64,
    reserved_bytes: u64,
    alive_bytes: u64,
    /// Sum of `WalFile.bytes_written` across all files. Maintained
    /// incrementally so `total_disk_bytes()` is O(1) on the put hot path.
    total_disk_bytes: u64,
    records_migrated: u64,
    /// True iff there are buffered writes since the last `sync()`. Set on
    /// every `write_put` / `write_state_change`; cleared by `sync()`.
    dirty: bool,
    /// Monotonic count of records written (puts + state changes).
    ops_written: u64,
    /// Value of `ops_written` covered by the last completed fsync. Pairs
    /// with `WalFile::zero_refs_at_op` to gate GC on durability.
    synced_ops: u64,
    /// External body store. When present, every WAL fsync is preceded by a
    /// body-store fsync so that any `BodyId` referenced by an acked WAL
    /// record is already durable on disk.
    body_store: Option<Arc<BodyStore>>,
    /// The TOAST recovery point last written (or, at startup, set): how
    /// far the body store is known durable. `sync` records a newer one
    /// after each TOAST fsync, and every new segment repeats it after its
    /// id mark, so GC never takes the last one.
    toast_mark: Option<DurablePoint>,
    /// The schedules the last replay found, until taken.
    replayed_schedules: ReplayedSchedules,
    /// Optional tap on the write path; see [`WalObserver`]. Absent in
    /// normal operation — a `None` check is the whole hot-path cost.
    observer: Option<Box<dyn WalObserver>>,
    #[allow(dead_code)] // held for flock side effect
    lock_fd: Option<File>,
    #[cfg(test)]
    sync_count: u64,
    /// Test-only: when set, every write and sync after this many more fails
    /// as a full disk would.
    #[cfg(test)]
    fail_io_after: Option<u32>,
}

impl Wal {
    /// Sequence number of the oldest (first) WAL file, or 0 if none.
    pub fn oldest_seq(&self) -> u64 {
        self.files.front().map(|f| f.seq).unwrap_or(0)
    }

    /// Sequence number of the current (last) writable WAL file, or 0 if none.
    pub fn current_seq(&self) -> u64 {
        self.files.back().map(|f| f.seq).unwrap_or(0)
    }

    /// Maximum file size for WAL files.
    pub fn max_file_size(&self) -> usize {
        self.max_file_size
    }

    /// Number of WAL files currently on disk.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Total bytes written across all WAL files.
    pub fn total_disk_bytes(&self) -> u64 {
        self.total_disk_bytes
    }

    /// Format versions of the WAL files on disk: the lowest, and the newest
    /// segment's. `Ok(None)` means the WAL directory is empty (no files yet).
    /// Reads only the first 12 bytes of each file — used at startup to decide
    /// whether legacy migration is required, before any expensive replay work.
    pub fn format_versions(&self) -> io::Result<Option<FormatVersions>> {
        use std::io::Read;
        let mut versions: Option<FormatVersions> = None;
        for f in self.files.iter() {
            let mut buf = [0u8; HEADER_SIZE];
            let mut file = match File::open(&f.path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if file.read_exact(&mut buf).is_err() {
                continue; // empty/truncated file — replay path will warn
            }
            if let Ok((v, _)) = read_header(&buf) {
                versions = Some(FormatVersions {
                    min: versions.map_or(v, |vs| vs.min.min(v)),
                    newest: v,
                });
            }
        }
        Ok(versions)
    }

    /// Number of job records migrated during compaction.
    pub fn records_migrated(&self) -> u64 {
        self.records_migrated
    }

    /// Increment the compaction migration counter.
    pub fn record_migration(&mut self) {
        self.records_migrated += 1;
    }

    /// Returns the oldest file's seq and how many jobs to migrate, if compaction is needed.
    ///
    /// Uses the beanstalkd waste-ratio strategy: ratio = waste / live.
    /// When ratio >= 2 (i.e. 2/3+ of WAL space is dead), migrate `ratio` jobs per tick
    /// from the oldest file. Self-regulating: more waste = more jobs moved per tick.
    pub fn compaction_target(&self) -> Option<(u64, usize)> {
        if self.files.len() <= 1 {
            return None;
        }

        let live_bytes = self.alive_bytes + self.reserved_bytes;
        if live_bytes == 0 {
            return None;
        }

        let total_space = self.files.len() as u64 * self.max_file_size as u64;
        let waste = total_space.saturating_sub(live_bytes);
        let ratio = waste / live_bytes;

        if ratio < 2 {
            return None;
        }

        let oldest_seq = self.files.front().unwrap().seq;
        Some((oldest_seq, ratio as usize))
    }

    pub fn open(dir: &Path, config: WalConfig) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let lock_fd = Self::acquire_lock(dir)?;

        let mut wal = Wal {
            dir: dir.to_path_buf(),
            max_file_size: config.max_file_size.unwrap_or(DEFAULT_MAX_FILE_SIZE),
            sync_interval: config.sync_interval,
            skip_corrupt: config.skip_corrupt,
            max_job_id: 0,
            last_sync_at: Instant::now(),
            files: VecDeque::new(),
            next_seq: 1,
            reserved_bytes: 0,
            alive_bytes: 0,
            total_disk_bytes: 0,
            records_migrated: 0,
            dirty: false,
            ops_written: 0,
            synced_ops: 0,
            body_store: None,
            toast_mark: None,
            replayed_schedules: ReplayedSchedules::default(),
            observer: None,
            lock_fd: Some(lock_fd),
            #[cfg(test)]
            sync_count: 0,
            #[cfg(test)]
            fail_io_after: None,
        };

        wal.scan_dir()?;

        Ok(wal)
    }

    /// Returns the configured fsync interval. After group commit, this
    /// is the **maximum ack staleness SLA** — the engine drives sync via
    /// `sync()` whenever its message channel drains, and the tick loop
    /// uses this interval as a backstop. `Duration::ZERO` means "no SLA,
    /// sync as soon as the channel is empty."
    pub fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    /// True iff there are buffered writes since the last successful sync.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Time since the last successful fsync.
    pub fn last_sync_elapsed(&self) -> Duration {
        self.last_sync_at.elapsed()
    }

    /// fsync TOAST then WAL; clear `dirty`. No-op on a clean WAL — safe to
    /// call from both group-commit hot paths (skip the syscalls when
    /// nothing's buffered) and shutdown (where being clean is fine).
    pub fn sync(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        #[cfg(test)]
        self.injected_failure()?;
        let point = self.pre_sync_body_store()?;
        if let Some(f) = self.files.back_mut()
            && let Some(fd) = f.fd.as_mut()
        {
            // Record how far TOAST is now durable, in the batch this fsync
            // makes durable. Written after the TOAST fsync returned, so a
            // mark on disk never names bytes a crash could still tear.
            if let Some(p) = point
                && point != self.toast_mark
            {
                let pos = Position {
                    seg: f.seq,
                    offset: f.bytes_written as u64,
                };
                let mark = serialize_toast_mark(p);
                fd.write_all(&mark)?;
                f.bytes_written += mark.len();
                self.total_disk_bytes += mark.len() as u64;
                self.toast_mark = point;
                if let Some(obs) = self.observer.as_mut() {
                    obs.on_record(pos, WalEvent::ToastMark(p));
                }
            }
            Self::flush_and_fsync(fd)?;
        }
        self.record_sync();
        self.dirty = false;
        let end = self.end_position();
        if let Some(obs) = self.observer.as_mut() {
            obs.on_sync(end);
        }
        Ok(())
    }

    /// Attach a body store whose fsync must complete before any WAL fsync.
    /// Called once at startup, after the body store has been opened and
    /// before any WAL writes.
    pub fn set_body_store(&mut self, bs: Arc<BodyStore>) {
        self.body_store = Some(bs);
    }

    /// Set the TOAST recovery point that the next segment opens with. At
    /// startup, the body store's end once it is open
    /// ([`BodyStore::synced_point`]): it supersedes the mark it was opened
    /// with, which `--wal-skip-corrupt` may have cut the store short of.
    pub fn set_toast_mark(&mut self, point: Option<DurablePoint>) {
        self.toast_mark = point;
    }

    /// Mark the WAL dirty if the body store's fsyncs have gone past the last
    /// recorded mark, so the next sync records a new one. Compaction moves
    /// bodies and fsyncs them with no WAL write of its own; without this an
    /// idle WAL would leave them past the recovery point indefinitely.
    pub fn note_toast_progress(&mut self) {
        if let Some(bs) = &self.body_store
            && bs.synced_point() > self.toast_mark
        {
            self.dirty = true;
        }
    }

    /// Attach an observer to the write path; see [`WalObserver`]. Called
    /// once at startup, before any writes — records appended while no
    /// observer is attached are only reachable through a file scan.
    pub fn set_observer(&mut self, obs: Box<dyn WalObserver>) {
        self.observer = Some(obs);
    }

    /// Position one-past the end of the log: where the next record will be
    /// appended. Every existing record sits at a position strictly below
    /// this. An empty WAL reports the start of the first future segment.
    pub fn end_position(&self) -> Position {
        match self.files.back() {
            Some(f) => Position {
                seg: f.seq,
                offset: f.bytes_written as u64,
            },
            None => Position {
                seg: self.next_seq,
                offset: HEADER_SIZE as u64,
            },
        }
    }

    /// fsync the body store (if attached) so referenced bodies are durable
    /// before we promise the same of any WAL record. The TOAST-then-WAL
    /// order means a crash mid-sync produces orphan bodies (recoverable as
    /// wasted space) rather than dangling references. Returns how far the
    /// body store is now durable.
    fn pre_sync_body_store(&self) -> io::Result<Option<DurablePoint>> {
        match &self.body_store {
            Some(bs) => Ok(bs.fsync()?),
            None => Ok(None),
        }
    }

    /// The latest TOAST recovery point the WAL recorded, read before the
    /// body store is opened so its startup scan can be judged by it. Every
    /// segment a v9 server creates repeats the latest mark after its id
    /// mark, so the newest segment whose opening reads has the answer, and
    /// none in it means there is none: older segments aren't read. One that
    /// can't be read or parsed is left to [`Wal::replay`] to report, and
    /// only its records before the damage count. `None` before v9, or with
    /// no TOAST writes yet.
    pub fn read_toast_mark(&self) -> Option<DurablePoint> {
        for f in self.files.iter().rev() {
            let Ok(data) = fs::read(&f.path) else {
                continue;
            };
            let Ok((version, _)) = read_header(&data) else {
                continue;
            };
            if version < WAL_VERSION_TOAST_MARKS {
                return None;
            }
            let mut latest = None;
            let mut offset = HEADER_SIZE;
            while let Ok((record, consumed)) = deserialize_record(&data[offset..], version) {
                if let WalRecord::ToastMark(point) = record {
                    latest = Some(point);
                }
                offset += consumed;
            }
            if latest.is_some() || offset > HEADER_SIZE {
                return latest;
            }
        }
        None
    }

    fn acquire_lock(dir: &Path) -> io::Result<File> {
        let lock_path = dir.join("lock");
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        // Use flock on unix
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let ret = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if ret != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "WAL directory is locked by another process",
                ));
            }
        }

        Ok(f)
    }

    fn scan_dir(&mut self) -> io::Result<()> {
        for seq in list_segment_seqs(&self.dir)? {
            let path = segment_path(&self.dir, seq);
            let meta = fs::metadata(&path)?;
            let bytes = meta.len() as usize;
            self.files.push_back(WalFile {
                seq,
                path,
                fd: None,
                refs: 0,
                bytes_written: bytes,
                zero_refs_at_op: 0,
            });
            self.total_disk_bytes += bytes as u64;
            if seq >= self.next_seq {
                self.next_seq = seq + 1;
            }
        }

        Ok(())
    }

    fn file_path(&self, seq: u64) -> PathBuf {
        segment_path(&self.dir, seq)
    }

    fn create_next_file(&mut self) -> io::Result<()> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let path = self.file_path(seq);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // Persist the dirent: without a directory fsync a crash can lose
        // the file entirely even after its contents are fsynced, and a
        // vanished segment drops every record in it — including delete
        // records, which would resurrect jobs from older segments.
        crate::body_store::fsync_dir(&self.dir)?;

        let mut fd = BufWriter::with_capacity(BUF_CAPACITY, file);
        write_header(&mut fd)?;
        // Open with the id high-water mark. GC never removes the newest
        // segment, so the mark survives every older segment being collected,
        // and replay can't hand out an id twice. The TOAST recovery point
        // follows for the same reason.
        let id_mark = serialize_id_mark(self.max_job_id);
        fd.write_all(&id_mark)?;
        let mut opening_bytes = HEADER_SIZE + id_mark.len();
        if let Some(point) = self.toast_mark {
            let toast_mark = serialize_toast_mark(point);
            fd.write_all(&toast_mark)?;
            opening_bytes += toast_mark.len();
        }
        // Make the header durable immediately. Without this, a crash any
        // time before the first sync leaves a 0-byte or partial file that
        // the next replay flags as corrupt ("bad header ... quarantined")
        // even though it never held a record — a crash-looping server
        // then mints one spurious .corrupt sidecar per restart.
        Self::flush_and_fsync(&mut fd)?;

        self.files.push_back(WalFile {
            seq,
            path,
            fd: Some(fd),
            refs: 0,
            bytes_written: opening_bytes,
            zero_refs_at_op: 0,
        });
        self.total_disk_bytes += opening_bytes as u64;

        if let Some(obs) = self.observer.as_mut() {
            let mut offset = HEADER_SIZE as u64;
            obs.on_record(
                Position { seg: seq, offset },
                WalEvent::IdMark {
                    high_water: self.max_job_id,
                },
            );
            offset += id_mark.len() as u64;
            if let Some(point) = self.toast_mark {
                obs.on_record(Position { seg: seq, offset }, WalEvent::ToastMark(point));
            }
        }

        Ok(())
    }

    fn current_file_mut(&mut self) -> io::Result<&mut WalFile> {
        if self.files.is_empty() || self.files.back().and_then(|f| f.fd.as_ref()).is_none() {
            self.create_next_file()?;
        }
        Ok(self.files.back_mut().unwrap())
    }

    fn should_rotate(&self) -> bool {
        self.files
            .back()
            .map(|f| f.bytes_written >= self.max_file_size)
            .unwrap_or(true)
    }

    /// Flush the userland buffer of `fd` and fsync the inner file.
    fn flush_and_fsync(fd: &mut BufWriter<File>) -> io::Result<()> {
        fd.flush()?;
        fd.get_ref().sync_all()
    }

    /// Record that a sync happened, updating tracking state. Only called
    /// after everything buffered so far is durable (`sync()` fsyncs the
    /// current file; rotation fsyncs the outgoing file, and all older
    /// files were fsynced at their own rotation).
    fn record_sync(&mut self) {
        #[cfg(test)]
        {
            self.sync_count += 1;
        }
        self.last_sync_at = Instant::now();
        self.synced_ops = self.ops_written;
    }

    fn rotate_if_needed(&mut self) -> io::Result<()> {
        if self.should_rotate() {
            // Close current file. Sync TOAST first so any BodyId references
            // about to land in the WAL are already durable. The next segment
            // opens with the point that reached.
            if let Some(point) = self.pre_sync_body_store()? {
                self.toast_mark = Some(point);
            }
            if let Some(f) = self.files.back_mut()
                && let Some(mut fd) = f.fd.take()
            {
                Self::flush_and_fsync(&mut fd)?;
                self.record_sync();
            }
            self.create_next_file()?;
        }
        Ok(())
    }

    // --- Space reservation ---

    pub fn reserve_put(&self, record_size: usize) -> bool {
        // The WAL creates new files on demand, so total space is unbounded.
        // Just verify the record + its future delete fit in a single file.
        let needed = record_size + STATE_CHANGE_RECORD_SIZE;
        needed <= self.max_file_size
    }

    // --- Write operations ---

    pub fn write_put(&mut self, job: &mut Job) -> io::Result<()> {
        self.max_job_id = self.max_job_id.max(job.id);
        let (pos, record) = self.append(&serialize_full_job(job))?;

        // Update job's WAL tracking
        let old_seq = job.wal_seq();
        let old_used = job.wal_used;
        job.set_wal_ref(record.seq, record.len as usize);

        // Decref old file
        if let Some(old) = old_seq {
            self.decref_file(old, old_used as usize);
        }

        self.hold(record);

        // Reserve space for future state change (delete).
        // Only add a reservation for NEW jobs (old_seq is None).
        // Compaction migrations (old_seq is Some) already have a reservation
        // from the original put — adding another would leak reserved_bytes
        // on every migration cycle.
        if old_seq.is_none() {
            self.reserved_bytes += STATE_CHANGE_RECORD_SIZE as u64;
        }

        if let Some(obs) = self.observer.as_mut() {
            obs.on_record(pos, WalEvent::Put { job: &*job });
        }
        Ok(())
    }

    pub fn write_state_change(
        &mut self,
        job: &mut Job,
        new_state: Option<JobState>,
        new_priority: u32,
        new_delay: Duration,
        expiry_epoch_secs: u64,
        reason: StateChangeReason,
    ) -> io::Result<()> {
        let delay_nanos = new_delay.as_nanos().min(u64::MAX as u128) as u64;
        // Stamp the wall-clock instant of this change so a delayed replay can
        // subtract the elapsed time (v7). For a release/kick-to-delayed, this
        // is when the delay started; for other changes it is simply unused.
        let change_epoch_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let (pos, _) = self.append(&serialize_state_change(
            job.id,
            new_state,
            new_priority,
            delay_nanos,
            expiry_epoch_secs,
            reason,
            change_epoch_secs,
        ))?;

        // Release reservation
        self.reserved_bytes = self
            .reserved_bytes
            .saturating_sub(STATE_CHANGE_RECORD_SIZE as u64);

        // For deletes, decref old file and reduce alive bytes
        if new_state.is_none() {
            if let Some(old_seq) = job.wal_seq() {
                self.decref_file(old_seq, job.wal_used as usize);
            }
            job.clear_wal_ref();
        } else {
            // For non-delete state changes (bury, release, kick), do NOT move
            // the job's WAL ref. The wal_file_seq must keep pointing at the file
            // containing the FullJob record, since StateChange records don't
            // carry the full job body. Moving the ref would allow GC to delete
            // the FullJob's file, causing silent data loss on replay.
        }

        if let Some(obs) = self.observer.as_mut() {
            obs.on_record(
                pos,
                WalEvent::StateChange {
                    job_id: job.id,
                    new_state,
                    new_priority,
                    new_delay_nanos: delay_nanos,
                    expiry_epoch_secs,
                    reason,
                    change_epoch_secs,
                },
            );
        }
        Ok(())
    }

    /// Write an `idp:` cooldown record: `key` in `tube` stays taken by
    /// `job_id` until `expires_epoch_secs`. A live cooldown's record holds a
    /// reference on its segment, returned so the caller can release it when
    /// the cooldown ends ([`Wal::release_record`]) and compaction can carry
    /// it forward. `expires_epoch_secs == 0` clears the cooldown instead,
    /// and holds no reference: it is only needed while an earlier record
    /// for the key survives, and GC collects segments oldest first.
    pub fn write_tombstone(
        &mut self,
        tube: &str,
        key: &str,
        job_id: u64,
        expires_epoch_secs: u64,
    ) -> io::Result<Option<RecordRef>> {
        self.max_job_id = self.max_job_id.max(job_id);
        let (pos, record) =
            self.append(&serialize_tombstone(job_id, tube, key, expires_epoch_secs))?;
        if let Some(obs) = self.observer.as_mut() {
            obs.on_record(
                pos,
                WalEvent::Tombstone {
                    job_id,
                    tube,
                    key,
                    expires_epoch_secs,
                },
            );
        }
        Ok((expires_epoch_secs != 0).then(|| self.hold(record)))
    }

    /// Journal `owner`'s schedule set, replacing its earlier one and every
    /// last fire written since. A set with entries holds a reference on its
    /// segment until the owner's next set is written, and compaction
    /// carries it forward. An empty one, the owner's removal, holds none:
    /// it is only needed while an earlier set survives, and GC collects
    /// segments oldest first.
    pub fn write_schedule_set<B: AsRef<[u8]>>(
        &mut self,
        owner: &str,
        entries: &[ScheduleRecord<B>],
    ) -> io::Result<Option<RecordRef>> {
        // A commit is capped far below this (`schedule::MAX_SET_BYTES`), but
        // the files an earlier tuber left weren't.
        if schedule_set_size(owner, entries) > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("schedules: {owner}'s set is too large for a WAL record"),
            ));
        }
        let (_, record) = self.append(&serialize_schedule_set(owner, entries))?;
        Ok((!entries.is_empty()).then(|| self.hold(record)))
    }

    /// Journal the last fire of `owner`'s entry `name`. It holds a reference
    /// on its segment until superseded: by the entry's next one, or by the
    /// owner's next set, which carries every entry's last fire.
    pub fn write_schedule_fired(
        &mut self,
        owner: &str,
        name: &str,
        last_fire: Option<Timestamp>,
    ) -> io::Result<RecordRef> {
        let (_, record) = self.append(&serialize_schedule_fired(owner, name, last_fire))?;
        Ok(self.hold(record))
    }

    /// Append `record` to the current segment, taking no reference, and
    /// return where it landed. Every record goes through here. Group
    /// commit: the bytes go into the BufWriter and `dirty` is set, but
    /// nothing is fsynced until the engine drains its channel and calls
    /// [`Wal::sync`]. The op is counted before a caller decrefs what the
    /// record supersedes: that segment's zero-refs stamp must include this
    /// record.
    fn append(&mut self, record: &[u8]) -> io::Result<(Position, RecordRef)> {
        #[cfg(test)]
        self.injected_failure()?;
        self.rotate_if_needed()?;

        let file = self.current_file_mut()?;
        // The record's position: assigned here, at layout time, so the
        // observer tee and any later file scan name this record identically.
        let pos = Position {
            seg: file.seq,
            offset: file.bytes_written as u64,
        };
        let fd = file
            .fd
            .as_mut()
            .ok_or_else(|| io::Error::other("WAL file not open for writing"))?;
        fd.write_all(record)?;
        file.bytes_written += record.len();
        let seq = file.seq;
        self.total_disk_bytes += record.len() as u64;
        self.ops_written += 1;
        self.dirty = true;
        Ok((
            pos,
            RecordRef {
                seq,
                len: record.len() as u32,
            },
        ))
    }

    /// The schedules the last [`Wal::replay`] found. Empty if taken already.
    pub fn take_replayed_schedules(&mut self) -> ReplayedSchedules {
        std::mem::take(&mut self.replayed_schedules)
    }

    /// Drop the reference a record holds on its segment, once what it
    /// records has ended (an expired cooldown) or been rewritten elsewhere.
    pub fn release_record(&mut self, record: RecordRef) {
        self.decref_file(record.seq, record.len as usize);
    }

    /// Take a reference on `record`'s segment, keeping it from GC until
    /// [`Wal::release_record`].
    fn hold(&mut self, record: RecordRef) -> RecordRef {
        self.incref_file(record.seq, record.len as usize);
        record
    }

    fn incref_file(&mut self, seq: u64, used: usize) {
        for f in self.files.iter_mut() {
            if f.seq == seq {
                f.refs += 1;
                break;
            }
        }
        self.alive_bytes += used as u64;
    }

    fn decref_file(&mut self, seq: u64, used: usize) {
        let ops_written = self.ops_written;
        for f in self.files.iter_mut() {
            if f.seq == seq {
                f.refs = f.refs.saturating_sub(1);
                if f.refs == 0 {
                    f.zero_refs_at_op = ops_written;
                }
                break;
            }
        }
        self.alive_bytes = self.alive_bytes.saturating_sub(used as u64);
    }

    // --- Replay ---

    /// Rebuild job state from the WAL. Returns `(jobs, next_job_id,
    /// idempotency tombstones, orphaned body ids, buried order)`. The
    /// buried order lists every job that ends replay in `Buried` state, in
    /// the WAL order of its last bury event — callers use it to rebuild
    /// each tube's buried FIFO oldest-first, which a bare iteration over
    /// the returned map cannot (hash order loses bury order).
    ///
    /// A crash can only damage the newest segment, by cutting its last
    /// writes short, and replay truncates that quietly. Anything else — a
    /// corrupt record or bad header in an older segment, or any segment
    /// that can't be read — means records that were once durable are lost,
    /// so replay returns an error before touching the disk, unless
    /// `skip_corrupt` lets it set the segment aside as `.corrupt` and go on.
    /// A copy it can't write is an error still, with the segment left whole.
    pub fn replay(&mut self) -> io::Result<ReplayOutcome> {
        let mut jobs: HashMap<u64, Box<Job>> = HashMap::new();
        let mut max_id: u64 = 0;
        // Live cooldowns by (tube, key), the latest record winning. Legacy
        // ones, rebuilt from a pre-v8 delete record, have no record of their
        // own (`record: None`).
        let mut cooldowns: HashMap<(Arc<str>, String), IdpTombstone> = HashMap::new();
        // Every record that leaves a job buried, in WAL order. Stale
        // entries (job later kicked, deleted, or re-buried) are filtered
        // after the loop; keeping raw events avoids per-transition
        // bookkeeping in the hot replay path.
        let mut bury_events: Vec<u64> = Vec::new();
        // Bodies referenced by jobs that the WAL says are deleted. The
        // runtime delete path calls `BodyStore::delete` after the WAL
        // delete record lands, but a crash between the two leaves the
        // body orphaned in TOAST. Replay collects those ids so the
        // caller can drop them after rebuilding state.
        let mut orphan_bodies: Vec<BodyId> = Vec::new();
        let mut schedules = ReplayedSchedules::default();
        let replay_time = SystemTime::now();
        let replay_epoch = replay_time
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Read and process each file
        let file_infos: Vec<(u64, PathBuf)> =
            self.files.iter().map(|f| (f.seq, f.path.clone())).collect();

        // Reset refs
        for f in self.files.iter_mut() {
            f.refs = 0;
        }
        self.alive_bytes = 0;
        self.reserved_bytes = 0;

        let total_segments = file_infos.len();
        let total_bytes = self.total_disk_bytes;
        let mut bytes_done: u64 = 0;
        // Tube-name intern cache: deserialization allocates a fresh
        // Arc<str> per FullJob record; swap it for a shared one so the
        // surviving job set holds one allocation per tube, not per job.
        let mut tube_names: std::collections::HashSet<Arc<str>> = std::collections::HashSet::new();
        let mut last_log = Instant::now();
        // Segments no longer on disk under their original name after this
        // replay — quarantined to `.corrupt` (unreadable/bad header) or
        // deleted outright (headerless newest segment). Dropped from
        // `self.files` after the loop so GC can't chase a missing path.
        // Not a corruption signal: benign empty-segment removals land here
        // too, so don't key alerts or metrics off this list.
        let mut removed_seqs: Vec<u64> = Vec::new();
        // Only the newest segment can be a benign headerless artifact (a
        // crash between segment creation and the header fsync). Any older
        // sub-header segment was once a full segment with fsynced records
        // and must be treated as corruption, not cleaned up.
        let newest_seq: u64 = file_infos.iter().map(|(s, _)| *s).max().unwrap_or(0);
        const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

        for (idx, (seq, path)) in file_infos.iter().enumerate() {
            let older = *seq != newest_seq;
            let data = match fs::read(path) {
                Ok(d) => d,
                Err(e) => {
                    if !self.skip_corrupt {
                        return Err(damaged_segment_error(
                            path,
                            &format!("is unreadable ({e})"),
                            older,
                        ));
                    }
                    match quarantine_corrupt_segment(path) {
                        Ok(dest) => tracing::error!(
                            "WAL: unreadable segment {:?} ({}); quarantined to {:?}",
                            path,
                            e,
                            dest
                        ),
                        Err(re) => tracing::error!(
                            "WAL: unreadable segment {:?} ({}); quarantine failed: {}",
                            path,
                            e,
                            re
                        ),
                    }
                    removed_seqs.push(*seq);
                    continue;
                }
            };

            let version = match read_header(&data) {
                Ok((v, _flags)) => v,
                Err(e) => {
                    // A *newest* file shorter than the segment header is
                    // the artifact of a crash between segment creation and
                    // the header fsync — it never held a record. Remove it
                    // quietly instead of minting a .corrupt sidecar and an
                    // ERROR on every restart of a crash-looping server.
                    // An older sub-header segment is different: it was
                    // written and fsynced past its header once (later
                    // segments exist), so a short read means it was
                    // truncated by a filesystem fault. That is data loss —
                    // records it held (including deletes) are gone — so it
                    // takes the quarantine path below, preserving the
                    // evidence and logging at ERROR.
                    if data.len() < HEADER_SIZE && *seq == newest_seq {
                        match fs::remove_file(path) {
                            Ok(()) => tracing::info!(
                                "WAL: removed empty segment {:?} ({} bytes, no header — \
                                 created just before a crash, holds no records)",
                                path,
                                data.len(),
                            ),
                            Err(re) => tracing::warn!(
                                "WAL: failed to remove empty segment {:?}: {}",
                                path,
                                re
                            ),
                        }
                        removed_seqs.push(*seq);
                        continue;
                    }
                    if older && !self.skip_corrupt {
                        return Err(damaged_segment_error(
                            path,
                            &format!("has a bad header ({e})"),
                            older,
                        ));
                    }
                    match quarantine_corrupt_segment(path) {
                        Ok(dest) => tracing::error!(
                            "WAL: bad header in {:?} ({}); quarantined to {:?}",
                            path,
                            e,
                            dest
                        ),
                        Err(re) => tracing::error!(
                            "WAL: bad header in {:?} ({}); quarantine failed: {}",
                            path,
                            e,
                            re
                        ),
                    }
                    removed_seqs.push(*seq);
                    continue;
                }
            };

            let mut offset = HEADER_SIZE;
            while offset < data.len() {
                match deserialize_record(&data[offset..], version) {
                    Ok((record, consumed)) => {
                        match record {
                            WalRecord::FullJob(mut job) => {
                                if job.id > max_id {
                                    max_id = job.id;
                                }
                                // Track WAL position
                                let record_size = consumed;
                                job.set_wal_ref(*seq, record_size);

                                match tube_names.get(job.tube_name.as_ref()) {
                                    Some(interned) => job.tube_name = Arc::clone(interned),
                                    None => {
                                        tube_names.insert(Arc::clone(&job.tube_name));
                                    }
                                }

                                // Remove old ref if replacing
                                if let Some(old_job) = jobs.get(&job.id)
                                    && let Some(old_seq) = old_job.wal_seq()
                                {
                                    self.decref_file(old_seq, old_job.wal_used as usize);
                                }

                                self.incref_file(*seq, record_size);
                                if job.state == JobState::Buried {
                                    bury_events.push(job.id);
                                }
                                // The record already owns a Box<Job>; move it in
                                // whole — no unbox-and-copy into the table slot.
                                jobs.insert(job.id, job);
                            }
                            WalRecord::StateChange {
                                job_id,
                                new_state,
                                new_priority,
                                new_delay_nanos,
                                expiry_epoch_secs,
                                reason,
                                change_epoch_secs,
                            } => {
                                if job_id > max_id {
                                    max_id = job_id;
                                }
                                match new_state {
                                    None => {
                                        // Deleted. A pre-v8 delete carries its
                                        // cooldown's expiry; the key comes from
                                        // the job's put record, if that survives.
                                        if expiry_epoch_secs > replay_epoch
                                            && let Some(job) = jobs.get(&job_id)
                                            && let Some((key, _)) = job.idempotency_key()
                                        {
                                            let slot = (job.tube_name.clone(), key.clone());
                                            let legacy = IdpTombstone {
                                                tube_name: job.tube_name.clone(),
                                                key: key.clone(),
                                                job_id,
                                                expires_at: UNIX_EPOCH
                                                    + Duration::from_secs(expiry_epoch_secs),
                                                record: None,
                                            };
                                            if let Some(old) = cooldowns.insert(slot, legacy)
                                                && let Some(r) = old.record
                                            {
                                                self.release_record(r);
                                            }
                                        }
                                        if let Some(old_job) = jobs.remove(&job_id) {
                                            if let Some(old_seq) = old_job.wal_seq() {
                                                self.decref_file(old_seq, old_job.wal_used as usize);
                                            }
                                            if let BodyRef::External(body_id, _) = old_job.body {
                                                orphan_bodies.push(body_id);
                                            }
                                        }
                                    }
                                    Some(state) => {
                                        if let Some(job) = jobs.get_mut(&job_id) {
                                            // Update state
                                            let replay_state = match state {
                                                JobState::Reserved => JobState::Ready,
                                                other => other,
                                            };
                                            job.state = replay_state;
                                            if replay_state == JobState::Buried {
                                                bury_events.push(job_id);
                                            }
                                            job.priority = new_priority;
                                            job.delay = Duration::from_nanos(new_delay_nanos);
                                            if replay_state == JobState::Delayed {
                                                // Replay with the *remaining*
                                                // delay when the change carries a
                                                // v7 timestamp (change_epoch_secs
                                                // = when the delay started), so a
                                                // released/kicked-to-delayed job
                                                // doesn't reset its countdown on
                                                // every restart. Pre-v7 records
                                                // (0) fall back to the full delay.
                                                let deadline = if change_epoch_secs > 0 {
                                                    let now_epoch = SystemTime::now()
                                                        .duration_since(UNIX_EPOCH)
                                                        .map(|d| d.as_secs())
                                                        .unwrap_or(change_epoch_secs);
                                                    let elapsed = Duration::from_secs(
                                                        now_epoch.saturating_sub(change_epoch_secs),
                                                    );
                                                    Instant::now() + job.delay.saturating_sub(elapsed)
                                                } else {
                                                    Instant::now() + job.delay
                                                };
                                                job.deadline_at = Some(deadline);
                                            } else {
                                                job.deadline_at = None;
                                            }
                                            job.reserver_id = None;

                                            // Increment counter based on reason
                                            match reason {
                                                StateChangeReason::Reserve => job.reserve_ct += 1,
                                                StateChangeReason::Release => job.release_ct += 1,
                                                StateChangeReason::Bury => job.bury_ct += 1,
                                                StateChangeReason::Kick => job.kick_ct += 1,
                                                StateChangeReason::Timeout => job.timeout_ct += 1,
                                                StateChangeReason::None => {}
                                            }

                                            // Do NOT update WAL tracking here.
                                            // The job's wal_file_seq must keep
                                            // pointing at its FullJob record.
                                        }
                                    }
                                }
                            }
                            WalRecord::Tombstone {
                                job_id,
                                tube,
                                key,
                                expires_epoch_secs,
                            } => {
                                max_id = max_id.max(job_id);
                                let tube_name: Arc<str> = match tube_names.get(tube.as_str()) {
                                    Some(interned) => Arc::clone(interned),
                                    None => {
                                        let name: Arc<str> = Arc::from(tube);
                                        tube_names.insert(Arc::clone(&name));
                                        name
                                    }
                                };
                                let slot = (Arc::clone(&tube_name), key.clone());
                                // This record supersedes any earlier one for
                                // the key, live or cleared.
                                if let Some(old) = cooldowns.remove(&slot)
                                    && let Some(r) = old.record
                                {
                                    self.release_record(r);
                                }
                                if expires_epoch_secs > replay_epoch {
                                    let record = self.hold(RecordRef {
                                        seq: *seq,
                                        len: consumed as u32,
                                    });
                                    cooldowns.insert(
                                        slot,
                                        IdpTombstone {
                                            tube_name,
                                            key,
                                            job_id,
                                            expires_at: UNIX_EPOCH
                                                + Duration::from_secs(expires_epoch_secs),
                                            record: Some(record),
                                        },
                                    );
                                }
                            }
                            WalRecord::IdMark { high_water } => {
                                max_id = max_id.max(high_water);
                            }
                            // Read before replay, by `read_toast_mark`.
                            WalRecord::ToastMark(_) => {}
                            WalRecord::ScheduleSet { owner, entries } => {
                                schedules.seen = true;
                                // Supersedes the owner's last set, and the
                                // last fires since: it carries its own.
                                if let Some((_, old)) = schedules.sets.remove(&owner) {
                                    self.release_record(old);
                                }
                                schedules.stamps.retain(|(o, _), (_, r)| {
                                    let keep = *o != owner;
                                    if !keep {
                                        self.release_record(*r);
                                    }
                                    keep
                                });
                                if !entries.is_empty() {
                                    let record = self.hold(RecordRef {
                                        seq: *seq,
                                        len: consumed as u32,
                                    });
                                    schedules.sets.insert(owner, (entries, record));
                                }
                            }
                            WalRecord::ScheduleFired {
                                owner,
                                name,
                                last_fire,
                            } => {
                                schedules.seen = true;
                                // Written only for an entry its owner's set
                                // holds; without the set there's no entry.
                                if schedules.sets.contains_key(&owner) {
                                    let record = self.hold(RecordRef {
                                        seq: *seq,
                                        len: consumed as u32,
                                    });
                                    if let Some((_, old)) =
                                        schedules.stamps.insert((owner, name), (last_fire, record))
                                    {
                                        self.release_record(old);
                                    }
                                }
                            }
                        }
                        offset += consumed;
                    }
                    Err(e) => {
                        let damage = format!(
                            "has a corrupt record at offset {offset} of {} bytes ({e})",
                            data.len()
                        );
                        if older && !self.skip_corrupt {
                            return Err(damaged_segment_error(path, &damage, older));
                        }
                        // Preserve the whole segment before truncating so a
                        // mid-segment bit-rot — valid records (including deletes)
                        // after the bad one — is operator-recoverable rather than
                        // physically destroyed (which would resurrect deleted
                        // jobs). Then cut it at the last good offset, so a later
                        // start doesn't need the flag again. An older segment
                        // only once the copy is kept: without one it is the only
                        // place those records are, so stop rather than cut. The
                        // newest regardless: appends continue past a torn tail.
                        let dest = corrupt_sidecar_path(path);
                        match crate::body_store::copy_durably(path, &dest) {
                            Ok(()) => tracing::error!(
                                "WAL: corrupt record in {:?} at offset {}: {}; copy kept as {:?}, \
                                 truncating",
                                path,
                                offset,
                                e,
                                dest
                            ),
                            Err(ce) if older => {
                                return Err(uncopied_segment_error(path, &damage, &ce));
                            }
                            Err(ce) => tracing::error!(
                                "WAL: corrupt record in {:?} at offset {}: {}; failed to keep a \
                                 copy ({}), truncating",
                                path,
                                offset,
                                e,
                                ce
                            ),
                        }
                        if let Ok(f) = OpenOptions::new().write(true).open(path)
                            && let Err(te) = f.set_len(offset as u64).and_then(|()| f.sync_data())
                        {
                            tracing::warn!("WAL: failed to truncate {:?}: {}", path, te);
                        }
                        break;
                    }
                }
            }

            bytes_done = bytes_done.saturating_add(data.len() as u64);
            let segments_done = idx + 1;
            // Always log the final segment so operators see a clear "done"
            // marker before the post-replay integrity passes start.
            if last_log.elapsed() >= PROGRESS_INTERVAL || segments_done == total_segments {
                let pct = if total_bytes > 0 {
                    (bytes_done as f64 / total_bytes as f64 * 100.0) as u32
                } else {
                    100
                };
                tracing::info!(
                    "WAL replay: segment {}/{}, {} / {} ({}%), {} jobs so far",
                    segments_done,
                    total_segments,
                    format_bytes(bytes_done),
                    format_bytes(total_bytes),
                    pct,
                    jobs.len(),
                );
                last_log = Instant::now();
            }
        }

        // Drop quarantined/deleted segments from tracking so GC never tries to unlink
        // the (now renamed) path, and the byte accounting stays accurate.
        if !removed_seqs.is_empty() {
            let removed_bytes: u64 = self
                .files
                .iter()
                .filter(|f| removed_seqs.contains(&f.seq))
                .map(|f| f.bytes_written as u64)
                .sum();
            self.files.retain(|f| !removed_seqs.contains(&f.seq));
            self.total_disk_bytes = self.total_disk_bytes.saturating_sub(removed_bytes);
        }

        // Reserve bytes for each live job's future delete
        self.reserved_bytes = jobs.len() as u64 * STATE_CHANGE_RECORD_SIZE as u64;

        // Create new writable file, opening with the id high-water mark.
        self.max_job_id = self.max_job_id.max(max_id);
        self.create_next_file()?;
        let tombstones: Vec<IdpTombstone> = cooldowns.into_values().collect();

        // Filter orphans against the final job set: a job may have been
        // recreated under the same id within the same WAL, in which case
        // the "orphan" is now live again and must not be deleted.
        let live_body_ids = crate::job::live_external_body_ids(jobs.values().map(Box::as_ref));
        orphan_bodies.retain(|id| !live_body_ids.contains(id));

        // Reduce raw bury events to the final order: keep only the last
        // event of each job that is still buried, preserving WAL
        // (chronological) order across the survivors.
        let mut seen_buried: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut buried_order: Vec<u64> = Vec::new();
        for &id in bury_events.iter().rev() {
            if jobs.get(&id).is_some_and(|j| j.state == JobState::Buried) && seen_buried.insert(id)
            {
                buried_order.push(id);
            }
        }
        buried_order.reverse();
        self.replayed_schedules = schedules;

        Ok((jobs, max_id + 1, tombstones, orphan_bodies, buried_order))
    }

    // --- GC and compaction ---

    pub fn gc(&mut self) {
        // Remove head files with refs == 0, but never the current writable
        // file. Two safety gates:
        //
        // - Durability: a file is only unlinked once the records that
        //   superseded its contents (migration FullJobs, delete
        //   StateChanges) are fsynced — `zero_refs_at_op <= synced_ops`.
        //   Unlinking earlier would destroy the only durable copy of a
        //   live job if we crash before the next sync.
        // - Ordering: stop at the first failed unlink. Files behind the
        //   head may hold the delete records for its jobs; removing them
        //   while the head survives would resurrect deleted jobs on
        //   replay. The head is retried on the next tick.
        while self.files.len() > 1 {
            let unlinkable = self
                .files
                .front()
                .map(|f| f.refs == 0 && f.zero_refs_at_op <= self.synced_ops)
                .unwrap_or(false);
            if !unlinkable {
                break;
            }
            let path = self.files.front().unwrap().path.clone();
            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!("WAL: failed to remove {:?}: {}", path, e);
                break;
            }
            let f = self.files.pop_front().unwrap();
            self.total_disk_bytes = self
                .total_disk_bytes
                .saturating_sub(f.bytes_written as u64);
            // Make the unlink durable before touching the next file, so a
            // crash can't persist a later unlink without this one.
            if let Err(e) = crate::body_store::fsync_dir(&self.dir) {
                tracing::warn!("WAL: dir fsync after GC unlink failed: {}", e);
                break;
            }
        }
    }

    /// Run GC and flush the BufWriter so that GC, stats, and replay see a
    /// consistent view of pending bytes. Tick-driven sync now happens in
    /// the engine task (see `serve()`'s tick branch and `ServerState::
    /// sync_wal`); this method no longer fsyncs.
    pub fn maintain(&mut self) {
        self.gc();
        if let Some(f) = self.files.back_mut()
            && let Some(fd) = f.fd.as_mut()
        {
            // Flush BufWriter into the file so subsequent reads (replay,
            // stats) see a coherent view. fsync is the engine's job.
            let _ = fd.flush();
        }
    }

    /// Last-gasp sync on shutdown. Same body as `sync()`; the wrapper exists
    /// so the engine's shutdown path doesn't need to plumb a `Result`.
    pub fn flush_and_sync(&mut self) {
        if let Err(e) = self.sync() {
            tracing::warn!("WAL flush_and_sync: {}", e);
        }
    }

    /// Test-only: number of `sync_all` calls observed since the WAL was opened.
    #[cfg(test)]
    pub fn sync_count(&self) -> u64 {
        self.sync_count
    }

    /// Close a WAL that has failed without flushing its write buffer.
    /// Dropping it would flush, putting on disk records whose writes were
    /// just reported as failed, and a restart would replay them. What
    /// already reached the kernel can't be called back, so this narrows the
    /// window rather than closing it. Only for a strict-mode WAL, whose
    /// buffer holds nothing acked.
    pub fn abandon(mut self) {
        for f in &mut self.files {
            if let Some(fd) = f.fd.take() {
                let _ = fd.into_parts();
            }
        }
    }

    /// Test-only: make every later write and sync fail, as a full disk would.
    #[cfg(test)]
    pub fn inject_io_failure(&mut self) {
        self.inject_io_failure_after(0);
    }

    /// Test-only: let `ops` more writes and syncs through (those that do
    /// something), then fail every one after.
    #[cfg(test)]
    pub fn inject_io_failure_after(&mut self, ops: u32) {
        self.fail_io_after = Some(ops);
    }

    #[cfg(test)]
    fn injected_failure(&mut self) -> io::Result<()> {
        match self.fail_io_after {
            None => Ok(()),
            Some(0) => Err(io::Error::other("injected WAL I/O failure")),
            Some(n) => {
                self.fail_io_after = Some(n - 1);
                Ok(())
            }
        }
    }
}

// --- Positioned reading ---

/// Read-only iterator over WAL records from a starting [`Position`].
///
/// A `WalReader` opens segment files independently of any live [`Wal`] —
/// it takes no locks and shares no state with the writer, so it can run
/// against a directory the engine is actively appending to. It exists for
/// consumers that need history before they can follow the live
/// [`WalObserver`] tee: catch-up scans, backups, audit tails.
///
/// The segment list is snapshotted at [`open`](WalReader::open); segments
/// created afterwards are not visible. To follow a growing log, drain the
/// reader, then re-open from [`position`](WalReader::position) — re-opening
/// at a boundary the reader reported is exact (no overlap, no gap), and a
/// consumer that also interleaves a live tee dedupes by comparing each
/// teed record's position against its cursor.
///
/// Two ends are distinguished deliberately:
/// - **The durable frontier.** The writer buffers up to `BUF_CAPACITY`
///   bytes in userland, so the newest segment's tail may be absent or
///   half-present in the file. A truncated parse *there* is not
///   corruption — the reader reports "no more records" and its position
///   stays put, ready to resume once the writer flushes.
/// - **Real corruption.** A truncated or bad record in any *sealed*
///   segment (one a newer segment follows) is an error. Unlike
///   [`Wal::replay`], the reader never quarantines or mutates files —
///   diagnosis belongs to the owning process, not an external scanner.
pub struct WalReader {
    dir: PathBuf,
    /// Segments still to visit, ascending. The last entry is the snapshot's
    /// newest — the only one where a truncated tail is benign.
    seqs: VecDeque<u64>,
    current: Option<SegmentCursor>,
    /// Where the next record will be read from; see [`position`](Self::position).
    pos: Position,
}

/// One loaded segment mid-iteration.
struct SegmentCursor {
    seq: u64,
    version: u32,
    data: Vec<u8>,
    offset: usize,
    /// Snapshot-newest: truncation here means "caught up", not corruption.
    newest: bool,
}

impl WalReader {
    /// Open a reader over `dir`, positioned at `from`. Records at
    /// positions `>= from` will be yielded; use [`Position::ZERO`] to read
    /// everything. `from` must be a record boundary previously reported by
    /// this module (an observer tee, a reader, or `ZERO`) — an arbitrary
    /// offset lands mid-record and reads garbage.
    pub fn open(dir: impl Into<PathBuf>, from: Position) -> io::Result<WalReader> {
        let dir = dir.into();
        let seqs: VecDeque<u64> = list_segment_seqs(&dir)?
            .into_iter()
            .filter(|&s| s >= from.seg)
            .collect();
        Ok(WalReader {
            dir,
            seqs,
            current: None,
            pos: from,
        })
    }

    /// The resume cursor: the position of the next record this reader
    /// would yield. After `next_record` returns `Ok(None)` this is where a
    /// later reader should [`open`](Self::open) from — it only ever moves
    /// forward, and it never points into the middle of a record.
    pub fn position(&self) -> Position {
        self.pos
    }

    /// Yield the next record and the position it was read from, or
    /// `Ok(None)` when the snapshot is exhausted (including a buffered
    /// tail not yet flushed by the writer — see the type docs).
    pub fn next_record(&mut self) -> Result<Option<(Position, WalRecord)>, WalError> {
        loop {
            let Some(cur) = self.current.as_mut() else {
                let Some(seq) = self.seqs.pop_front() else {
                    return Ok(None);
                };
                let newest = self.seqs.is_empty();
                let data = match fs::read(segment_path(&self.dir, seq)) {
                    Ok(d) => d,
                    // Compacted away between listing and read: its live
                    // records were migrated to newer segments we'll visit.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(WalError::Io(e)),
                };
                let version = match read_header(&data) {
                    Ok((v, _flags)) => v,
                    // A sub-header newest segment is the benign artifact of
                    // a crash between creation and the header fsync (replay
                    // removes it quietly for the same reason).
                    Err(WalError::Truncated) if newest => return Ok(None),
                    Err(e) => return Err(e),
                };
                // First segment resumes at the cursor; later ones start at
                // the top. ZERO's offset clamps up to the header.
                let start = if seq == self.pos.seg {
                    (self.pos.offset as usize).max(HEADER_SIZE)
                } else {
                    HEADER_SIZE
                };
                self.pos = Position { seg: seq, offset: start as u64 };
                self.current = Some(SegmentCursor { seq, version, data, offset: start, newest });
                continue;
            };

            // `offset > len` happens when resuming at a boundary the writer
            // has not flushed yet — same answer as an exact-end segment:
            // nothing more to read here.
            if cur.offset >= cur.data.len() {
                let last = self.seqs.is_empty();
                self.current = None;
                if last {
                    return Ok(None);
                }
                continue;
            }

            match deserialize_record(&cur.data[cur.offset..], cur.version) {
                Ok((record, consumed)) => {
                    let pos = Position { seg: cur.seq, offset: cur.offset as u64 };
                    cur.offset += consumed;
                    self.pos = Position { seg: cur.seq, offset: cur.offset as u64 };
                    return Ok(Some((pos, record)));
                }
                Err(WalError::Truncated) if cur.newest => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::BodyId;

    /// Switch a test-built job to a synthetic external `BodyRef`. The WAL's
    /// v5 serializer requires `BodyRef::External`; unit tests don't run a
    /// body store, so we fabricate a deterministic id (the job's own id)
    /// and CRC, and assert on them where roundtripping matters.
    fn external_body(job: &mut Job) {
        job.body = BodyRef::External(BodyId(job.id), Some(0xC0FF_EE00));
    }

    fn make_test_job(id: u64, _body: &[u8]) -> Job {
        let mut job = Job::new(
            id,
            100,
            Duration::from_secs(5),
            Duration::from_secs(30),
            Vec::new(),
            "test-tube".to_string(),
        );
        job.reserve_ct = 3;
        job.timeout_ct = 1;
        job.release_ct = 2;
        job.bury_ct = 0;
        job.kick_ct = 1;
        job.ext_mut().idempotency_key = Some(("idem-key".to_string(), 60));
        job.ext_mut().group = Some("group1".to_string());
        job.ext_mut().concurrency_key = Some(("conc".to_string(), 3));
        external_body(&mut job);
        job
    }

    #[test]
    fn test_serialize_deserialize_full_job() {
        let job = make_test_job(42, b"hello world");
        let record = serialize_full_job(&job);
        let (rec, consumed) = deserialize_record(&record, WAL_VERSION).unwrap();
        assert_eq!(consumed, record.len());

        if let WalRecord::FullJob(j) = rec {
            assert_eq!(j.id, 42);
            assert_eq!(j.priority, 100);
            assert_eq!(j.delay, Duration::from_secs(5));
            assert_eq!(j.ttr, Duration::from_secs(30));
            assert!(matches!(&j.body, BodyRef::External(..)));
            assert_eq!(j.tube_name.as_ref(), "test-tube");
            assert_eq!(j.reserve_ct, 3);
            assert_eq!(j.timeout_ct, 1);
            assert_eq!(j.release_ct, 2);
            assert_eq!(j.bury_ct, 0);
            assert_eq!(j.kick_ct, 1);
            assert_eq!(
                j.idempotency_key().cloned(),
                Some(("idem-key".to_string(), 60))
            );
            assert_eq!(j.group().map(|s| s.as_str()), Some("group1"));
            assert!(j.after_group().is_none());
            assert_eq!(j.concurrency_key().cloned(), Some(("conc".to_string(), 3)));
            // Reserved replays as Ready
            assert_eq!(j.state, JobState::Delayed); // original was delayed (delay > 0)
            // The round-trip preserves the BodyId reference and the body's
            // CRC (v9), not the bytes.
            assert!(matches!(
                j.body,
                BodyRef::External(BodyId(42), Some(0xC0FF_EE00))
            ));
        } else {
            panic!("expected FullJob");
        }
    }

    #[test]
    fn test_delayed_job_replays_with_remaining_delay() {
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Delay started an hour ago; total delay 60s ⇒ long expired. On replay
        // the remaining delay must collapse to ~0 (deadline now/past), not reset
        // to a fresh 60s each restart.
        let mut expired = Job::new(
            1,
            0,
            Duration::from_secs(60),
            Duration::from_secs(30),
            Vec::new(),
            "t".to_string(),
        );
        expired.body = BodyRef::new_inline(b"x".to_vec());
        expired.created_at_epoch = now_epoch - 3600;
        let record = serialize_full_job(&expired);
        let (rec, _) = deserialize_record(&record, WAL_VERSION).unwrap();
        let WalRecord::FullJob(j) = rec else {
            panic!("expected FullJob");
        };
        assert_eq!(j.state, JobState::Delayed);
        assert!(
            j.deadline_at.unwrap() <= Instant::now(),
            "expired-delay job must replay ready-soon, not reset to the full delay"
        );

        // A freshly-created delayed job keeps essentially all of its delay.
        let mut fresh = Job::new(
            2,
            0,
            Duration::from_secs(60),
            Duration::from_secs(30),
            Vec::new(),
            "t".to_string(),
        );
        fresh.body = BodyRef::new_inline(b"y".to_vec());
        fresh.created_at_epoch = now_epoch;
        let record = serialize_full_job(&fresh);
        let (rec, _) = deserialize_record(&record, WAL_VERSION).unwrap();
        let WalRecord::FullJob(j) = rec else {
            panic!("expected FullJob");
        };
        assert!(
            j.deadline_at.unwrap() > Instant::now() + Duration::from_secs(30),
            "fresh delayed job must keep most of its delay"
        );
    }

    #[test]
    fn test_serialize_deserialize_full_job_tiny_body() {
        let mut job = make_test_job(7, b"");
        job.body = BodyRef::new_inline(b"uuid-like".to_vec());
        assert!(matches!(job.body, BodyRef::Tiny { len: 9, .. }));

        let record = serialize_full_job(&job);
        let (rec, consumed) = deserialize_record(&record, WAL_VERSION).unwrap();
        assert_eq!(consumed, record.len());

        let WalRecord::FullJob(j) = rec else {
            panic!("expected FullJob");
        };
        assert!(matches!(j.body, BodyRef::Tiny { len: 9, .. }));
        assert_eq!(j.body.as_inline_bytes(), Some(&b"uuid-like"[..]));
    }

    #[test]
    fn test_serialize_deserialize_full_job_heap_body() {
        let mut job = make_test_job(8, b"");
        let payload = vec![0xABu8; 64];
        job.body = BodyRef::new_inline(payload.clone());
        assert!(matches!(job.body, BodyRef::Heap(_)));

        let record = serialize_full_job(&job);
        let (rec, consumed) = deserialize_record(&record, WAL_VERSION).unwrap();
        assert_eq!(consumed, record.len());

        let WalRecord::FullJob(j) = rec else {
            panic!("expected FullJob");
        };
        assert!(matches!(j.body, BodyRef::Heap(_)));
        assert_eq!(j.body.as_inline_bytes(), Some(payload.as_slice()));
    }

    #[test]
    fn test_serialize_deserialize_full_job_empty_body() {
        let mut job = make_test_job(9, b"");
        job.body = BodyRef::new_inline(Vec::new());
        assert!(matches!(job.body, BodyRef::Tiny { len: 0, .. }));

        let record = serialize_full_job(&job);
        let (rec, _) = deserialize_record(&record, WAL_VERSION).unwrap();

        let WalRecord::FullJob(j) = rec else {
            panic!("expected FullJob");
        };
        assert!(matches!(j.body, BodyRef::Tiny { len: 0, .. }));
        assert_eq!(j.body.as_inline_bytes(), Some(&[][..]));
    }

    #[test]
    fn test_serialize_deserialize_state_change() {
        let record = serialize_state_change(
            99,
            Some(JobState::Buried),
            500,
            0,
            0,
            StateChangeReason::Bury,
            1_700_000_000,
        );
        assert_eq!(record.len(), STATE_CHANGE_RECORD_SIZE);

        let (rec, consumed) = deserialize_record(&record, WAL_VERSION).unwrap();
        assert_eq!(consumed, STATE_CHANGE_RECORD_SIZE);

        if let WalRecord::StateChange {
            job_id,
            new_state,
            new_priority,
            new_delay_nanos,
            expiry_epoch_secs,
            reason,
            change_epoch_secs,
        } = rec
        {
            assert_eq!(job_id, 99);
            assert_eq!(new_state, Some(JobState::Buried));
            assert_eq!(new_priority, 500);
            assert_eq!(new_delay_nanos, 0);
            assert_eq!(expiry_epoch_secs, 0);
            assert_eq!(reason, StateChangeReason::Bury);
            assert_eq!(change_epoch_secs, 1_700_000_000);
        } else {
            panic!("expected StateChange");
        }
    }

    #[test]
    fn test_serialize_deserialize_state_change_deleted() {
        let record = serialize_state_change(77, None, 0, 0, 0, StateChangeReason::None, 0);
        let (rec, _) = deserialize_record(&record, WAL_VERSION).unwrap();

        if let WalRecord::StateChange { new_state, .. } = rec {
            assert!(new_state.is_none());
        } else {
            panic!("expected StateChange");
        }
    }

    #[test]
    fn test_crc32_validation() {
        let mut record =
            serialize_state_change(99, Some(JobState::Ready), 0, 0, 0, StateChangeReason::None, 0);
        // Corrupt a byte in the middle
        record[5] ^= 0xFF;
        let result = deserialize_record(&record, WAL_VERSION);
        assert!(matches!(result, Err(WalError::BadCrc)));
    }

    #[test]
    fn test_file_header_roundtrip() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();
        assert_eq!(buf.len(), HEADER_SIZE);
        read_header(&buf).unwrap();
    }

    #[test]
    fn test_invalid_magic() {
        let buf = b"XXXX\x01\x00\x00\x00\x00\x00\x00\x00";
        assert!(matches!(read_header(buf), Err(WalError::BadMagic)));
    }

    #[test]
    fn test_option_string_encoding() {
        let mut buf = Vec::new();

        // None
        write_option_string(&mut buf, None);
        let mut off = 0;
        assert_eq!(read_option_string(&buf, &mut off).unwrap(), None);

        // Some("")
        buf.clear();
        write_option_string(&mut buf, Some(""));
        off = 0;
        assert_eq!(read_option_string(&buf, &mut off).unwrap(), None); // empty = len 0 = None

        // Some("abc")
        buf.clear();
        write_option_string(&mut buf, Some("abc"));
        off = 0;
        assert_eq!(
            read_option_string(&buf, &mut off).unwrap(),
            Some("abc".to_string())
        );
    }

    #[test]
    fn test_state_encoding_roundtrip() {
        for state in [
            JobState::Ready,
            JobState::Reserved,
            JobState::Delayed,
            JobState::Buried,
        ] {
            let byte = state_to_u8(state);
            assert_eq!(u8_to_state(byte), Some(state));
        }
        assert_eq!(u8_to_state(STATE_DELETED), None);
    }

    #[test]
    fn test_estimate_full_job_size() {
        let job = make_test_job(1, b"data");
        let estimated = estimate_full_job_size(&job);
        let actual = serialize_full_job(&job).len();
        assert_eq!(estimated, actual);
    }

    // --- Corruption / edge-case tests ---

    #[test]
    fn test_truncated_full_job_header() {
        // Only 5 bytes — not enough for the 13-byte header
        let data = vec![RECORD_TYPE_FULL_JOB, 1, 2, 3, 4];
        assert!(matches!(
            deserialize_record(&data, WAL_VERSION),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_truncated_full_job_payload() {
        // Valid header claiming a large payload, but data is short
        let mut data = vec![RECORD_TYPE_FULL_JOB];
        data.extend_from_slice(&42u64.to_le_bytes()); // job_id
        data.extend_from_slice(&1000u32.to_le_bytes()); // payload_len = 1000
        // Only 13 bytes total, nowhere near 1000 + 4 CRC
        assert!(matches!(
            deserialize_record(&data, WAL_VERSION),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_corrupted_full_job_crc() {
        let job = make_test_job(1, b"hello");
        let mut record = serialize_full_job(&job);
        // Corrupt the last byte (CRC)
        let len = record.len();
        record[len - 1] ^= 0xFF;
        assert!(matches!(deserialize_record(&record, WAL_VERSION), Err(WalError::BadCrc)));
    }

    #[test]
    fn test_corrupted_full_job_body() {
        let job = make_test_job(1, b"hello");
        let mut record = serialize_full_job(&job);
        // Corrupt a payload byte (not the CRC itself)
        record[20] ^= 0xFF;
        assert!(matches!(deserialize_record(&record, WAL_VERSION), Err(WalError::BadCrc)));
    }

    #[test]
    fn test_truncated_state_change() {
        // Too short for STATE_CHANGE_RECORD_SIZE
        let data = vec![RECORD_TYPE_STATE_CHANGE, 0, 0, 0];
        assert!(matches!(
            deserialize_record(&data, WAL_VERSION),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_corrupted_state_change_crc() {
        let mut record =
            serialize_state_change(1, Some(JobState::Ready), 100, 0, 0, StateChangeReason::None, 0);
        let len = record.len();
        record[len - 2] ^= 0xFF;
        assert!(matches!(deserialize_record(&record, WAL_VERSION), Err(WalError::BadCrc)));
    }

    #[test]
    fn test_invalid_state_byte_in_full_job() {
        let job = make_test_job(1, b"x");
        let mut record = serialize_full_job(&job);
        // The state byte is at payload offset 4+8+8+8 = 28 from payload start.
        // Payload starts at byte 13 of the record. So state byte is at 13+28 = 41.
        let state_offset = 13 + 4 + 8 + 8 + 8; // = 41
        record[state_offset] = 0xEE; // invalid state
        // Recompute CRC: everything before last 4 bytes
        let crc_offset = record.len() - 4;
        let crc = crc32fast::hash(&record[..crc_offset]);
        record[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            deserialize_record(&record, WAL_VERSION),
            Err(WalError::InvalidData)
        ));
    }

    #[test]
    fn test_invalid_state_byte_in_state_change() {
        let mut record =
            serialize_state_change(1, Some(JobState::Ready), 0, 0, 0, StateChangeReason::None, 0);
        // State byte is at offset 13 in state change record
        record[13] = 0xEE; // invalid state (not 0-3 or 0xFF)
        // Recompute CRC
        let crc_offset = record.len() - 4;
        let crc = crc32fast::hash(&record[..crc_offset]);
        record[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            deserialize_record(&record, WAL_VERSION),
            Err(WalError::InvalidData)
        ));
    }

    #[test]
    fn test_unknown_record_type() {
        let data = vec![0xFF; STATE_CHANGE_RECORD_SIZE];
        assert!(matches!(
            deserialize_record(&data, WAL_VERSION),
            Err(WalError::UnknownRecordType(0xFF))
        ));
    }

    #[test]
    fn test_empty_data() {
        assert!(matches!(deserialize_record(&[], WAL_VERSION), Err(WalError::Truncated)));
    }

    #[test]
    fn test_bad_version_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(WAL_MAGIC);
        buf.extend_from_slice(&99u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // flags
        assert!(matches!(read_header(&buf), Err(WalError::BadVersion(99))));
    }

    #[test]
    fn test_truncated_header() {
        assert!(matches!(
            read_header(b"TW"),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_replay_skips_corrupted_records() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write two valid jobs, then corrupt the file by appending garbage
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job1);
            wal.write_put(&mut job1).unwrap();

            let mut job2 = Job::new(
                2,
                20,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body2".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job2);
            wal.write_put(&mut job2).unwrap();
        }

        // Append garbage to the WAL file to simulate a partial/corrupted write
        let wal_file = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap();
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(wal_file.path())
                .unwrap();
            f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01]).unwrap();
        }

        // Replay should recover both valid jobs, skip the garbage
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, next_id, _, _, _) = wal.replay().unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(jobs.contains_key(&1));
        assert!(jobs.contains_key(&2));
        assert!(next_id >= 3);
    }

    #[test]
    fn test_replay_quarantines_corrupt_segment() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // One durable job in a segment.
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
        }

        let seg = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap()
            .path();

        // Corrupt the segment's magic so its header fails to parse.
        {
            let mut f = OpenOptions::new().write(true).open(&seg).unwrap();
            f.write_all(b"XXXX").unwrap();
        }

        // Replay must quarantine the segment (preserve it), not skip-then-GC it.
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, _, _, _, _) = wal.replay().unwrap();
        assert!(jobs.is_empty(), "corrupt segment's jobs are not loaded");
        assert!(!seg.exists(), "corrupt segment must be renamed away");
        assert!(
            corrupt_sidecar_path(&seg).exists(),
            "segment must be preserved as .corrupt for recovery"
        );

        // GC must not choke on the (now untracked) quarantined file.
        wal.gc();
    }

    #[test]
    fn test_replay_removes_headerless_newest_segment_quietly() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // One durable job in segment 1.
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
        }

        // Simulate a crash between segment creation and the header fsync:
        // the newest segment exists but is shorter than the header.
        let stub = dir_path.join(format!("{}{:06}", FILE_PREFIX, 2));
        fs::write(&stub, b"TB").unwrap();

        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, _, _, _, _) = wal.replay().unwrap();
        assert_eq!(jobs.len(), 1, "jobs from older segments are unaffected");
        assert!(!stub.exists(), "headerless newest segment is removed");
        assert!(
            !corrupt_sidecar_path(&stub).exists(),
            "a benign creation-crash artifact must not mint a .corrupt sidecar"
        );
    }

    #[test]
    fn test_tombstone_and_id_mark_records_round_trip() {
        let record = serialize_tombstone(42, "emails", "nightly", 1_900_000_000);
        match deserialize_record(&record, WAL_VERSION).unwrap() {
            (
                WalRecord::Tombstone {
                    job_id,
                    tube,
                    key,
                    expires_epoch_secs,
                },
                consumed,
            ) => {
                assert_eq!(
                    (job_id, tube.as_str(), key.as_str()),
                    (42, "emails", "nightly")
                );
                assert_eq!(expires_epoch_secs, 1_900_000_000);
                assert_eq!(consumed, record.len());
            }
            other => panic!("expected a tombstone, got {other:?}"),
        }
        let mark = serialize_id_mark(99);
        assert_eq!(mark.len(), ID_MARK_RECORD_SIZE);
        assert!(matches!(
            deserialize_record(&mark, WAL_VERSION).unwrap(),
            (WalRecord::IdMark { high_water: 99 }, ID_MARK_RECORD_SIZE)
        ));
    }

    #[test]
    fn test_v7_segments_do_not_accept_v8_record_types() {
        let record = serialize_tombstone(1, "default", "k", 1_900_000_000);
        assert!(matches!(
            deserialize_record(&record, 7),
            Err(WalError::UnknownRecordType(RECORD_TYPE_TOMBSTONE))
        ));
        assert!(matches!(
            deserialize_record(&serialize_id_mark(1), 7),
            Err(WalError::UnknownRecordType(RECORD_TYPE_ID_MARK))
        ));
    }

    fn epoch_in(secs: i64) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        (now + secs) as u64
    }

    #[test]
    fn test_replay_keeps_the_latest_tombstone_for_each_key() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            wal.write_tombstone("default", "a", 1, epoch_in(3600))
                .unwrap();
            wal.write_tombstone("default", "a", 2, epoch_in(7200))
                .unwrap();
            wal.write_tombstone("default", "b", 3, epoch_in(3600))
                .unwrap();
            wal.write_tombstone("default", "b", 3, 0).unwrap(); // cleared
            wal.write_tombstone("default", "c", 4, epoch_in(-10))
                .unwrap(); // expired
            wal.write_tombstone("other", "a", 5, epoch_in(3600))
                .unwrap();
            wal.sync().unwrap();
        }
        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        let (_, next_id, tombstones, ..) = wal.replay().unwrap();
        let mut got: Vec<(String, String, u64)> = tombstones
            .iter()
            .map(|t| (t.tube_name.to_string(), t.key.clone(), t.job_id))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("default".to_string(), "a".to_string(), 2),
                ("other".to_string(), "a".to_string(), 5)
            ]
        );
        assert!(tombstones.iter().all(|t| t.record.is_some()));
        assert_eq!(
            next_id, 6,
            "tombstone job ids count toward the high-water mark"
        );
    }

    #[test]
    fn test_live_tombstone_pins_its_segment_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1)).unwrap();
        let record = wal
            .write_tombstone("default", "k", 1, epoch_in(3600))
            .unwrap()
            .expect("a live tombstone holds a reference");
        let mut job = make_test_job(2, b"x");
        external_body(&mut job);
        wal.write_put(&mut job).unwrap();
        wal.write_state_change(
            &mut job,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        wal.sync().unwrap();
        wal.gc();
        assert!(
            segment(dir.path(), record.seq).exists(),
            "pinned by the live tombstone"
        );

        wal.release_record(record);
        wal.write_put(&mut make_test_job(3, b"y")).unwrap();
        wal.sync().unwrap();
        wal.gc();
        assert!(
            !segment(dir.path(), record.seq).exists(),
            "collected once released"
        );
    }

    #[test]
    fn test_id_mark_keeps_the_next_id_once_older_segments_are_gone() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            let mut job = make_test_job(9, b"x");
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            wal.write_state_change(
                &mut job,
                None,
                0,
                Duration::ZERO,
                0,
                StateChangeReason::None,
            )
            .unwrap();
            wal.sync().unwrap();
        }
        {
            // Replay opens a new segment; every older one holds nothing live.
            let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            let (_, next_id, ..) = wal.replay().unwrap();
            assert_eq!(next_id, 10);
            wal.gc();
            assert_eq!(wal.file_count(), 1, "only the new segment is left");
        }
        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        let (_, next_id, ..) = wal.replay().unwrap();
        assert_eq!(next_id, 10, "the id mark carries the high-water mark");
    }

    #[test]
    fn test_v9_records_round_trip_and_v8_segments_refuse_them() {
        let point = DurablePoint {
            seq: 3,
            offset: 4096,
        };
        let mark = serialize_toast_mark(point);
        assert_eq!(mark.len(), TOAST_MARK_RECORD_SIZE);
        assert!(matches!(
            deserialize_record(&mark, WAL_VERSION).unwrap(),
            (WalRecord::ToastMark(p), TOAST_MARK_RECORD_SIZE) if p == point
        ));
        assert!(matches!(
            deserialize_record(&mark, 8),
            Err(WalError::UnknownRecordType(RECORD_TYPE_TOAST_MARK))
        ));
        // A body CRC (kind 2) is v9 only; a v8 record has none (kind 1).
        assert!(matches!(
            deserialize_record(&serialize_full_job(&make_test_job(5, b"")), 8),
            Err(WalError::InvalidData)
        ));
        let mut legacy = make_test_job(6, b"");
        legacy.body = BodyRef::External(BodyId(6), None);
        match deserialize_record(&serialize_full_job(&legacy), 8).unwrap() {
            (WalRecord::FullJob(j), _) => {
                assert!(matches!(j.body, BodyRef::External(BodyId(6), None)))
            }
            other => panic!("expected a FullJob, got {other:?}"),
        }
    }

    fn ts(s: &str) -> jiff::Timestamp {
        s.parse().unwrap()
    }

    fn schedule_record(line: &str, last_fire: Option<&str>) -> ScheduleRecord {
        ScheduleRecord {
            line: line.to_string(),
            body: b"{}".to_vec(),
            synced_at: ts("2026-10-11T03:00:30Z"),
            last_fire: last_fire.map(ts),
        }
    }

    #[test]
    fn test_schedule_records_round_trip_and_v8_segments_refuse_them() {
        let entries = vec![
            schedule_record(
                "rollup maint 1024 0 120 2 idp:rollup 5 * * * *",
                Some("2026-10-11T03:05:00.25Z"),
            ),
            schedule_record(
                "retention maint 10 5 60 2 catchup:600 0 2 * * * Australia/Melbourne",
                None,
            ),
        ];
        let set = serialize_schedule_set("splat", &entries);
        match deserialize_record(&set, WAL_VERSION).unwrap() {
            (WalRecord::ScheduleSet { owner, entries: got }, len) => {
                assert_eq!(owner, "splat");
                assert_eq!(got, entries);
                assert_eq!(len, set.len());
            }
            other => panic!("expected a ScheduleSet, got {other:?}"),
        }

        let fired = serialize_schedule_fired("splat", "rollup", Some(ts("2026-10-11T04:05:00Z")));
        match deserialize_record(&fired, WAL_VERSION).unwrap() {
            (
                WalRecord::ScheduleFired {
                    owner,
                    name,
                    last_fire,
                },
                len,
            ) => {
                assert_eq!((owner.as_str(), name.as_str()), ("splat", "rollup"));
                assert_eq!(last_fire, Some(ts("2026-10-11T04:05:00Z")));
                assert_eq!(len, fired.len());
            }
            other => panic!("expected a ScheduleFired, got {other:?}"),
        }
        // A fire put back: the entry hasn't fired.
        let unfired = serialize_schedule_fired("splat", "rollup", None);
        assert!(matches!(
            deserialize_record(&unfired, WAL_VERSION).unwrap(),
            (WalRecord::ScheduleFired { last_fire: None, .. }, _)
        ));

        assert!(matches!(
            deserialize_record(&set, 8),
            Err(WalError::UnknownRecordType(RECORD_TYPE_SCHEDULE_SET))
        ));
        assert!(matches!(
            deserialize_record(&fired, 8),
            Err(WalError::UnknownRecordType(RECORD_TYPE_SCHEDULE_FIRED))
        ));
    }

    #[test]
    fn test_replay_keeps_each_owner_s_latest_schedule_set_and_the_stamps_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let a = schedule_record("a jobs 0 0 60 2 @hourly", None);
        let b = schedule_record("b jobs 0 0 60 2 @daily", None);
        let a_fired = schedule_record("a jobs 0 0 60 2 @hourly", Some("2026-10-11T04:00:00Z"));
        {
            let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            wal.write_schedule_set("splat", &[a, b.clone()]).unwrap();
            // Superseded by the next set, which carries its own last fires.
            wal.write_schedule_fired("splat", "b", Some(ts("2026-10-11T04:00:00Z")))
                .unwrap();
            wal.write_schedule_set("splat", std::slice::from_ref(&a_fired))
                .unwrap();
            wal.write_schedule_fired("splat", "a", Some(ts("2026-10-11T05:00:00Z")))
                .unwrap();
            wal.write_schedule_set("booko", &[b]).unwrap();
            wal.write_schedule_set::<Vec<u8>>("booko", &[]).unwrap();
            wal.sync().unwrap();
        }
        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        wal.replay().unwrap();
        let replayed = wal.take_replayed_schedules();
        assert!(replayed.seen);
        assert_eq!(replayed.sets.len(), 1, "booko removed its entries");
        assert_eq!(replayed.sets["splat"].0, vec![a_fired]);
        assert_eq!(replayed.stamps.len(), 1, "b's stamp went with its set");
        assert_eq!(
            replayed.stamps[&("splat".to_string(), "a".to_string())].0,
            Some(ts("2026-10-11T05:00:00Z"))
        );
    }

    #[test]
    fn test_a_wal_without_schedule_records_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        wal.write_put(&mut make_test_job(1, b"x")).unwrap();
        wal.sync().unwrap();
        drop(wal);
        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        wal.replay().unwrap();
        assert!(!wal.take_replayed_schedules().seen);
    }

    #[test]
    fn test_live_schedule_records_pin_their_segments_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1)).unwrap();
        let set = wal
            .write_schedule_set("splat", &[schedule_record("a jobs 0 0 60 2 @hourly", None)])
            .unwrap()
            .expect("a set with entries holds a reference");
        let stamp = wal
            .write_schedule_fired("splat", "a", Some(ts("2026-10-11T04:00:00Z")))
            .unwrap();
        let mut job = make_test_job(2, b"x");
        external_body(&mut job);
        wal.write_put(&mut job).unwrap();
        wal.write_state_change(
            &mut job,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        wal.sync().unwrap();
        wal.gc();
        assert!(segment(dir.path(), set.seq).exists(), "pinned by the set");
        assert!(segment(dir.path(), stamp.seq).exists(), "pinned by the stamp");

        wal.release_record(set);
        wal.release_record(stamp);
        assert!(
            wal.write_schedule_set::<Vec<u8>>("splat", &[]).unwrap().is_none(),
            "an owner's removal holds none"
        );
        wal.write_put(&mut make_test_job(3, b"y")).unwrap();
        wal.sync().unwrap();
        wal.gc();
        assert!(!segment(dir.path(), set.seq).exists(), "collected once released");
        assert!(!segment(dir.path(), stamp.seq).exists(), "collected once released");
    }

    /// A WAL with a body store attached, as the server runs it.
    fn wal_with_body_store(dir: &Path) -> (Wal, Arc<BodyStore>) {
        let bs = Arc::new(BodyStore::open(&dir.join("toast"), 4096).unwrap());
        let mut wal = Wal::open(dir, WalConfig::default()).unwrap();
        wal.set_body_store(Arc::clone(&bs));
        (wal, bs)
    }

    /// Put job `id` with a 300-byte TOAST body.
    fn put_with_toast_body(wal: &mut Wal, bs: &BodyStore, id: u64) -> Job {
        let (body_id, crc) = bs.write_body_with_crc(&[id as u8; 300]).unwrap();
        let mut job = make_test_job(id, b"");
        job.body = BodyRef::External(body_id, Some(crc));
        wal.write_put(&mut job).unwrap();
        job
    }

    #[test]
    fn test_sync_records_how_far_toast_was_durable() {
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, bs) = wal_with_body_store(dir.path());
        put_with_toast_body(&mut wal, &bs, 1);
        wal.sync().unwrap();
        let first = bs.synced_point();
        assert!(first.is_some());
        assert_eq!(wal.read_toast_mark(), first);

        put_with_toast_body(&mut wal, &bs, 2);
        wal.sync().unwrap();
        assert!(bs.synced_point() > first);
        assert_eq!(
            wal.read_toast_mark(),
            bs.synced_point(),
            "the latest mark wins"
        );
    }

    #[test]
    fn test_toast_fsyncs_without_a_wal_write_still_get_a_mark() {
        // Compaction appends the bodies it moves and fsyncs them, but writes
        // nothing to the WAL, so an idle WAL never synced again and left
        // them past the recovery point.
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, bs) = wal_with_body_store(dir.path());
        put_with_toast_body(&mut wal, &bs, 1);
        wal.sync().unwrap();
        bs.write_body(&[7; 300]).unwrap();
        bs.fsync().unwrap();
        assert!(!wal.is_dirty());

        wal.note_toast_progress();
        assert!(wal.is_dirty());
        wal.sync().unwrap();
        assert_eq!(wal.read_toast_mark(), bs.synced_point());
    }

    #[test]
    fn test_toast_mark_survives_older_segments_being_collected() {
        let dir = tempfile::tempdir().unwrap();
        let point = {
            let (mut wal, bs) = wal_with_body_store(dir.path());
            let mut job = put_with_toast_body(&mut wal, &bs, 1);
            wal.write_state_change(
                &mut job,
                None,
                0,
                Duration::ZERO,
                0,
                StateChangeReason::None,
            )
            .unwrap();
            wal.sync().unwrap();
            bs.synced_point()
        };
        {
            // As startup does: the new segment replay opens repeats the mark.
            let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            wal.set_toast_mark(wal.read_toast_mark());
            wal.replay().unwrap();
            wal.gc();
            assert_eq!(wal.file_count(), 1, "only the new segment is left");
        }
        let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        assert_eq!(wal.read_toast_mark(), point);
    }

    /// Job 1 with a TOAST body, synced so its segment holds a mark; then a
    /// restart whose new segment opens with `opening_mark`. The point the
    /// first sync reached, and the new segment's path.
    fn restart_with_toast_mark(
        dir: &Path,
        opening_mark: impl FnOnce(&Wal) -> Option<DurablePoint>,
    ) -> (Option<DurablePoint>, PathBuf) {
        let point = {
            let (mut wal, bs) = wal_with_body_store(dir);
            put_with_toast_body(&mut wal, &bs, 1);
            wal.sync().unwrap();
            bs.synced_point()
        };
        assert!(point.is_some());
        let mut wal = Wal::open(dir, WalConfig::default()).unwrap();
        wal.set_toast_mark(opening_mark(&wal));
        wal.replay().unwrap();
        wal.sync().unwrap();
        assert_eq!(wal.file_count(), 2, "job 1 keeps the first segment");
        (point, wal.files.back().unwrap().path.clone())
    }

    #[test]
    fn test_read_toast_mark_stops_at_the_newest_segment() {
        // Every segment opens with the mark current when it was made, so a
        // newest segment without one means there is none: as at a start
        // that found the body store empty. Looking further read the whole
        // WAL, and a store whose bodies all stay inline never has a mark.
        let dir = tempfile::tempdir().unwrap();
        restart_with_toast_mark(dir.path(), |_| None);

        let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        assert_eq!(wal.read_toast_mark(), None);
    }

    #[test]
    fn test_read_toast_mark_looks_past_a_newest_segment_damaged_at_its_start() {
        // Its opening records don't read, so they can't say there's no mark.
        let dir = tempfile::tempdir().unwrap();
        let (point, newest) = restart_with_toast_mark(dir.path(), |wal| wal.read_toast_mark());
        let mut data = fs::read(&newest).unwrap();
        data[HEADER_SIZE + ID_MARK_RECORD_SIZE - 1] ^= 0xFF;
        fs::write(&newest, &data).unwrap();

        let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        assert_eq!(wal.read_toast_mark(), point);
    }

    #[test]
    fn test_replay_refuses_truncated_older_segment() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // One durable job in segment 1.
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
        }

        let seg1 = dir_path.join(format!("{}{:06}", FILE_PREFIX, 1));
        assert!(seg1.exists());

        // A newer segment exists, so segment 1 was once fsynced past its
        // header. Truncate it below HEADER_SIZE to simulate a filesystem
        // fault eating its records.
        let seg2 = dir_path.join(format!("{}{:06}", FILE_PREFIX, 2));
        let mut seg2_header = Vec::new();
        write_header(&mut seg2_header).unwrap();
        fs::write(&seg2, seg2_header).unwrap();
        {
            let f = OpenOptions::new().write(true).open(&seg1).unwrap();
            f.set_len(3).unwrap();
        }

        // The truncated older segment is data loss: replay must stop, and
        // leave it exactly as found.
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let msg = replay_refusal(&mut wal);
        assert!(msg.contains("binlog.000001"), "{msg}");
        assert_eq!(
            fs::metadata(&seg1).unwrap().len(),
            3,
            "segment left as found"
        );
        assert!(!corrupt_sidecar_path(&seg1).exists());
    }

    /// Four segments of one record each: put 1, put 2, delete 1, put 3. A
    /// clean replay leaves jobs 2 and 3.
    fn write_one_record_per_segment(dir: &Path) {
        let mut wal = Wal::open(dir, WalConfig::with_max_file_size(1)).unwrap();
        let mut jobs: Vec<Job> = (1..=3)
            .map(|id| {
                let mut job = Job::new(
                    id,
                    10,
                    Duration::ZERO,
                    Duration::from_secs(60),
                    b"body".to_vec(),
                    "default".to_string(),
                );
                external_body(&mut job);
                job
            })
            .collect();
        wal.write_put(&mut jobs[0]).unwrap();
        wal.write_put(&mut jobs[1]).unwrap();
        wal.write_state_change(
            &mut jobs[0],
            None,
            10,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        wal.write_put(&mut jobs[2]).unwrap();
        wal.sync().unwrap();
        assert_eq!(wal.file_count(), 4, "one record per segment");
    }

    fn segment(dir: &Path, seq: u64) -> PathBuf {
        dir.join(format!("{}{:06}", FILE_PREFIX, seq))
    }

    /// Replay `wal`, which must refuse; the refusal's message.
    fn replay_refusal(wal: &mut Wal) -> String {
        match wal.replay() {
            Ok((jobs, ..)) => panic!("replay went ahead, with jobs {:?}", jobs.keys()),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("--wal-skip-corrupt"),
                    "names the way out: {msg}"
                );
                msg
            }
        }
    }

    #[test]
    fn test_replay_refuses_corrupt_record_in_older_segment() {
        // Segment 3 holds job 1's delete. Skipping it would bring job 1
        // back, so replay must stop rather than truncate and carry on.
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg3 = segment(dir.path(), 3);
        let mut data = fs::read(&seg3).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&seg3, &data).unwrap();

        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        let msg = replay_refusal(&mut wal);
        assert!(msg.contains("binlog.000003"), "{msg}");
        // The record after the segment's opening id mark.
        let offset = HEADER_SIZE + ID_MARK_RECORD_SIZE;
        assert!(msg.contains(&format!("offset {offset}")), "{msg}");
        assert_eq!(fs::read(&seg3).unwrap(), data, "segment left as found");
        assert!(!corrupt_sidecar_path(&seg3).exists());
    }

    #[test]
    fn test_replay_refuses_bad_header_in_older_segment() {
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg1 = segment(dir.path(), 1);
        let mut data = fs::read(&seg1).unwrap();
        data[..4].copy_from_slice(b"XXXX");
        fs::write(&seg1, &data).unwrap();

        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        let msg = replay_refusal(&mut wal);
        assert!(msg.contains("binlog.000001"), "{msg}");
        assert_eq!(fs::read(&seg1).unwrap(), data, "segment left as found");
        assert!(!corrupt_sidecar_path(&seg1).exists());
    }

    #[test]
    fn test_crash_recovery_then_a_new_segment_does_not_refuse_the_next_restart() {
        // A torn tail in the newest segment is truncated on replay. The next
        // write starts a new segment, so the repaired one becomes an older
        // segment, and the restart after that must not mistake it for damage.
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg4 = segment(dir.path(), 4);
        OpenOptions::new()
            .append(true)
            .open(&seg4)
            .unwrap()
            .write_all(&[0xDE, 0xAD, 0xBE])
            .unwrap();

        {
            let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            let (mut jobs, ..) = wal.replay().unwrap();
            let job = jobs.get_mut(&3).unwrap();
            wal.write_state_change(job, None, 10, Duration::ZERO, 0, StateChangeReason::None)
                .unwrap();
            wal.sync().unwrap();
            assert_eq!(wal.file_count(), 5, "the write started segment 5");
        }

        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        let (jobs, ..) = wal.replay().unwrap();
        assert_eq!(jobs.keys().copied().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn test_replay_skip_corrupt_keeps_a_copy_and_truncates_older_segment() {
        // The override restores the old behaviour, with its cost: job 1's
        // delete is lost, so job 1 comes back.
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg3 = segment(dir.path(), 3);
        let mut data = fs::read(&seg3).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&seg3, &data).unwrap();

        let config = WalConfig {
            skip_corrupt: true,
            ..WalConfig::default()
        };
        let mut wal = Wal::open(dir.path(), config).unwrap();
        let (jobs, ..) = wal.replay().unwrap();
        let mut ids: Vec<u64> = jobs.keys().copied().collect();
        ids.sort();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "job 1's delete was in the damaged record"
        );
        assert_eq!(fs::read(corrupt_sidecar_path(&seg3)).unwrap(), data);
        let kept = HEADER_SIZE + ID_MARK_RECORD_SIZE;
        assert_eq!(fs::metadata(&seg3).unwrap().len(), kept as u64);
    }

    #[test]
    fn test_replay_skip_corrupt_refuses_an_older_segment_it_cannot_copy() {
        // Cutting it without a copy would destroy the records past the
        // damage, which may still be recoverable. A directory at the copy's
        // path stands in for a full disk.
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg3 = segment(dir.path(), 3);
        let mut data = fs::read(&seg3).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&seg3, &data).unwrap();
        let copy = corrupt_sidecar_path(&seg3);
        fs::create_dir(&copy).unwrap();
        let config = WalConfig {
            skip_corrupt: true,
            ..WalConfig::default()
        };

        let mut wal = Wal::open(dir.path(), config.clone()).unwrap();
        let msg = replay_refusal(&mut wal);
        assert!(msg.contains("binlog.000003"), "{msg}");
        assert!(msg.contains("couldn't keep a copy"), "{msg}");
        assert!(fs::read(&seg3).unwrap() == data, "left whole");
        drop(wal);

        // Once the copy can be written, the same switch gets past it.
        fs::remove_dir(&copy).unwrap();
        let mut wal = Wal::open(dir.path(), config).unwrap();
        wal.replay().unwrap();
        assert!(fs::read(&copy).unwrap() == data, "the copy is whole");
        let kept = HEADER_SIZE + ID_MARK_RECORD_SIZE;
        assert_eq!(fs::metadata(&seg3).unwrap().len(), kept as u64);
    }

    #[test]
    fn test_replay_cuts_a_torn_newest_segment_without_a_copy() {
        // Appends continue past a torn tail, so it is cut whether or not
        // its copy is kept.
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg4 = segment(dir.path(), 4);
        let whole = fs::metadata(&seg4).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&seg4)
            .unwrap()
            .write_all(&[0xDE, 0xAD, 0xBE])
            .unwrap();
        fs::create_dir(corrupt_sidecar_path(&seg4)).unwrap();

        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        wal.replay().unwrap();
        assert_eq!(fs::metadata(&seg4).unwrap().len(), whole);
    }

    #[test]
    fn test_replay_skip_corrupt_quarantines_truncated_older_segment() {
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg1 = segment(dir.path(), 1);
        OpenOptions::new()
            .write(true)
            .open(&seg1)
            .unwrap()
            .set_len(3)
            .unwrap();

        let config = WalConfig {
            skip_corrupt: true,
            ..WalConfig::default()
        };
        let mut wal = Wal::open(dir.path(), config).unwrap();
        let (jobs, ..) = wal.replay().unwrap();
        let mut ids: Vec<u64> = jobs.keys().copied().collect();
        ids.sort();
        assert_eq!(ids, vec![2, 3]);
        assert!(!seg1.exists(), "truncated segment must be renamed away");
        assert!(
            corrupt_sidecar_path(&seg1).exists(),
            "truncated older segment must be quarantined, not deleted"
        );
        // GC must not choke on the (now untracked) quarantined file.
        wal.gc();
    }

    #[test]
    fn test_replay_refuses_unreadable_segment() {
        // A directory where segment 2 should be fails the read, standing in
        // for EIO or a permissions mistake. Neither is a crash artifact, and
        // setting the segment aside would drop records that are fine.
        let dir = tempfile::tempdir().unwrap();
        write_one_record_per_segment(dir.path());
        let seg2 = segment(dir.path(), 2);
        fs::remove_file(&seg2).unwrap();
        fs::create_dir(&seg2).unwrap();

        let mut wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        let msg = replay_refusal(&mut wal);
        assert!(msg.contains("binlog.000002"), "{msg}");
        assert!(seg2.is_dir(), "segment left as found");
        assert!(!corrupt_sidecar_path(&seg2).exists());
    }

    #[test]
    fn test_replay_returns_bury_order_not_id_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1024 * 1024)).unwrap();

        // Five ready jobs.
        let mut jobs_by_id: HashMap<u64, Job> = HashMap::new();
        for id in 1u64..=5 {
            let mut job = Job::new(
                id,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            jobs_by_id.insert(id, job);
        }

        let bury = |wal: &mut Wal, jobs_by_id: &mut HashMap<u64, Job>, id: u64| {
            wal.write_state_change(
                jobs_by_id.get_mut(&id).unwrap(),
                Some(JobState::Buried),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Bury,
            )
            .unwrap();
        };

        // Bury in an order distinct from id order: 4, 2, 5, 1. Then kick
        // job 2 back to ready and re-bury it — its final position must be
        // its *last* bury, not its first. Job 3 stays ready.
        for id in [4u64, 2, 5, 1] {
            bury(&mut wal, &mut jobs_by_id, id);
        }
        wal.write_state_change(
            jobs_by_id.get_mut(&2).unwrap(),
            Some(JobState::Ready),
            10,
            Duration::ZERO,
            0,
            StateChangeReason::Kick,
        )
        .unwrap();
        bury(&mut wal, &mut jobs_by_id, 2);
        drop(wal);

        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, _, _, _, buried_order) = wal.replay().unwrap();
        assert_eq!(jobs.len(), 5);
        assert_eq!(
            buried_order,
            vec![4, 5, 1, 2],
            "buried order must follow the last bury event of each job, in WAL order"
        );
    }

    #[test]
    fn test_state_change_to_delayed_replays_remaining_delay() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Two ready jobs on disk.
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            for id in [1u64, 2] {
                let mut job = Job::new(
                    id,
                    10,
                    Duration::ZERO,
                    Duration::from_secs(60),
                    b"body".to_vec(),
                    "default".to_string(),
                );
                external_body(&mut job);
                wal.write_put(&mut job).unwrap();
            }
        }

        // Hand-craft two release-to-delayed records so we control change_epoch
        // (the live API always stamps `now`). Job 1: delay started 2h ago with a
        // 1h delay ⇒ already expired. Job 2: pre-v7 record (epoch 0) ⇒ replays
        // the full delay.
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let one_hour_nanos = Duration::from_secs(3600).as_nanos() as u64;
        let expired = serialize_state_change(
            1,
            Some(JobState::Delayed),
            10,
            one_hour_nanos,
            0,
            StateChangeReason::Release,
            now_epoch - 7200,
        );
        let no_ts = serialize_state_change(
            2,
            Some(JobState::Delayed),
            10,
            one_hour_nanos,
            0,
            StateChangeReason::Release,
            0,
        );

        let wal_file = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap();
        {
            let mut f = OpenOptions::new().append(true).open(wal_file.path()).unwrap();
            f.write_all(&expired).unwrap();
            f.write_all(&no_ts).unwrap();
        }

        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, _, _, _, _) = wal.replay().unwrap();

        let j1 = jobs.get(&1).unwrap();
        assert_eq!(j1.state, JobState::Delayed);
        assert!(
            j1.deadline_at.unwrap() <= Instant::now(),
            "expired release-delay must replay ready-soon, not the full hour"
        );

        let j2 = jobs.get(&2).unwrap();
        assert_eq!(j2.state, JobState::Delayed);
        assert!(
            j2.deadline_at.unwrap() > Instant::now() + Duration::from_secs(1800),
            "pre-v7 record must fall back to the full delay"
        );
    }

    #[test]
    fn test_replay_skips_file_with_bad_header() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write one valid job
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job1);
            wal.write_put(&mut job1).unwrap();
        }

        // Overwrite the WAL file header with garbage
        let wal_file = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap();
        {
            let mut data = fs::read(wal_file.path()).unwrap();
            data[0..4].copy_from_slice(b"BAAD");
            fs::write(wal_file.path(), &data).unwrap();
        }

        // Replay should skip the bad file, recover nothing
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, _, _, _, _) = wal.replay().unwrap();
        assert!(jobs.is_empty());
    }

    #[test]
    fn test_replay_truncated_record_mid_job() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write two jobs
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job1);
            wal.write_put(&mut job1).unwrap();
            let mut job2 = Job::new(
                2,
                20,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body2".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job2);
            wal.write_put(&mut job2).unwrap();
        }

        // Truncate file to cut off the second job mid-record
        let wal_file = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap();
        {
            let data = fs::read(wal_file.path()).unwrap();
            // Keep header + first job + a few bytes of the second
            let truncated_len = data.len() - 10;
            fs::write(wal_file.path(), &data[..truncated_len]).unwrap();
        }

        // Should recover job 1 only
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, next_id, _, _, _) = wal.replay().unwrap();
        assert_eq!(jobs.len(), 1);
        assert!(jobs.contains_key(&1));
        assert!(next_id >= 2);
    }

    #[test]
    fn test_file_count_and_total_disk_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

        // Before any writes, no files
        assert_eq!(wal.file_count(), 0);
        assert_eq!(wal.total_disk_bytes(), 0);

        // Write a job — triggers file creation
        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body1".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job1);
        wal.write_put(&mut job1).unwrap();

        assert_eq!(wal.file_count(), 1);
        assert!(
            wal.total_disk_bytes() > 0,
            "should have bytes after a write"
        );

        let bytes_after_one = wal.total_disk_bytes();

        // Write another job — same file
        let mut job2 = Job::new(
            2,
            20,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body2".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job2);
        wal.write_put(&mut job2).unwrap();

        assert_eq!(wal.file_count(), 1);
        assert!(
            wal.total_disk_bytes() > bytes_after_one,
            "total bytes should increase after second write"
        );
    }

    #[test]
    fn test_file_count_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Very small max file size to force rotation
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(64)).unwrap();

        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body1".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job1);
        wal.write_put(&mut job1).unwrap();

        let mut job2 = Job::new(
            2,
            20,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body2".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job2);
        wal.write_put(&mut job2).unwrap();

        assert!(
            wal.file_count() >= 2,
            "should have multiple files after rotation, got {}",
            wal.file_count()
        );

        // total_disk_bytes should cover all files
        let total = wal.total_disk_bytes();
        assert!(total > 0);
    }

    #[test]
    fn test_wal_write_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write some records
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job1);
            wal.write_put(&mut job1).unwrap();

            let mut job2 = Job::new(
                2,
                20,
                Duration::from_secs(10),
                Duration::from_secs(60),
                b"body2".to_vec(),
                "other".to_string(),
            );
            external_body(&mut job2);
            wal.write_put(&mut job2).unwrap();

            // Delete job 1
            wal.write_state_change(
                &mut job1,
                None,
                0,
                Duration::ZERO,
                0,
                StateChangeReason::None,
            )
            .unwrap();
        }

        // Replay
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let (jobs, next_id, _, _, _) = wal.replay().unwrap();

            // Job 1 was deleted
            assert!(!jobs.contains_key(&1));
            // Job 2 should exist
            assert!(jobs.contains_key(&2));
            let j2 = &jobs[&2];
            assert_eq!(j2.priority, 20);
            assert!(matches!(&j2.body, BodyRef::External(..)));
            assert_eq!(j2.tube_name.as_ref(), "other");
            assert_eq!(j2.state, JobState::Delayed);
            assert!(next_id >= 3);
        }
    }

    #[test]
    fn test_compaction_target_none_when_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1024 * 1024)).unwrap();

        let mut job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job);
        wal.write_put(&mut job).unwrap();

        // Only one file — no compaction target
        assert!(wal.compaction_target().is_none());
    }

    #[test]
    fn test_compaction_target_none_when_low_waste() {
        let dir = tempfile::tempdir().unwrap();
        // Use a large file size so all jobs fit in two files with low waste
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(4096)).unwrap();

        // Write jobs that span exactly 2 files
        for i in 1..=50 {
            let mut job = Job::new(
                i,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
        }

        assert!(wal.file_count() > 1, "should have multiple files");
        // All jobs alive — alive_bytes is substantial relative to total space,
        // so waste / live_bytes < 2 and no compaction should trigger
        assert!(
            wal.compaction_target().is_none(),
            "should not trigger compaction when all jobs are alive"
        );
    }

    #[test]
    fn test_compaction_target_some_when_high_waste() {
        let dir = tempfile::tempdir().unwrap();
        // Small file size to force many files
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(128)).unwrap();

        // Write many jobs then delete them all except one
        let mut jobs: Vec<Job> = Vec::new();
        for i in 1..=20 {
            let mut job = Job::new(
                i,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            jobs.push(job);
        }

        assert!(wal.file_count() > 1, "should have multiple files");

        // Delete all but the last job
        for job in jobs.iter_mut().take(19) {
            wal.write_state_change(job, None, 0, Duration::ZERO, 0, StateChangeReason::None)
                .unwrap();
        }

        // Now most files have dead data — waste ratio should be high
        let target = wal.compaction_target();
        assert!(
            target.is_some(),
            "should have a compaction target with high waste"
        );
        let (seq, count) = target.unwrap();
        assert_eq!(seq, wal.oldest_seq());
        assert!(count >= 2, "ratio should be >= 2, got {}", count);
    }

    #[test]
    fn test_gc_after_all_refs_removed() {
        let dir = tempfile::tempdir().unwrap();
        // Very small file size to force each job into its own file
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(64)).unwrap();

        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body1".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job1);
        wal.write_put(&mut job1).unwrap();

        let mut job2 = Job::new(
            2,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body2".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job2);
        wal.write_put(&mut job2).unwrap();

        let mut job3 = Job::new(
            3,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body3".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job3);
        wal.write_put(&mut job3).unwrap();

        let count_before_gc = wal.file_count();
        assert!(count_before_gc > 1, "should have multiple files");

        // Delete all jobs so all files become reclaimable except current
        wal.write_state_change(
            &mut job1,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        wal.write_state_change(
            &mut job2,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        wal.write_state_change(
            &mut job3,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();

        // GC only unlinks once the superseding delete records are durable.
        wal.sync().unwrap();
        wal.gc();
        assert!(
            wal.file_count() < count_before_gc,
            "gc should remove files with 0 refs: before={}, after={}",
            count_before_gc,
            wal.file_count()
        );
    }

    /// GC must not unlink a file whose superseding records (compaction
    /// migrations, deletes) are still buffered. Regression test: gc() ran
    /// at tick start, before the tick's migration writes were fsynced —
    /// power loss in that window destroyed the only durable copy of
    /// migrated jobs.
    #[test]
    fn test_gc_waits_for_sync_of_superseding_records() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny file size: every record rotates into its own file.
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(64)).unwrap();

        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body1".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job1);
        wal.write_put(&mut job1).unwrap();

        let mut job2 = Job::new(
            2,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body2".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job2);
        wal.write_put(&mut job2).unwrap();
        wal.sync().unwrap();

        assert!(wal.file_count() > 1, "need a sealed file to GC");

        // Compaction-style migration: rewrite job1 into the current file.
        // Its old file's refs drop to zero, but the migration record is
        // only buffered.
        wal.write_put(&mut job1).unwrap();
        assert_eq!(
            wal.files.front().unwrap().refs,
            0,
            "old file fully superseded by the migration"
        );

        let count_before_gc = wal.file_count();
        wal.gc();
        assert_eq!(
            wal.file_count(),
            count_before_gc,
            "gc must not unlink a file whose superseding record is unsynced"
        );

        wal.sync().unwrap();
        wal.gc();
        assert!(
            wal.file_count() < count_before_gc,
            "after sync the superseded file is reclaimable: before={}, after={}",
            count_before_gc,
            wal.file_count()
        );
    }

    #[test]
    fn test_compaction_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(128)).unwrap();

        // Create jobs spanning multiple files
        let mut jobs: Vec<Job> = Vec::new();
        for i in 1..=20 {
            let mut job = Job::new(
                i,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"data".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            jobs.push(job);
        }

        let files_before = wal.file_count();

        // Delete most jobs (keep only job 20 in the last file)
        for job in jobs.iter_mut().take(19) {
            wal.write_state_change(job, None, 0, Duration::ZERO, 0, StateChangeReason::None)
                .unwrap();
        }

        // Run gc — should remove leading files with 0 refs
        wal.maintain();
        let files_after = wal.file_count();
        assert!(
            files_after <= files_before,
            "gc should remove files: before={}, after={}",
            files_before,
            files_after
        );

        // The remaining live job (20) may still pin one old file.
        // Simulate compaction: re-write job 20 to move it to the current file.
        let job20 = &mut jobs[19];
        if let Some(old_seq) = job20.wal_seq() {
            let current_seq = wal.current_seq();
            if old_seq != current_seq {
                // Re-write to current file (this is what the server does)
                external_body(job20);
                wal.write_put(job20).unwrap();
                wal.record_migration();
                wal.maintain(); // gc again
                // Should have fewer files now
                assert!(
                    wal.file_count() < files_before,
                    "compaction should reduce file count"
                );
            }
        }
    }

    #[test]
    fn test_state_change_does_not_lose_job_after_gc() {
        let dir = tempfile::tempdir().unwrap();

        // Use a tiny max file size so the put and state change land in different files
        {
            let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(64)).unwrap();

            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"important-data".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();

            // State change goes to a new file due to small max_file_size
            let pri = job.priority;
            wal.write_state_change(
                &mut job,
                Some(JobState::Buried),
                pri,
                Duration::ZERO,
                0,
                StateChangeReason::Bury,
            )
            .unwrap();

            assert!(
                wal.file_count() >= 2,
                "expected multiple WAL files, got {}",
                wal.file_count()
            );

            // GC should NOT remove the file containing the FullJob
            wal.gc();
            assert!(
                wal.file_count() >= 2,
                "GC must not delete the FullJob's file"
            );
        }

        // Reopen and replay — job must survive
        {
            let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(64)).unwrap();
            let (jobs, _next_id, _tombstones, _orphans, _) = wal.replay().unwrap();

            assert!(jobs.contains_key(&1), "job 1 must survive replay after GC");
            let job = jobs.get(&1).unwrap();
            assert_eq!(job.state, JobState::Buried);
            assert!(matches!(&job.body, BodyRef::External(..)));
        }
    }

    #[test]
    fn test_compaction_migration_does_not_leak_reserved_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(512)).unwrap();

        let mut job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"hello".to_vec(),
            "default".to_string(),
        );

        // Initial put — should add one reservation
        external_body(&mut job);
        wal.write_put(&mut job).unwrap();
        let after_put = wal.reserved_bytes;
        assert_eq!(after_put, STATE_CHANGE_RECORD_SIZE as u64);

        // Simulate compaction migration — should NOT add another reservation
        external_body(&mut job);
        wal.write_put(&mut job).unwrap();
        assert_eq!(
            wal.reserved_bytes, after_put,
            "compaction migration must not increase reserved_bytes"
        );

        // A third migration — still no increase
        external_body(&mut job);
        wal.write_put(&mut job).unwrap();
        assert_eq!(
            wal.reserved_bytes, after_put,
            "repeated migrations must not leak reserved_bytes"
        );

        // State change (delete) should consume the single reservation
        wal.write_state_change(
            &mut job,
            None,
            10,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        assert_eq!(wal.reserved_bytes, 0, "delete should consume reservation");
    }

    #[test]
    fn test_reserve_put_rejects_oversized_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(dir.path(), WalConfig::with_max_file_size(512)).unwrap();

        // A record that fits
        assert!(wal.reserve_put(100));

        // A record larger than max_file_size
        assert!(!wal.reserve_put(1024));
    }

    #[test]
    fn test_replay_restores_counters_from_reason() {
        // Verifies that state change reason bytes allow replay to reconstruct
        // per-job counters. Simulates: put → reserve → release → reserve →
        // timeout → reserve → bury → kick → bury (final state: Buried).
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"test-body".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();

            // reserve #1
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // release #1
            job.release_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Ready),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Release,
            )
            .unwrap();
            // reserve #2
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // timeout #1
            job.timeout_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Ready),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Timeout,
            )
            .unwrap();
            // reserve #3
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // bury #1
            job.bury_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Buried),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Bury,
            )
            .unwrap();
            // kick #1
            job.kick_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Ready),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Kick,
            )
            .unwrap();
            // reserve #4 (to bury again)
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // bury #2
            job.bury_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Buried),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Bury,
            )
            .unwrap();
        }

        // Replay and verify all counters
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let (jobs, _, _, _, _) = wal.replay().unwrap();

            let job = jobs.get(&1).expect("job 1 should exist after replay");
            assert_eq!(job.state, JobState::Buried);
            assert_eq!(job.reserve_ct, 4, "reserve_ct: 4 reserves");
            assert_eq!(job.release_ct, 1, "release_ct: 1 release");
            assert_eq!(job.timeout_ct, 1, "timeout_ct: 1 timeout");
            assert_eq!(job.bury_ct, 2, "bury_ct: 2 buries");
            assert_eq!(job.kick_ct, 1, "kick_ct: 1 kick");
        }
    }

    #[test]
    fn test_replay_v3_state_change_no_counter_update() {
        // Verify that v3 StateChange records (payload_len=21, no reason byte)
        // are still readable — counters just stay at 0 (backward compatible).
        let v3_record = {
            // Manually build a v3 state change record (payload_len=21, no reason byte)
            let mut record = Vec::new();
            record.push(RECORD_TYPE_STATE_CHANGE);
            record.extend_from_slice(&1u64.to_le_bytes()); // job_id
            record.extend_from_slice(&STATE_CHANGE_PAYLOAD_LEN_V3.to_le_bytes());
            record.push(state_to_u8(JobState::Buried)); // state
            record.extend_from_slice(&10u32.to_le_bytes()); // priority
            record.extend_from_slice(&0u64.to_le_bytes()); // delay_nanos
            record.extend_from_slice(&0u64.to_le_bytes()); // expiry_epoch_secs
            let crc = crc32fast::hash(&record);
            record.extend_from_slice(&crc.to_le_bytes());
            record
        };
        assert_eq!(v3_record.len(), 38); // old size

        let (rec, consumed) = deserialize_record(&v3_record, WAL_VERSION).unwrap();
        assert_eq!(consumed, 38);
        if let WalRecord::StateChange {
            reason, new_state, ..
        } = rec
        {
            assert_eq!(reason, StateChangeReason::None);
            assert_eq!(new_state, Some(JobState::Buried));
        } else {
            panic!("expected StateChange");
        }
    }

    // --- Sync-mode / buffering tests ---

    fn config_with_interval(max: usize, interval: Duration) -> WalConfig {
        WalConfig {
            max_file_size: Some(max),
            sync_interval: interval,
            skip_corrupt: false,
        }
    }

    #[test]
    fn test_writes_set_dirty_and_sync_clears_it() {
        // Group commit invariant: writes buffer + set dirty; explicit
        // sync() does the fsync and clears dirty. Multiple writes between
        // syncs share a single fsync.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::ZERO),
        )
        .unwrap();

        assert!(!wal.is_dirty(), "fresh WAL is clean");
        let baseline = wal.sync_count();

        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"a".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job1);
        wal.write_put(&mut job1).unwrap();
        let mut job2 = Job::new(
            2,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"b".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job2);
        wal.write_put(&mut job2).unwrap();
        wal.write_state_change(
            &mut job1,
            None,
            10,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();

        // Writes do not fsync — dirty flag is the only signal.
        assert!(wal.is_dirty(), "writes must mark WAL dirty");
        assert_eq!(
            wal.sync_count() - baseline,
            0,
            "writes must not trigger fsync"
        );

        // One sync covers all three writes.
        wal.sync().unwrap();
        assert!(!wal.is_dirty(), "sync() clears dirty");
        assert_eq!(
            wal.sync_count() - baseline,
            1,
            "one sync covers the whole batch"
        );

        // Calling sync() when clean is a no-op.
        wal.sync().unwrap();
        assert_eq!(
            wal.sync_count() - baseline,
            1,
            "sync() on clean WAL must not fsync"
        );
    }

    #[test]
    fn test_maintain_no_longer_fsyncs() {
        // Tick-driven sync moved to the engine; maintain() just runs GC and
        // flushes the BufWriter. Keep this test as a regression guard so
        // future refactors don't accidentally re-introduce a fsync here.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::from_millis(500)),
        )
        .unwrap();

        let mut job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body".to_vec(),
            "default".to_string(),
        );
        external_body(&mut job);
        wal.write_put(&mut job).unwrap();
        let baseline = wal.sync_count();

        for _ in 0..5 {
            wal.maintain();
        }
        assert_eq!(
            wal.sync_count(),
            baseline,
            "maintain must not fsync (engine owns sync now)"
        );

        // Even past the interval, maintain doesn't sync.
        std::thread::sleep(Duration::from_millis(550));
        wal.maintain();
        assert_eq!(wal.sync_count(), baseline);

        // The engine's job: an explicit sync() is the only way bytes hit disk.
        wal.sync().unwrap();
        assert_eq!(wal.sync_count(), baseline + 1);
    }

    #[test]
    fn test_buffered_write_replays_after_maintain() {
        // Guards the invariant that maintain()'s flush makes buffered data
        // visible to a subsequent replay on a fresh Wal instance.
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = Wal::open(
                dir.path(),
                config_with_interval(1024 * 1024, Duration::from_secs(3600)),
            )
            .unwrap();
            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"buffered".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            // Force a flush through maintain (the interval is far in the future,
            // so no fsync, but the BufWriter flush must still run).
            wal.maintain();
            // Drop without calling flush_and_sync — relying on maintain's flush.
        }

        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::from_secs(3600)),
        )
        .unwrap();
        let (jobs, _, _, _, _) = wal.replay().unwrap();
        assert!(jobs.contains_key(&1), "job should survive buffered replay");
        assert!(matches!(&jobs[&1].body, BodyRef::External(..)));
    }

    #[test]
    fn test_flush_and_sync_makes_data_durable() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = Wal::open(
                dir.path(),
                config_with_interval(1024 * 1024, Duration::from_secs(3600)),
            )
            .unwrap();
            let before = wal.sync_count();
            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"shutdown".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            wal.flush_and_sync();
            assert!(wal.sync_count() > before, "flush_and_sync must always sync");
        }

        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::from_secs(3600)),
        )
        .unwrap();
        let (jobs, _, _, _, _) = wal.replay().unwrap();
        assert!(jobs.contains_key(&1));
    }

    /// A job that's `put` and then `delete`d in the same WAL must surface
    /// its `BodyId` in the orphan list returned from `replay`. Without
    /// this, a server that crashes between WAL fsync and the runtime
    /// `BodyStore::delete` call leaks the body on disk forever.
    #[test]
    fn test_replay_returns_orphan_body_ids_for_deleted_jobs() {
        let dir = tempfile::TempDir::new().unwrap();
        {
            let mut wal = Wal::open(dir.path(), config_with_interval(1024 * 1024, Duration::from_secs(3600))).unwrap();
            let mut job = Job::new(
                42,
                100,
                Duration::from_secs(5),
                Duration::from_secs(60),
                b"orphan-me".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            wal.write_state_change(
                &mut job,
                None,
                0,
                Duration::ZERO,
                0,
                StateChangeReason::None,
            )
            .unwrap();
            wal.flush_and_sync();
        }

        let mut wal = Wal::open(dir.path(), config_with_interval(1024 * 1024, Duration::from_secs(3600))).unwrap();
        let (jobs, _, _, orphans, _) = wal.replay().unwrap();
        assert!(jobs.is_empty(), "deleted job must not survive replay");
        assert_eq!(orphans, vec![BodyId(42)]);
    }

    /// A job that's deleted and then re-put under the same id within the
    /// same WAL must NOT appear as an orphan — its body is live.
    #[test]
    fn test_replay_orphan_filter_keeps_recreated_jobs() {
        let dir = tempfile::TempDir::new().unwrap();
        {
            let mut wal = Wal::open(dir.path(), config_with_interval(1024 * 1024, Duration::from_secs(3600))).unwrap();
            let mut job = Job::new(
                7,
                100,
                Duration::from_secs(5),
                Duration::from_secs(60),
                b"v1".to_vec(),
                "default".to_string(),
            );
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            wal.write_state_change(
                &mut job,
                None,
                0,
                Duration::ZERO,
                0,
                StateChangeReason::None,
            )
            .unwrap();
            // Re-put job id 7 with the same BodyId (deterministic id == job.id).
            external_body(&mut job);
            wal.write_put(&mut job).unwrap();
            wal.flush_and_sync();
        }

        let mut wal = Wal::open(dir.path(), config_with_interval(1024 * 1024, Duration::from_secs(3600))).unwrap();
        let (jobs, _, _, orphans, _) = wal.replay().unwrap();
        assert!(jobs.contains_key(&7), "re-put job must survive replay");
        assert!(
            orphans.is_empty(),
            "live body must not be reported as orphan: {:?}",
            orphans
        );
    }

    // --- WalObserver seam ---

    /// What a [`RecordingObserver`] keeps per event: enough to identify the
    /// record without borrowing from the WAL.
    #[derive(Debug, Clone, PartialEq)]
    enum SeenRecord {
        Put {
            job_id: u64,
        },
        StateChange {
            job_id: u64,
            new_state: Option<JobState>,
            reason: StateChangeReason,
        },
        Tombstone {
            job_id: u64,
            tube: String,
            key: String,
            expires_epoch_secs: u64,
        },
        IdMark {
            high_water: u64,
        },
        ToastMark(DurablePoint),
        ScheduleSet {
            owner: String,
            entries: Vec<ScheduleRecord>,
        },
        ScheduleFired {
            owner: String,
            name: String,
            last_fire: Option<jiff::Timestamp>,
        },
    }

    /// What a segment file holds for a record, in the same terms as the tee.
    fn seen_in_file(rec: &WalRecord) -> SeenRecord {
        match rec {
            WalRecord::FullJob(job) => SeenRecord::Put { job_id: job.id },
            WalRecord::StateChange {
                job_id,
                new_state,
                reason,
                ..
            } => SeenRecord::StateChange {
                job_id: *job_id,
                new_state: *new_state,
                reason: *reason,
            },
            WalRecord::Tombstone {
                job_id,
                tube,
                key,
                expires_epoch_secs,
            } => SeenRecord::Tombstone {
                job_id: *job_id,
                tube: tube.clone(),
                key: key.clone(),
                expires_epoch_secs: *expires_epoch_secs,
            },
            WalRecord::IdMark { high_water } => SeenRecord::IdMark {
                high_water: *high_water,
            },
            WalRecord::ToastMark(point) => SeenRecord::ToastMark(*point),
            WalRecord::ScheduleSet { owner, entries } => SeenRecord::ScheduleSet {
                owner: owner.clone(),
                entries: entries.clone(),
            },
            WalRecord::ScheduleFired {
                owner,
                name,
                last_fire,
            } => SeenRecord::ScheduleFired {
                owner: owner.clone(),
                name: name.clone(),
                last_fire: *last_fire,
            },
        }
    }

    /// Observer that records everything it sees behind shared handles the
    /// test keeps, since the Box itself is owned by the Wal.
    struct RecordingObserver {
        events: std::sync::Arc<std::sync::Mutex<Vec<(Position, SeenRecord)>>>,
        syncs: std::sync::Arc<std::sync::Mutex<Vec<Position>>>,
    }

    impl WalObserver for RecordingObserver {
        fn on_record(&mut self, pos: Position, event: WalEvent<'_>) {
            let seen = match event {
                WalEvent::Put { job } => SeenRecord::Put { job_id: job.id },
                WalEvent::StateChange {
                    job_id,
                    new_state,
                    reason,
                    ..
                } => SeenRecord::StateChange {
                    job_id,
                    new_state,
                    reason,
                },
                WalEvent::Tombstone {
                    job_id,
                    tube,
                    key,
                    expires_epoch_secs,
                } => SeenRecord::Tombstone {
                    job_id,
                    tube: tube.to_string(),
                    key: key.to_string(),
                    expires_epoch_secs,
                },
                WalEvent::IdMark { high_water } => SeenRecord::IdMark { high_water },
                WalEvent::ToastMark(point) => SeenRecord::ToastMark(point),
                WalEvent::ScheduleSet { owner, entries } => SeenRecord::ScheduleSet {
                    owner: owner.to_string(),
                    entries: entries
                        .iter()
                        .map(|e| ScheduleRecord {
                            line: e.line.to_string(),
                            body: e.body.to_vec(),
                            synced_at: e.synced_at,
                            last_fire: e.last_fire,
                        })
                        .collect(),
                },
                WalEvent::ScheduleFired {
                    owner,
                    name,
                    last_fire,
                } => SeenRecord::ScheduleFired {
                    owner: owner.to_string(),
                    name: name.to_string(),
                    last_fire,
                },
            };
            self.events.lock().unwrap().push((pos, seen));
        }

        fn on_sync(&mut self, end: Position) {
            self.syncs.lock().unwrap().push(end);
        }
    }

    type SharedEvents = std::sync::Arc<std::sync::Mutex<Vec<(Position, SeenRecord)>>>;
    type SharedSyncs = std::sync::Arc<std::sync::Mutex<Vec<Position>>>;

    fn attach_recording_observer(wal: &mut Wal) -> (SharedEvents, SharedSyncs) {
        let events: SharedEvents = Default::default();
        let syncs: SharedSyncs = Default::default();
        wal.set_observer(Box::new(RecordingObserver {
            events: events.clone(),
            syncs: syncs.clone(),
        }));
        (events, syncs)
    }

    #[test]
    fn test_observer_positions_are_record_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (events, _syncs) = attach_recording_observer(&mut wal);

        let mut job1 = make_test_job(1, b"one");
        let mut job2 = make_test_job(2, b"two");
        wal.write_put(&mut job1).unwrap();
        wal.write_put(&mut job2).unwrap();
        wal.write_state_change(
            &mut job1,
            Some(JobState::Buried),
            5,
            Duration::ZERO,
            0,
            StateChangeReason::Bury,
        )
        .unwrap();
        wal.write_state_change(&mut job2, None, 0, Duration::ZERO, 0, StateChangeReason::None)
            .unwrap();
        wal.sync().unwrap(); // flush the BufWriter so the file scan below sees every byte

        let events = events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .map(|(_, e)| e.clone())
                .collect::<Vec<_>>(),
            vec![
                // The put that opens the segment counts in its mark.
                SeenRecord::IdMark { high_water: 1 },
                SeenRecord::Put { job_id: 1 },
                SeenRecord::Put { job_id: 2 },
                SeenRecord::StateChange {
                    job_id: 1,
                    new_state: Some(JobState::Buried),
                    reason: StateChangeReason::Bury,
                },
                SeenRecord::StateChange {
                    job_id: 2,
                    new_state: None,
                    reason: StateChangeReason::None,
                },
            ],
        );

        // Positions: the segment's id mark starts right after the header,
        // the first put right after that, strictly increasing thereafter.
        assert_eq!(events[0].0, Position { seg: 1, offset: HEADER_SIZE as u64 });
        assert_eq!(
            events[1].0,
            Position { seg: 1, offset: (HEADER_SIZE + ID_MARK_RECORD_SIZE) as u64 }
        );
        for w in events.windows(2) {
            assert!(w[0].0 < w[1].0, "positions must increase: {:?}", *events);
        }

        // Every observed position must be a parseable record boundary in
        // the segment file, and parse to the same record the tee reported.
        let seg_path = wal.file_path(1);
        let data = fs::read(&seg_path).unwrap();
        let (version, _) = read_header(&data).unwrap();
        for (pos, seen) in events.iter() {
            let (rec, _consumed) =
                deserialize_record(&data[pos.offset as usize..], version).unwrap_or_else(|e| {
                    panic!("offset {} is not a record boundary: {:?}", pos.offset, e)
                });
            assert_eq!(&seen_in_file(&rec), seen, "tee/file record mismatch at {pos:?}");
        }
    }

    #[test]
    fn test_observer_position_tracks_rotation() {
        let dir = tempfile::tempdir().unwrap();
        // Force rotation after every record: any bytes_written past the
        // header exceeds this max on the next write's rotate check.
        let mut wal =
            Wal::open(dir.path(), WalConfig::with_max_file_size(HEADER_SIZE + 1)).unwrap();
        let (events, _syncs) = attach_recording_observer(&mut wal);

        let mut job1 = make_test_job(1, b"one");
        let mut job2 = make_test_job(2, b"two");
        wal.write_put(&mut job1).unwrap();
        wal.write_put(&mut job2).unwrap();

        // Each segment opens with its id mark; the put follows it.
        let first_record = (HEADER_SIZE + ID_MARK_RECORD_SIZE) as u64;
        let puts: Vec<Position> = events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, e)| matches!(e, SeenRecord::Put { .. }))
            .map(|(pos, _)| *pos)
            .collect();
        assert_eq!(puts[0], Position { seg: 1, offset: first_record });
        assert_eq!(
            puts[1],
            Position { seg: 2, offset: first_record },
            "second put must land at the top of the rotated segment"
        );
    }

    #[test]
    fn test_observer_sync_covers_all_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (events, syncs) = attach_recording_observer(&mut wal);

        let mut job = make_test_job(9, b"nine");
        wal.write_put(&mut job).unwrap();
        wal.sync().unwrap();

        {
            let events = events.lock().unwrap();
            let syncs = syncs.lock().unwrap();
            assert_eq!(syncs.len(), 1, "one fsync, one on_sync");
            let end = syncs[0];
            assert!(
                events.iter().all(|(pos, _)| *pos < end),
                "every record must sit strictly below the synced end: {:?} !< {:?}",
                events,
                end
            );
            assert_eq!(end, wal.end_position());
        }

        // A clean sync (nothing dirty) must not report again.
        wal.sync().unwrap();
        assert_eq!(syncs.lock().unwrap().len(), 1);
    }

    /// The tee reports every record the segment files hold, at the position
    /// it holds it — not just puts and state changes, but tombstones and the
    /// marks a segment opens with or a sync appends. A record the tee missed
    /// would be invisible to a consumer that follows it past the attach.
    #[test]
    fn test_observer_tees_every_record_in_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let bs = Arc::new(BodyStore::open(&dir.path().join("toast"), 4096).unwrap());
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(512)).unwrap();
        wal.set_body_store(Arc::clone(&bs));
        let (events, _syncs) = attach_recording_observer(&mut wal);

        for id in 1..=6 {
            let mut job = put_with_toast_body(&mut wal, &bs, id);
            wal.write_tombstone("test-tube", &format!("k{id}"), id, 1_900_000_000)
                .unwrap();
            wal.write_state_change(&mut job, None, 0, Duration::ZERO, 0, StateChangeReason::None)
                .unwrap();
            wal.write_tombstone("test-tube", &format!("k{id}"), id, 0).unwrap();
            wal.sync().unwrap();
        }
        assert!(wal.file_count() > 1, "the test needs a rotation");

        let mut in_files = Vec::new();
        for seq in wal.oldest_seq()..=wal.current_seq() {
            let data = fs::read(wal.file_path(seq)).unwrap();
            let (version, _) = read_header(&data).unwrap();
            let mut offset = HEADER_SIZE;
            while offset < data.len() {
                let (rec, consumed) = deserialize_record(&data[offset..], version).unwrap();
                let pos = Position { seg: seq, offset: offset as u64 };
                in_files.push((pos, seen_in_file(&rec)));
                offset += consumed;
            }
        }
        let teed = events.lock().unwrap().clone();
        for kind in ["Tombstone", "IdMark", "ToastMark"] {
            assert!(
                in_files.iter().any(|(_, r)| format!("{r:?}").starts_with(kind)),
                "the test should write a {kind}"
            );
        }
        assert_eq!(teed, in_files);
    }

    // --- WalReader ---

    /// Drain a reader into `(Position, SeenRecord)` pairs — the same shape
    /// the observer tee records, so tests can compare the two directly.
    fn drain_reader(reader: &mut WalReader) -> Vec<(Position, SeenRecord)> {
        let mut out = Vec::new();
        while let Some((pos, record)) = reader.next_record().unwrap() {
            out.push((pos, seen_in_file(&record)));
        }
        out
    }

    #[test]
    fn test_reader_yields_what_the_tee_saw() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny max segment size so the log spans several segments — the
        // reader must walk rotations exactly as the tee positioned them.
        let mut wal =
            Wal::open(dir.path(), WalConfig::with_max_file_size(HEADER_SIZE + 1)).unwrap();
        let (events, _syncs) = attach_recording_observer(&mut wal);

        let mut job1 = make_test_job(1, b"one");
        let mut job2 = make_test_job(2, b"two");
        let mut job3 = make_test_job(3, b"three");
        wal.write_put(&mut job1).unwrap();
        wal.write_put(&mut job2).unwrap();
        wal.write_state_change(
            &mut job1,
            Some(JobState::Buried),
            5,
            Duration::ZERO,
            0,
            StateChangeReason::Bury,
        )
        .unwrap();
        wal.write_put(&mut job3).unwrap();
        wal.write_state_change(&mut job2, None, 0, Duration::ZERO, 0, StateChangeReason::None)
            .unwrap();
        wal.sync().unwrap(); // flush so the reader can see every byte

        let mut reader = WalReader::open(dir.path(), Position::ZERO).unwrap();
        let read = drain_reader(&mut reader);
        assert_eq!(read, *events.lock().unwrap());
        assert_eq!(
            reader.position(),
            wal.end_position(),
            "a drained reader's cursor is the log's end"
        );
    }

    #[test]
    fn test_reader_resumes_from_mid_log_position() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal =
            Wal::open(dir.path(), WalConfig::with_max_file_size(HEADER_SIZE + 1)).unwrap();
        let (events, _syncs) = attach_recording_observer(&mut wal);

        for id in 1..=5 {
            let mut job = make_test_job(id, b"x");
            wal.write_put(&mut job).unwrap();
        }
        wal.sync().unwrap();
        let events = events.lock().unwrap();

        // Read two records, capture the cursor, then resume with a fresh
        // reader — exactly the drain/re-open loop a follower runs.
        let mut first = WalReader::open(dir.path(), Position::ZERO).unwrap();
        first.next_record().unwrap().unwrap();
        first.next_record().unwrap().unwrap();
        let cursor = first.position();
        drop(first);

        let mut resumed = WalReader::open(dir.path(), cursor).unwrap();
        let rest = drain_reader(&mut resumed);
        assert_eq!(
            rest, events[2..],
            "resume must yield exactly the records after the cursor"
        );

        // Resuming from the tee's own positions works identically: open at
        // the 4th record's teed position, get it and everything after.
        let mut from_tee = WalReader::open(dir.path(), events[3].0).unwrap();
        assert_eq!(drain_reader(&mut from_tee), events[3..]);
    }

    #[test]
    fn test_reader_stops_at_buffered_tail_then_sees_it_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1024 * 1024)).unwrap();

        // Unsynced: the record sits in the BufWriter, invisible to a file
        // read — only the id mark the segment opened with is on disk. The
        // reader must report a clean end after it, not an error, and hold
        // its cursor at the unflushed boundary.
        let mut job = make_test_job(1, b"buffered");
        wal.write_put(&mut job).unwrap();

        let mut reader = WalReader::open(dir.path(), Position::ZERO).unwrap();
        assert_eq!(
            drain_reader(&mut reader),
            vec![(
                Position { seg: 1, offset: HEADER_SIZE as u64 },
                // The put that opens the segment counts in its mark.
                SeenRecord::IdMark { high_water: 1 }
            )]
        );
        let cursor = reader.position();
        assert_eq!(
            cursor,
            Position { seg: 1, offset: (HEADER_SIZE + ID_MARK_RECORD_SIZE) as u64 }
        );

        // After the flush, resuming from that cursor yields the record.
        wal.sync().unwrap();
        let mut resumed = WalReader::open(dir.path(), cursor).unwrap();
        assert_eq!(
            drain_reader(&mut resumed),
            vec![(cursor, SeenRecord::Put { job_id: 1 })]
        );
    }
}
