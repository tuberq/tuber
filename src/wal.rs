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

use crate::body_store::BodyStore;
use crate::job::{BodyId, BodyRef, Job, JobState};

// --- Constants ---

const WAL_MAGIC: &[u8; 4] = b"TWAL";
/// Current WAL format version.
///
/// - v3: original format.
/// - v4: state-change records gained a `reason` byte.
/// - v5: `FullJob` records carry a `BodyId` instead of inline body bytes.
///       Bodies live in the external body store ("TOAST").
/// - v6: `FullJob` body field is a per-record `body_kind` discriminant
///       followed by either inline bytes (kind=0) or a `BodyId` (kind=1).
///       Small bodies stay inline in the WAL record; only larger ones
///       are pushed to the body store. The discriminant decouples the
///       on-disk format from the runtime threshold choice — changing
///       `HEAP_INLINE_MAX` doesn't require a format bump.
/// - v7: state-change records gained a `change_epoch_secs` (wall-clock
///       second the change was applied). A release/kick-to-delayed job
///       replays with its remaining delay instead of resetting to the
///       full delay on every restart. Pre-v7 records replay full-delay.
pub const WAL_VERSION: u32 = 7;
const WAL_VERSION_MIN: u32 = 3; // Oldest version we can still read

// v6 body_kind discriminant values
const BODY_KIND_INLINE: u8 = 0x00;
const BODY_KIND_EXTERNAL: u8 = 0x01;
const HEADER_SIZE: usize = 12; // magic(4) + version(4) + flags(4)
const RECORD_TYPE_FULL_JOB: u8 = 0x01;
const RECORD_TYPE_STATE_CHANGE: u8 = 0x02;
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

/// An idempotency tombstone recovered from WAL replay.
#[derive(Debug)]
pub struct IdpTombstone {
    pub tube_name: std::sync::Arc<str>,
    pub key: String,
    pub job_id: u64,
    pub expires_at: SystemTime,
}

/// What [`Wal::replay`] returns: `(jobs, next_job_id, idempotency
/// tombstones, orphaned body ids, buried order)`.
pub type ReplayOutcome = (
    HashMap<u64, Job>,
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
}

// --- Serialization ---

pub fn serialize_full_job(job: &Job) -> Vec<u8> {
    let mut payload = Vec::new();

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
    write_option_string(&mut payload, job.idempotency_key().map(|(k, _)| k.as_str()));
    payload.extend_from_slice(
        &job.idempotency_key()
            .map_or(0u32, |(_, ttl)| *ttl)
            .to_le_bytes(),
    );

    // group, after_group
    write_option_string(&mut payload, job.group().map(String::as_str));
    write_option_string(&mut payload, job.after_group().map(String::as_str));

    // concurrency_key + limit
    write_option_string(&mut payload, job.concurrency_key().map(|(k, _)| k.as_str()));
    payload.extend_from_slice(
        &job.concurrency_key()
            .map_or(0u32, |(_, l)| *l)
            .to_le_bytes(),
    );

    // body — v6: `body_kind` discriminant + variant payload.
    //   inline   → kind=0x00, then `body_len (u32 LE)` then `len` bytes.
    //   external → kind=0x01, then `body_id (u64 LE)`.
    if let Some(bytes) = job.body.as_inline_bytes() {
        payload.push(BODY_KIND_INLINE);
        payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(bytes);
    } else {
        let BodyRef::External(id) = &job.body else {
            unreachable!("as_inline_bytes returned None ⇒ External");
        };
        payload.push(BODY_KIND_EXTERNAL);
        payload.extend_from_slice(&id.0.to_le_bytes());
    }

    // Build full record: type + job_id + payload_len + payload + crc
    let mut record = Vec::with_capacity(1 + 8 + 4 + payload.len() + 4);
    record.push(RECORD_TYPE_FULL_JOB);
    record.extend_from_slice(&job.id.to_le_bytes());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&payload);

    let crc = crc32fast::hash(&record);
    record.extend_from_slice(&crc.to_le_bytes());

    record
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

/// On-wire size of the v6 body section for a FullJob record.
/// `body_external == true` selects the BodyId encoding; otherwise the
/// body rides inline as `len + bytes`.
fn body_section_bytes(body_len: usize, body_external: bool) -> usize {
    if body_external {
        1 + 8 // body_kind + BodyId
    } else {
        1 + 4 + body_len // body_kind + body_len + bytes
    }
}

pub fn estimate_full_job_size(job: &Job) -> usize {
    let body_external = matches!(job.body, BodyRef::External(_));
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
        _ => Err(WalError::UnknownRecordType(record_type)),
    }
}

fn deserialize_full_job(data: &[u8], version: u32) -> Result<(WalRecord, usize), WalError> {
    // type(1) + job_id(8) + payload_len(4) = 13 byte header minimum
    if data.len() < 13 {
        return Err(WalError::Truncated);
    }

    let job_id = u64::from_le_bytes(data[1..9].try_into().map_err(|_| WalError::Truncated)?);
    let payload_len =
        u32::from_le_bytes(data[9..13].try_into().map_err(|_| WalError::Truncated)?) as usize;

    let total_len = 1 + 8 + 4 + payload_len + 4; // +4 for CRC
    if data.len() < total_len {
        return Err(WalError::Truncated);
    }

    // Verify CRC
    let stored_crc = u32::from_le_bytes(
        data[total_len - 4..total_len]
            .try_into()
            .map_err(|_| WalError::Truncated)?,
    );
    let computed_crc = crc32fast::hash(&data[..total_len - 4]);
    if stored_crc != computed_crc {
        return Err(WalError::BadCrc);
    }

    // Parse payload
    let payload = &data[13..13 + payload_len];
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
                BodyRef::External(crate::job::BodyId(body_id))
            }
            _ => return Err(WalError::InvalidData),
        }
    } else if version == 5 {
        let body_id = read_u64!();
        BodyRef::External(crate::job::BodyId(body_id))
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
fn corrupt_sidecar_path(path: &Path) -> PathBuf {
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
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            max_file_size: None,
            sync_interval: DEFAULT_SYNC_INTERVAL,
        }
    }
}

impl WalConfig {
    /// Convenience for callers that only need to override the file size.
    pub fn with_max_file_size(max: usize) -> Self {
        WalConfig {
            max_file_size: Some(max),
            sync_interval: DEFAULT_SYNC_INTERVAL,
        }
    }
}

pub struct Wal {
    dir: PathBuf,
    max_file_size: usize,
    sync_interval: Duration,
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
    #[allow(dead_code)] // held for flock side effect
    lock_fd: Option<File>,
    #[cfg(test)]
    sync_count: u64,
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

    /// Lowest format version found across all WAL files on disk. `Ok(None)`
    /// means the WAL directory is empty (no files yet). Reads only the first
    /// 12 bytes of each file — used at startup to decide whether legacy
    /// migration is required, before any expensive replay work.
    pub fn min_format_version(&self) -> io::Result<Option<u32>> {
        use std::io::Read;
        let mut min: Option<u32> = None;
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
                min = Some(min.map_or(v, |m| m.min(v)));
            }
        }
        Ok(min)
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
            lock_fd: Some(lock_fd),
            #[cfg(test)]
            sync_count: 0,
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
        self.pre_sync_body_store()?;
        if let Some(f) = self.files.back_mut()
            && let Some(fd) = f.fd.as_mut()
        {
            Self::flush_and_fsync(fd)?;
        }
        self.record_sync();
        self.dirty = false;
        Ok(())
    }

    /// Attach a body store whose fsync must complete before any WAL fsync.
    /// Called once at startup, after the body store has been opened and
    /// before any WAL writes.
    pub fn set_body_store(&mut self, bs: Arc<BodyStore>) {
        self.body_store = Some(bs);
    }

    /// fsync the body store (if attached) so referenced bodies are durable
    /// before we promise the same of any WAL record. The TOAST-then-WAL
    /// order means a crash mid-sync produces orphan bodies (recoverable as
    /// wasted space) rather than dangling references.
    fn pre_sync_body_store(&self) -> io::Result<()> {
        if let Some(bs) = &self.body_store {
            bs.fsync()?;
        }
        Ok(())
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
        let mut seqs: Vec<u64> = Vec::new();

        for entry in fs::read_dir(&self.dir)? {
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

        for seq in seqs {
            let path = self.dir.join(format!("{}{:06}", FILE_PREFIX, seq));
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
        self.dir.join(format!("{}{:06}", FILE_PREFIX, seq))
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
            bytes_written: HEADER_SIZE,
            zero_refs_at_op: 0,
        });
        self.total_disk_bytes += HEADER_SIZE as u64;

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
            // about to land in the WAL are already durable.
            self.pre_sync_body_store()?;
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
        self.rotate_if_needed()?;

        let record = serialize_full_job(job);
        let record_len = record.len();

        // Group commit: bytes go into the BufWriter and `dirty` is set, but
        // no fsync happens here. The engine task batches the fsync across
        // every write that landed since the last sync.
        let file = self.current_file_mut()?;
        let fd = file
            .fd
            .as_mut()
            .ok_or_else(|| io::Error::other("WAL file not open for writing"))?;
        fd.write_all(&record)?;
        file.bytes_written += record_len;
        let file_seq = file.seq;
        self.total_disk_bytes += record_len as u64;
        // Count the op before the decref below: the old file's zero-refs
        // stamp must include this record, since this record is what
        // supersedes it.
        self.ops_written += 1;

        // Update job's WAL tracking
        let old_seq = job.wal_seq();
        let old_used = job.wal_used;
        job.set_wal_ref(file_seq, record_len);

        // Decref old file
        if let Some(old) = old_seq {
            self.decref_file(old, old_used as usize);
        }

        // Incref new file
        self.incref_file(file_seq, record_len);

        // Reserve space for future state change (delete).
        // Only add a reservation for NEW jobs (old_seq is None).
        // Compaction migrations (old_seq is Some) already have a reservation
        // from the original put — adding another would leak reserved_bytes
        // on every migration cycle.
        if old_seq.is_none() {
            self.reserved_bytes += STATE_CHANGE_RECORD_SIZE as u64;
        }

        self.dirty = true;
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
        self.rotate_if_needed()?;

        let delay_nanos = new_delay.as_nanos().min(u64::MAX as u128) as u64;
        // Stamp the wall-clock instant of this change so a delayed replay can
        // subtract the elapsed time (v7). For a release/kick-to-delayed, this
        // is when the delay started; for other changes it is simply unused.
        let change_epoch_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let record = serialize_state_change(
            job.id,
            new_state,
            new_priority,
            delay_nanos,
            expiry_epoch_secs,
            reason,
            change_epoch_secs,
        );
        let record_len = record.len();

        // Group commit: writes go to the BufWriter and `dirty` is set, but
        // no fsync until the engine drains its channel and calls sync().
        let file = self.current_file_mut()?;
        let fd = file
            .fd
            .as_mut()
            .ok_or_else(|| io::Error::other("WAL file not open for writing"))?;
        fd.write_all(&record)?;
        file.bytes_written += record_len;
        self.total_disk_bytes += record_len as u64;
        // Count the op before the delete-decref below (see write_put).
        self.ops_written += 1;

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

        self.dirty = true;
        Ok(())
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
    pub fn replay(&mut self) -> io::Result<ReplayOutcome> {
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let mut max_id: u64 = 0;
        let mut tombstones: Vec<IdpTombstone> = Vec::new();
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
        let replay_time = SystemTime::now();

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
        // Segments quarantined this replay (moved to `.corrupt`); dropped from
        // `self.files` after the loop so GC can't chase the renamed path.
        let mut quarantined_seqs: Vec<u64> = Vec::new();
        // Only the newest segment can be a benign headerless artifact (a
        // crash between segment creation and the header fsync). Any older
        // sub-header segment was once a full segment with fsynced records
        // and must be treated as corruption, not cleaned up.
        let newest_seq: u64 = file_infos.iter().map(|(s, _)| *s).max().unwrap_or(0);
        const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

        for (idx, (seq, path)) in file_infos.iter().enumerate() {
            let data = match fs::read(path) {
                Ok(d) => d,
                Err(e) => {
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
                    quarantined_seqs.push(*seq);
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
                        quarantined_seqs.push(*seq);
                        continue;
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
                    quarantined_seqs.push(*seq);
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
                                jobs.insert(job.id, *job);
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
                                        // Deleted — check for idempotency tombstone
                                        if expiry_epoch_secs > 0 {
                                            let expires_at =
                                                UNIX_EPOCH + Duration::from_secs(expiry_epoch_secs);
                                            if expires_at > replay_time {
                                                // Tombstone still active — extract idp info from job before removing
                                                if let Some(job) = jobs.get(&job_id)
                                                    && let Some((key, _)) = job.idempotency_key()
                                                {
                                                    tombstones.push(IdpTombstone {
                                                        tube_name: job.tube_name.clone(),
                                                        key: key.clone(),
                                                        job_id,
                                                        expires_at,
                                                    });
                                                }
                                            }
                                        }
                                        if let Some(old_job) = jobs.remove(&job_id) {
                                            if let Some(old_seq) = old_job.wal_seq() {
                                                self.decref_file(old_seq, old_job.wal_used as usize);
                                            }
                                            if let BodyRef::External(body_id) = old_job.body {
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
                        }
                        offset += consumed;
                    }
                    Err(e) => {
                        tracing::error!(
                            "WAL: corrupt record in {:?} at offset {}: {}; preserving a .corrupt copy then truncating",
                            path,
                            offset,
                            e
                        );
                        // Preserve the whole segment before truncating so a
                        // mid-segment bit-rot — valid records (including deletes)
                        // after the bad one — is operator-recoverable rather than
                        // physically destroyed (which would resurrect deleted
                        // jobs). The active file is still truncated to the last
                        // good offset so the WAL can continue.
                        let dest = corrupt_sidecar_path(path);
                        if let Err(ce) = fs::copy(path, &dest) {
                            tracing::error!(
                                "WAL: failed to preserve corrupt segment {:?}: {}",
                                path,
                                ce
                            );
                        }
                        if let Ok(f) = OpenOptions::new().write(true).open(path) {
                            if let Err(te) = f.set_len(offset as u64) {
                                tracing::warn!("WAL: failed to truncate {:?}: {}", path, te);
                            }
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

        // Drop quarantined segments from tracking so GC never tries to unlink
        // the (now renamed) path, and the byte accounting stays accurate.
        if !quarantined_seqs.is_empty() {
            let removed_bytes: u64 = self
                .files
                .iter()
                .filter(|f| quarantined_seqs.contains(&f.seq))
                .map(|f| f.bytes_written as u64)
                .sum();
            self.files.retain(|f| !quarantined_seqs.contains(&f.seq));
            self.total_disk_bytes = self.total_disk_bytes.saturating_sub(removed_bytes);
        }

        // Reserve bytes for each live job's future delete
        self.reserved_bytes = jobs.len() as u64 * STATE_CHANGE_RECORD_SIZE as u64;

        // Create new writable file
        self.create_next_file()?;

        // Filter orphans against the final job set: a job may have been
        // recreated under the same id within the same WAL, in which case
        // the "orphan" is now live again and must not be deleted.
        let live_body_ids = crate::job::live_external_body_ids(jobs.values());
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
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::BodyId;

    /// Switch a test-built job to a synthetic external `BodyRef`. The WAL's
    /// v5 serializer requires `BodyRef::External`; unit tests don't run a
    /// body store, so we fabricate a deterministic id (the job's own id)
    /// and assert on it where roundtripping matters.
    fn external_body(job: &mut Job) {
        job.body = BodyRef::External(BodyId(job.id));
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
            assert!(matches!(&j.body, BodyRef::External(_)));
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
            // v5 round-trip preserves the BodyId reference, not the bytes.
            assert!(matches!(j.body, BodyRef::External(BodyId(42))));
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
            read_header(&[b'T', b'W']),
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
    fn test_replay_quarantines_truncated_older_segment() {
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

        // The truncated middle segment is data loss: it must be preserved
        // as .corrupt evidence, never silently deleted.
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, _, _, _, _) = wal.replay().unwrap();
        assert!(jobs.is_empty());
        assert!(!seg1.exists(), "truncated segment must be renamed away");
        assert!(
            corrupt_sidecar_path(&seg1).exists(),
            "truncated older segment must be quarantined, not deleted"
        );
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
            assert!(matches!(&j2.body, BodyRef::External(_)));
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
            assert!(matches!(&job.body, BodyRef::External(_)));
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
        assert!(matches!(&jobs[&1].body, BodyRef::External(_)));
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
}
