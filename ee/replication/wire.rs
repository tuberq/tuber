//! The replication wire format.
//!
//! A frame is:
//!
//! ```text
//! [kind u8][flags u8][len u32 LE][pos_len u8][pos utf8][payload]
//! ```
//!
//! `len` covers everything after itself (pos_len + pos + payload). The
//! position rides in its `Display`/`FromStr` string form — `Position`'s
//! fields are private by design (a position is log identity, and identity
//! grows a generation number once failover exists), and the string form
//! is the one encoding the WAL module promises can gain a component
//! without invalidating cursors already in flight.
//!
//! For `RECORD` frames the payload is a serialized WAL record in the
//! current WAL version — *the streamed format is the WAL format*. FullJob
//! records have their body rewritten inline (`BODY_KIND_INLINE`) by
//! [`serialize_for_wire`], which is the record shape the OSS replayer
//! already migrates into TOAST on the other side.

use tuber::body_store::{BodyStore, BodyStoreError};
use tuber::job::{BodyId, BodyRef};
use tuber::wal::{
    Position, WAL_VERSION, WalRecord, deserialize_record, serialize_full_job, serialize_id_mark,
    serialize_schedule_fired, serialize_schedule_set, serialize_state_change, serialize_toast_mark,
    serialize_tombstone,
};

/// A WAL record at a position.
pub const FRAME_RECORD: u8 = 0x01;
/// The primary's durable frontier advanced to `pos` (one-past, not a
/// record position). No payload.
pub const FRAME_DURABLE: u8 = 0x02;
/// Replica → primary, the first and only frame the replica sends: the
/// position it last applied. The primary streams everything above it —
/// catch-up scan first, then the live tee. `Position::ZERO` bootstraps
/// a new replica (the WAL is its own base backup). No payload.
pub const FRAME_HELLO: u8 = 0x03;

/// The record's external body could not be resolved at ship time — the
/// job was deleted between tee and ship, so its body was already
/// dereferenced. The frame carries an empty inline body; a delete
/// StateChange for the same job id follows in the stream (deletes are
/// WAL records too), and replicas don't serve reserves, so nothing can
/// observe the placeholder body.
pub const FLAG_BODY_MISSING: u8 = 0b0000_0001;

const FRAME_HEADER: usize = 1 + 1 + 4; // kind + flags + len

#[derive(Debug)]
pub enum Frame {
    Record {
        pos: Position,
        record: WalRecord,
        flags: u8,
    },
    Durable {
        end: Position,
    },
    Hello {
        last_applied: Position,
    },
}

#[derive(Debug)]
pub enum WireError {
    /// Unknown frame kind — protocol mismatch, drop the connection.
    UnknownKind(u8),
    /// The frame's position field did not parse.
    BadPosition,
    /// The payload did not parse as a WAL record.
    BadRecord,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::UnknownKind(k) => write!(f, "unknown frame kind {k:#04x}"),
            WireError::BadPosition => write!(f, "frame position is not a valid WAL position"),
            WireError::BadRecord => write!(f, "frame payload is not a valid WAL record"),
        }
    }
}

impl std::error::Error for WireError {}

/// A job's external body could not be read for any reason but the job
/// having gone: damaged TOAST bytes, bytes that aren't the ones the WAL
/// recorded, an IO error, or no body store to read from. The record can't
/// be shipped faithfully, and a placeholder would hand the replica an
/// empty body for a live job.
#[derive(Debug)]
pub struct BodyUnreadable {
    pub job_id: u64,
    pub body_id: BodyId,
    /// `None` when the primary has no body store at all.
    pub error: Option<BodyStoreError>,
}

impl std::fmt::Display for BodyUnreadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.error {
            Some(e) => write!(f, "job {}'s body is unreadable: {e}", self.job_id),
            None => write!(
                f,
                "job {}'s body {} is external but there is no body store",
                self.job_id, self.body_id.0
            ),
        }
    }
}

impl std::error::Error for BodyUnreadable {}

/// Serialize a record for the wire: resolve an external body through the
/// primary's live body store, check it is the body the WAL recorded, and
/// inline it. Returns the record bytes and the frame flags. `body_store`
/// reads go through the live index, so a body relocated by TOAST
/// compaction between tee and ship still resolves.
pub fn serialize_for_wire(
    record: &WalRecord,
    body_store: Option<&BodyStore>,
) -> Result<(Vec<u8>, u8), BodyUnreadable> {
    Ok(match record {
        WalRecord::FullJob(job) => match &job.body {
            BodyRef::External(id, crc) => {
                let unreadable = |error| BodyUnreadable {
                    job_id: job.id,
                    body_id: *id,
                    error,
                };
                let bs = body_store.ok_or_else(|| unreadable(None))?;
                match bs.read_body_checked(*id, *crc) {
                    Ok(bytes) => {
                        let mut inlined = job.as_ref().clone();
                        inlined.body = BodyRef::new_inline(bytes);
                        (serialize_full_job(&inlined), 0)
                    }
                    // Deleted between tee and ship; see FLAG_BODY_MISSING.
                    Err(BodyStoreError::NotFound(_)) => {
                        let mut placeholder = job.as_ref().clone();
                        placeholder.body = BodyRef::new_inline(Vec::new());
                        (serialize_full_job(&placeholder), FLAG_BODY_MISSING)
                    }
                    Err(e) => return Err(unreadable(Some(e))),
                }
            }
            // Already inline (compaction migrations of tiny bodies, or
            // pre-TOAST records surfaced by a catch-up scan).
            _ => (serialize_full_job(job), 0),
        },
        WalRecord::StateChange {
            job_id,
            new_state,
            new_priority,
            new_delay_nanos,
            expiry_epoch_secs,
            reason,
            change_epoch_secs,
        } => (
            serialize_state_change(
                *job_id,
                *new_state,
                *new_priority,
                *new_delay_nanos,
                *expiry_epoch_secs,
                *reason,
                *change_epoch_secs,
            ),
            0,
        ),
        WalRecord::Tombstone {
            job_id,
            tube,
            key,
            expires_epoch_secs,
        } => (
            serialize_tombstone(*job_id, tube, key, *expires_epoch_secs),
            0,
        ),
        WalRecord::IdMark { high_water } => (serialize_id_mark(*high_water), 0),
        WalRecord::ToastMark(point) => (serialize_toast_mark(*point), 0),
        WalRecord::ScheduleSet { owner, entries } => (serialize_schedule_set(owner, entries), 0),
        WalRecord::ScheduleFired {
            owner,
            name,
            last_fire,
        } => (serialize_schedule_fired(owner, name, *last_fire), 0),
    })
}

fn encode_frame(kind: u8, flags: u8, pos: Position, payload: &[u8]) -> Vec<u8> {
    let pos_str = pos.to_string();
    let mut out = Vec::with_capacity(FRAME_HEADER + 1 + pos_str.len() + payload.len());
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&((1 + pos_str.len() + payload.len()) as u32).to_le_bytes());
    out.push(pos_str.len() as u8);
    out.extend_from_slice(pos_str.as_bytes());
    out.extend_from_slice(payload);
    out
}

pub fn encode_record(pos: Position, record_bytes: &[u8], flags: u8) -> Vec<u8> {
    encode_frame(FRAME_RECORD, flags, pos, record_bytes)
}

pub fn encode_durable(end: Position) -> Vec<u8> {
    encode_frame(FRAME_DURABLE, 0, end, &[])
}

pub fn encode_hello(last_applied: Position) -> Vec<u8> {
    encode_frame(FRAME_HELLO, 0, last_applied, &[])
}

/// Decode one frame from the front of `buf`. `Ok(None)` means the buffer
/// holds only a partial frame — read more bytes and retry. On success
/// returns the frame and the number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<Option<(Frame, usize)>, WireError> {
    if buf.len() < FRAME_HEADER {
        return Ok(None);
    }
    let kind = buf[0];
    let flags = buf[1];
    let len = u32::from_le_bytes(buf[2..6].try_into().unwrap()) as usize;
    let total = FRAME_HEADER + len;
    if buf.len() < total {
        return Ok(None);
    }
    let body = &buf[FRAME_HEADER..total];
    let [pos_len, rest @ ..] = body else {
        return Err(WireError::BadPosition);
    };
    let pos_len = *pos_len as usize;
    if rest.len() < pos_len {
        return Err(WireError::BadPosition);
    }
    let pos: Position = std::str::from_utf8(&rest[..pos_len])
        .map_err(|_| WireError::BadPosition)?
        .parse()
        .map_err(|_| WireError::BadPosition)?;
    let payload = &rest[pos_len..];
    let frame = match kind {
        FRAME_DURABLE => Frame::Durable { end: pos },
        FRAME_HELLO => Frame::Hello { last_applied: pos },
        FRAME_RECORD => {
            let (record, consumed) =
                deserialize_record(payload, WAL_VERSION).map_err(|_| WireError::BadRecord)?;
            if consumed != payload.len() {
                return Err(WireError::BadRecord);
            }
            Frame::Record { pos, record, flags }
        }
        other => return Err(WireError::UnknownKind(other)),
    };
    Ok(Some((frame, total)))
}

/// Read one complete frame from `r`, using `buf` as the carry-over buffer
/// between calls (bytes past the decoded frame stay in it for the next
/// call). Blocks until a frame is complete; a peer that hangs up
/// mid-frame surfaces as `UnexpectedEof`.
pub fn read_frame<R: std::io::Read>(r: &mut R, buf: &mut Vec<u8>) -> std::io::Result<Frame> {
    loop {
        match decode(buf) {
            Ok(Some((frame, consumed))) => {
                buf.drain(..consumed);
                return Ok(frame);
            }
            Ok(None) => {
                let mut chunk = [0u8; 8192];
                let n = r.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "peer closed mid-frame",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
            }
        }
    }
}
