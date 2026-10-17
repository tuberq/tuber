//! Recurring schedules on disk: in the WAL, when persistence is on.
//!
//! An owner's schedule set record holds all its entries, and is written on
//! each commit that changes them, synced before `SYNCED`. A schedule fired
//! record holds one entry's last fire, and is written and synced before a
//! tick's fires are put, so a crash between the two loses that tick rather
//! than firing it twice. Kept apart so a fire writes a few bytes per entry
//! rather than every entry's body.
//!
//! Each live record pins its segment, as a live job's record does: an
//! owner's set until its next one, and an entry's last fire until its next
//! one or its owner's next set, which carries every entry's last fire.
//! Compaction rewrites an owner's set to move it, and its last fires, out of
//! the oldest segment.
//!
//! An entry is stored as its `schedule-put` line and parsed back through the
//! protocol parser on restore, so a restored entry passes exactly the checks
//! a synced one did.
//!
//! Before WAL v9 the schedules were two files beside the WAL, each replaced
//! whole: `schedules` (definitions) and `schedules.fired` (last fires). A
//! start that finds them moves their entries into the WAL and removes them.
//! Layout, little-endian:
//!
//! ```text
//! magic[4] version:u32 count:u32 record* crc32:u32     (crc over all before it)
//! schedules record:       owner line synced_at body
//! schedules.fired record: owner name last_fire
//! str/bytes = len:u32 + bytes;  time = seconds:i64 nanos:i32
//! ```
//!
//! A file whose checksum fails is damage and is set aside, while a
//! checksummed file of another version stops startup rather than being
//! thrown away.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use jiff::Timestamp;

use super::{Catchup, Entry, EntryStats, Line, Schedules};
use crate::wal::{RecordRef, ReplayedSchedules, ScheduleRecord, Wal};

const DEFS_FILE: &str = "schedules";
const STAMPS_FILE: &str = "schedules.fired";
const DEFS_MAGIC: &[u8; 4] = b"TSCH";
const STAMPS_MAGIC: &[u8; 4] = b"TSTP";
const VERSION: u32 = 1;

/// The WAL records holding the schedules, and so pinning their segments.
#[derive(Debug, Default)]
pub struct ScheduleRefs {
    /// Each owner's latest set.
    sets: HashMap<String, RecordRef>,
    /// Last fires written since their owner's set, by owner then name.
    stamps: HashMap<String, HashMap<String, RecordRef>>,
}

impl ScheduleRefs {
    /// Rebuild the entries a replay found, and the references their
    /// records hold.
    pub fn restore(wal: &mut Wal, replayed: ReplayedSchedules) -> (ScheduleRefs, Schedules) {
        let mut refs = ScheduleRefs::default();
        let mut schedules = Schedules::default();
        let ReplayedSchedules {
            sets, mut stamps, ..
        } = replayed;
        for (owner, (records, set)) in sets {
            refs.sets.insert(owner.clone(), set);
            for r in records {
                let Some(mut entry) = stored_entry(&owner, &r.line, r.body, r.synced_at, r.last_fire)
                else {
                    continue;
                };
                if let Some((last_fire, stamp)) =
                    stamps.remove(&(owner.clone(), entry.line.name.clone()))
                {
                    entry.last_fire = last_fire;
                    refs.stamps
                        .entry(owner.clone())
                        .or_default()
                        .insert(entry.line.name.clone(), stamp);
                }
                entry.next_fire = entry.line.spec.next_after(entry.recorded());
                schedules.restore(entry);
            }
        }
        // Last fires of entries that didn't come back.
        for (_, stamp) in stamps.into_values() {
            wal.release_record(stamp);
        }
        (refs, schedules)
    }

    /// Journal `owner`'s entries as `schedules` holds them now, releasing
    /// the records that supersedes.
    pub fn write_set(&mut self, wal: &mut Wal, schedules: &Schedules, owner: &str) -> io::Result<()> {
        let records: Vec<ScheduleRecord> = schedules
            .owner_entries(owner)
            .map(|e| ScheduleRecord {
                line: wire_line(&e.line, e.body.len()),
                body: e.body.clone(),
                synced_at: e.synced_at,
                last_fire: e.last_fire,
            })
            .collect();
        let set = wal.write_schedule_set(owner, &records)?;
        if let Some(old) = self.sets.remove(owner) {
            wal.release_record(old);
        }
        for stamp in self.stamps.remove(owner).into_iter().flat_map(HashMap::into_values) {
            wal.release_record(stamp);
        }
        if let Some(set) = set {
            self.sets.insert(owner.to_string(), set);
        }
        Ok(())
    }

    /// Journal the last fire of `owner`'s entry `name`.
    pub fn write_fired(
        &mut self,
        wal: &mut Wal,
        owner: &str,
        name: &str,
        last_fire: Option<Timestamp>,
    ) -> io::Result<()> {
        let stamp = wal.write_schedule_fired(owner, name, last_fire)?;
        if let Some(old) = self
            .stamps
            .entry(owner.to_string())
            .or_default()
            .insert(name.to_string(), stamp)
        {
            wal.release_record(old);
        }
        Ok(())
    }

    /// Owners with a record in segment `seq`, whose sets compaction must
    /// rewrite for it to be collected.
    pub fn owners_in(&self, seq: u64) -> BTreeSet<String> {
        let sets = self.sets.iter().filter(|(_, r)| r.seq == seq);
        let stamps = self
            .stamps
            .iter()
            .filter(|(_, names)| names.values().any(|r| r.seq == seq));
        sets.map(|(owner, _)| owner.clone())
            .chain(stamps.map(|(owner, _)| owner.clone()))
            .collect()
    }
}

/// An entry as stored, or `None`, logged, if its line no longer parses.
/// Its next fire is left for the caller.
fn stored_entry(
    owner: &str,
    line_text: &str,
    body: Vec<u8>,
    synced_at: Timestamp,
    last_fire: Option<Timestamp>,
) -> Option<Entry> {
    let line = match crate::protocol::parse_schedule_put_line(line_text) {
        Ok(line) => line,
        Err(resp) => {
            // Most likely a zone gone from both the host's tzdata and the
            // copy built into tuber. The app's next sync puts it back, or is
            // told why not.
            tracing::error!(
                owner,
                line = line_text,
                reply = ?resp,
                "stored schedule entry no longer parses; dropped",
            );
            return None;
        }
    };
    Some(Entry {
        owner: owner.to_string(),
        next_fire: None,
        line,
        body,
        synced_at,
        last_fire,
        stats: EntryStats::default(),
    })
}

/// Read the schedule files an earlier tuber kept in `dir`: `None` if there
/// are none. A damaged definitions file is an error; a damaged stamps file
/// is logged and ignored, so missed ticks are judged from each entry's sync
/// time.
pub fn read_files(dir: &Path) -> io::Result<Option<Schedules>> {
    let Some(defs) = read(dir, DEFS_FILE, DEFS_MAGIC)? else {
        return Ok(None);
    };
    let stamps = read(dir, STAMPS_FILE, STAMPS_MAGIC).and_then(|bytes| match bytes {
        Some(bytes) => parse_stamps(&bytes),
        None => Ok(HashMap::new()),
    });
    let stamps = match stamps {
        Ok(stamps) => stamps,
        Err(e) if e.kind() == io::ErrorKind::Unsupported => return Err(e),
        Err(e) => {
            tracing::error!("{STAMPS_FILE}: {e}; last fires lost");
            HashMap::new()
        }
    };

    let mut schedules = Schedules::default();
    let mut r = Reader::records(&defs);
    for _ in 0..r.count {
        let owner = r.string()?;
        let line_text = r.string()?;
        let synced_at = r.time()?;
        let body = r.bytes()?.to_vec();
        let Some(mut entry) = stored_entry(&owner, &line_text, body, synced_at, None) else {
            continue;
        };
        entry.last_fire = stamps.get(&(owner, entry.line.name.clone())).copied();
        entry.next_fire = entry.line.spec.next_after(entry.recorded());
        schedules.restore(entry);
    }
    Ok(Some(schedules))
}

/// Remove the schedule files from `dir`, once the WAL holds their entries.
pub fn remove_files(dir: &Path) -> io::Result<()> {
    let mut removed = false;
    for name in [DEFS_FILE, STAMPS_FILE] {
        // A replace cut short by a crash leaves its temporary file behind.
        for path in [dir.join(name), dir.join(format!("{name}.tmp"))] {
            match fs::remove_file(&path) {
                Ok(()) => removed = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    if removed {
        crate::body_store::fsync_dir(dir)?;
    }
    Ok(())
}

/// Write `schedules` as an earlier tuber would have.
#[cfg(test)]
pub fn write_files(dir: &Path, schedules: &Schedules) -> io::Result<()> {
    let mut w = Writer::new(DEFS_MAGIC, schedules.len());
    for e in schedules.iter() {
        w.string(&e.owner);
        w.string(&wire_line(&e.line, e.body.len()));
        w.time(e.synced_at);
        w.bytes(&e.body);
    }
    fs::write(dir.join(DEFS_FILE), w.finish())?;
    let fired: Vec<(&Entry, Timestamp)> = schedules
        .iter()
        .filter_map(|e| Some((e, e.last_fire?)))
        .collect();
    let mut w = Writer::new(STAMPS_MAGIC, fired.len());
    for (e, last_fire) in fired {
        w.string(&e.owner);
        w.string(&e.line.name);
        w.time(last_fire);
    }
    fs::write(dir.join(STAMPS_FILE), w.finish())
}

/// Move a damaged definitions file out of the way, to `schedules.corrupt`
/// as the WAL names its corrupt segments — or `schedules.corrupt.N`, the
/// first that's free, so an earlier one is never overwritten.
pub fn set_aside_corrupt(dir: &Path) -> io::Result<PathBuf> {
    let path = dir.join(DEFS_FILE);
    let base = crate::wal::corrupt_sidecar_path(&path);
    let aside = std::iter::once(base.clone())
        .chain((1..).map(|n| base.with_extension(format!("corrupt.{n}"))))
        .find(|p| !p.exists())
        .expect("an unbounded range has a free name");
    fs::rename(&path, &aside)?;
    Ok(aside)
}

/// `line` as the `schedule-put` arguments that would recreate it.
fn wire_line(line: &Line, body_len: usize) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(64 + line.spec_text.len());
    let _ = write!(
        s,
        "{} {} {} {} {} {}",
        line.name, line.tube, line.pri, line.delay, line.ttr, body_len
    );
    if !line.tags.is_empty() {
        let _ = write!(s, " {}", line.tags);
    }
    match line.catchup {
        Catchup::Once => {}
        Catchup::Within(secs) => {
            let _ = write!(s, " catchup:{secs}");
        }
        Catchup::Never => s.push_str(" catchup:0"),
    }
    let _ = write!(s, " {}", line.spec_text);
    s
}

fn parse_stamps(bytes: &[u8]) -> io::Result<HashMap<(String, String), Timestamp>> {
    let mut r = Reader::records(bytes);
    let mut stamps = HashMap::new();
    for _ in 0..r.count {
        let owner = r.string()?;
        let name = r.string()?;
        stamps.insert((owner, name), r.time()?);
    }
    Ok(stamps)
}

/// The file's bytes once its magic, version and checksum check out, or
/// `None` if it doesn't exist.
fn read(dir: &Path, name: &str, magic: &[u8; 4]) -> io::Result<Option<Vec<u8>>> {
    let bytes = match fs::read(dir.join(name)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{name}: {what}"));
    if bytes.len() < 16 || &bytes[..4] != magic {
        return Err(bad("not a tuber schedule file"));
    }
    // Checksum before version, so a damaged version field reads as damage
    // rather than as a format this binary doesn't know.
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(bad("checksum mismatch"));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != VERSION {
        // Not damage: schedules a different tuber wrote, most likely a newer
        // one before a downgrade. Refuse rather than discard them.
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "{name}: format version {version}, this binary reads {VERSION}. Run the \
                 tuber that wrote it, or move {name} aside to start without schedules \
                 until apps re-sync"
            ),
        ));
    }
    Ok(Some(bytes))
}

#[cfg(test)]
struct Writer {
    buf: Vec<u8>,
}

#[cfg(test)]
impl Writer {
    fn new(magic: &[u8; 4], count: usize) -> Writer {
        let mut buf = Vec::new();
        buf.extend_from_slice(magic);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&(count as u32).to_le_bytes());
        Writer { buf }
    }

    fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(b);
    }

    fn string(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    fn time(&mut self, t: Timestamp) {
        self.buf.extend_from_slice(&t.as_second().to_le_bytes());
        self.buf
            .extend_from_slice(&t.subsec_nanosecond().to_le_bytes());
    }

    fn finish(mut self) -> Vec<u8> {
        let crc = crc32fast::hash(&self.buf);
        self.buf.extend_from_slice(&crc.to_le_bytes());
        self.buf
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    count: u32,
}

impl<'a> Reader<'a> {
    /// A reader over the records of a file `read` has checked.
    fn records(file: &'a [u8]) -> Reader<'a> {
        let count = u32::from_le_bytes(file[8..12].try_into().unwrap());
        Reader {
            buf: &file[12..file.len() - 4],
            count,
        }
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schedule file truncated",
            ));
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap());
        self.take(len as usize)
    }

    fn string(&mut self) -> io::Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn time(&mut self) -> io::Result<Timestamp> {
        let secs = i64::from_le_bytes(self.take(8)?.try_into().unwrap());
        let nanos = i32::from_le_bytes(self.take(4)?.try_into().unwrap());
        Timestamp::new(secs, nanos).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::Staging;

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn schedules_with(lines: &[&str]) -> Schedules {
        let mut s = Schedules::default();
        let mut staging = Staging::new("splat".to_string(), 0);
        for text in lines {
            let line = crate::protocol::parse_schedule_put_line(text).unwrap();
            staging
                .entries
                .insert(line.name.clone(), (line, b"{\"class\":\"Job\"}".to_vec()));
        }
        s.commit(staging, ts("2026-10-11T03:00:30Z"));
        s
    }

    /// Replay `dir`'s WAL and restore the schedules it holds.
    fn restored(dir: &Path) -> Schedules {
        let mut wal = Wal::open(dir, crate::wal::WalConfig::default()).unwrap();
        wal.replay().unwrap();
        let replayed = wal.take_replayed_schedules();
        ScheduleRefs::restore(&mut wal, replayed).1
    }

    fn assert_same(loaded: &Schedules, s: &Schedules) {
        assert_eq!(loaded.len(), s.len());
        for e in s.iter() {
            let l = loaded.get(&e.owner, &e.line.name).unwrap();
            assert_eq!(l.line, e.line);
            assert_eq!(l.body, e.body);
            assert_eq!(l.synced_at, e.synced_at);
            assert_eq!(l.last_fire, e.last_fire);
        }
        assert_eq!(loaded.bytes(), s.bytes());
    }

    fn three_entries() -> Schedules {
        let mut s = schedules_with(&[
            "rollup splat.maint 1024 0 120 15 idp:rollup con:1 5 * * * *",
            "retention splat.maint 10 5 60 15 con:jobs:2 catchup:600 0 2 * * * Australia/Melbourne",
            "checkpoint splat.checkins 1024 0 120 15 catchup:0 every 10m",
        ]);
        s.get_mut("splat", "rollup").unwrap().last_fire = Some(ts("2026-10-11T03:05:00.25Z"));
        s
    }

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let s = three_entries();
        {
            let mut wal = Wal::open(dir.path(), crate::wal::WalConfig::default()).unwrap();
            let mut refs = ScheduleRefs::default();
            refs.write_set(&mut wal, &s, "splat").unwrap();
            wal.sync().unwrap();
        }
        let loaded = restored(dir.path());
        assert_same(&loaded, &s);
        // Next fire resumes from the last fire.
        let rollup = loaded.get("splat", "rollup").unwrap();
        assert_eq!(rollup.next_fire, Some(ts("2026-10-11T04:05:00Z")));
    }

    #[test]
    fn a_last_fire_written_after_the_set_wins() {
        let dir = tempfile::tempdir().unwrap();
        let s = schedules_with(&["rollup t 0 0 1 15 5 * * * *"]);
        {
            let mut wal = Wal::open(dir.path(), crate::wal::WalConfig::default()).unwrap();
            let mut refs = ScheduleRefs::default();
            refs.write_set(&mut wal, &s, "splat").unwrap();
            let t = Some(ts("2026-10-11T03:05:00Z"));
            refs.write_fired(&mut wal, "splat", "rollup", t).unwrap();
            assert_eq!(refs.owners_in(1), BTreeSet::from(["splat".to_string()]));
            wal.sync().unwrap();
        }
        let loaded = restored(dir.path());
        let e = loaded.get("splat", "rollup").unwrap();
        assert_eq!(e.last_fire, Some(ts("2026-10-11T03:05:00Z")));
        assert_eq!(e.next_fire, Some(ts("2026-10-11T04:05:00Z")));
    }

    #[test]
    fn files_an_earlier_tuber_wrote_read_back() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_files(dir.path()).unwrap().is_none());
        let s = three_entries();
        write_files(dir.path(), &s).unwrap();
        let loaded = read_files(dir.path()).unwrap().unwrap();
        assert_same(&loaded, &s);

        remove_files(dir.path()).unwrap();
        assert!(read_files(dir.path()).unwrap().is_none());
        assert!(!dir.path().join(STAMPS_FILE).exists());
    }

    #[test]
    fn a_damaged_definitions_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let s = schedules_with(&["rollup t 0 0 1 15 5 * * * *"]);
        write_files(dir.path(), &s).unwrap();
        let path = dir.path().join(DEFS_FILE);
        let mut bytes = fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let err = read_files(dir.path()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_damaged_stamps_file_loses_only_last_fires() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = schedules_with(&["rollup t 0 0 1 15 5 * * * *"]);
        s.get_mut("splat", "rollup").unwrap().last_fire = Some(ts("2026-10-11T03:05:00Z"));
        write_files(dir.path(), &s).unwrap();
        fs::write(dir.path().join(STAMPS_FILE), b"garbage").unwrap();
        let loaded = read_files(dir.path()).unwrap().unwrap();
        let e = loaded.get("splat", "rollup").unwrap();
        assert_eq!(e.last_fire, None);
        assert_eq!(
            e.next_fire,
            Some(ts("2026-10-11T03:05:00Z")),
            "judged from the sync time"
        );
    }
}
