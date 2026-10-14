//! The schedule files in the binlog directory.
//!
//! `schedules` holds every entry's definition and is rewritten on each
//! commit. `schedules.fired` holds each entry's last fire and is rewritten
//! before a tick's fires are put, so a crash between the two loses that tick
//! rather than firing it twice. Kept apart so a fire rewrites a few bytes per
//! entry rather than every entry's body.
//!
//! Neither is a WAL record: WAL segments are reclaimed by tracking the live
//! jobs each holds, and entries would have to join that for state that
//! changes a few times per deploy. A fired job is an ordinary job in the WAL.
//!
//! Both files are replaced whole: written to `<name>.tmp`, fsynced, renamed
//! over the old file, and the directory fsynced. Layout, little-endian:
//!
//! ```text
//! magic[4] version:u32 count:u32 record* crc32:u32     (crc over all before it)
//! schedules record:       owner line synced_at body
//! schedules.fired record: owner name last_fire
//! str/bytes = len:u32 + bytes;  time = seconds:i64 nanos:i32
//! ```
//!
//! Every format version must keep the magic, the version field and the
//! trailing crc32 over the rest: a file whose checksum fails is damage and is
//! set aside, while a checksummed file of another version is a newer tuber's
//! schedules and stops startup rather than being thrown away.
//!
//! A definition is stored as its `schedule-put` line and parsed back through
//! the protocol parser on load, so a stored entry passes exactly the checks a
//! synced one did.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use jiff::Timestamp;

use super::{Catchup, Entry, EntryStats, Line, Schedules};

const DEFS_FILE: &str = "schedules";
const STAMPS_FILE: &str = "schedules.fired";
const DEFS_MAGIC: &[u8; 4] = b"TSCH";
const STAMPS_MAGIC: &[u8; 4] = b"TSTP";
const VERSION: u32 = 1;

/// The two files and their sizes, which count toward `--max-storage-bytes`.
#[derive(Debug)]
pub struct ScheduleFiles {
    dir: PathBuf,
    defs_bytes: u64,
    stamps_bytes: u64,
}

impl ScheduleFiles {
    /// Read the entries stored in `dir`. A missing file is an empty set. A
    /// damaged definitions file is an error; a damaged stamps file is logged
    /// and ignored, so missed ticks are judged from each entry's sync time.
    pub fn open(dir: &Path) -> io::Result<(ScheduleFiles, Schedules)> {
        let mut files = ScheduleFiles {
            dir: dir.to_path_buf(),
            defs_bytes: 0,
            stamps_bytes: 0,
        };
        let mut schedules = Schedules::default();
        let Some(defs) = read(dir, DEFS_FILE, DEFS_MAGIC)? else {
            return Ok((files, schedules));
        };
        files.defs_bytes = defs.len() as u64;

        let stamps = read(dir, STAMPS_FILE, STAMPS_MAGIC).and_then(|bytes| {
            let Some(bytes) = bytes else {
                return Ok(HashMap::new());
            };
            files.stamps_bytes = bytes.len() as u64;
            parse_stamps(&bytes)
        });
        let stamps = match stamps {
            Ok(stamps) => stamps,
            Err(e) if e.kind() == io::ErrorKind::Unsupported => return Err(e),
            Err(e) => {
                tracing::error!("{STAMPS_FILE}: {e}; last fires lost");
                HashMap::new()
            }
        };

        let mut r = Reader::records(&defs);
        for _ in 0..r.count {
            let owner = r.string()?;
            let line_text = r.string()?;
            let synced_at = r.time()?;
            let body = r.bytes()?.to_vec();
            let line = match crate::protocol::parse_schedule_put_line(&line_text) {
                Ok(line) => line,
                Err(resp) => {
                    // Most likely a zone gone from both the host's tzdata
                    // and the copy built into tuber.
                    // The app's next sync puts it back, or is told why not.
                    tracing::error!(
                        owner,
                        line = line_text,
                        reply = ?resp,
                        "{DEFS_FILE}: stored entry no longer parses; dropped",
                    );
                    continue;
                }
            };
            let last_fire = stamps.get(&(owner.clone(), line.name.clone())).copied();
            let mut entry = Entry {
                owner,
                next_fire: None,
                line,
                body,
                synced_at,
                last_fire,
                stats: EntryStats::default(),
            };
            entry.next_fire = entry.line.spec.next_after(entry.recorded());
            schedules.restore(entry);
        }
        Ok((files, schedules))
    }

    pub fn disk_bytes(&self) -> u64 {
        self.defs_bytes + self.stamps_bytes
    }

    /// Rewrite both files after a commit.
    pub fn write_all(&mut self, schedules: &Schedules) -> io::Result<()> {
        let mut w = Writer::new(DEFS_MAGIC, schedules.len(), self.defs_bytes);
        for e in schedules.iter() {
            w.string(&e.owner);
            w.string(&wire_line(&e.line, e.body.len()));
            w.time(e.synced_at);
            w.bytes(&e.body);
        }
        self.defs_bytes = replace(&self.dir, DEFS_FILE, &w.finish())?;
        self.write_stamps(schedules)
    }

    /// Rewrite the last fires, before the fires they record are put.
    pub fn write_stamps(&mut self, schedules: &Schedules) -> io::Result<()> {
        let fired: Vec<(&Entry, Timestamp)> = schedules
            .iter()
            .filter_map(|e| Some((e, e.last_fire?)))
            .collect();
        let mut w = Writer::new(STAMPS_MAGIC, fired.len(), self.stamps_bytes);
        for (e, last_fire) in fired {
            w.string(&e.owner);
            w.string(&e.line.name);
            w.time(last_fire);
        }
        self.stamps_bytes = replace(&self.dir, STAMPS_FILE, &w.finish())?;
        Ok(())
    }
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

/// Replace `dir/name` with `bytes` atomically; returns the size written.
fn replace(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<u64> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, dir.join(name))?;
    // The rename is only durable once the directory entry is.
    crate::body_store::fsync_dir(dir)?;
    Ok(bytes.len() as u64)
}

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    /// `size_hint`: the file's last size, so the buffer rarely regrows.
    fn new(magic: &[u8; 4], count: usize, size_hint: u64) -> Writer {
        let mut buf = Vec::with_capacity(size_hint.max(256) as usize);
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

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = schedules_with(&[
            "rollup splat.maint 1024 0 120 15 idp:rollup con:1 5 * * * *",
            "retention splat.maint 10 5 60 15 con:jobs:2 catchup:600 0 2 * * * Australia/Melbourne",
            "checkpoint splat.checkins 1024 0 120 15 catchup:0 every 10m",
        ]);
        s.get_mut("splat", "rollup").unwrap().last_fire = Some(ts("2026-10-11T03:05:00.25Z"));

        let (mut files, empty) = ScheduleFiles::open(dir.path()).unwrap();
        assert!(empty.is_empty());
        files.write_all(&s).unwrap();
        assert!(files.disk_bytes() > 0);

        let (files2, loaded) = ScheduleFiles::open(dir.path()).unwrap();
        assert_eq!(files2.disk_bytes(), files.disk_bytes());
        assert_eq!(loaded.len(), 3);
        for e in s.iter() {
            let l = loaded.get(&e.owner, &e.line.name).unwrap();
            assert_eq!(l.line, e.line);
            assert_eq!(l.body, e.body);
            assert_eq!(l.synced_at, e.synced_at);
            assert_eq!(l.last_fire, e.last_fire);
        }
        assert_eq!(loaded.bytes(), s.bytes());
        // Next fire resumes from the last fire.
        let rollup = loaded.get("splat", "rollup").unwrap();
        assert_eq!(rollup.next_fire, Some(ts("2026-10-11T04:05:00Z")));
    }

    #[test]
    fn stamps_are_rewritten_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = schedules_with(&["rollup t 0 0 1 15 5 * * * *"]);
        let (mut files, _) = ScheduleFiles::open(dir.path()).unwrap();
        files.write_all(&s).unwrap();
        let defs_before = fs::read(dir.path().join(DEFS_FILE)).unwrap();

        s.get_mut("splat", "rollup").unwrap().last_fire = Some(ts("2026-10-11T03:05:00Z"));
        files.write_stamps(&s).unwrap();
        assert_eq!(fs::read(dir.path().join(DEFS_FILE)).unwrap(), defs_before);
        let (_, loaded) = ScheduleFiles::open(dir.path()).unwrap();
        assert_eq!(
            loaded.get("splat", "rollup").unwrap().last_fire,
            Some(ts("2026-10-11T03:05:00Z"))
        );
    }

    #[test]
    fn a_damaged_definitions_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let s = schedules_with(&["rollup t 0 0 1 15 5 * * * *"]);
        let (mut files, _) = ScheduleFiles::open(dir.path()).unwrap();
        files.write_all(&s).unwrap();
        let path = dir.path().join(DEFS_FILE);
        let mut bytes = fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let err = ScheduleFiles::open(dir.path()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_damaged_stamps_file_loses_only_last_fires() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = schedules_with(&["rollup t 0 0 1 15 5 * * * *"]);
        s.get_mut("splat", "rollup").unwrap().last_fire = Some(ts("2026-10-11T03:05:00Z"));
        let (mut files, _) = ScheduleFiles::open(dir.path()).unwrap();
        files.write_all(&s).unwrap();
        fs::write(dir.path().join(STAMPS_FILE), b"garbage").unwrap();
        let (_, loaded) = ScheduleFiles::open(dir.path()).unwrap();
        let e = loaded.get("splat", "rollup").unwrap();
        assert_eq!(e.last_fire, None);
        assert_eq!(
            e.next_fire,
            Some(ts("2026-10-11T03:05:00Z")),
            "judged from the sync time"
        );
    }
}
