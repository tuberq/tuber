//! Recurring schedules: entries an app registers with a schedule sync, each
//! put into its tube by the server at the times its spec names. See
//! docs/scheduler.md for the design.
//!
//! An entry is identified by (owner, name), both chosen by the app. A sync
//! (`schedule-begin` / `schedule-put`… / `schedule-commit`) carries the
//! owner's complete set and replaces it in one step: entries missing from
//! the set are removed, and an entry whose definition is unchanged keeps its
//! next fire, last fire and counters.

pub mod spec;
pub mod store;

use std::collections::BTreeMap;

use jiff::Timestamp;

use crate::job::JobExt;
use crate::protocol::Response;
use crate::tube::IdpDedups;
use spec::Spec;

/// Most entries held across all owners. Keeps the schedule files and the
/// per-tick scan bounded.
pub const MAX_ENTRIES: usize = 10_000;

/// Memory charged per entry against `--max-jobs-size`, beside its body and
/// strings. Same order as a job's `JOB_OVERHEAD_BYTES`.
const ENTRY_OVERHEAD_BYTES: u64 = 512;

/// What to do with a tick that passed while it couldn't fire — tuber was
/// down, or the clock jumped forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Catchup {
    /// Fire once, however many ticks were missed and however long ago.
    Once,
    /// Fire once if the latest missed tick is at most this many seconds old.
    Within(u32),
    /// Never fire a missed tick (`catchup:0`).
    Never,
}

impl Catchup {
    pub fn describe(self) -> String {
        match self {
            Catchup::Once => "once".to_string(),
            Catchup::Within(secs) => format!("once within {secs}s"),
            Catchup::Never => "never".to_string(),
        }
    }
}

/// Everything a `schedule-put` line carries; the body follows separately.
#[derive(Clone, Debug, PartialEq)]
pub struct Line {
    pub name: String,
    pub tube: String,
    pub pri: u32,
    pub delay: u32,
    pub ttr: u32,
    pub tags: JobExt,
    pub catchup: Catchup,
    pub spec: Spec,
    /// The spec as the app wrote it, for display.
    pub spec_text: String,
}

impl Line {
    /// Whether `self` with `body` would put the same jobs at the same times
    /// as `other` with `other_body`. The spec is compared parsed, so two
    /// spellings of one schedule are the same definition.
    fn same_definition(&self, body: &[u8], other: &Line, other_body: &[u8]) -> bool {
        self.tube == other.tube
            && self.pri == other.pri
            && self.delay == other.delay
            && self.ttr == other.ttr
            && self.tags == other.tags
            && self.catchup == other.catchup
            && self.spec == other.spec
            && body == other_body
    }
}

/// Memory an entry is charged against `--max-jobs-size`.
pub fn memory_cost(owner: &str, line: &Line, body: &[u8]) -> u64 {
    (owner.len() + line.name.len() + line.tube.len() + line.spec_text.len() + body.len()) as u64
        + ENTRY_OVERHEAD_BYTES
}

/// What each fire of an entry has produced. In memory only: reset by a
/// restart, like the per-tube counters.
#[derive(Clone, Debug, Default)]
pub struct EntryStats {
    pub inserted: u64,
    /// `idp:` dedups by the state of the job matched.
    pub deduped: IdpDedups,
    /// Refused puts: `DRAINING`, `OUT_OF_MEMORY`, `OUT_OF_STORAGE`, …
    pub rejected: u64,
    /// Consecutive fires that deduped against a buried job.
    pub buried_streak: u64,
    /// The reply a client would have got from the latest fire.
    pub last_result: Option<String>,
}

impl EntryStats {
    pub fn fires(&self) -> u64 {
        self.inserted + self.deduped.total() + self.rejected
    }
}

#[derive(Debug)]
pub struct Entry {
    pub owner: String,
    pub line: Line,
    pub body: Vec<u8>,
    /// When this definition took effect: added, or last changed.
    pub synced_at: Timestamp,
    pub last_fire: Option<Timestamp>,
    pub next_fire: Option<Timestamp>,
    pub stats: EntryStats,
}

impl Entry {
    /// The latest time recorded for this entry: its last fire, or its sync
    /// if that's later. Ticks up to here are done; a restart resumes after.
    pub fn recorded(&self) -> Timestamp {
        self.last_fire
            .map_or(self.synced_at, |last| last.max(self.synced_at))
    }
}

/// A sync in progress on one connection. Nothing in it is live until it is
/// committed; dropping it (`schedule-abort`, or the connection closing)
/// leaves the owner's entries as they were.
#[derive(Debug)]
pub struct Staging {
    pub owner: String,
    pub entries: BTreeMap<String, (Line, Vec<u8>)>,
    /// A `schedule-put` in this sync was refused. Commit aborts, so a bad
    /// line can't silently drop an entry from the owner's set.
    pub poisoned: bool,
    /// `memory_cost` of everything staged.
    pub bytes: u64,
    /// `memory_cost` of the owner's entries when the sync began: what a
    /// commit will free, so staging up to it needs no new room.
    pub held_bytes: u64,
}

impl Staging {
    pub fn new(owner: String, held_bytes: u64) -> Staging {
        Staging {
            owner,
            entries: BTreeMap::new(),
            poisoned: false,
            bytes: 0,
            held_bytes,
        }
    }

    /// Stage one entry, returning its memory cost. `in_use` is everything
    /// already counted against `limit` (`--max-jobs-size`), this sync's
    /// staging included. The sync is credited with the owner's current
    /// entries, since its commit replaces them: resyncing an unchanged set
    /// needs no room beyond what the set already has.
    pub fn stage(
        &mut self,
        line: Result<Box<Line>, Response>,
        body: Option<Vec<u8>>,
        limit: Option<u64>,
        in_use: u64,
    ) -> Result<u64, Response> {
        let line = *line?;
        let body = body.ok_or(Response::InternalError)?;
        if self.entries.contains_key(&line.name) {
            return Err(Response::BadFormat);
        }
        let cost = memory_cost(&self.owner, &line, &body);
        let growth = (self.bytes + cost).saturating_sub(self.held_bytes);
        let over_budget =
            limit.is_some_and(|limit| in_use.saturating_sub(self.bytes) + growth > limit);
        if self.entries.len() >= MAX_ENTRIES || over_budget {
            return Err(Response::OutOfMemory);
        }
        self.bytes += cost;
        self.entries.insert(line.name.clone(), (line, body));
        Ok(cost)
    }
}

/// Reply counts for `schedule-commit`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncCounts {
    pub added: u32,
    pub changed: u32,
    pub removed: u32,
    pub unchanged: u32,
}

/// Every entry, by owner then name. An owner with no entries has no map.
#[derive(Debug, Default)]
pub struct Schedules {
    owners: BTreeMap<String, BTreeMap<String, Entry>>,
    /// Sum of `memory_cost` over every entry.
    bytes: u64,
}

impl Schedules {
    pub fn len(&self) -> usize {
        self.owners.values().map(BTreeMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn get(&self, owner: &str, name: &str) -> Option<&Entry> {
        self.owners.get(owner)?.get(name)
    }

    pub fn get_mut(&mut self, owner: &str, name: &str) -> Option<&mut Entry> {
        self.owners.get_mut(owner)?.get_mut(name)
    }

    /// Add an entry read back from disk.
    pub fn restore(&mut self, entry: Entry) {
        self.bytes += memory_cost(&entry.owner, &entry.line, &entry.body);
        let names = self.owners.entry(entry.owner.clone()).or_default();
        if let Some(old) = names.insert(entry.line.name.clone(), entry) {
            self.bytes -= memory_cost(&old.owner, &old.line, &old.body);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.owners.values().flat_map(BTreeMap::values)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Entry> {
        self.owners.values_mut().flat_map(BTreeMap::values_mut)
    }

    pub fn owner_entries(&self, owner: &str) -> impl Iterator<Item = &Entry> {
        self.owners
            .get(owner)
            .into_iter()
            .flat_map(BTreeMap::values)
    }

    /// (entries, memory) `owner` holds now.
    pub fn owner_usage(&self, owner: &str) -> (usize, u64) {
        self.owner_entries(owner).fold((0, 0), |(n, bytes), e| {
            (n + 1, bytes + memory_cost(&e.owner, &e.line, &e.body))
        })
    }

    /// Replace the staged owner's entries with the staged set, as of `now`.
    /// New and changed entries first fire at their first tick strictly after
    /// `now`, so a sync never fires anything itself.
    pub fn commit(&mut self, staging: Staging, now: Timestamp) -> SyncCounts {
        let owner = staging.owner;
        let mut old = self.owners.remove(&owner).unwrap_or_default();
        for e in old.values() {
            self.bytes -= memory_cost(&e.owner, &e.line, &e.body);
        }

        let mut counts = SyncCounts::default();
        let mut new = BTreeMap::new();
        for (name, (line, body)) in staging.entries {
            let entry = match old.remove(&name) {
                None => {
                    counts.added += 1;
                    Entry {
                        owner: owner.clone(),
                        next_fire: line.spec.next_after(now),
                        line,
                        body,
                        synced_at: now,
                        last_fire: None,
                        stats: EntryStats::default(),
                    }
                }
                Some(mut e) if e.line.same_definition(&e.body, &line, &body) => {
                    counts.unchanged += 1;
                    // A respelling of the same schedule: show the new text,
                    // change nothing that fires.
                    e.line.spec_text = line.spec_text;
                    e
                }
                Some(mut e) => {
                    counts.changed += 1;
                    let from = e.last_fire.map_or(now, |last| last.max(now));
                    e.next_fire = line.spec.next_after(from);
                    e.line = line;
                    e.body = body;
                    e.synced_at = now;
                    e
                }
            };
            self.bytes += memory_cost(&entry.owner, &entry.line, &entry.body);
            new.insert(name, entry);
        }
        counts.removed = old.len() as u32;
        if !new.is_empty() {
            self.owners.insert(owner, new);
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(name: &str, spec: &str) -> Line {
        Line {
            name: name.to_string(),
            tube: "maint".to_string(),
            pri: 1024,
            delay: 0,
            ttr: 120,
            tags: JobExt::default(),
            catchup: Catchup::Once,
            spec: Spec::parse(spec).unwrap(),
            spec_text: spec.to_string(),
        }
    }

    fn staging(owner: &str, lines: Vec<Line>) -> Staging {
        let mut s = Staging::new(owner.to_string(), 0);
        for l in lines {
            s.entries.insert(l.name.clone(), (l, b"{}".to_vec()));
        }
        s
    }

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn counts(added: u32, changed: u32, removed: u32, unchanged: u32) -> SyncCounts {
        SyncCounts {
            added,
            changed,
            removed,
            unchanged,
        }
    }

    #[test]
    fn sync_adds_changes_removes_and_keeps() {
        let mut s = Schedules::default();
        let t0 = ts("2026-10-11T03:00:30Z");
        let first = vec![
            line("rollup", "5 * * * *"),
            line("checkpoint", "every 10m"),
            line("stats", "7 * * * *"),
        ];
        assert_eq!(
            s.commit(staging("splat", first.clone()), t0),
            counts(3, 0, 0, 0)
        );
        assert_eq!(
            s.get("splat", "rollup").unwrap().next_fire,
            Some(ts("2026-10-11T03:05:00Z"))
        );

        // Same set again: nothing moves.
        let t1 = ts("2026-10-11T03:01:30Z");
        assert_eq!(s.commit(staging("splat", first), t1), counts(0, 0, 0, 3));
        assert_eq!(s.get("splat", "rollup").unwrap().synced_at, t0);

        // Edit one, drop one, rename one.
        let next = vec![
            line("rollup", "15 * * * *"),
            line("stats_hourly", "7 * * * *"),
        ];
        assert_eq!(s.commit(staging("splat", next), t1), counts(1, 1, 2, 0));
        let rollup = s.get("splat", "rollup").unwrap();
        assert_eq!(rollup.next_fire, Some(ts("2026-10-11T03:15:00Z")));
        assert_eq!(rollup.synced_at, t1);
        assert!(s.get("splat", "checkpoint").is_none());
        assert!(s.get("splat", "stats").is_none());
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn respelled_spec_is_unchanged() {
        let mut s = Schedules::default();
        let t0 = ts("2026-10-11T03:00:00Z");
        s.commit(
            staging(
                "splat",
                vec![line("retention", "0 2 * * * Australia/Melbourne")],
            ),
            t0,
        );
        let respelled = vec![line("retention", "CRON_TZ=Australia/Melbourne 0 2 * * *")];
        assert_eq!(
            s.commit(staging("splat", respelled), t0),
            counts(0, 0, 0, 1)
        );
        assert_eq!(
            s.get("splat", "retention").unwrap().line.spec_text,
            "CRON_TZ=Australia/Melbourne 0 2 * * *"
        );
    }

    #[test]
    fn body_tags_and_catchup_are_part_of_the_definition() {
        let mut s = Schedules::default();
        let t0 = ts("2026-10-11T03:00:00Z");
        s.commit(staging("splat", vec![line("a", "@hourly")]), t0);

        let mut st = staging("splat", vec![]);
        st.entries
            .insert("a".into(), (line("a", "@hourly"), b"other".to_vec()));
        assert_eq!(s.commit(st, t0), counts(0, 1, 0, 0));

        let mut tagged = line("a", "@hourly");
        tagged.tags.idempotency_key = Some(("a".into(), 0));
        let mut st = staging("splat", vec![]);
        st.entries
            .insert("a".into(), (tagged.clone(), b"other".to_vec()));
        assert_eq!(s.commit(st, t0), counts(0, 1, 0, 0));

        tagged.catchup = Catchup::Never;
        let mut st = staging("splat", vec![]);
        st.entries.insert("a".into(), (tagged, b"other".to_vec()));
        assert_eq!(s.commit(st, t0), counts(0, 1, 0, 0));
    }

    #[test]
    fn owners_are_namespaces() {
        let mut s = Schedules::default();
        let t0 = ts("2026-10-11T03:00:00Z");
        s.commit(
            staging("splat", vec![line("a", "@hourly"), line("b", "@daily")]),
            t0,
        );
        assert_eq!(
            s.commit(staging("booko", vec![line("a", "@hourly")]), t0),
            counts(1, 0, 0, 0)
        );
        assert_eq!(s.commit(staging("booko", vec![]), t0), counts(0, 0, 1, 0));
        assert_eq!(s.owner_entries("splat").count(), 2);
        assert_eq!(s.owner_entries("booko").count(), 0);
    }

    #[test]
    fn memory_is_tracked_through_commits() {
        let mut s = Schedules::default();
        let t0 = ts("2026-10-11T03:00:00Z");
        s.commit(
            staging("splat", vec![line("a", "@hourly"), line("b", "@daily")]),
            t0,
        );
        let (n, bytes) = s.owner_usage("splat");
        assert_eq!((n as u64, bytes), (2, s.bytes()));
        s.commit(staging("splat", vec![line("a", "@hourly")]), t0);
        assert_eq!(s.owner_usage("splat").1, s.bytes());
        s.commit(staging("splat", vec![]), t0);
        assert_eq!(s.bytes(), 0);
    }
}
