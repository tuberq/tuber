//! Schedule sync and inspection commands. See `crate::schedule`.

use jiff::{SignedDuration, Timestamp};

use super::*;
use crate::schedule::spec::format_time;
use crate::schedule::{self, Catchup, Entry, Line, MAX_ENTRIES, Staging};

impl ServerState {
    pub(super) fn cmd_schedule_begin(&mut self, conn_id: u64, owner: String) -> Response {
        let (_, held_bytes) = self.schedules.owner_usage(&owner);
        let Some(conn) = self.conns.get_mut(&conn_id) else {
            return Response::InternalError;
        };
        if let Some(sync) = conn.schedule_sync.as_mut() {
            // A client that begins twice has lost track of its sync; refuse
            // whatever it commits next rather than guess which set it meant.
            sync.poisoned = true;
            return Response::BadFormat;
        }
        conn.schedule_sync = Some(Staging::new(owner.clone(), held_bytes));
        Response::Syncing(owner)
    }

    pub(super) fn cmd_schedule_put(
        &mut self,
        conn_id: u64,
        line: Result<Box<Line>, Response>,
        body: Option<Vec<u8>>,
    ) -> Response {
        let (limit, in_use) = (self.max_job_bytes, self.memory_in_use());
        let Some(sync) = self
            .conns
            .get_mut(&conn_id)
            .and_then(|c| c.schedule_sync.as_mut())
        else {
            return Response::NotSyncing;
        };
        match sync.stage(line, body, limit, in_use) {
            Ok(cost) => {
                self.staged_schedule_bytes += cost;
                Response::Staged
            }
            Err(resp) => {
                sync.poisoned = true;
                resp
            }
        }
    }

    /// End `conn_id`'s sync, if it has one, releasing what it staged from
    /// the memory budget.
    pub(super) fn take_staging(&mut self, conn_id: u64) -> Option<Staging> {
        let sync = self.conns.get_mut(&conn_id)?.schedule_sync.take()?;
        self.staged_schedule_bytes = self.staged_schedule_bytes.saturating_sub(sync.bytes);
        Some(sync)
    }

    pub(super) fn cmd_schedule_commit(&mut self, conn_id: u64) -> Response {
        let Some(sync) = self.take_staging(conn_id) else {
            return Response::NotSyncing;
        };
        if sync.poisoned {
            return Response::Aborted;
        }
        let (held, held_bytes) = self.schedules.owner_usage(&sync.owner);
        if self.schedules.len() - held + sync.entries.len() > MAX_ENTRIES {
            return Response::OutOfMemory;
        }
        // Only a commit that grows the owner's set needs room. One that keeps
        // or shrinks it always goes through, as a delete does on a full
        // queue — otherwise a full disk would stop an app removing an entry.
        // On disk it needs room for the whole set, which it writes again.
        let growth = sync.bytes.saturating_sub(held_bytes);
        if growth > 0 {
            if self.memory_limit_exceeded(growth) {
                return Response::OutOfMemory;
            }
            if self.storage_limit_exceeded(sync.stored) {
                return Response::OutOfStorage;
            }
        }
        let owner = sync.owner.clone();
        let counts = self.schedules.commit(sync, Timestamp::now());
        // Durable before the reply. On failure the new set stands in memory
        // and the client hears INTERNAL_ERROR, as a put whose WAL write
        // fails does; the WAL is dropped, so a restart restores the set last
        // written. A commit that changes nothing writes nothing, unless the
        // stored set holds entries startup dropped; and once the WAL has
        // failed none can be kept.
        let unsaved = self.schedules.take_unsaved() || self.schedule_refs.is_stale(&owner);
        if (unsaved || self.wal_failed) && !(self.journal_schedule_set(&owner) && self.sync_wal())
        {
            tracing::error!(owner, "schedule sync not persisted");
            return Response::InternalError;
        }
        tracing::info!(
            owner,
            added = counts.added,
            changed = counts.changed,
            removed = counts.removed,
            unchanged = counts.unchanged,
            "schedule sync committed",
        );
        Response::Synced(counts)
    }

    /// Journal `owner`'s set as it stands, unsynced. False once the WAL
    /// has failed; true without one (no `-b`), as there's nothing to write.
    pub(super) fn journal_schedule_set(&mut self, owner: &str) -> bool {
        let written = match self.wal.as_mut() {
            Some(wal) => self.schedule_refs.write_set(wal, &self.schedules, owner),
            None => Ok(()),
        };
        if let Err(e) = written {
            self.fail_wal("schedule set write", e);
        }
        !self.wal_failed
    }

    /// Journal each entry's last fire, unsynced. False once the WAL has
    /// failed.
    fn journal_last_fires(&mut self, fires: &[(String, String, Option<Timestamp>)]) -> bool {
        let written = match self.wal.as_mut() {
            Some(wal) => fires.iter().try_for_each(|(owner, name, last_fire)| {
                self.schedule_refs.write_fired(wal, owner, name, *last_fire)
            }),
            None => Ok(()),
        };
        if let Err(e) = written {
            self.fail_wal("schedule fire write", e);
        }
        !self.wal_failed
    }

    /// Restore the schedules a WAL replay found. A WAL holding none may be
    /// one an earlier tuber kept beside two files: their entries move into
    /// it, and the files are removed.
    pub(super) fn restore_schedules(
        &mut self,
        dir: &std::path::Path,
        replayed: crate::wal::ReplayedSchedules,
    ) -> io::Result<()> {
        let Some(wal) = self.wal.as_mut() else {
            return Ok(());
        };
        if replayed.seen {
            let (refs, schedules) = schedule::store::ScheduleRefs::restore(wal, replayed);
            self.schedule_refs = refs;
            self.schedules = schedules;
        } else {
            let files = match schedule::store::read_files(dir) {
                Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                    // A damaged schedule file mustn't keep the queue down.
                    // Set it aside and start without schedules: each app's
                    // next sync restores its own. (One from a different
                    // tuber version is `Unsupported`, not damage, and stops
                    // startup.)
                    let aside = schedule::store::set_aside_corrupt(dir)?;
                    tracing::error!("{e}; moved to {aside:?}, no schedules until apps re-sync");
                    None
                }
                other => other?,
            };
            if let Some(schedules) = files {
                self.schedules = schedules;
                let owners: Vec<String> = self.schedules.owners().map(str::to_string).collect();
                for owner in &owners {
                    self.journal_schedule_set(owner);
                }
                if !self.sync_wal() {
                    return Err(io::Error::other(
                        "schedules: could not move the schedule files into the WAL",
                    ));
                }
                tracing::info!(
                    "schedules: moved {} entries from the schedule files into the WAL",
                    self.schedules.len()
                );
            }
        }
        // Moved, or left by a crash before a move removed them. The WAL
        // holds the schedules now, so one that won't go only needs a word;
        // the next start tries again.
        if let Err(e) = schedule::store::remove_files(dir) {
            tracing::warn!("schedules: could not remove the old schedule files: {e}");
        }
        if !self.schedules.is_empty() {
            tracing::info!("schedules: restored {} entries from {:?}", self.schedules.len(), dir);
        }
        Ok(())
    }

    pub(super) fn cmd_schedule_abort(&mut self, conn_id: u64) -> Response {
        match self.take_staging(conn_id) {
            Some(_) => Response::Aborted,
            None => Response::NotSyncing,
        }
    }

    pub(super) fn cmd_list_schedules(&self, owner: Option<&str>) -> Response {
        let mut yaml = "---\n".to_string();
        let mut list =
            |e: &schedule::Entry| yaml.push_str(&format!("- {} {}\n", e.owner, e.line.name));
        match owner {
            Some(owner) => self.schedules.owner_entries(owner).for_each(&mut list),
            None => self.schedules.iter().for_each(&mut list),
        }
        Response::Ok(yaml.into_bytes())
    }

    pub(super) fn cmd_stats_schedule(&self, owner: &str, name: &str) -> Response {
        let Some(e) = self.schedules.get(owner, name) else {
            return Response::NotFound;
        };
        let tz = e.line.spec.time_zone();
        let time =
            |t: Option<Timestamp>| t.map_or_else(|| "\"\"".to_string(), |t| format_time(t, &tz));
        let next2 = e.next_fire.and_then(|t| e.line.spec.next_after(t));
        let next3 = next2.and_then(|t| e.line.spec.next_after(t));
        let s = &e.stats;
        let yaml = format!(
            "---\n\
             owner: \"{}\"\n\
             name: \"{}\"\n\
             spec: \"{}\"\n\
             tube: \"{}\"\n\
             pri: {}\n\
             delay: {}\n\
             ttr: {}\n\
             tags: \"{}\"\n\
             catchup: \"{}\"\n\
             synced-at: {}\n\
             next-fire: {}\n\
             next-fire-2: {}\n\
             next-fire-3: {}\n\
             last-fire: {}\n\
             last-result: \"{}\"\n\
             total-fires: {}\n\
             total-inserted: {}\n\
             total-deduped-ready: {}\n\
             total-deduped-reserved: {}\n\
             total-deduped-delayed: {}\n\
             total-deduped-buried: {}\n\
             total-deduped-deleted: {}\n\
             total-rejected: {}\n\
             buried-streak: {}\n",
            e.owner,
            e.line.name,
            e.line.spec_text,
            e.line.tube,
            e.line.pri,
            e.line.delay,
            e.line.ttr,
            e.line.tags,
            e.line.catchup.describe(),
            time(Some(e.synced_at)),
            time(e.next_fire),
            time(next2),
            time(next3),
            time(e.last_fire),
            s.last_result.as_deref().unwrap_or(""),
            s.fires(),
            s.inserted,
            s.deduped.ready,
            s.deduped.reserved,
            s.deduped.delayed,
            s.deduped.buried,
            s.deduped.deleted,
            s.rejected,
            s.buried_streak,
        );
        Response::Ok(yaml.into_bytes())
    }
}

/// A tick fired at most this late is on time, not missed: `catchup:` governs
/// downtime and forward clock steps, not a slow engine step.
const ON_TIME: SignedDuration = SignedDuration::from_secs(5);

/// How far ahead of now an entry's last fire or sync may sit and still be
/// honoured — nothing at or before it fires again — after the clock steps
/// back. Further ahead than this, the clock was wrong when it was recorded
/// (a bad RTC put right by NTP, say), and waiting for the clock to pass it
/// would silence the entry for as long as the clock was out.
const CLOCK_STEP_TOLERANCE: SignedDuration = SignedDuration::from_hours(1);

/// If `e` was recorded (fired or synced) beyond `horizon`, the clock was
/// wrong then: resume it from `now` rather than wait for the clock to pass
/// the bogus time. Returns whether it did.
fn correct_future_clock(e: &mut Entry, now: Timestamp, horizon: Timestamp) -> bool {
    let recorded = e.recorded();
    if recorded <= horizon {
        return false;
    }
    tracing::warn!(
        owner = %e.owner,
        name = %e.line.name,
        recorded = %format_time(recorded, &e.line.spec.time_zone()),
        "schedule's last fire or sync is in the future: the clock was wrong \
         then; resuming from now",
    );
    e.last_fire = e.last_fire.filter(|&last| last <= now);
    e.synced_at = e.synced_at.min(now);
    e.next_fire = e.line.spec.next_after(now);
    true
}

/// Whether `e`'s due `tick` should fire at `now`. A run of missed ticks
/// fires once unless `catchup:` bounds how stale the latest of them may be;
/// a tick within [`ON_TIME`] is on time, not missed, whatever the bound.
fn catchup_allows(e: &Entry, tick: Timestamp, now: Timestamp) -> bool {
    let bound = match e.line.catchup {
        Catchup::Once => return true,
        Catchup::Within(secs) => SignedDuration::from_secs(secs.into()),
        Catchup::Never => SignedDuration::ZERO,
    };
    // The latest tick is no older than `tick`: if that's on time, so is it.
    if now.duration_since(tick) <= ON_TIME {
        return true;
    }
    let since = tick
        .checked_sub(SignedDuration::from_nanos(1))
        .unwrap_or(tick);
    let latest = e.line.spec.latest_at_or_before(now, since).unwrap_or(tick);
    let fire = now.duration_since(latest) <= ON_TIME.max(bound);
    if !fire {
        tracing::info!(
            owner = %e.owner,
            name = %e.line.name,
            missed = %format_time(latest, &e.line.spec.time_zone()),
            catchup = %e.line.catchup.describe(),
            "schedule skipping a missed tick",
        );
    }
    fire
}

impl ServerState {
    /// Fire every entry whose next tick is at or before `now`, once however
    /// many of its ticks have passed: a run of missed ticks (tuber down, or
    /// the clock stepped forward) is caught up with one fire, subject to the
    /// entry's `catchup:`. Called from the tick with the wall clock.
    pub(super) fn fire_due_schedules(&mut self, now: Timestamp) {
        // Every put would be refused. Leave due ticks due, unstamped: they
        // are missed, not fired, and `catchup:` decides their fate once the
        // drain ends or the server restarts. Stamping them would hide them
        // from both, and from the last-fire staleness alert.
        if self.drain_mode || self.wal_failed {
            return;
        }
        // Persist corrected times, so a restart doesn't find them in the
        // future again. Unsynced: lost to a crash, they're corrected again.
        let horizon = now + CLOCK_STEP_TOLERANCE;
        let corrected: std::collections::BTreeSet<String> = self
            .schedules
            .iter_mut()
            .filter_map(|e| correct_future_clock(e, now, horizon).then(|| e.owner.clone()))
            .collect();
        for owner in &corrected {
            if !self.journal_schedule_set(owner) {
                tracing::error!(owner, "schedule clock correction not persisted");
                return;
            }
        }

        let mut due: Vec<(String, String, Option<Timestamp>)> = Vec::new();
        for e in self.schedules.iter_mut() {
            let Some(tick) = e.next_fire.filter(|&t| t <= now) else {
                continue;
            };
            let fire = catchup_allows(e, tick, now);
            e.next_fire = e.line.spec.next_after(now);
            if fire {
                due.push((e.owner.clone(), e.line.name.clone(), e.last_fire));
                e.last_fire = Some(now);
            }
        }
        if due.is_empty() {
            return;
        }

        // Stamp first: once the stamps are on disk a crash can lose these
        // fires but never repeat them, so they're synced before anything is
        // put, not left in the WAL's buffer with the puts. If they can't be
        // written, don't fire — the same at-most-once promise, kept by
        // losing the tick now.
        let stamps: Vec<_> = due
            .iter()
            .map(|(owner, name, _)| (owner.clone(), name.clone(), Some(now)))
            .collect();
        if !(self.journal_last_fires(&stamps) && self.sync_wal()) {
            tracing::error!("schedule fires not stamped, so not fired");
            for (owner, name, previous) in due {
                if let Some(entry) = self.schedules.get_mut(&owner, &name) {
                    entry.last_fire = previous;
                }
                self.record_fire(&owner, &name, &Response::InternalError);
            }
            return;
        }

        let mut unstamped = Vec::new();
        for (owner, name, previous) in due {
            let Some(e) = self.schedules.get(&owner, &name) else {
                continue;
            };
            let (tube, tags, body) = (e.line.tube.clone(), e.line.tags.clone(), e.body.clone());
            let (pri, delay, ttr) = (e.line.pri, e.line.delay, e.line.ttr);
            let wal_was_failed = self.wal_failed;
            let resp = self.put_job(None, tube, pri, delay, ttr, Some(body), tags);
            self.record_fire(&owner, &name, &resp);
            // Refused before anything was queued: the tick is lost, as for
            // any refused put, but it wasn't a fire, so last-fire keeps its
            // age. That includes INTERNAL_ERROR from a failed TOAST write, or
            // after an earlier fire failed the WAL, but not from the put
            // that failed it: its record may be on disk, and a restart bring
            // the job back.
            let queued_nothing = match resp {
                Response::OutOfMemory | Response::OutOfStorage | Response::JobTooBig => true,
                Response::InternalError => wal_was_failed || !self.wal_failed,
                _ => false,
            };
            if queued_nothing && let Some(e) = self.schedules.get_mut(&owner, &name) {
                e.last_fire = previous;
                unstamped.push((owner, name, previous));
            }
        }
        // After a restart the refused tick counts as missed and `catchup:`
        // may fire it: still at most once, since nothing was queued. Not
        // once the WAL has failed, though: the stamp stays, and the tick is
        // lost rather than caught up.
        if !unstamped.is_empty() && !self.journal_last_fires(&unstamped) {
            tracing::error!("refused schedule fires not unstamped: the WAL has failed");
        }
    }

    fn record_fire(&mut self, owner: &str, name: &str, resp: &Response) {
        let Some(e) = self.schedules.get_mut(owner, name) else {
            return;
        };
        let reply = resp.serialize();
        let reply = String::from_utf8_lossy(&reply).trim_end().to_string();
        let s = &mut e.stats;
        match resp {
            Response::Inserted(_) => {
                s.inserted += 1;
                s.buried_streak = 0;
            }
            Response::InsertedDup(id, state, _) => {
                let state = JobState::from_protocol_str(state);
                s.deduped.count(state);
                if state == Some(JobState::Buried) {
                    s.buried_streak += 1;
                    // The 09-09 failure: the put "succeeded", queued nothing,
                    // and will keep doing so until that job is kicked.
                    tracing::warn!(
                        owner,
                        name,
                        job = id,
                        streak = s.buried_streak,
                        "schedule fire matched a buried job's idempotency key; nothing was queued",
                    );
                } else {
                    s.buried_streak = 0;
                }
            }
            _ => {
                s.rejected += 1;
                s.buried_streak = 0;
                tracing::warn!(owner, name, result = %reply, "schedule fire refused");
            }
        }
        s.last_result = Some(reply);
    }
}
