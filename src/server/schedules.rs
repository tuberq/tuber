//! Schedule sync and inspection commands. See `crate::schedule`.

use jiff::{SignedDuration, Timestamp};

use super::*;
use crate::schedule::spec::format_time;
use crate::schedule::{self, Catchup, Line, MAX_ENTRIES, Staging};

impl ServerState {
    pub(super) fn cmd_schedule_begin(&mut self, conn_id: u64, owner: String) -> Response {
        let Some(conn) = self.conns.get_mut(&conn_id) else {
            return Response::InternalError;
        };
        if let Some(sync) = conn.schedule_sync.as_mut() {
            // A client that begins twice has lost track of its sync; refuse
            // whatever it commits next rather than guess which set it meant.
            sync.poisoned = true;
            return Response::BadFormat;
        }
        conn.schedule_sync = Some(Staging::new(owner.clone()));
        Response::Syncing(owner)
    }

    pub(super) fn cmd_schedule_put(
        &mut self,
        conn_id: u64,
        line: Result<Box<Line>, Response>,
        body: Option<Vec<u8>>,
    ) -> Response {
        let Some(mut sync) = self
            .conns
            .get_mut(&conn_id)
            .and_then(|c| c.schedule_sync.take())
        else {
            return Response::NotSyncing;
        };
        let resp = self.stage(&mut sync, line, body);
        if resp != Response::Staged {
            sync.poisoned = true;
        }
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.schedule_sync = Some(sync);
        }
        resp
    }

    fn stage(
        &self,
        sync: &mut Staging,
        line: Result<Box<Line>, Response>,
        body: Option<Vec<u8>>,
    ) -> Response {
        let line = match line {
            Ok(line) => *line,
            Err(resp) => return resp,
        };
        let Some(body) = body else {
            return Response::InternalError;
        };
        if sync.entries.contains_key(&line.name) {
            return Response::BadFormat;
        }
        let cost = schedule::memory_cost(&sync.owner, &line, &body);
        if sync.entries.len() >= MAX_ENTRIES || self.memory_limit_exceeded(sync.bytes + cost) {
            return Response::OutOfMemory;
        }
        sync.bytes += cost;
        sync.entries.insert(line.name.clone(), (line, body));
        Response::Staged
    }

    pub(super) fn cmd_schedule_commit(&mut self, conn_id: u64) -> Response {
        let Some(sync) = self
            .conns
            .get_mut(&conn_id)
            .and_then(|c| c.schedule_sync.take())
        else {
            return Response::NotSyncing;
        };
        if sync.poisoned {
            return Response::Aborted;
        }
        let (held, held_bytes) = self.schedules.owner_usage(&sync.owner);
        if self.schedules.len() - held + sync.entries.len() > MAX_ENTRIES
            || self.memory_limit_exceeded(sync.bytes.saturating_sub(held_bytes))
        {
            return Response::OutOfMemory;
        }
        let owner = sync.owner.clone();
        let counts = self.schedules.commit(sync, Timestamp::now());
        tracing::info!(
            owner,
            added = counts.added,
            changed = counts.changed,
            removed = counts.removed,
            unchanged = counts.unchanged,
            "schedule sync committed",
        );
        Response::Synced {
            added: counts.added,
            changed: counts.changed,
            removed: counts.removed,
            unchanged: counts.unchanged,
        }
    }

    pub(super) fn cmd_schedule_abort(&mut self, conn_id: u64) -> Response {
        match self
            .conns
            .get_mut(&conn_id)
            .and_then(|c| c.schedule_sync.take())
        {
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
        let [ready, reserved, delayed, buried, deleted] = s.deduped;
        let fires = s.inserted + s.deduped.iter().sum::<u64>() + s.rejected;
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
            fires,
            s.inserted,
            ready,
            reserved,
            delayed,
            buried,
            deleted,
            s.rejected,
            s.buried_streak,
        );
        Response::Ok(yaml.into_bytes())
    }
}

/// A tick fired at most this late is on time, not missed: `catchup:` governs
/// downtime and forward clock steps, not a slow engine step.
const ON_TIME: SignedDuration = SignedDuration::from_secs(5);

impl ServerState {
    /// Fire every entry whose next tick is at or before `now`, once however
    /// many of its ticks have passed: a run of missed ticks (tuber down, or
    /// the clock stepped forward) is caught up with one fire, subject to the
    /// entry's `catchup:`. Called from the tick with the wall clock.
    pub(super) fn fire_due_schedules(&mut self, now: Timestamp) {
        let mut due: Vec<(String, String)> = Vec::new();
        for e in self.schedules.iter_mut() {
            let Some(tick) = e.next_fire.filter(|&t| t <= now) else {
                continue;
            };
            let fire = match e.line.catchup {
                Catchup::Once => true,
                bound => {
                    let since = tick
                        .checked_sub(SignedDuration::from_nanos(1))
                        .unwrap_or(tick);
                    let latest = e.line.spec.latest_at_or_before(now, since).unwrap_or(tick);
                    let age = now.duration_since(latest);
                    let fire = age <= ON_TIME
                        || matches!(bound, Catchup::Within(secs) if age <= SignedDuration::from_secs(secs.into()));
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
            };
            e.next_fire = e.line.spec.next_after(now);
            if fire {
                e.last_fire = Some(now);
                due.push((e.owner.clone(), e.line.name.clone()));
            }
        }

        for (owner, name) in due {
            let Some(e) = self.schedules.get(&owner, &name) else {
                continue;
            };
            let line = e.line.clone();
            let resp = self.put_job(
                None,
                line.tube,
                line.pri,
                line.delay,
                line.ttr,
                Some(e.body.clone()),
                line.tags.idempotency_key,
                line.tags.group,
                line.tags.after_group,
                line.tags.concurrency_key,
            );
            self.record_fire(&owner, &name, &resp);
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
                let i = match *state {
                    "READY" => 0,
                    "RESERVED" => 1,
                    "DELAYED" => 2,
                    "BURIED" => 3,
                    _ => 4,
                };
                s.deduped[i] += 1;
                if i == 3 {
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
