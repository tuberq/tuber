//! Schedule sync and inspection commands. See `crate::schedule`.

use jiff::Timestamp;

use super::*;
use crate::schedule::spec::format_time;
use crate::schedule::{self, Line, MAX_ENTRIES, Staging};

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
