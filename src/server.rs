use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{ConnState, ReserveMode, WatchedTube};
use crate::job::{Job, JobState, URGENT_THRESHOLD};
use crate::protocol::{self, Command, Response, MAX_DELETE_BATCH};
use crate::tube::{Tube, TubeStats};
use crate::wal::{IdpTombstone, Wal};

/// EWMA smoothing factor for all timing stats (processing time, queue time).
const EWMA_ALPHA: f64 = 0.1;

/// Threshold separating "fast" from "slow" jobs for dual EWMA tracking (seconds).
const FAST_THRESHOLD: f64 = 0.1;

// Op index constants matching beanstalkd (see tmp/prot.c)
const OP_PUT: usize = 1;
const OP_PEEKJOB: usize = 2;
const OP_RESERVE: usize = 3;
const OP_DELETE: usize = 4;
const OP_RELEASE: usize = 5;
const OP_BURY: usize = 6;
const OP_KICK: usize = 7;
const OP_STATS: usize = 8;
const OP_STATSJOB: usize = 9;
const OP_PEEK_BURIED: usize = 10;
const OP_USE: usize = 11;
const OP_WATCH: usize = 12;
const OP_IGNORE: usize = 13;
const OP_LIST_TUBES: usize = 14;
const OP_LIST_TUBE_USED: usize = 15;
const OP_LIST_TUBES_WATCHED: usize = 16;
const OP_STATS_TUBE: usize = 17;
const OP_PEEK_READY: usize = 18;
const OP_PEEK_DELAYED: usize = 19;
const OP_RESERVE_TIMEOUT: usize = 20;
const OP_TOUCH: usize = 21;
const OP_PAUSE_TUBE: usize = 23;
const OP_RESERVE_MODE: usize = 26;
const OP_PEEK_RESERVED: usize = 27;

/// Message from a connection task to the engine.
struct EngineMsg {
    conn_id: u64,
    payload: EnginePayload,
}

enum EnginePayload {
    Command {
        cmd: Command,
        body: Option<Vec<u8>>,
        reply_tx: oneshot::Sender<Response>,
    },
    Disconnect,
    Shutdown,
}

/// Waiting reservation request, stored when no job is immediately available.
struct WaitingReserve {
    conn_id: u64,
    reply_tx: oneshot::Sender<Response>,
    deadline: Option<Instant>,
}

#[derive(Debug, Default)]
struct GlobalStats {
    urgent_ct: u64,
    buried_ct: u64,
    reserved_ct: u64,
    waiting_ct: u64,
    total_jobs_ct: u64,
    total_delete_ct: u64,
    timeout_ct: u64,
    op_ct: [u64; 28],
    total_connections: u64,
}

/// State for a job group (grp:/aft: feature).
#[derive(Debug)]
struct GroupState {
    /// Number of jobs with `grp:<id>` that haven't been deleted yet.
    pending: u64,
    /// Number of jobs with `grp:<id>` that are currently buried.
    buried: u64,
    /// Job IDs with `aft:<id>` that are held waiting for group completion.
    waiting_jobs: Vec<u64>,
}

impl GroupState {
    fn new() -> Self {
        GroupState {
            pending: 0,
            buried: 0,
            waiting_jobs: Vec::new(),
        }
    }

    /// Group is complete when all members are deleted and none are buried.
    fn is_complete(&self) -> bool {
        self.pending == 0 && self.buried == 0
    }

    /// Group can be cleaned up when complete and no after-jobs waiting.
    fn is_idle(&self) -> bool {
        self.is_complete() && self.waiting_jobs.is_empty()
    }
}

/// All server state, owned by the engine task.
struct ServerState {
    jobs: HashMap<u64, Job>,
    tubes: HashMap<String, Tube>,
    conns: HashMap<u64, ConnState>,
    next_job_id: u64,
    max_job_size: u32,
    drain_mode: bool,
    ready_ct: u64,
    started_at: Instant,
    rng_state: u64,
    stats: GlobalStats,
    /// Connections waiting for a job via reserve.
    waiters: Vec<WaitingReserve>,
    /// Optional write-ahead log for persistence.
    wal: Option<Wal>,
    /// Hex-encoded random instance ID (16 chars).
    instance_id: String,
    /// Optional user-assigned instance name.
    name: Option<String>,
    /// Cached system info from uname.
    hostname: String,
    os: String,
    platform: String,
    /// Job group tracking for grp:/aft: features.
    groups: HashMap<String, GroupState>,
    /// Active reservation count per concurrency key.
    concurrency_keys: HashMap<String, u32>,
    /// Concurrency limit per key (max concurrent reservations allowed).
    concurrency_limits: HashMap<String, u32>,
}

impl ServerState {
    fn new(max_job_size: u32, name: Option<String>) -> Self {
        let mut tubes = HashMap::new();
        tubes.insert("default".to_string(), Tube::new("default"));

        // Generate instance_id from /dev/urandom (fallback: pid + timestamp)
        let instance_id = {
            let mut bytes = [0u8; 8];
            let got_random = std::fs::File::open("/dev/urandom")
                .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut bytes))
                .is_ok();
            if !got_random {
                let pid = std::process::id() as u64;
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                bytes[..8].copy_from_slice(&(pid ^ ts).to_le_bytes());
            }
            let mut hex = String::with_capacity(16);
            for b in &bytes {
                use std::fmt::Write;
                let _ = write!(hex, "{:02x}", b);
            }
            hex
        };

        // Seed PRNG from the same random bytes used for instance_id
        let rng_seed = {
            let mut seed_bytes = [0u8; 8];
            let got_random = std::fs::File::open("/dev/urandom")
                .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut seed_bytes))
                .is_ok();
            if !got_random {
                let pid = std::process::id() as u64;
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                seed_bytes = (pid ^ ts).to_le_bytes();
            }
            u64::from_le_bytes(seed_bytes)
        };

        // Cache system info from uname
        let (hostname, os, platform) = {
            let mut utsname: libc::utsname = unsafe { std::mem::zeroed() };
            let ret = unsafe { libc::uname(&mut utsname) };
            if ret == 0 {
                let to_string = |arr: &[libc::c_char]| {
                    unsafe { std::ffi::CStr::from_ptr(arr.as_ptr()) }
                        .to_string_lossy()
                        .into_owned()
                };
                (
                    to_string(&utsname.nodename),
                    format!(
                        "{} {}",
                        to_string(&utsname.sysname),
                        to_string(&utsname.release)
                    ),
                    to_string(&utsname.machine),
                )
            } else {
                (
                    "unknown".to_string(),
                    "unknown".to_string(),
                    "unknown".to_string(),
                )
            }
        };

        ServerState {
            jobs: HashMap::new(),
            tubes,
            conns: HashMap::new(),
            next_job_id: 1,

            max_job_size,
            drain_mode: false,
            ready_ct: 0,
            started_at: Instant::now(),
            rng_state: rng_seed,
            stats: GlobalStats::default(),
            waiters: Vec::new(),
            wal: None,
            instance_id,
            name,
            hostname,
            os,
            platform,
            groups: HashMap::new(),
            concurrency_keys: HashMap::new(),
            concurrency_limits: HashMap::new(),
        }
    }

    fn unregister_conn(&mut self, conn_id: u64) {
        // Release all reserved jobs back to ready
        if let Some(conn) = self.conns.remove(&conn_id) {
            // Remove from waiting lists
            self.remove_waiter(conn_id);

            // Decrement tube counters
            if let Some(t) = self.tubes.get_mut(&conn.use_tube) {
                t.using_ct = t.using_ct.saturating_sub(1);
            }
            for w in &conn.watched {
                if let Some(t) = self.tubes.get_mut(&w.name) {
                    t.watching_ct = t.watching_ct.saturating_sub(1);
                    t.waiting_conns.retain(|&c| c != conn_id);
                }
            }

            // Re-enqueue reserved jobs
            for job_id in conn.reserved_jobs {
                self.release_concurrency_key(job_id);
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.state = JobState::Ready;
                    job.reserver_id = None;
                    job.deadline_at = None;
                    let tube_name = job.tube_name.clone();
                    let key = job.ready_key();
                    let id = job.id;
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.ready.insert(key, id);
                        self.ready_ct += 1;
                        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
                        tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
                        if key.0 < URGENT_THRESHOLD {
                            self.stats.urgent_ct += 1;
                            tube.stat.urgent_ct += 1;
                        }
                    }
                }
            }

            self.process_queue();
        }
    }

    fn ensure_tube(&mut self, name: &str) -> bool {
        if !self.tubes.contains_key(name) {
            self.tubes.insert(name.to_string(), Tube::new(name));
        }
        true
    }

    /// Check if a job's concurrency key already has an active reservation.
    fn is_concurrency_blocked(&self, job_id: u64) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.as_ref())
            .map(|(key, _limit)| {
                let count = self.concurrency_keys.get(key).copied().unwrap_or(0);
                let limit = self.concurrency_limits.get(key).copied().unwrap_or(1);
                count >= limit
            })
            .unwrap_or(false)
    }

    /// Increment the concurrency counter for a job's key and register limit.
    fn acquire_concurrency_key(&mut self, job_id: u64) {
        if let Some((key, limit)) = self
            .jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.clone())
        {
            *self.concurrency_keys.entry(key.clone()).or_insert(0) += 1;
            // Use max of existing and new limit (safest — never blocks more than intended)
            let entry = self.concurrency_limits.entry(key).or_insert(0);
            *entry = (*entry).max(limit);
        }
    }

    /// Decrement and clean up the concurrency counter for a job's key.
    fn release_concurrency_key(&mut self, job_id: u64) {
        if let Some((key, _limit)) = self
            .jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.clone())
            && let Some(count) = self.concurrency_keys.get_mut(&key)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.concurrency_keys.remove(&key);
                self.concurrency_limits.remove(&key);
            }
        }
    }

    /// Find the best unblocked ready job from a tube.
    /// Fast path: if top job is not blocked, return it.
    /// Slow path: scan heap entries for first unblocked job.
    fn find_best_unblocked_ready(&self, tube: &Tube) -> Option<((u32, u64), u64)> {
        if let Some(&entry) = tube.ready.peek() {
            if !self.is_concurrency_blocked(entry.1) {
                return Some(entry);
            }
            // Slow path: collect, sort, find first unblocked
            let mut entries: Vec<((u32, u64), u64)> = tube
                .ready
                .entries()
                .iter()
                .map(|&(k, id)| (k, id))
                .collect();
            entries.sort();
            for entry in entries {
                if !self.is_concurrency_blocked(entry.1) {
                    return Some(entry);
                }
            }
            None
        } else {
            None
        }
    }

    fn handle_command(&mut self, conn_id: u64, cmd: Command, body: Option<Vec<u8>>) -> Response {
        // Auto-register connection if not known
        if let std::collections::hash_map::Entry::Vacant(e) = self.conns.entry(conn_id) {
            e.insert(ConnState::new(conn_id));
            self.stats.total_connections += 1;
            self.ensure_tube("default");
            if let Some(t) = self.tubes.get_mut("default") {
                t.watching_ct += 1;
                t.using_ct += 1;
            }
        }

        match cmd {
            Command::Put {
                pri,
                delay,
                ttr,
                bytes,
                idempotency_key,
                group,
                after_group,
                concurrency_key,
            } => {
                self.stats.op_ct[OP_PUT] += 1;
                self.cmd_put(
                    conn_id,
                    pri,
                    delay,
                    ttr,
                    bytes,
                    body,
                    idempotency_key,
                    group,
                    after_group,
                    concurrency_key,
                )
            }
            Command::Use { tube } => {
                self.stats.op_ct[OP_USE] += 1;
                self.cmd_use(conn_id, &tube)
            }
            Command::Reserve => {
                self.stats.op_ct[OP_RESERVE] += 1;
                self.cmd_reserve(conn_id, None)
            }
            Command::ReserveWithTimeout { timeout } => {
                self.stats.op_ct[OP_RESERVE_TIMEOUT] += 1;
                self.cmd_reserve(conn_id, Some(timeout))
            }
            Command::ReserveJob { id } => {
                self.stats.op_ct[OP_RESERVE] += 1;
                self.cmd_reserve_job(conn_id, id)
            }
            Command::ReserveMode { mode } => {
                self.stats.op_ct[OP_RESERVE_MODE] += 1;
                self.cmd_reserve_mode(conn_id, &mode)
            }
            Command::ReserveBatch { count } => {
                self.stats.op_ct[OP_RESERVE] += 1;
                self.cmd_reserve_batch(conn_id, count)
            }
            Command::Delete { id } => {
                self.stats.op_ct[OP_DELETE] += 1;
                self.cmd_delete(conn_id, id)
            }
            Command::DeleteBatch { ids } => {
                self.stats.op_ct[OP_DELETE] += ids.len() as u64;
                self.cmd_delete_batch(conn_id, ids)
            }
            Command::Release { id, pri, delay } => {
                self.stats.op_ct[OP_RELEASE] += 1;
                self.cmd_release(conn_id, id, pri, delay)
            }
            Command::Bury { id, pri } => {
                self.stats.op_ct[OP_BURY] += 1;
                self.cmd_bury(conn_id, id, pri)
            }
            Command::Touch { id } => {
                self.stats.op_ct[OP_TOUCH] += 1;
                self.cmd_touch(conn_id, id)
            }
            Command::Watch { tube, weight } => {
                self.stats.op_ct[OP_WATCH] += 1;
                self.cmd_watch(conn_id, &tube, weight)
            }
            Command::Ignore { tube } => {
                self.stats.op_ct[OP_IGNORE] += 1;
                self.cmd_ignore(conn_id, &tube)
            }
            Command::Peek { id } => {
                self.stats.op_ct[OP_PEEKJOB] += 1;
                self.cmd_peek(id)
            }
            Command::PeekReady => {
                self.stats.op_ct[OP_PEEK_READY] += 1;
                self.cmd_peek_ready(conn_id)
            }
            Command::PeekDelayed => {
                self.stats.op_ct[OP_PEEK_DELAYED] += 1;
                self.cmd_peek_delayed(conn_id)
            }
            Command::PeekBuried => {
                self.stats.op_ct[OP_PEEK_BURIED] += 1;
                self.cmd_peek_buried(conn_id)
            }
            Command::PeekReserved => {
                self.stats.op_ct[OP_PEEK_RESERVED] += 1;
                self.cmd_peek_reserved(conn_id)
            }
            Command::Kick { bound } => {
                self.stats.op_ct[OP_KICK] += 1;
                self.cmd_kick(conn_id, bound)
            }
            Command::KickJob { id } => {
                self.stats.op_ct[OP_KICK] += 1;
                self.cmd_kick_job(id)
            }
            Command::StatsJob { id } => {
                self.stats.op_ct[OP_STATSJOB] += 1;
                self.cmd_stats_job(id)
            }
            Command::StatsTube { tube } => {
                self.stats.op_ct[OP_STATS_TUBE] += 1;
                self.cmd_stats_tube(&tube)
            }
            Command::StatsGroup { group } => {
                self.stats.op_ct[OP_STATS] += 1;
                self.cmd_stats_group(&group)
            }
            Command::Stats => {
                self.stats.op_ct[OP_STATS] += 1;
                self.cmd_stats()
            }
            Command::ListTubes => {
                self.stats.op_ct[OP_LIST_TUBES] += 1;
                self.cmd_list_tubes()
            }
            Command::ListTubeUsed => {
                self.stats.op_ct[OP_LIST_TUBE_USED] += 1;
                self.cmd_list_tube_used(conn_id)
            }
            Command::ListTubesWatched => {
                self.stats.op_ct[OP_LIST_TUBES_WATCHED] += 1;
                self.cmd_list_tubes_watched(conn_id)
            }
            Command::PauseTube { tube, delay } => {
                self.stats.op_ct[OP_PAUSE_TUBE] += 1;
                self.cmd_pause_tube(&tube, delay)
            }
            Command::FlushTube { tube } => self.cmd_flush_tube(&tube),
            Command::Drain => {
                self.drain_mode = true;
                tracing::info!("entering drain mode (requested by connection {})", conn_id);
                Response::Draining
            }
            Command::Undrain => {
                self.drain_mode = false;
                tracing::info!("exiting drain mode (requested by connection {})", conn_id);
                Response::NotDraining
            }
            Command::Quit => Response::Deleted, // handled at connection level
        }
    }

    // --- Command implementations ---

    #[allow(clippy::too_many_arguments)]
    fn cmd_put(
        &mut self,
        conn_id: u64,
        pri: u32,
        delay: u32,
        ttr: u32,
        _bytes: u32,
        body: Option<Vec<u8>>,
        idempotency_key: Option<(String, u32)>,
        group: Option<String>,
        after_group: Option<String>,
        concurrency_key: Option<(String, u32)>,
    ) -> Response {
        if self.drain_mode {
            return Response::Draining;
        }

        let body = match body {
            Some(b) => b,
            None => return Response::InternalError,
        };

        // Mark connection as producer
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_producer();
        }

        // Minimum TTR is 1 second
        let ttr = ttr.max(1);

        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());

        self.ensure_tube(&tube_name);

        // Idempotency dedup: if key already exists for a live job, return original ID + state.
        // If the new put has a higher priority (lower number), upgrade the existing job's priority.
        if let Some(ref key_tuple) = idempotency_key
            && let Some(&existing_id) = self.tubes.get(&tube_name).and_then(|t| t.idempotency_keys.get(&key_tuple.0))
        {
            let upgraded_pri = if let Some(existing_job) = self.jobs.get_mut(&existing_id)
                && pri < existing_job.priority
            {
                let old_pri = existing_job.priority;
                let state = existing_job.state;
                let delay = existing_job.delay;
                existing_job.priority = pri;

                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    // If job is Ready, re-sort the ready heap
                    if state == JobState::Ready {
                        tube.ready.remove_by_id(existing_id);
                        tube.ready.insert((pri, existing_id), existing_id);
                    }
                    // Update urgent stats if crossing threshold
                    if pri < URGENT_THRESHOLD && old_pri >= URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        tube.stat.urgent_ct += 1;
                    }
                }

                self.wal_write_state_change(existing_id, Some(state), pri, delay, 0);

                Some(pri)
            } else {
                None
            };

            let state_str = self.jobs.get(&existing_id)
                .expect("job must exist — just looked up by idempotency key")
                .state
                .as_protocol_str();
            return Response::InsertedDup(existing_id, state_str, upgraded_pri);
        }

        // Idempotency cooldown dedup: if key is in cooldown period, return original ID (job is deleted)
        if let Some(ref key_tuple) = idempotency_key
            && let Some(tube) = self.tubes.get_mut(&tube_name)
            && let Some(&(original_id, expiry)) = tube.idempotency_cooldowns.get(&key_tuple.0)
        {
            if SystemTime::now() < expiry {
                return Response::InsertedDup(original_id, "DELETED", None);
            } else {
                tube.idempotency_cooldowns.remove(&key_tuple.0);
            }
        }

        // WAL: check space reservation
        if let Some(wal) = &self.wal {
            // Estimate record size for reservation check
            let idp_key_str = idempotency_key.as_ref().map(|(k, _)| k.clone());
            let est_size = crate::wal::estimate_full_job_size_raw(
                &tube_name,
                body.len(),
                &idp_key_str,
                &group,
                &after_group,
                &concurrency_key,
            );
            if !wal.reserve_put(est_size) {
                tracing::warn!(
                    "WAL: OUT_OF_MEMORY — job record ({est_size} bytes) exceeds max file size ({})",
                    wal.max_file_size(),
                );
                return Response::OutOfMemory;
            }
        }

        let id = self.next_job_id;
        self.next_job_id += 1;

        let mut job = Job::new(
            id,
            pri,
            Duration::from_secs(delay as u64),
            Duration::from_secs(ttr as u64),
            body,
            tube_name.clone(),
        );

        // Set extension fields before inserting
        job.idempotency_key = idempotency_key;
        job.group = group;
        job.after_group = after_group;
        job.concurrency_key = concurrency_key;

        // Register concurrency limit
        if let Some((ref key, limit)) = job.concurrency_key {
            let entry = self.concurrency_limits.entry(key.clone()).or_insert(0);
            *entry = (*entry).max(limit);
        }

        // Register idempotency key in tube index
        if let Some(ref key_tuple) = job.idempotency_key {
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.idempotency_keys.insert(key_tuple.0.clone(), id);
            }
        }

        // Track group membership
        if let Some(ref grp) = job.group {
            let gs = self
                .groups
                .entry(grp.clone())
                .or_insert_with(GroupState::new);
            gs.pending += 1;
        }

        // Check if this is an after-group job that should be held
        let hold_for_group = if let Some(ref ag) = job.after_group {
            let gs = self
                .groups
                .entry(ag.clone())
                .or_insert_with(GroupState::new);
            !gs.is_complete()
        } else {
            false
        };

        // Enqueue
        if delay > 0 {
            let deadline = Instant::now() + Duration::from_secs(delay as u64);
            job.state = JobState::Delayed;
            job.deadline_at = Some(deadline);
            self.jobs.insert(id, job);
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.delay.insert((deadline, id), id);
            }
        } else if hold_for_group {
            // Hold this after-job: mark as delayed with no deadline (held indefinitely)
            job.state = JobState::Delayed;
            job.deadline_at = None;
            let after_group_name = job.after_group.clone();
            self.jobs.insert(id, job);
            // Add to group's waiting list (will be promoted when group completes)
            if let Some(ref ag) = after_group_name
                && let Some(gs) = self.groups.get_mut(ag)
            {
                gs.waiting_jobs.push(id);
            }
        } else {
            let key = job.ready_key();
            self.jobs.insert(id, job);
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.ready.insert(key, id);
            }
            self.ready_ct += 1;
            if pri < URGENT_THRESHOLD {
                self.stats.urgent_ct += 1;
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.urgent_ct += 1;
                }
            }
        }

        self.stats.total_jobs_ct += 1;
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.total_jobs_ct += 1;
        }

        // WAL: write put record
        self.wal_write_put(id);

        if !self.waiters.is_empty() {
            self.process_queue();
        }

        Response::Inserted(id)
    }

    fn cmd_use(&mut self, conn_id: u64, tube: &str) -> Response {
        self.ensure_tube(tube);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            // Decrement old tube
            if let Some(old) = self.tubes.get_mut(&conn.use_tube) {
                old.using_ct = old.using_ct.saturating_sub(1);
            }
            conn.use_tube = tube.to_string();
        }
        if let Some(t) = self.tubes.get_mut(tube) {
            t.using_ct += 1;
        }
        Response::Using(tube.to_string())
    }

    /// Find the next ready job for a connection, respecting reserve mode.
    fn find_next_ready_job(&mut self, conn_id: u64) -> Option<u64> {
        let reserve_mode = self
            .conns
            .get(&conn_id)
            .map(|c| c.reserve_mode)
            .unwrap_or(ReserveMode::Fifo);

        match reserve_mode {
            ReserveMode::Weighted => self
                .select_weighted_job(conn_id)
                .or_else(|| self.find_ready_job_for_conn(conn_id)),
            ReserveMode::WeightedFair => self
                .select_weighted_fair_job(conn_id)
                .or_else(|| self.find_ready_job_for_conn(conn_id)),
            ReserveMode::Fifo => self.find_ready_job_for_conn(conn_id),
        }
    }

    fn cmd_reserve(&mut self, conn_id: u64, timeout: Option<u32>) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_worker();
        }

        // Check deadline_soon
        if self.conn_deadline_soon(conn_id, Instant::now()) && !self.conn_has_ready_job(conn_id) {
            return Response::DeadlineSoon;
        }

        if let Some(job_id) = self.find_next_ready_job(conn_id) {
            return self.do_reserve(conn_id, job_id);
        }

        // No job available -- this will need to wait.
        // Return TimedOut for timeout=0, or let the connection task handle
        // waiting via the waiter mechanism.
        if timeout == Some(0) {
            return Response::TimedOut;
        }

        // For blocking reserve, we return a sentinel that tells the connection
        // task to use the waiting mechanism.
        Response::TimedOut // Will be overridden by the wait mechanism
    }

    fn cmd_reserve_job(&mut self, conn_id: u64, id: u64) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_worker();
        }

        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state == JobState::Reserved {
            return Response::NotFound;
        }

        // Check concurrency constraint
        if self.is_concurrency_blocked(id) {
            return Response::NotFound;
        }

        let state = job.state;
        let tube_name = job.tube_name.clone();

        // Remove from current state
        match state {
            JobState::Ready => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.ready.remove_by_id(id);
                }
                self.ready_ct = self.ready_ct.saturating_sub(1);
                if let Some(j) = self.jobs.get(&id)
                    && j.priority < URGENT_THRESHOLD
                {
                    self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
                    }
                }
            }
            JobState::Buried => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.buried.retain(|&jid| jid != id);
                    self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);
                    tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                }
            }
            JobState::Delayed => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.delay.remove_by_id(id);
                }
            }
            _ => return Response::NotFound,
        }

        self.do_reserve_inner(conn_id, id)
    }

    fn do_reserve(&mut self, conn_id: u64, job_id: u64) -> Response {
        // Remove from ready heap
        let (tube_name, is_urgent) = match self.jobs.get(&job_id) {
            Some(j) => (j.tube_name.clone(), j.priority < URGENT_THRESHOLD),
            None => return Response::NotFound,
        };
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.ready.remove_by_id(job_id);
            if is_urgent {
                tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
            }
        }
        self.ready_ct = self.ready_ct.saturating_sub(1);
        if is_urgent {
            self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
        }

        self.do_reserve_inner(conn_id, job_id)
    }

    fn do_reserve_inner(&mut self, conn_id: u64, job_id: u64) -> Response {
        let now = Instant::now();
        let (body, tube_name, created_at) = match self.jobs.get_mut(&job_id) {
            Some(job) => {
                let body = job.body.clone();
                let tube_name = job.tube_name.clone();
                let created_at = job.created_at;
                job.state = JobState::Reserved;
                job.reserver_id = Some(conn_id);
                job.reserved_at = Some(now);
                job.deadline_at = Some(now + job.ttr);
                job.reserve_ct += 1;
                (body, tube_name, created_at)
            }
            None => return Response::NotFound,
        };
        self.acquire_concurrency_key(job_id);
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.reserved_ct += 1;
            tube.stat.total_reserve_ct += 1;

            let queue_secs = now.duration_since(created_at).as_secs_f64();
            TubeStats::record_timing(
                &mut tube.stat.queue_time_ewma, &mut tube.stat.queue_time_samples,
                &mut tube.stat.queue_time_min, &mut tube.stat.queue_time_max,
                queue_secs, EWMA_ALPHA,
            );
        }
        self.stats.reserved_ct += 1;

        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.reserved_jobs.push(job_id);
        }

        Response::Reserved { id: job_id, body }
    }

    fn cmd_reserve_mode(&mut self, conn_id: u64, mode: &str) -> Response {
        match mode {
            "fifo" => {
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    conn.reserve_mode = ReserveMode::Fifo;
                }
                Response::Using("fifo".to_string())
            }
            "weighted" => {
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    conn.reserve_mode = ReserveMode::Weighted;
                }
                Response::Using("weighted".to_string())
            }
            "weighted-fair" => {
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    conn.reserve_mode = ReserveMode::WeightedFair;
                }
                Response::Using("weighted-fair".to_string())
            }
            _ => Response::BadFormat,
        }
    }

    fn cmd_reserve_batch(&mut self, conn_id: u64, count: u32) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_worker();
        }

        let mut collected: Vec<(u64, Vec<u8>)> = Vec::new();

        for _ in 0..count {
            let Some(jid) = self.find_next_ready_job(conn_id) else {
                break;
            };
            let resp = self.do_reserve(conn_id, jid);
            if let Response::Reserved { id, body } = resp {
                collected.push((id, body));
            } else {
                break;
            }
        }

        Response::ReservedBatch(collected)
    }

    fn cmd_delete(&mut self, conn_id: u64, id: u64) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        let state = job.state;
        let tube_name = job.tube_name.clone();
        let pri = job.priority;
        let reserved_at = job.reserved_at;
        let idempotency_key = job.idempotency_key.clone();
        let group_name = job.group.clone();
        let has_concurrency_key = job.concurrency_key.is_some();

        match state {
            JobState::Reserved => {
                // Must be reserved by this connection
                if job.reserver_id != Some(conn_id) {
                    return Response::NotFound;
                }
                if has_concurrency_key {
                    self.release_concurrency_key(id);
                }
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    if let Some(pos) = conn.reserved_jobs.iter().position(|&jid| jid == id) {
                        conn.reserved_jobs.swap_remove(pos);
                    }
                }
                self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
                }
            }
            JobState::Ready => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.ready.remove_by_id(id);
                }
                self.ready_ct = self.ready_ct.saturating_sub(1);
                if pri < URGENT_THRESHOLD {
                    self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
                    }
                }
            }
            JobState::Buried => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.buried.retain(|&jid| jid != id);
                    tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                }
                self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);
            }
            JobState::Delayed => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.delay.remove_by_id(id);
                }
            }
        }

        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.total_delete_ct += 1;

            if state == JobState::Reserved
                && let Some(ra) = reserved_at
            {
                let secs = Instant::now().duration_since(ra).as_secs_f64();

                TubeStats::record_timing(
                    &mut tube.stat.processing_time_ewma, &mut tube.stat.processing_time_samples,
                    &mut tube.stat.processing_time_min, &mut tube.stat.processing_time_max,
                    secs, EWMA_ALPHA,
                );

                if secs < FAST_THRESHOLD {
                    TubeStats::update_ewma(
                        &mut tube.stat.processing_time_ewma_fast,
                        &mut tube.stat.processing_time_samples_fast,
                        secs, EWMA_ALPHA,
                    );
                    tube.stat.record_fast_sample(secs);
                } else {
                    TubeStats::update_ewma(
                        &mut tube.stat.processing_time_ewma_slow,
                        &mut tube.stat.processing_time_samples_slow,
                        secs, EWMA_ALPHA,
                    );
                    tube.stat.record_slow_sample(secs);
                }
            }
        }
        self.stats.total_delete_ct += 1;

        // Remove idempotency key from tube index (with optional cooldown)
        let mut expiry_epoch_secs: u64 = 0;
        if let Some(ref key_tuple) = idempotency_key
            && let Some(tube) = self.tubes.get_mut(&tube_name)
        {
            tube.idempotency_keys.remove(&key_tuple.0);
            if key_tuple.1 > 0 {
                let expires_at = SystemTime::now() + Duration::from_secs(key_tuple.1 as u64);
                expiry_epoch_secs = expires_at
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                tube.idempotency_cooldowns
                    .insert(key_tuple.0.clone(), (id, expires_at));
            }
        }
        if let Some(ref grp) = group_name
            && let Some(gs) = self.groups.get_mut(grp)
        {
            gs.pending = gs.pending.saturating_sub(1);
            if state == JobState::Buried {
                gs.buried = gs.buried.saturating_sub(1);
            }
        }

        // WAL: write delete state change (with tombstone expiry if applicable)
        self.wal_write_state_change(id, None, 0, Duration::ZERO, expiry_epoch_secs);

        self.jobs.remove(&id);

        // Check if any group completed and promote waiting after-jobs
        if let Some(ref grp) = group_name {
            self.check_group_completion(grp);
        }

        Response::Deleted
    }

    fn cmd_delete_batch(&mut self, conn_id: u64, ids: Vec<u64>) -> Response {
        let mut deleted: u32 = 0;
        let mut not_found: u32 = 0;
        for id in ids {
            match self.cmd_delete(conn_id, id) {
                Response::Deleted => deleted += 1,
                _ => not_found += 1,
            }
        }
        Response::DeletedBatch { deleted, not_found }
    }

    fn cmd_flush_tube(&mut self, tube_name: &str) -> Response {
        let tube = match self.tubes.get(tube_name) {
            Some(t) => t,
            None => return Response::NotFound,
        };

        // Collect all job IDs from ready, delay, and buried queues
        let mut job_ids: Vec<u64> = Vec::new();
        job_ids.extend(tube.ready.ids());
        job_ids.extend(tube.delay.ids());
        job_ids.extend(tube.buried.iter());

        // Find reserved jobs belonging to this tube
        let reserved_ids: Vec<u64> = self
            .jobs
            .values()
            .filter(|j| j.tube_name == tube_name && j.state == JobState::Reserved)
            .map(|j| j.id)
            .collect();
        job_ids.extend(&reserved_ids);

        let count = job_ids.len() as u32;
        if count == 0 {
            // Still clear cooldowns even if no jobs
            if let Some(tube) = self.tubes.get_mut(tube_name) {
                tube.idempotency_cooldowns.clear();
            }
            return Response::Flushed(0);
        }

        // Track stats adjustments
        let mut ready_removed = 0u64;
        let mut urgent_removed = 0u64;
        let mut buried_removed = 0u64;
        let mut reserved_removed = 0u64;
        // Categorize jobs by state for stats
        for &id in &job_ids {
            if let Some(job) = self.jobs.get(&id) {
                match job.state {
                    JobState::Ready => {
                        ready_removed += 1;
                        if job.priority < URGENT_THRESHOLD {
                            urgent_removed += 1;
                        }
                    }
                    JobState::Reserved => reserved_removed += 1,
                    JobState::Buried => buried_removed += 1,
                    JobState::Delayed => {}
                }
            }
        }

        // Remove reserved jobs from owning connections and release concurrency keys
        for &id in &reserved_ids {
            self.release_concurrency_key(id);
            if let Some(job) = self.jobs.get(&id)
                && let Some(reserver_id) = job.reserver_id
                && let Some(conn) = self.conns.get_mut(&reserver_id)
            {
                conn.reserved_jobs.retain(|&jid| jid != id);
            }
        }

        // Clear tube queues
        let tube = self.tubes.get_mut(tube_name).unwrap();
        tube.ready.clear();
        tube.delay.clear();
        tube.buried.clear();
        tube.idempotency_keys.clear();
        tube.idempotency_cooldowns.clear();

        // Update tube stats
        tube.stat.total_delete_ct += count as u64;
        tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(reserved_removed);
        tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(buried_removed);
        tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(urgent_removed);

        // Update global stats
        self.stats.total_delete_ct += count as u64;
        self.ready_ct = self.ready_ct.saturating_sub(ready_removed);
        self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(urgent_removed);
        self.stats.buried_ct = self.stats.buried_ct.saturating_sub(buried_removed);
        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(reserved_removed);

        // Group tracking: decrement pending/buried counts for flushed jobs
        let mut affected_groups: Vec<String> = Vec::new();
        for &id in &job_ids {
            if let Some(job) = self.jobs.get(&id) {
                if let Some(ref grp) = job.group
                    && let Some(gs) = self.groups.get_mut(grp)
                {
                    gs.pending = gs.pending.saturating_sub(1);
                    if job.state == JobState::Buried {
                        gs.buried = gs.buried.saturating_sub(1);
                    }
                    if !affected_groups.contains(grp) {
                        affected_groups.push(grp.clone());
                    }
                }
                // Also remove held after-jobs from group waiting lists
                if let Some(ref ag) = job.after_group
                    && let Some(gs) = self.groups.get_mut(ag)
                {
                    gs.waiting_jobs.retain(|&jid| jid != id);
                }
            }
        }

        // WAL: write delete for each job, then remove from jobs map
        for &id in &job_ids {
            self.wal_write_state_change(id, None, 0, Duration::ZERO, 0);
            self.jobs.remove(&id);
        }

        // Check if any affected groups completed
        for grp in &affected_groups {
            self.check_group_completion(grp);
        }

        Response::Flushed(count)
    }

    fn cmd_release(&mut self, conn_id: u64, id: u64, pri: u32, delay: u32) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state != JobState::Reserved || job.reserver_id != Some(conn_id) {
            return Response::NotFound;
        }

        let tube_name = job.tube_name.clone();

        // Remove from reserved
        self.release_concurrency_key(id);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.reserved_jobs.retain(|&jid| jid != id);
        }
        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
        }

        // Update job
        if let Some(job) = self.jobs.get_mut(&id) {
            job.priority = pri;
            job.delay = Duration::from_secs(delay as u64);
            job.release_ct += 1;
            job.reserver_id = None;
            job.reserved_at = None;
        }

        // Enqueue
        if delay > 0 {
            let deadline = Instant::now() + Duration::from_secs(delay as u64);
            if let Some(job) = self.jobs.get_mut(&id) {
                job.state = JobState::Delayed;
                job.deadline_at = Some(deadline);
            }
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.delay.insert((deadline, id), id);
            }
        } else {
            if let Some(job) = self.jobs.get_mut(&id) {
                job.state = JobState::Ready;
                job.deadline_at = None;
                let key = job.ready_key();
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.ready.insert(key, id);
                }
            }
            self.ready_ct += 1;
            if pri < URGENT_THRESHOLD {
                self.stats.urgent_ct += 1;
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.urgent_ct += 1;
                }
            }
        }

        // WAL: write release state change
        let wal_state = if delay > 0 {
            JobState::Delayed
        } else {
            JobState::Ready
        };
        self.wal_write_state_change(
            id,
            Some(wal_state),
            pri,
            Duration::from_secs(delay as u64),
            0,
        );

        self.process_queue();
        Response::Released
    }

    fn cmd_bury(&mut self, conn_id: u64, id: u64, pri: u32) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state != JobState::Reserved || job.reserver_id != Some(conn_id) {
            return Response::NotFound;
        }

        let tube_name = job.tube_name.clone();

        // Remove from reserved
        self.release_concurrency_key(id);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.reserved_jobs.retain(|&jid| jid != id);
        }
        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
        }

        if let Some(job) = self.jobs.get_mut(&id) {
            job.priority = pri;
            job.state = JobState::Buried;
            job.reserver_id = None;
            job.reserved_at = None;
            job.deadline_at = None;
            job.bury_ct += 1;
        }

        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.buried.push_back(id);
            tube.stat.buried_ct += 1;
            tube.stat.total_bury_ct += 1;
        }
        self.stats.buried_ct += 1;

        // Group tracking: buried jobs block group completion
        if let Some(job) = self.jobs.get(&id)
            && let Some(ref grp) = job.group
            && let Some(gs) = self.groups.get_mut(grp)
        {
            gs.buried += 1;
        }

        // WAL: write bury state change
        self.wal_write_state_change(id, Some(JobState::Buried), pri, Duration::ZERO, 0);

        Response::Buried
    }

    fn cmd_touch(&mut self, conn_id: u64, id: u64) -> Response {
        let job = match self.jobs.get_mut(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state != JobState::Reserved || job.reserver_id != Some(conn_id) {
            return Response::NotFound;
        }

        job.deadline_at = Some(Instant::now() + job.ttr);
        Response::Touched
    }

    fn cmd_watch(&mut self, conn_id: u64, tube: &str, weight: u32) -> Response {
        self.ensure_tube(tube);

        if let Some(conn) = self.conns.get_mut(&conn_id) {
            // Update weight if already watching, otherwise add
            if let Some(w) = conn.watched.iter_mut().find(|w| w.name == tube) {
                w.weight = weight;
            } else {
                conn.watched.push(WatchedTube {
                    name: tube.to_string(),
                    weight,
                });
                if let Some(t) = self.tubes.get_mut(tube) {
                    t.watching_ct += 1;
                }
            }
            Response::Watching(conn.watched.len())
        } else {
            Response::InternalError
        }
    }

    fn cmd_ignore(&mut self, conn_id: u64, tube: &str) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            if conn.watched.len() < 2 {
                return Response::NotIgnored;
            }
            let was_watching = conn.watched.iter().any(|w| w.name == tube);
            conn.watched.retain(|w| w.name != tube);
            if was_watching && let Some(t) = self.tubes.get_mut(tube) {
                t.watching_ct = t.watching_ct.saturating_sub(1);
            }
            Response::Watching(conn.watched.len())
        } else {
            Response::InternalError
        }
    }

    fn cmd_peek(&self, id: u64) -> Response {
        match self.jobs.get(&id) {
            Some(job) => Response::Found {
                id,
                body: job.body.clone(),
            },
            None => Response::NotFound,
        }
    }

    fn cmd_peek_ready(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name)
            && let Some(&(_, job_id)) = tube.ready.peek()
            && let Some(job) = self.jobs.get(&job_id)
        {
            return Response::Found {
                id: job_id,
                body: job.body.clone(),
            };
        }
        Response::NotFound
    }

    fn cmd_peek_delayed(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name)
            && let Some(&(_, job_id)) = tube.delay.peek()
            && let Some(job) = self.jobs.get(&job_id)
        {
            return Response::Found {
                id: job_id,
                body: job.body.clone(),
            };
        }
        Response::NotFound
    }

    fn cmd_peek_buried(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name)
            && let Some(&job_id) = tube.buried.front()
            && let Some(job) = self.jobs.get(&job_id)
        {
            return Response::Found {
                id: job_id,
                body: job.body.clone(),
            };
        }
        Response::NotFound
    }

    fn cmd_peek_reserved(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        let found = self
            .jobs
            .values()
            .filter(|j| j.state == JobState::Reserved && j.tube_name == tube_name)
            .min_by_key(|j| j.id);
        match found {
            Some(job) => Response::Found {
                id: job.id,
                body: job.body.clone(),
            },
            None => Response::NotFound,
        }
    }

    fn cmd_kick(&mut self, conn_id: u64, bound: u32) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());

        let mut kicked = 0u32;

        // Kick buried first, then delayed
        let has_buried = self
            .tubes
            .get(&tube_name)
            .map(|t| !t.buried.is_empty())
            .unwrap_or(false);

        if has_buried {
            for _ in 0..bound {
                let job_id = {
                    let tube = match self.tubes.get_mut(&tube_name) {
                        Some(t) => t,
                        None => break,
                    };
                    match tube.buried.pop_front() {
                        Some(id) => {
                            tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                            id
                        }
                        None => break,
                    }
                };
                self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);

                // Group tracking: un-bury decrements buried count
                if let Some(job) = self.jobs.get(&job_id)
                    && let Some(ref grp) = job.group
                    && let Some(gs) = self.groups.get_mut(grp)
                {
                    gs.buried = gs.buried.saturating_sub(1);
                }

                // Re-enqueue as ready
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.state = JobState::Ready;
                    job.deadline_at = None;
                    job.kick_ct += 1;
                    let key = job.ready_key();
                    let tn = job.tube_name.clone();
                    if let Some(tube) = self.tubes.get_mut(&tn) {
                        tube.ready.insert(key, job_id);
                    }
                    self.ready_ct += 1;
                    if key.0 < URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        if let Some(tube) = self.tubes.get_mut(&tn) {
                            tube.stat.urgent_ct += 1;
                        }
                    }
                }
                // WAL: write kick state change
                self.wal_write_state_change(job_id, Some(JobState::Ready), 0, Duration::ZERO, 0);
                kicked += 1;
            }
        } else {
            // Kick delayed
            for _ in 0..bound {
                let job_id = {
                    let tube = match self.tubes.get_mut(&tube_name) {
                        Some(t) => t,
                        None => break,
                    };
                    match tube.delay.pop() {
                        Some((_, id)) => id,
                        None => break,
                    }
                };

                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.state = JobState::Ready;
                    job.deadline_at = None;
                    job.kick_ct += 1;
                    let key = job.ready_key();
                    let tn = job.tube_name.clone();
                    if let Some(tube) = self.tubes.get_mut(&tn) {
                        tube.ready.insert(key, job_id);
                    }
                    self.ready_ct += 1;
                    if key.0 < URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        if let Some(tube) = self.tubes.get_mut(&tn) {
                            tube.stat.urgent_ct += 1;
                        }
                    }
                }
                // WAL: write kick state change
                self.wal_write_state_change(job_id, Some(JobState::Ready), 0, Duration::ZERO, 0);
                kicked += 1;
            }
        }

        self.process_queue();
        Response::Kicked(kicked)
    }

    fn cmd_kick_job(&mut self, id: u64) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        let state = job.state;
        let tube_name = job.tube_name.clone();

        match state {
            JobState::Buried => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.buried.retain(|&jid| jid != id);
                    tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                }
                self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);

                // Group tracking: un-bury decrements buried count
                if let Some(job) = self.jobs.get(&id)
                    && let Some(ref grp) = job.group
                    && let Some(gs) = self.groups.get_mut(grp)
                {
                    gs.buried = gs.buried.saturating_sub(1);
                }
            }
            JobState::Delayed => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.delay.remove_by_id(id);
                }
            }
            _ => return Response::NotFound,
        }

        if let Some(job) = self.jobs.get_mut(&id) {
            job.state = JobState::Ready;
            job.deadline_at = None;
            job.kick_ct += 1;
            let key = job.ready_key();
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.ready.insert(key, id);
            }
            self.ready_ct += 1;
            if key.0 < URGENT_THRESHOLD {
                self.stats.urgent_ct += 1;
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.urgent_ct += 1;
                }
            }
        }

        // WAL: write kick state change
        self.wal_write_state_change(id, Some(JobState::Ready), 0, Duration::ZERO, 0);

        self.process_queue();
        Response::KickedOne
    }

    fn cmd_pause_tube(&mut self, tube: &str, delay: u32) -> Response {
        let tube = match self.tubes.get_mut(tube) {
            Some(t) => t,
            None => return Response::NotFound,
        };

        let delay_dur = Duration::from_secs(delay.max(1) as u64);
        tube.pause = delay_dur;
        tube.unpause_at = Some(Instant::now() + delay_dur);
        tube.stat.pause_ct += 1;

        Response::Paused
    }

    fn cmd_stats_job(&self, id: u64) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        let now = Instant::now();
        let age = now.duration_since(job.created_at).as_secs() as i64;
        let time_left = match job.state {
            JobState::Reserved | JobState::Delayed => job
                .deadline_at
                .map(|d| {
                    if d > now {
                        d.duration_since(now).as_secs() as i64
                    } else {
                        0
                    }
                })
                .unwrap_or(0),
            _ => 0,
        };

        let time_reserved = if job.state == JobState::Reserved {
            job.reserved_at
                .map(|ra| now.duration_since(ra).as_secs() as i64)
                .unwrap_or(0)
        } else {
            0
        };

        let yaml = format!(
            "---\n\
             id: {}\n\
             tube: \"{}\"\n\
             state: {}\n\
             pri: {}\n\
             age: {}\n\
             delay: {}\n\
             ttr: {}\n\
             time-left: {}\n\
             time-reserved: {}\n\
             file: {}\n\
             reserves: {}\n\
             timeouts: {}\n\
             releases: {}\n\
             buries: {}\n\
             kicks: {}\n\
             idempotency-key: {}\n\
             idempotency-ttl: {}\n\
             group: {}\n\
             after-group: {}\n\
             concurrency-key: {}\n\
             concurrency-limit: {}\n",
            job.id,
            job.tube_name,
            job.state.as_str(),
            job.priority,
            age,
            job.delay.as_secs(),
            job.ttr.as_secs(),
            time_left,
            time_reserved,
            job.wal_file_seq.unwrap_or(0),
            job.reserve_ct,
            job.timeout_ct,
            job.release_ct,
            job.bury_ct,
            job.kick_ct,
            job.idempotency_key
                .as_ref()
                .map(|(k, _)| k.as_str())
                .unwrap_or(""),
            job.idempotency_key
                .as_ref()
                .map(|(_, ttl)| *ttl)
                .unwrap_or(0),
            job.group.as_deref().unwrap_or(""),
            job.after_group.as_deref().unwrap_or(""),
            job.concurrency_key
                .as_ref()
                .map(|(k, _)| k.as_str())
                .unwrap_or(""),
            job.concurrency_key.as_ref().map(|(_, l)| *l).unwrap_or(0),
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats_tube(&self, tube_name: &str) -> Response {
        let tube = match self.tubes.get(tube_name) {
            Some(t) => t,
            None => return Response::NotFound,
        };

        let pause_time_left = tube
            .unpause_at
            .map(|u| {
                let now = Instant::now();
                if u > now {
                    u.duration_since(now).as_secs() as i64
                } else {
                    0
                }
            })
            .unwrap_or(0);

        let (p50, p95, p99) = tube.stat.percentiles();

        let bury_rate = tube.stat.bury_rate();

        let yaml = format!(
            "---\n\
             name: \"{}\"\n\
             current-jobs-urgent: {}\n\
             current-jobs-ready: {}\n\
             current-jobs-reserved: {}\n\
             current-jobs-delayed: {}\n\
             current-jobs-buried: {}\n\
             total-jobs: {}\n\
             current-using: {}\n\
             current-watching: {}\n\
             current-waiting: {}\n\
             cmd-delete: {}\n\
             cmd-pause-tube: {}\n\
             pause: {}\n\
             pause-time-left: {}\n\
             total-reserves: {}\n\
             total-timeouts: {}\n\
             total-buries: {}\n\
             bury-rate: {:.6}\n\
             processing-time-ewma: {:.6}\n\
             processing-time-min: {:.6}\n\
             processing-time-max: {:.6}\n\
             processing-time-samples: {}\n\
             processing-time-fast-threshold: {:.6}\n\
             processing-time-ewma-fast: {:.6}\n\
             processing-time-samples-fast: {}\n\
             processing-time-ewma-slow: {:.6}\n\
             processing-time-samples-slow: {}\n\
             processing-time-p50: {:.6}\n\
             processing-time-p95: {:.6}\n\
             processing-time-p99: {:.6}\n\
             queue-time-ewma: {:.6}\n\
             queue-time-min: {:.6}\n\
             queue-time-max: {:.6}\n\
             queue-time-samples: {}\n",
            tube.name,
            tube.stat.urgent_ct,
            tube.ready.len(),
            tube.stat.reserved_ct,
            tube.delay.len(),
            tube.stat.buried_ct,
            tube.stat.total_jobs_ct,
            tube.using_ct,
            tube.watching_ct,
            tube.stat.waiting_ct,
            tube.stat.total_delete_ct,
            tube.stat.pause_ct,
            tube.pause.as_secs(),
            pause_time_left,
            tube.stat.total_reserve_ct,
            tube.stat.total_timeout_ct,
            tube.stat.total_bury_ct,
            bury_rate,
            tube.stat.processing_time_ewma,
            tube.stat.processing_time_min.unwrap_or(0.0),
            tube.stat.processing_time_max.unwrap_or(0.0),
            tube.stat.processing_time_samples,
            FAST_THRESHOLD,
            tube.stat.processing_time_ewma_fast,
            tube.stat.processing_time_samples_fast,
            tube.stat.processing_time_ewma_slow,
            tube.stat.processing_time_samples_slow,
            p50,
            p95,
            p99,
            tube.stat.queue_time_ewma,
            tube.stat.queue_time_min.unwrap_or(0.0),
            tube.stat.queue_time_max.unwrap_or(0.0),
            tube.stat.queue_time_samples,
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats_group(&self, group_name: &str) -> Response {
        let gs = match self.groups.get(group_name) {
            Some(gs) => gs,
            None => return Response::NotFound,
        };

        let yaml = format!(
            "---\n\
             name: \"{}\"\n\
             pending: {}\n\
             buried: {}\n\
             complete: {}\n\
             waiting-jobs: {}\n",
            group_name,
            gs.pending,
            gs.buried,
            gs.is_complete(),
            gs.waiting_jobs.len(),
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats(&self) -> Response {
        let delayed_ct: usize = self.tubes.values().map(|t| t.delay.len()).sum();
        let uptime = Instant::now().duration_since(self.started_at).as_secs();

        // rusage stats
        let (rusage_utime, rusage_stime, rusage_maxrss) = {
            let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
            let utime = format!("{}.{:06}", usage.ru_utime.tv_sec, usage.ru_utime.tv_usec);
            let stime = format!("{}.{:06}", usage.ru_stime.tv_sec, usage.ru_stime.tv_usec);
            // On macOS ru_maxrss is in bytes; on Linux it's in kilobytes
            let maxrss = if cfg!(target_os = "linux") {
                usage.ru_maxrss * 1024
            } else {
                usage.ru_maxrss
            };
            (utime, stime, maxrss)
        };

        // WAL stats
        let (binlog_oldest, binlog_current, binlog_max_size, binlog_file_count, binlog_total_bytes) =
            match &self.wal {
                Some(wal) => (
                    wal.oldest_seq(),
                    wal.current_seq(),
                    wal.max_file_size(),
                    wal.file_count(),
                    wal.total_disk_bytes(),
                ),
                None => (0, 0, 0, 0, 0),
            };

        let yaml = format!(
            "---\n\
             current-jobs-urgent: {}\n\
             current-jobs-ready: {}\n\
             current-jobs-reserved: {}\n\
             current-jobs-delayed: {}\n\
             current-jobs-buried: {}\n\
             cmd-put: {}\n\
             cmd-peek: {}\n\
             cmd-peek-ready: {}\n\
             cmd-peek-delayed: {}\n\
             cmd-peek-buried: {}\n\
             cmd-peek-reserved: {}\n\
             cmd-reserve: {}\n\
             cmd-reserve-with-timeout: {}\n\
             cmd-delete: {}\n\
             cmd-release: {}\n\
             cmd-use: {}\n\
             cmd-watch: {}\n\
             cmd-ignore: {}\n\
             cmd-bury: {}\n\
             cmd-kick: {}\n\
             cmd-touch: {}\n\
             cmd-stats: {}\n\
             cmd-stats-job: {}\n\
             cmd-stats-tube: {}\n\
             cmd-list-tubes: {}\n\
             cmd-list-tube-used: {}\n\
             cmd-list-tubes-watched: {}\n\
             cmd-pause-tube: {}\n\
             cmd-reserve-mode: {}\n\
             job-timeouts: {}\n\
             total-jobs: {}\n\
             max-job-size: {}\n\
             current-tubes: {}\n\
             current-connections: {}\n\
             current-producers: {}\n\
             current-workers: {}\n\
             current-waiting: {}\n\
             total-connections: {}\n\
             pid: {}\n\
             version: \"tuber {}\"\n\
             rusage-utime: {}\n\
             rusage-stime: {}\n\
             rusage-maxrss: {}\n\
             uptime: {}\n\
             binlog-oldest-index: {}\n\
             binlog-current-index: {}\n\
             binlog-records-migrated: {}\n\
             binlog-records-written: 0\n\
             binlog-max-size: {}\n\
             binlog-enabled: {}\n\
             binlog-file-count: {}\n\
             binlog-total-bytes: {}\n\
             current-concurrency-keys: {}\n\
             draining: {}\n\
             id: {}\n\
             name: {}\n\
             hostname: {}\n\
             os: {}\n\
             platform: {}\n",
            self.stats.urgent_ct,
            self.ready_ct,
            self.stats.reserved_ct,
            delayed_ct,
            self.stats.buried_ct,
            self.stats.op_ct[OP_PUT],
            self.stats.op_ct[OP_PEEKJOB],
            self.stats.op_ct[OP_PEEK_READY],
            self.stats.op_ct[OP_PEEK_DELAYED],
            self.stats.op_ct[OP_PEEK_BURIED],
            self.stats.op_ct[OP_PEEK_RESERVED],
            self.stats.op_ct[OP_RESERVE],
            self.stats.op_ct[OP_RESERVE_TIMEOUT],
            self.stats.op_ct[OP_DELETE],
            self.stats.op_ct[OP_RELEASE],
            self.stats.op_ct[OP_USE],
            self.stats.op_ct[OP_WATCH],
            self.stats.op_ct[OP_IGNORE],
            self.stats.op_ct[OP_BURY],
            self.stats.op_ct[OP_KICK],
            self.stats.op_ct[OP_TOUCH],
            self.stats.op_ct[OP_STATS],
            self.stats.op_ct[OP_STATSJOB],
            self.stats.op_ct[OP_STATS_TUBE],
            self.stats.op_ct[OP_LIST_TUBES],
            self.stats.op_ct[OP_LIST_TUBE_USED],
            self.stats.op_ct[OP_LIST_TUBES_WATCHED],
            self.stats.op_ct[OP_PAUSE_TUBE],
            self.stats.op_ct[OP_RESERVE_MODE],
            self.stats.timeout_ct,
            self.stats.total_jobs_ct,
            self.max_job_size,
            self.tubes.len(),
            self.conns.len(),
            self.conns.values().filter(|c| c.is_producer()).count(),
            self.conns.values().filter(|c| c.is_worker()).count(),
            self.stats.waiting_ct,
            self.stats.total_connections,
            std::process::id(),
            env!("CARGO_PKG_VERSION"),
            rusage_utime,
            rusage_stime,
            rusage_maxrss,
            uptime,
            binlog_oldest,
            binlog_current,
            self.wal.as_ref().map(|w| w.records_migrated()).unwrap_or(0),
            binlog_max_size,
            if self.wal.is_some() { "true" } else { "false" },
            binlog_file_count,
            binlog_total_bytes,
            self.concurrency_keys.len(),
            if self.drain_mode { "true" } else { "false" },
            self.instance_id,
            self.name.as_deref().unwrap_or(""),
            self.hostname,
            self.os,
            self.platform,
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_list_tubes(&self) -> Response {
        let mut yaml = "---\n".to_string();
        for name in self.tubes.keys() {
            yaml.push_str(&format!("- {name}\n"));
        }
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_list_tube_used(&self, conn_id: u64) -> Response {
        let tube = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());
        Response::Using(tube)
    }

    fn cmd_list_tubes_watched(&self, conn_id: u64) -> Response {
        let mut yaml = "---\n".to_string();
        if let Some(conn) = self.conns.get(&conn_id) {
            for w in &conn.watched {
                yaml.push_str(&format!("- {}\n", w.name));
            }
        }
        Response::Ok(yaml.into_bytes())
    }

    // --- Internal helpers ---

    fn conn_deadline_soon(&self, conn_id: u64, now: Instant) -> bool {
        let conn = match self.conns.get(&conn_id) {
            Some(c) => c,
            None => return false,
        };
        if conn.reserved_jobs.is_empty() {
            return false;
        }
        let margin = Duration::from_secs(1);
        for &job_id in &conn.reserved_jobs {
            if let Some(job) = self.jobs.get(&job_id)
                && let Some(deadline) = job.deadline_at
                && deadline <= now + margin
            {
                return true;
            }
        }
        false
    }

    fn conn_has_ready_job(&self, conn_id: u64) -> bool {
        let conn = match self.conns.get(&conn_id) {
            Some(c) => c,
            None => return false,
        };
        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if tube.has_ready() {
                    return true;
                }
            }
        }
        false
    }

    fn find_ready_job_for_conn(&self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;
        let mut best: Option<((u32, u64), u64)> = None; // (key, job_id)

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if let Some(entry) = self.find_best_unblocked_ready(tube) {
                    let (key, jid) = entry;
                    match &best {
                        None => best = Some((key, jid)),
                        Some((bk, _)) => {
                            if key < *bk {
                                best = Some((key, jid));
                            }
                        }
                    }
                }
            }
        }

        best.map(|(_, id)| id)
    }

    fn select_weighted_job(&mut self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;

        // Sum weights of tubes with ready, unpaused jobs
        let mut total_weight: u32 = 0;
        let mut candidates: Vec<(u64, u32)> = Vec::new(); // (job_id, weight)

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() || !tube.has_ready() {
                    continue;
                }
                if let Some((_, job_id)) = self.find_best_unblocked_ready(tube) {
                    total_weight += w.weight;
                    candidates.push((job_id, w.weight));
                }
            }
        }

        if total_weight == 0 {
            return None;
        }

        // Simple xorshift-based PRNG for weighted selection
        self.rng_state = self.rng_state.wrapping_add(1);
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        let r = (x as u32) % total_weight;
        let mut cumulative = 0u32;
        for (job_id, weight) in &candidates {
            cumulative += weight;
            if r < cumulative {
                return Some(*job_id);
            }
        }

        candidates.last().map(|(id, _)| *id)
    }

    /// Weighted-fair job selection: adjusts weights by processing time so that
    /// worker-time (not job-count) is allocated proportional to weights.
    /// effective_weight = raw_weight / processing_time_ewma
    fn select_weighted_fair_job(&mut self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;

        let mut candidates: Vec<(u64, f64)> = Vec::new(); // (job_id, effective_weight)
        let mut total_weight: f64 = 0.0;

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() || !tube.has_ready() {
                    continue;
                }
                if let Some((_, job_id)) = self.find_best_unblocked_ready(tube) {
                    let ewma = tube.stat.processing_time_ewma;
                    let effective = if ewma > 0.0 {
                        w.weight as f64 / ewma
                    } else {
                        w.weight as f64
                    };
                    total_weight += effective;
                    candidates.push((job_id, effective));
                }
            }
        }

        if total_weight == 0.0 {
            return None;
        }

        // Same xorshift PRNG as select_weighted_job
        self.rng_state = self.rng_state.wrapping_add(1);
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;

        // Use f64 for the random threshold to match f64 weights
        let r = (x as f64 / u64::MAX as f64) * total_weight;
        let mut cumulative: f64 = 0.0;
        for (job_id, weight) in &candidates {
            cumulative += weight;
            if r < cumulative {
                return Some(*job_id);
            }
        }

        candidates.last().map(|(id, _)| *id)
    }

    /// Check if a group is complete and promote any waiting after-jobs to ready.
    fn check_group_completion(&mut self, group_name: &str) {
        let is_complete = self
            .groups
            .get(group_name)
            .map(|gs| gs.is_complete())
            .unwrap_or(false);

        if !is_complete {
            return;
        }

        // Take waiting jobs out of the group
        let waiting_jobs = self
            .groups
            .get_mut(group_name)
            .map(|gs| std::mem::take(&mut gs.waiting_jobs))
            .unwrap_or_default();

        // Promote each waiting after-job to ready
        for job_id in &waiting_jobs {
            if let Some(job) = self.jobs.get_mut(job_id)
                && job.state == JobState::Delayed
                && job.deadline_at.is_none()
            {
                job.state = JobState::Ready;
                let key = job.ready_key();
                let tn = job.tube_name.clone();
                if let Some(tube) = self.tubes.get_mut(&tn) {
                    tube.ready.insert(key, *job_id);
                }
                self.ready_ct += 1;
                if key.0 < URGENT_THRESHOLD {
                    self.stats.urgent_ct += 1;
                    if let Some(tube) = self.tubes.get_mut(&tn) {
                        tube.stat.urgent_ct += 1;
                    }
                }
            }
        }

        // Clean up idle groups
        if self
            .groups
            .get(group_name)
            .map(|gs| gs.is_idle())
            .unwrap_or(false)
        {
            self.groups.remove(group_name);
        }

        if !waiting_jobs.is_empty() {
            self.process_queue();
        }
    }

    /// Try to match waiting connections with ready jobs.
    fn process_queue(&mut self) {
        let now = Instant::now();

        // First pass: collect indices of timed-out waiters
        let mut timed_out_indices = Vec::new();

        for (i, waiter) in self.waiters.iter().enumerate() {
            if let Some(deadline) = waiter.deadline
                && now >= deadline
            {
                timed_out_indices.push(i);
            }
        }

        // Remove timed-out waiters from back to front
        let mut timed_out = Vec::new();
        for &i in timed_out_indices.iter().rev() {
            timed_out.push(self.waiters.remove(i));
        }

        for waiter in timed_out {
            if self.conn_deadline_soon(waiter.conn_id, now) {
                let _ = waiter.reply_tx.send(Response::DeadlineSoon);
            } else {
                let _ = waiter.reply_tx.send(Response::TimedOut);
            }
        }

        // Second pass: try to fulfill remaining waiters one at a time.
        // Each successful reserve changes the ready queue, so we re-check
        // from the start after each fulfillment.
        loop {
            // Use immutable check to find a candidate waiter with a ready job.
            // This works for both FIFO and weighted modes since
            // find_ready_job_for_conn_inner just checks availability.
            let mut fulfilled_idx = None;
            for (i, waiter) in self.waiters.iter().enumerate() {
                if self.find_ready_job_for_conn_inner(waiter.conn_id).is_some() {
                    fulfilled_idx = Some(i);
                    break;
                }
            }

            match fulfilled_idx {
                Some(i) => {
                    let waiter = self.waiters.remove(i);
                    // Select the job respecting the connection's reserve mode.
                    if let Some(job_id) = self.find_job_for_waiting_conn(waiter.conn_id) {
                        let resp = self.do_reserve(waiter.conn_id, job_id);
                        let _ = waiter.reply_tx.send(resp);
                    } else {
                        // Job was grabbed between check and reserve; re-queue the waiter
                        self.waiters.insert(i, waiter);
                        break;
                    }
                }
                None => break,
            }
        }
    }

    /// Same as find_ready_job_for_conn but uses inner state directly.
    fn find_ready_job_for_conn_inner(&self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;
        let mut best: Option<((u32, u64), u64)> = None;

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if let Some(entry) = self.find_best_unblocked_ready(tube) {
                    let (key, jid) = entry;
                    match &best {
                        None => best = Some((key, jid)),
                        Some((bk, _)) => {
                            if key < *bk {
                                best = Some((key, jid));
                            }
                        }
                    }
                }
            }
        }

        best.map(|(_, id)| id)
    }

    /// Find a job for a waiting connection, respecting its reserve mode.
    fn find_job_for_waiting_conn(&mut self, conn_id: u64) -> Option<u64> {
        let mode = self.conns.get(&conn_id)?.reserve_mode;
        match mode {
            ReserveMode::Weighted => self.select_weighted_job(conn_id),
            ReserveMode::WeightedFair => self.select_weighted_fair_job(conn_id),
            ReserveMode::Fifo => self.find_ready_job_for_conn_inner(conn_id),
        }
    }

    fn remove_waiter(&mut self, conn_id: u64) {
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].conn_id == conn_id {
                let waiter = self.waiters.remove(i);
                let _ = waiter.reply_tx.send(Response::TimedOut);
            } else {
                i += 1;
            }
        }
    }

    // --- WAL helpers ---

    fn wal_write_put(&mut self, job_id: u64) {
        if let Some(wal) = self.wal.as_mut() {
            // We need to temporarily take the job out to satisfy borrow checker
            if let Some(mut job) = self.jobs.remove(&job_id) {
                if let Err(e) = wal.write_put(&mut job) {
                    tracing::error!("WAL write_put error: {}, disabling WAL", e);
                    self.wal = None;
                }
                self.jobs.insert(job_id, job);
            }
        }
    }

    fn wal_write_state_change(
        &mut self,
        job_id: u64,
        state: Option<JobState>,
        pri: u32,
        delay: Duration,
        expiry_epoch_secs: u64,
    ) {
        if let Some(wal) = self.wal.as_mut()
            && let Some(mut job) = self.jobs.remove(&job_id)
        {
            if let Err(e) = wal.write_state_change(&mut job, state, pri, delay, expiry_epoch_secs) {
                tracing::error!("WAL write_state_change error: {}, disabling WAL", e);
                self.wal = None;
            }
            self.jobs.insert(job_id, job);
        }
    }

    /// Tick: promote delayed jobs, expire TTR, unpause tubes.
    fn tick(&mut self) {
        let now = Instant::now();

        // Promote delayed jobs to ready
        let tube_names: Vec<String> = self.tubes.keys().cloned().collect();
        for tube_name in &tube_names {
            loop {
                let should_promote = self
                    .tubes
                    .get(tube_name)
                    .and_then(|t| t.delay.peek().map(|&((deadline, _), _)| deadline <= now))
                    .unwrap_or(false);

                if !should_promote {
                    break;
                }

                let job_id = self
                    .tubes
                    .get_mut(tube_name)
                    .and_then(|t| t.delay.pop().map(|(_, id)| id));

                if let Some(job_id) = job_id {
                    // Check if this is an aft: job whose group isn't complete yet
                    let hold_for_group = self
                        .jobs
                        .get(&job_id)
                        .and_then(|j| j.after_group.as_ref())
                        .and_then(|ag| self.groups.get(ag))
                        .map(|gs| !gs.is_complete())
                        .unwrap_or(false);

                    if hold_for_group {
                        // Keep as delayed with no deadline; add to group waiting list
                        if let Some(job) = self.jobs.get_mut(&job_id) {
                            job.deadline_at = None;
                        }
                        if let Some(ag) = self.jobs.get(&job_id).and_then(|j| j.after_group.clone())
                            && let Some(gs) = self.groups.get_mut(&ag)
                        {
                            gs.waiting_jobs.push(job_id);
                        }
                    } else if let Some(job) = self.jobs.get_mut(&job_id) {
                        job.state = JobState::Ready;
                        job.deadline_at = None;
                        let key = job.ready_key();
                        if let Some(tube) = self.tubes.get_mut(tube_name) {
                            tube.ready.insert(key, job_id);
                        }
                        self.ready_ct += 1;
                        if key.0 < URGENT_THRESHOLD {
                            self.stats.urgent_ct += 1;
                            if let Some(tube) = self.tubes.get_mut(tube_name) {
                                tube.stat.urgent_ct += 1;
                            }
                        }
                    }
                }
            }
        }

        // Unpause tubes
        for tube in self.tubes.values_mut() {
            if let Some(unpause_at) = tube.unpause_at
                && now >= unpause_at
            {
                tube.pause = Duration::ZERO;
                tube.unpause_at = None;
            }
        }

        // Expire reserved jobs past TTR
        let conn_ids: Vec<u64> = self.conns.keys().cloned().collect();
        for conn_id in conn_ids {
            let expired_jobs: Vec<u64> = {
                let conn = match self.conns.get(&conn_id) {
                    Some(c) => c,
                    None => continue,
                };
                conn.reserved_jobs
                    .iter()
                    .filter(|&&jid| {
                        self.jobs
                            .get(&jid)
                            .and_then(|j| j.deadline_at)
                            .map(|d| now >= d)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            };

            for job_id in expired_jobs {
                // Remove from connection's reserved list
                self.release_concurrency_key(job_id);
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    conn.reserved_jobs.retain(|&jid| jid != job_id);
                }
                self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
                self.stats.timeout_ct += 1;

                // Re-enqueue as ready
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.timeout_ct += 1;
                    job.state = JobState::Ready;
                    job.reserver_id = None;
                    job.reserved_at = None;
                    job.deadline_at = None;
                    let key = job.ready_key();
                    let tube_name = job.tube_name.clone();
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
                        tube.stat.total_timeout_ct += 1;
                        tube.ready.insert(key, job_id);
                    }
                    self.ready_ct += 1;
                    if key.0 < URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        if let Some(tube) = self.tubes.get_mut(&tube_name) {
                            tube.stat.urgent_ct += 1;
                        }
                    }
                }
            }
        }

        // Proactive DEADLINE_SOON: wake waiting clients whose reserved jobs
        // are within the 1-second safety margin.
        let mut deadline_soon_indices = Vec::new();
        for (i, waiter) in self.waiters.iter().enumerate() {
            if self.conn_deadline_soon(waiter.conn_id, now) {
                deadline_soon_indices.push(i);
            }
        }
        for &i in deadline_soon_indices.iter().rev() {
            let waiter = self.waiters.remove(i);
            let _ = waiter.reply_tx.send(Response::DeadlineSoon);
        }

        // Check waiting connection timeouts
        // Collect indices of timed-out waiters
        let timed_out: Vec<usize> = self
            .waiters
            .iter()
            .enumerate()
            .filter(|(_, w)| w.deadline.map(|d| now >= d).unwrap_or(false))
            .map(|(i, _)| i)
            .collect();

        // Remove from back to front
        let mut expired = Vec::new();
        for &i in timed_out.iter().rev() {
            expired.push(self.waiters.remove(i));
        }

        // Now send responses (no longer borrowing self.waiters)
        for waiter in expired {
            if self.conn_deadline_soon(waiter.conn_id, now) {
                let _ = waiter.reply_tx.send(Response::DeadlineSoon);
            } else {
                let _ = waiter.reply_tx.send(Response::TimedOut);
            }
        }

        // Try to fulfill remaining waiters with newly ready jobs
        self.process_queue();

        // Clean up expired idempotency cooldowns
        let sys_now = SystemTime::now();
        for tube in self.tubes.values_mut() {
            tube.idempotency_cooldowns
                .retain(|_, (_, expiry)| *expiry > sys_now);
        }

        // WAL maintenance (GC, sync, compaction)
        if let Some(wal) = self.wal.as_mut() {
            wal.maintain();
        }
        if let Some(target) = self.wal.as_ref().and_then(|w| w.compaction_target()) {
            let (target_seq, count) = target;
            let migrate_ids: Vec<u64> = self
                .jobs
                .iter()
                .filter(|(_, job)| job.wal_file_seq == Some(target_seq))
                .map(|(id, _)| *id)
                .take(count)
                .collect();

            for job_id in migrate_ids {
                if let (Some(wal), Some(job)) =
                    (self.wal.as_mut(), self.jobs.get_mut(&job_id))
                {
                    if let Err(e) = wal.write_put(job) {
                        tracing::error!("WAL compaction write error: {}, disabling WAL", e);
                        self.wal = None;
                        break;
                    }
                    wal.record_migration();
                }
            }
        }

        // New connections default to "use default" + "watch default", so always keep it
        self.tubes.retain(|name, tube| name == "default" || !tube.is_idle());
    }

    /// Restore jobs from WAL replay into server state.
    fn restore_jobs(
        &mut self,
        jobs: HashMap<u64, Job>,
        next_job_id: u64,
        tombstones: Vec<IdpTombstone>,
    ) {
        self.next_job_id = next_job_id;

        // Collect after_group job IDs for a second pass
        let mut after_group_jobs: Vec<u64> = Vec::new();

        for (id, job) in jobs {
            let tube_name = job.tube_name.clone();
            let state = job.state;
            let pri = job.priority;
            let idempotency_key = job.idempotency_key.clone();
            let group = job.group.clone();
            let after_group = job.after_group.clone();

            self.ensure_tube(&tube_name);

            match state {
                JobState::Ready => {
                    let key = job.ready_key();
                    self.jobs.insert(id, job);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.ready.insert(key, id);
                    }
                    self.ready_ct += 1;
                    if pri < URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        if let Some(tube) = self.tubes.get_mut(&tube_name) {
                            tube.stat.urgent_ct += 1;
                        }
                    }
                    self.stats.total_jobs_ct += 1;
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.total_jobs_ct += 1;
                    }
                }
                JobState::Delayed => {
                    let deadline = job
                        .deadline_at
                        .unwrap_or_else(|| Instant::now() + job.delay);
                    self.jobs.insert(id, job);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.delay.insert((deadline, id), id);
                    }
                    self.stats.total_jobs_ct += 1;
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.total_jobs_ct += 1;
                    }
                }
                JobState::Buried => {
                    self.jobs.insert(id, job);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.buried.push_back(id);
                        tube.stat.buried_ct += 1;
                    }
                    self.stats.buried_ct += 1;
                    self.stats.total_jobs_ct += 1;
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.total_jobs_ct += 1;
                    }
                }
                JobState::Reserved => {
                    // Reserved jobs replay as Ready (handled by WAL deserialization)
                    // This shouldn't happen, but handle it gracefully
                    let key = (pri, id);
                    self.jobs.insert(id, job);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.ready.insert(key, id);
                    }
                    self.ready_ct += 1;
                    self.stats.total_jobs_ct += 1;
                }
            }

            // Register concurrency limit
            if let Some(job) = self.jobs.get(&id) {
                if let Some((ref key, limit)) = job.concurrency_key {
                    let entry = self.concurrency_limits.entry(key.clone()).or_insert(0);
                    *entry = (*entry).max(limit);
                }
            }

            // Register idempotency key in tube index
            if let Some(ref key_tuple) = idempotency_key
                && let Some(tube) = self.tubes.get_mut(&tube_name)
            {
                tube.idempotency_keys.insert(key_tuple.0.clone(), id);
            }

            // Rebuild group state
            if let Some(ref grp) = group {
                let gs = self
                    .groups
                    .entry(grp.clone())
                    .or_insert_with(GroupState::new);
                gs.pending += 1;
                if state == JobState::Buried {
                    gs.buried += 1;
                }
            }

            if after_group.is_some() {
                after_group_jobs.push(id);
            }
        }

        // Second pass: check after-group jobs and hold if group is not complete
        for job_id in after_group_jobs {
            let ag = self.jobs.get(&job_id).and_then(|j| j.after_group.clone());
            if let Some(ag) = ag {
                let group_incomplete = self
                    .groups
                    .get(&ag)
                    .map(|gs| !gs.is_complete())
                    .unwrap_or(false);

                if group_incomplete {
                    // If the job is currently ready, move it to held (delayed with no deadline)
                    if let Some(job) = self.jobs.get(&job_id)
                        && job.state == JobState::Ready
                    {
                        let tube_name = job.tube_name.clone();
                        let pri = job.priority;
                        if let Some(tube) = self.tubes.get_mut(&tube_name) {
                            tube.ready.remove_by_id(job_id);
                        }
                        self.ready_ct = self.ready_ct.saturating_sub(1);
                        if pri < URGENT_THRESHOLD {
                            self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
                            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                                tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
                            }
                        }
                        if let Some(job) = self.jobs.get_mut(&job_id) {
                            job.state = JobState::Delayed;
                            job.deadline_at = None;
                        }
                    }
                    // Add to group waiting list
                    let gs = self.groups.entry(ag).or_insert_with(GroupState::new);
                    gs.waiting_jobs.push(job_id);
                }
            }
        }

        // Restore idempotency tombstones from WAL
        for tombstone in tombstones {
            self.ensure_tube(&tombstone.tube_name);
            if let Some(tube) = self.tubes.get_mut(&tombstone.tube_name) {
                tube.idempotency_cooldowns
                    .insert(tombstone.key, (tombstone.job_id, tombstone.expires_at));
            }
        }
    }
}

/// Start the beanstalkd server.
pub async fn run(
    addr: &str,
    port: u16,
    max_job_size: u32,
    wal_dir: Option<&str>,
    metrics_port: Option<u16>,
    name: Option<String>,
) -> io::Result<()> {
    let listener = TcpListener::bind((addr, port)).await?;
    if let Some(ref n) = name {
        tracing::info!("tuber v{} [{}] listening on {}:{}", env!("CARGO_PKG_VERSION"), n, addr, port);
    } else {
        tracing::info!("tuber v{} listening on {}:{}", env!("CARGO_PKG_VERSION"), addr, port);
    }

    if let Some(mp) = metrics_port {
        let listen_addr = listener.local_addr()?.ip();
        let beanstalk_addr = format!("{listen_addr}:{port}");
        tokio::spawn(async move {
            if let Err(e) = crate::metrics::serve(listen_addr, mp, beanstalk_addr).await {
                tracing::error!("metrics server error: {e}");
            }
        });
    }

    let wal_path = wal_dir.map(Path::new);
    run_with_listener(listener, max_job_size, wal_path, name).await
}

pub async fn run_with_listener(
    listener: TcpListener,
    max_job_size: u32,
    wal_dir: Option<&Path>,
    name: Option<String>,
) -> io::Result<()> {
    let (engine_tx, mut engine_rx) = mpsc::channel::<EngineMsg>(1024);
    let mut state = ServerState::new(max_job_size, name);

    // WAL: open and replay if configured
    if let Some(dir) = wal_dir {
        let mut wal = Wal::open(dir, None)?;
        let (jobs, next_id, tombstones) = wal.replay()?;
        let job_count = jobs.len();
        let tombstone_count = tombstones.len();
        state.restore_jobs(jobs, next_id, tombstones);
        state.wal = Some(wal);
        tracing::info!(
            "WAL: replayed {} jobs and {} idempotency tombstones from {:?}",
            job_count,
            tombstone_count,
            dir
        );
    }

    // Engine task
    let _engine_handle = tokio::spawn(async move {
        let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
        let mut sigusr1 =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                .expect("failed to register SIGUSR1 handler");

        loop {
            tokio::select! {
                msg = engine_rx.recv() => {
                    let msg = match msg {
                        Some(m) => m,
                        None => break, // all senders dropped
                    };
                    match msg.payload {
                        EnginePayload::Command { cmd, body, reply_tx } => {
                            let is_reserve = matches!(cmd,
                                Command::Reserve | Command::ReserveWithTimeout { .. }
                            );
                            let timeout = match &cmd {
                                Command::ReserveWithTimeout { timeout } => Some(*timeout),
                                Command::Reserve => None, // infinite wait
                                _ => None,
                            };

                            let resp = state.handle_command(msg.conn_id, cmd, body);

                            // If reserve returned TimedOut but wasn't a timeout=0 request,
                            // this means no job was available and we should wait.
                            if is_reserve && matches!(resp, Response::TimedOut)
                                && timeout != Some(0) {
                                    let deadline = timeout.map(|t| {
                                        Instant::now() + Duration::from_secs(t as u64)
                                    });
                                    state.waiters.push(WaitingReserve {
                                        conn_id: msg.conn_id,
                                        reply_tx,
                                        deadline,
                                    });
                                    continue;
                                }

                            let _ = reply_tx.send(resp);
                        }
                        EnginePayload::Disconnect => {
                            state.unregister_conn(msg.conn_id);
                        }
                        EnginePayload::Shutdown => {
                            tracing::info!("engine shutting down, flushing WAL");
                            if let Some(wal) = &mut state.wal {
                                wal.maintain();
                            }
                            break;
                        }
                    }
                }
                _ = tick_interval.tick() => {
                    state.tick();
                }
                _ = sigusr1.recv() => {
                    tracing::info!("received SIGUSR1, entering drain mode");
                    state.drain_mode = true;
                }
                else => break,
            }
        }
        tracing::info!("engine stopped");
    });

    // Accept loop with graceful shutdown on SIGINT/SIGTERM
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("failed to register SIGINT handler");
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, peer) = result?;
                let _ = socket.set_nodelay(true);
                tracing::debug!("accepted connection from {}", peer);

                let tx = engine_tx.clone();
                tokio::spawn(async move {
                    handle_connection(socket, tx, max_job_size).await;
                });
            }
            _ = sigint.recv() => {
                tracing::info!("received SIGINT, shutting down gracefully");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, shutting down gracefully");
                break;
            }
        }
    }

    // Send shutdown to engine and wait for it to flush WAL
    let _ = engine_tx
        .send(EngineMsg {
            conn_id: 0,
            payload: EnginePayload::Shutdown,
        })
        .await;

    // Drop the sender so the engine loop exits after processing Shutdown
    drop(engine_tx);

    Ok(())
}

/// Atomic counter for connection IDs (simpler than engine round-trip).
static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);


/// Maximum digits in a u64 value (18446744073709551615).
const MAX_U64_DIGITS: usize = 20;

/// Maximum command line length in bytes, including the trailing \r\n.
/// The longest possible command is delete-batch with MAX_DELETE_BATCH (1000) u64 IDs:
///   "delete-batch "                                  =  13
///   1000 × (u64 + space)                             = 21000  (1000 × (20 + 1))
///   "\r\n"                                           =   2
///   Total                                            = 21015
///
/// For reference, put with all extensions is 891 bytes (well under this limit).
const MAX_LINE_LEN: u64 = (13
    + MAX_DELETE_BATCH * (MAX_U64_DIGITS + 1)
    + 2) as u64;

async fn handle_connection(
    socket: tokio::net::TcpStream,
    engine_tx: mpsc::Sender<EngineMsg>,
    max_job_size: u32,
) {
    let conn_id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line_buf = String::new();
    let mut resp_buf = Vec::with_capacity(256);

    loop {
        line_buf.clear();
        // Limit read to MAX_LINE_LEN to prevent unbounded memory growth from
        // clients that send data without a newline. Matches beanstalkd behavior.
        let _n = match (&mut reader)
            .take(MAX_LINE_LEN)
            .read_line(&mut line_buf)
            .await
        {
            Ok(0) => break, // EOF
            Ok(n) => {
                if !line_buf.ends_with('\n') {
                    // Line exceeded MAX_LINE_LEN without a newline — bad client
                    let _ = writer.write_all(b"BAD_FORMAT\r\n").await;
                    break;
                }
                n
            }
            Err(e) => {
                tracing::debug!("read error for conn {}: {}", conn_id, e);
                break;
            }
        };

        // Strip trailing \r\n
        let cmd_str = line_buf.trim_end_matches('\n').trim_end_matches('\r');

        // Parse command
        let cmd = match protocol::parse_command(cmd_str) {
            Ok(cmd) => cmd,
            Err(resp) => {
                resp_buf.clear();
                resp.serialize_into(&mut resp_buf);
                let _ = writer.write_all(&resp_buf).await;
                continue;
            }
        };

        // Handle quit
        if matches!(cmd, Command::Quit) {
            break;
        }

        // If it's a put command, read the body
        let body = if let Command::Put { bytes, .. } = &cmd {
            let body_size = *bytes as usize;

            // Check size BEFORE allocating to prevent OOM from malicious clients
            if body_size > max_job_size as usize {
                let _ = writer.write_all(b"JOB_TOO_BIG\r\n").await;
                // Drain the body + \r\n so the connection stays usable
                let to_drain = body_size as u64 + 2;
                if tokio::io::copy(&mut (&mut reader).take(to_drain), &mut tokio::io::sink())
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }

            let mut body_buf = vec![0u8; body_size + 2]; // +2 for \r\n
            match reader.read_exact(&mut body_buf).await {
                Ok(_) => {
                    // Verify trailing \r\n
                    if body_buf[body_size] != b'\r' || body_buf[body_size + 1] != b'\n' {
                        let _ = writer.write_all(b"EXPECTED_CRLF\r\n").await;
                        continue;
                    }
                    body_buf.truncate(body_size);

                    Some(body_buf)
                }
                Err(e) => {
                    tracing::debug!("body read error for conn {}: {}", conn_id, e);
                    break;
                }
            }
        } else {
            None
        };

        // Send to engine and await response
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = engine_tx
            .send(EngineMsg {
                conn_id,
                payload: EnginePayload::Command {
                    cmd,
                    body,
                    reply_tx,
                },
            })
            .await;

        match reply_rx.await {
            Ok(resp) => {
                resp_buf.clear();
                resp.serialize_into(&mut resp_buf);
                if writer.write_all(&resp_buf).await.is_err() {
                    break;
                }
            }
            Err(_) => break, // engine dropped
        }
    }

    // Disconnect
    let _ = engine_tx
        .send(EngineMsg {
            conn_id,
            payload: EnginePayload::Disconnect,
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state() -> ServerState {
        ServerState::new(65535, None)
    }

    fn register(state: &mut ServerState) -> u64 {
        static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        state.conns.insert(id, ConnState::new(id));
        state.stats.total_connections += 1;
        state.ensure_tube("default");
        if let Some(t) = state.tubes.get_mut("default") {
            t.watching_ct += 1;
            t.using_ct += 1;
        }
        id
    }

    fn put_cmd(pri: u32, delay: u32, ttr: u32, bytes: u32) -> Command {
        Command::Put {
            pri,
            delay,
            ttr,
            bytes,
            idempotency_key: None,
            group: None,
            after_group: None,
            concurrency_key: None,
        }
    }

    #[test]
    fn test_put_and_reserve() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(c, put_cmd(0, 0, 10, 5), Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        let resp = s.handle_command(c, Command::Reserve, None);
        match resp {
            Response::Reserved { id, body } => {
                assert_eq!(id, 1);
                assert_eq!(body, b"hello");
            }
            _ => panic!("expected Reserved, got {:?}", resp),
        }
    }

    #[test]
    fn test_priority_ordering() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(5, 0, 10, 1), Some(b"a".to_vec()));
        s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"b".to_vec()));
        s.handle_command(c, put_cmd(3, 0, 10, 1), Some(b"c".to_vec()));

        // Should get priority 1 first
        let resp = s.handle_command(c, Command::Reserve, None);
        match resp {
            Response::Reserved { body, .. } => assert_eq!(body, b"b"),
            _ => panic!("expected Reserved"),
        }
    }

    #[test]
    fn test_fifo_same_priority() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"a".to_vec()));
        s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"b".to_vec()));
        s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"c".to_vec()));

        let resp = s.handle_command(c, Command::Reserve, None);
        match resp {
            Response::Reserved { body, .. } => assert_eq!(body, b"a"),
            _ => panic!("expected Reserved"),
        }
        let resp = s.handle_command(c, Command::Reserve, None);
        match resp {
            Response::Reserved { body, .. } => assert_eq!(body, b"b"),
            _ => panic!("expected Reserved"),
        }
    }

    #[test]
    fn test_delete_ready() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
        assert!(matches!(resp, Response::Deleted));

        let resp = s.handle_command(c, Command::Peek { id: 1 }, None);
        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn test_bury_and_kick() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        let resp = s.handle_command(c, Command::Reserve, None);
        let id = match resp {
            Response::Reserved { id, .. } => id,
            _ => panic!("expected Reserved"),
        };

        let resp = s.handle_command(c, Command::Bury { id, pri: 0 }, None);
        assert!(matches!(resp, Response::Buried));

        let resp = s.handle_command(c, Command::Kick { bound: 1 }, None);
        assert!(matches!(resp, Response::Kicked(1)));

        // Job should be ready again
        let resp = s.handle_command(c, Command::PeekReady, None);
        assert!(matches!(resp, Response::Found { .. }));
    }

    #[test]
    fn test_multi_tube() {
        let mut s = make_state();
        let producer = register(&mut s);
        let consumer = register(&mut s);

        // Producer uses "emails"
        s.handle_command(
            producer,
            Command::Use {
                tube: "emails".into(),
            },
            None,
        );
        s.handle_command(producer, put_cmd(0, 0, 10, 5), Some(b"hello".to_vec()));

        // Consumer watches "emails", reserve should find it
        s.handle_command(
            consumer,
            Command::Watch {
                tube: "emails".into(),
                weight: 1,
            },
            None,
        );
        let resp = s.handle_command(consumer, Command::Reserve, None);
        match resp {
            Response::Reserved { body, .. } => assert_eq!(body, b"hello"),
            _ => panic!("expected Reserved, got {:?}", resp),
        }
    }

    #[test]
    fn test_release_to_ready() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(5, 0, 10, 1), Some(b"x".to_vec()));
        let resp = s.handle_command(c, Command::Reserve, None);
        let id = match resp {
            Response::Reserved { id, .. } => id,
            _ => panic!(),
        };

        let resp = s.handle_command(
            c,
            Command::Release {
                id,
                pri: 0,
                delay: 0,
            },
            None,
        );
        assert!(matches!(resp, Response::Released));

        // Should be back in ready with new priority
        let resp = s.handle_command(c, Command::PeekReady, None);
        assert!(matches!(resp, Response::Found { .. }));
    }

    #[test]
    fn test_touch() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        let resp = s.handle_command(c, Command::Reserve, None);
        let id = match resp {
            Response::Reserved { id, .. } => id,
            _ => panic!(),
        };

        let resp = s.handle_command(c, Command::Touch { id }, None);
        assert!(matches!(resp, Response::Touched));
    }

    #[test]
    fn test_drain_mode() {
        let mut s = make_state();
        let c = register(&mut s);
        s.drain_mode = true;

        let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        assert!(matches!(resp, Response::Draining));
    }

    #[test]
    fn test_undrain_mode() {
        let mut s = make_state();
        let c = register(&mut s);

        // Enter drain mode
        let resp = s.handle_command(c, Command::Drain, None);
        assert!(matches!(resp, Response::Draining));

        // Put should fail
        let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        assert!(matches!(resp, Response::Draining));

        // Undrain
        let resp = s.handle_command(c, Command::Undrain, None);
        assert!(matches!(resp, Response::NotDraining));

        // Put should work again
        let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        assert!(matches!(resp, Response::Inserted(_)));
    }

    #[test]
    fn test_close_releases_jobs() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        s.handle_command(c, Command::Reserve, None);

        // Disconnect
        s.unregister_conn(c);

        // Job should be back in ready (for another consumer)
        assert_eq!(s.ready_ct, 1);
    }

    #[test]
    fn test_reserve_mode_weighted() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(
            c,
            Command::ReserveMode {
                mode: "weighted".into(),
            },
            None,
        );
        assert!(matches!(resp, Response::Using(m) if m == "weighted"));

        let resp = s.handle_command(
            c,
            Command::ReserveMode {
                mode: "fifo".into(),
            },
            None,
        );
        assert!(matches!(resp, Response::Using(m) if m == "fifo"));

        let resp = s.handle_command(
            c,
            Command::ReserveMode {
                mode: "invalid".into(),
            },
            None,
        );
        assert!(matches!(resp, Response::BadFormat));
    }

    #[test]
    fn test_reserve_mode_weighted_fair() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(
            c,
            Command::ReserveMode {
                mode: "weighted-fair".into(),
            },
            None,
        );
        assert!(matches!(resp, Response::Using(m) if m == "weighted-fair"));

        // Switch back to fifo
        let resp = s.handle_command(
            c,
            Command::ReserveMode {
                mode: "fifo".into(),
            },
            None,
        );
        assert!(matches!(resp, Response::Using(m) if m == "fifo"));
    }

    #[test]
    fn test_weighted_fair_favors_fast_tubes() {
        let mut s = make_state();
        let c = register(&mut s);

        // Set up weighted-fair mode
        s.handle_command(
            c,
            Command::ReserveMode {
                mode: "weighted-fair".into(),
            },
            None,
        );

        // Watch "fast" tube (weight 1) and "slow" tube (weight 1)
        s.handle_command(
            c,
            Command::Watch {
                tube: "fast".into(),
                weight: 1,
            },
            None,
        );
        s.handle_command(
            c,
            Command::Watch {
                tube: "slow".into(),
                weight: 1,
            },
            None,
        );
        s.handle_command(
            c,
            Command::Ignore {
                tube: "default".into(),
            },
            None,
        );

        // Put jobs into both tubes
        s.handle_command(
            c,
            Command::Use {
                tube: "fast".into(),
            },
            None,
        );
        for _ in 0..100 {
            s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"f".to_vec()));
        }

        s.handle_command(
            c,
            Command::Use {
                tube: "slow".into(),
            },
            None,
        );
        for _ in 0..100 {
            s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"s".to_vec()));
        }

        // Set processing time EWMAs: fast=0.1s, slow=10.0s (100x slower)
        s.tubes.get_mut("fast").unwrap().stat.processing_time_ewma = 0.1;
        s.tubes.get_mut("fast").unwrap().stat.processing_time_samples = 100;
        s.tubes.get_mut("slow").unwrap().stat.processing_time_ewma = 10.0;
        s.tubes.get_mut("slow").unwrap().stat.processing_time_samples = 100;

        // Reserve many jobs and count which tube they came from
        let mut fast_count = 0u32;
        let mut slow_count = 0u32;
        for _ in 0..50 {
            let resp = s.handle_command(c, Command::Reserve, None);
            if let Response::Reserved { id, .. } = resp {
                let job = s.jobs.get(&id).unwrap();
                if job.tube_name == "fast" {
                    fast_count += 1;
                } else {
                    slow_count += 1;
                }
                // Delete so the job doesn't stay reserved
                s.handle_command(c, Command::Delete { id }, None);
            }
        }

        // With equal weights and 100x processing time difference,
        // fast tube should get ~99% of reserves.
        // Be generous: fast should get at least 80% (40 out of 50).
        assert!(
            fast_count > 40,
            "weighted-fair should heavily favor fast tube: fast={fast_count}, slow={slow_count}"
        );
    }

    #[test]
    fn test_weighted_fair_fallback_no_ewma() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(
            c,
            Command::ReserveMode {
                mode: "weighted-fair".into(),
            },
            None,
        );

        // Watch two tubes, neither has processing time data
        s.handle_command(
            c,
            Command::Watch {
                tube: "a".into(),
                weight: 3,
            },
            None,
        );
        s.handle_command(
            c,
            Command::Watch {
                tube: "b".into(),
                weight: 1,
            },
            None,
        );
        s.handle_command(
            c,
            Command::Ignore {
                tube: "default".into(),
            },
            None,
        );

        // Put one job in each
        s.handle_command(c, Command::Use { tube: "a".into() }, None);
        s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
        s.handle_command(c, Command::Use { tube: "b".into() }, None);
        s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"b".to_vec()));

        // Both tubes have ewma=0 so effective weights should fall back to raw weights.
        // Just verify we can reserve both without panicking.
        let resp = s.handle_command(c, Command::Reserve, None);
        assert!(matches!(resp, Response::Reserved { .. }));
    }

    #[test]
    fn test_watch_and_ignore() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(
            c,
            Command::Watch {
                tube: "foo".into(),
                weight: 1,
            },
            None,
        );
        assert!(matches!(resp, Response::Watching(2)));

        let resp = s.handle_command(
            c,
            Command::Ignore {
                tube: "default".into(),
            },
            None,
        );
        assert!(matches!(resp, Response::Watching(1)));

        // Can't ignore last tube
        let resp = s.handle_command(c, Command::Ignore { tube: "foo".into() }, None);
        assert!(matches!(resp, Response::NotIgnored));
    }

    #[test]
    fn test_kick_job_buried() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
        s.handle_command(c, Command::Reserve, None);
        s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

        let resp = s.handle_command(c, Command::KickJob { id: 1 }, None);
        assert!(matches!(resp, Response::KickedOne));
    }

    #[test]
    fn test_pause_tube() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(
            c,
            Command::PauseTube {
                tube: "default".into(),
                delay: 60,
            },
            None,
        );
        assert!(matches!(resp, Response::Paused));

        assert!(s.tubes.get("default").unwrap().is_paused());
    }

    #[test]
    fn test_stats_job() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(100, 0, 60, 5), Some(b"hello".to_vec()));

        let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
        match resp {
            Response::Ok(data) => {
                let yaml = String::from_utf8(data).unwrap();
                assert!(yaml.contains("id: 1"));
                assert!(yaml.contains("state: ready"));
                assert!(yaml.contains("pri: 100"));
            }
            _ => panic!("expected Ok"),
        }
    }

    #[test]
    fn test_stats_job_with_extensions() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: Some("grp1".into()),
            after_group: Some("grp0".into()),
            concurrency_key: Some(("con1".into(), 1)),
        };
        s.handle_command(c, cmd, Some(b"hello".to_vec()));

        let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
        match resp {
            Response::Ok(data) => {
                let yaml = String::from_utf8(data).unwrap();
                assert!(yaml.contains("idempotency-key: mykey"), "yaml: {}", yaml);
                assert!(yaml.contains("group: grp1"), "yaml: {}", yaml);
                assert!(yaml.contains("after-group: grp0"), "yaml: {}", yaml);
                assert!(yaml.contains("concurrency-key: con1"), "yaml: {}", yaml);
                assert!(yaml.contains("concurrency-limit: 1"), "yaml: {}", yaml);
            }
            _ => panic!("expected Ok"),
        }
    }

    #[test]
    fn test_list_tubes() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(
            c,
            Command::Use {
                tube: "emails".into(),
            },
            None,
        );

        let resp = s.handle_command(c, Command::ListTubes, None);
        match resp {
            Response::Ok(data) => {
                let yaml = String::from_utf8(data).unwrap();
                assert!(yaml.contains("- default"));
                assert!(yaml.contains("- emails"));
            }
            _ => panic!("expected Ok"),
        }
    }

    #[test]
    fn test_concurrency_key_limit_n() {
        // Put 3 jobs with con:api:2, reserve 2 (succeed), 3rd blocked, delete 1, 3rd succeeds
        let mut s = make_state();
        let c1 = register(&mut s);
        let c2 = register(&mut s);

        let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
            let cmd = Command::Put {
                pri: 0,
                delay: 0,
                ttr: 120,
                bytes: body.len() as u32,
                idempotency_key: None,
                group: None,
                after_group: None,
                concurrency_key: Some(("api".into(), 2)),
            };
            s.handle_command(conn, cmd, Some(body.to_vec()))
        };

        // Insert 3 jobs
        assert!(matches!(con_put(&mut s, c1, b"j1"), Response::Inserted(1)));
        assert!(matches!(con_put(&mut s, c1, b"j2"), Response::Inserted(2)));
        assert!(matches!(con_put(&mut s, c1, b"j3"), Response::Inserted(3)));

        // Reserve first two — should succeed
        let r1 = s.handle_command(c1, Command::Reserve, None);
        assert!(matches!(r1, Response::Reserved { id: 1, .. }));

        let r2 = s.handle_command(c2, Command::Reserve, None);
        assert!(matches!(r2, Response::Reserved { id: 2, .. }));

        // Third reserve should be blocked (2 already reserved, limit is 2)
        let r3 = s.handle_command(c1, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(matches!(r3, Response::TimedOut));

        // Delete one reserved job — frees a slot
        let resp = s.handle_command(c1, Command::Delete { id: 1 }, None);
        assert!(matches!(resp, Response::Deleted));

        // Now reserve should succeed
        let r4 = s.handle_command(c1, Command::Reserve, None);
        assert!(matches!(r4, Response::Reserved { id: 3, .. }));
    }

    #[test]
    fn test_concurrency_key_default_limit_one() {
        // con:key (no :N) defaults to limit 1
        let mut s = make_state();
        let c1 = register(&mut s);
        let c2 = register(&mut s);

        let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
            let cmd = Command::Put {
                pri: 0,
                delay: 0,
                ttr: 120,
                bytes: body.len() as u32,
                idempotency_key: None,
                group: None,
                after_group: None,
                concurrency_key: Some(("single".into(), 1)),
            };
            s.handle_command(conn, cmd, Some(body.to_vec()))
        };

        con_put(&mut s, c1, b"j1");
        con_put(&mut s, c1, b"j2");

        // Reserve first — succeeds
        let r1 = s.handle_command(c1, Command::Reserve, None);
        assert!(matches!(r1, Response::Reserved { id: 1, .. }));

        // Second reserve should be blocked
        let r2 = s.handle_command(c2, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(matches!(r2, Response::TimedOut));
    }

    #[test]
    fn test_idempotency_ttl_cooldown() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put with idp:key:5
        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 5)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        // Reserve and delete
        let resp = s.handle_command(c, Command::Reserve, None);
        assert!(matches!(resp, Response::Reserved { id: 1, .. }));
        let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
        assert!(matches!(resp, Response::Deleted));

        // Re-put with same key should return original ID with DELETED state (cooldown active)
        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 5)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "DELETED", None)));
    }

    #[test]
    fn test_idempotency_no_ttl_no_cooldown() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put with idp:key (no TTL)
        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        // Reserve and delete
        let resp = s.handle_command(c, Command::Reserve, None);
        assert!(matches!(resp, Response::Reserved { id: 1, .. }));
        let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
        assert!(matches!(resp, Response::Deleted));

        // Re-put should get new ID (no cooldown)
        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::Inserted(2)));
    }

    #[test]
    fn test_idempotency_ttl_flush_clears_cooldowns() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put with cooldown
        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 60)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        // Delete to start cooldown, then flush
        let resp = s.handle_command(c, Command::Reserve, None);
        assert!(matches!(resp, Response::Reserved { id: 1, .. }));
        let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
        assert!(matches!(resp, Response::Deleted));

        // Flush tube clears cooldowns
        let resp = s.handle_command(
            c,
            Command::FlushTube {
                tube: "default".into(),
            },
            None,
        );
        assert!(matches!(resp, Response::Flushed(0)));

        // Re-put should get new ID
        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 60)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::Inserted(2)));
    }

    #[test]
    fn test_idempotency_dedup_returns_ready_state() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        // Duplicate put returns InsertedDup with READY state
        let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "READY", None)));
    }

    #[test]
    fn test_idempotency_dedup_returns_reserved_state() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 60,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

        // Reserve the job
        s.handle_command(c, Command::Reserve, None);

        // Duplicate put returns InsertedDup with RESERVED state
        let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "RESERVED", None)));
    }

    #[test]
    fn test_idempotency_dedup_returns_buried_state() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 60,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

        // Reserve and bury
        s.handle_command(c, Command::Reserve, None);
        s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

        // Duplicate put returns InsertedDup with BURIED state
        let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "BURIED", None)));
    }

    #[test]
    fn test_idempotency_dedup_returns_delayed_state() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 3600,
            ttr: 60,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

        // Duplicate put returns InsertedDup with DELAYED state
        let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "DELAYED", None)));
    }

    #[test]
    fn test_idempotency_cooldown_dedup_returns_deleted_state() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 60)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd, Some(b"hello".to_vec()));

        // Reserve and delete (starts cooldown)
        s.handle_command(c, Command::Reserve, None);
        s.handle_command(c, Command::Delete { id: 1 }, None);

        // Re-put during cooldown returns InsertedDup with DELETED state
        let cmd2 = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 60)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "DELETED", None)));
    }

    #[test]
    fn test_idempotency_priority_upgrade_ready() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put job at pri 100
        let cmd1 = Command::Put {
            pri: 100,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        // Put another job at pri 75 (no IDP) to compare ordering
        let cmd2 = Command::Put {
            pri: 75,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd2, Some(b"other".to_vec()));
        assert!(matches!(resp, Response::Inserted(2)));

        // Duplicate put at pri 50 — should upgrade
        let cmd3 = Command::Put {
            pri: 50,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd3, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "READY", Some(50))));

        // Verify job 1 (now pri 50) is reserved before job 2 (pri 75)
        let reserve = Command::ReserveWithTimeout { timeout: 0 };
        let resp = s.handle_command(c, reserve, None);
        match resp {
            Response::Reserved { id, .. } => assert_eq!(id, 1),
            other => panic!("expected Reserved, got {:?}", other),
        }
    }

    #[test]
    fn test_idempotency_priority_upgrade_no_downgrade() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put job at pri 50
        let cmd1 = Command::Put {
            pri: 50,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        // Duplicate put at pri 100 — should NOT downgrade
        let cmd2 = Command::Put {
            pri: 100,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "READY", None)));

        // Verify job still has pri 50
        assert_eq!(s.jobs.get(&1).unwrap().priority, 50);
    }

    #[test]
    fn test_idempotency_priority_upgrade_reserved() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put and reserve a job at pri 100
        let cmd1 = Command::Put {
            pri: 100,
            delay: 0,
            ttr: 60,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd1, Some(b"hello".to_vec()));
        let reserve = Command::ReserveWithTimeout { timeout: 0 };
        s.handle_command(c, reserve, None);

        // Duplicate put at pri 30 — should upgrade even though reserved
        let cmd2 = Command::Put {
            pri: 30,
            delay: 0,
            ttr: 60,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "RESERVED", Some(30))));

        // Verify the job's priority was updated
        assert_eq!(s.jobs.get(&1).unwrap().priority, 30);
    }

    #[test]
    fn test_idempotency_priority_upgrade_buried() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put, reserve, and bury a job at pri 100
        let cmd1 = Command::Put {
            pri: 100,
            delay: 0,
            ttr: 60,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd1, Some(b"hello".to_vec()));
        let reserve = Command::ReserveWithTimeout { timeout: 0 };
        s.handle_command(c, reserve, None);
        let bury = Command::Bury { id: 1, pri: 100 };
        s.handle_command(c, bury, None);

        // Duplicate put at pri 20 — should upgrade buried job
        let cmd2 = Command::Put {
            pri: 20,
            delay: 0,
            ttr: 60,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "BURIED", Some(20))));
        assert_eq!(s.jobs.get(&1).unwrap().priority, 20);
    }

    #[test]
    fn test_idempotency_priority_upgrade_delayed() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put a delayed job at pri 100
        let cmd1 = Command::Put {
            pri: 100,
            delay: 3600,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
        assert!(matches!(resp, Response::Inserted(1)));

        // Duplicate put at pri 40 — should upgrade delayed job
        let cmd2 = Command::Put {
            pri: 40,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "DELAYED", Some(40))));
        assert_eq!(s.jobs.get(&1).unwrap().priority, 40);
    }

    #[test]
    fn test_idempotency_priority_upgrade_urgent_stats() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put job at pri 2000 (not urgent)
        let cmd1 = Command::Put {
            pri: 2000,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd1, Some(b"hello".to_vec()));
        assert_eq!(s.stats.urgent_ct, 0);

        // Duplicate put at pri 500 (urgent, < 1024) — should update urgent count
        let cmd2 = Command::Put {
            pri: 500,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 0)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
        assert!(matches!(resp, Response::InsertedDup(1, "READY", Some(500))));
        assert_eq!(s.stats.urgent_ct, 1);
    }

    #[test]
    fn test_stats_group_not_found() {
        let s = make_state();
        let resp = s.cmd_stats_group("nonexistent");
        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn test_stats_group_pending() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: Some("g1".into()),
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd, Some(b"hello".to_vec()));

        let resp = s.cmd_stats_group("g1");
        match resp {
            Response::Ok(data) => {
                let yaml = String::from_utf8(data).unwrap();
                assert!(yaml.contains("name: \"g1\""), "yaml: {}", yaml);
                assert!(yaml.contains("pending: 1"), "yaml: {}", yaml);
                assert!(yaml.contains("buried: 0"), "yaml: {}", yaml);
                assert!(yaml.contains("complete: false"), "yaml: {}", yaml);
                assert!(yaml.contains("waiting-jobs: 0"), "yaml: {}", yaml);
            }
            _ => panic!("expected Ok, got {:?}", resp),
        }
    }

    #[test]
    fn test_chained_aft_pipeline() {
        // Verify the ETL pipeline scenario: extract -> transform -> load
        // where each stage is held until the previous stage's group completes.
        let mut s = make_state();
        let c = register(&mut s);

        // Step 1: Put 2 extract jobs tagged with grp:extract
        let resp = s.handle_command(
            c,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: None,
                group: Some("extract".into()),
                after_group: None,
                concurrency_key: None,
            },
            Some(b"ext-1".to_vec()),
        );
        assert!(matches!(resp, Response::Inserted(1)));

        let resp = s.handle_command(
            c,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: None,
                group: Some("extract".into()),
                after_group: None,
                concurrency_key: None,
            },
            Some(b"ext-2".to_vec()),
        );
        assert!(matches!(resp, Response::Inserted(2)));

        // Step 2: Put 1 transform job with aft:extract grp:transform
        // It should be held because the extract group is not yet complete.
        let resp = s.handle_command(
            c,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: None,
                group: Some("transform".into()),
                after_group: Some("extract".into()),
                concurrency_key: None,
            },
            Some(b"xform".to_vec()),
        );
        assert!(matches!(resp, Response::Inserted(3)));

        // Step 3: Put 1 load job with aft:transform
        // It should be held because the transform group is not yet complete.
        let resp = s.handle_command(
            c,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: None,
                group: None,
                after_group: Some("transform".into()),
                concurrency_key: None,
            },
            Some(b"load!".to_vec()),
        );
        assert!(matches!(resp, Response::Inserted(4)));

        // Step 4: Verify the transform job (3) and load job (4) are not reservable
        // while the extract group still has pending jobs.
        // Only extract jobs 1 and 2 should be available.
        let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(
            matches!(r, Response::Reserved { id: 1, .. }),
            "expected extract job 1, got {:?}",
            r
        );
        let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(
            matches!(r, Response::Reserved { id: 2, .. }),
            "expected extract job 2, got {:?}",
            r
        );
        // No more ready jobs -- transform is still held
        let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(
            matches!(r, Response::TimedOut),
            "expected TimedOut (transform held), got {:?}",
            r
        );

        // Step 5: Delete both extract jobs. This completes the extract group,
        // which should promote the transform job to ready.
        s.handle_command(c, Command::Delete { id: 1 }, None);
        s.handle_command(c, Command::Delete { id: 2 }, None);

        // Step 6: Verify the transform job is now reservable.
        let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(
            matches!(r, Response::Reserved { id: 3, .. }),
            "expected transform job 3 after extract completes, got {:?}",
            r
        );
        // Load job is still held -- transform group not yet complete
        let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(
            matches!(r, Response::TimedOut),
            "expected TimedOut (load held), got {:?}",
            r
        );

        // Step 7: Delete the transform job. This completes the transform group,
        // which should promote the load job to ready.
        s.handle_command(c, Command::Delete { id: 3 }, None);

        // Step 8: Verify the load job is now reservable.
        let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
        assert!(
            matches!(r, Response::Reserved { id: 4, .. }),
            "expected load job 4 after transform completes, got {:?}",
            r
        );
    }

    #[test]
    fn test_stats_group_buried() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 60,
            bytes: 5,
            idempotency_key: None,
            group: Some("g1".into()),
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd, Some(b"hello".to_vec()));

        // Reserve and bury
        s.handle_command(c, Command::Reserve, None);
        s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

        let resp = s.cmd_stats_group("g1");
        match resp {
            Response::Ok(data) => {
                let yaml = String::from_utf8(data).unwrap();
                assert!(yaml.contains("pending: 1"), "yaml: {}", yaml);
                assert!(yaml.contains("buried: 1"), "yaml: {}", yaml);
                assert!(yaml.contains("complete: false"), "yaml: {}", yaml);
            }
            _ => panic!("expected Ok, got {:?}", resp),
        }
    }

    #[test]
    fn test_stats_job_idempotency_ttl() {
        let mut s = make_state();
        let c = register(&mut s);

        let cmd = Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: Some(("mykey".into(), 30)),
            group: None,
            after_group: None,
            concurrency_key: None,
        };
        s.handle_command(c, cmd, Some(b"hello".to_vec()));

        let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
        match resp {
            Response::Ok(data) => {
                let yaml = String::from_utf8(data).unwrap();
                assert!(yaml.contains("idempotency-key: mykey"), "yaml: {}", yaml);
                assert!(yaml.contains("idempotency-ttl: 30"), "yaml: {}", yaml);
            }
            _ => panic!("expected Ok"),
        }
    }

    #[test]
    fn test_delete_batch_all_reserved() {
        let mut s = make_state();
        let c = register(&mut s);

        // Put and reserve 3 jobs
        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"b".to_vec()));
        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"c".to_vec()));
        s.handle_command(c, Command::Reserve, None);
        s.handle_command(c, Command::Reserve, None);
        s.handle_command(c, Command::Reserve, None);

        let resp = s.handle_command(c, Command::DeleteBatch { ids: vec![1, 2, 3] }, None);
        assert_eq!(
            resp,
            Response::DeletedBatch {
                deleted: 3,
                not_found: 0
            }
        );
    }

    #[test]
    fn test_delete_batch_mixed() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
        s.handle_command(c, Command::Reserve, None);

        let resp = s.handle_command(
            c,
            Command::DeleteBatch {
                ids: vec![1, 999, 1000],
            },
            None,
        );
        assert_eq!(
            resp,
            Response::DeletedBatch {
                deleted: 1,
                not_found: 2
            }
        );
    }

    #[test]
    fn test_delete_batch_all_not_found() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(c, Command::DeleteBatch { ids: vec![99, 100] }, None);
        assert_eq!(
            resp,
            Response::DeletedBatch {
                deleted: 0,
                not_found: 2
            }
        );
    }

    #[test]
    fn test_delete_batch_other_conn_reserved() {
        let mut s = make_state();
        let c1 = register(&mut s);
        let c2 = register(&mut s);

        // c1 reserves a job
        s.handle_command(c1, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
        s.handle_command(c1, Command::Reserve, None);

        // c2 tries to delete-batch it — should be not_found (not reserved by c2)
        let resp = s.handle_command(c2, Command::DeleteBatch { ids: vec![1] }, None);
        assert_eq!(
            resp,
            Response::DeletedBatch {
                deleted: 0,
                not_found: 1
            }
        );
    }
}
