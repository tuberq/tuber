use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{ConnState, ReserveMode, WatchedTube};
use crate::job::{Job, JobState, URGENT_THRESHOLD};
use crate::protocol::{self, Command, Response};
use crate::tube::Tube;
use crate::wal::Wal;

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
    op_ct: [u64; 27],
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
    #[allow(dead_code)]
    next_conn_id: u64,
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
    /// Cached system info from uname.
    hostname: String,
    os: String,
    platform: String,
    /// Job group tracking for grp:/aft: features.
    groups: HashMap<String, GroupState>,
    /// Active reservation count per concurrency key.
    concurrency_keys: HashMap<String, u32>,
}

impl ServerState {
    fn new(max_job_size: u32) -> Self {
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
            next_conn_id: 1,
            max_job_size,
            drain_mode: false,
            ready_ct: 0,
            started_at: Instant::now(),
            rng_state: 0,
            stats: GlobalStats::default(),
            waiters: Vec::new(),
            wal: None,
            instance_id,
            hostname,
            os,
            platform,
            groups: HashMap::new(),
            concurrency_keys: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn register_conn(&mut self) -> u64 {
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        self.conns.insert(id, ConnState::new(id));
        self.stats.total_connections += 1;
        // Ensure default tube exists and bump watching_ct
        self.ensure_tube("default");
        if let Some(t) = self.tubes.get_mut("default") {
            t.watching_ct += 1;
            t.using_ct += 1;
        }
        id
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
            .and_then(|key| self.concurrency_keys.get(key))
            .map(|&count| count > 0)
            .unwrap_or(false)
    }

    /// Increment the concurrency counter for a job's key.
    fn acquire_concurrency_key(&mut self, job_id: u64) {
        if let Some(key) = self
            .jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.clone())
        {
            *self.concurrency_keys.entry(key).or_insert(0) += 1;
        }
    }

    /// Decrement and clean up the concurrency counter for a job's key.
    fn release_concurrency_key(&mut self, job_id: u64) {
        if let Some(key) = self
            .jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.clone())
        {
            if let Some(count) = self.concurrency_keys.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.concurrency_keys.remove(&key);
                }
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
        if !self.conns.contains_key(&conn_id) {
            self.conns.insert(conn_id, ConnState::new(conn_id));
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
            } => self.cmd_put(
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
            ),
            Command::Use { tube } => self.cmd_use(conn_id, &tube),
            Command::Reserve => self.cmd_reserve(conn_id, None),
            Command::ReserveWithTimeout { timeout } => self.cmd_reserve(conn_id, Some(timeout)),
            Command::ReserveJob { id } => self.cmd_reserve_job(conn_id, id),
            Command::ReserveMode { mode } => self.cmd_reserve_mode(conn_id, &mode),
            Command::Delete { id } => self.cmd_delete(conn_id, id),
            Command::Release { id, pri, delay } => self.cmd_release(conn_id, id, pri, delay),
            Command::Bury { id, pri } => self.cmd_bury(conn_id, id, pri),
            Command::Touch { id } => self.cmd_touch(conn_id, id),
            Command::Watch { tube, weight } => self.cmd_watch(conn_id, &tube, weight),
            Command::Ignore { tube } => self.cmd_ignore(conn_id, &tube),
            Command::Peek { id } => self.cmd_peek(id),
            Command::PeekReady => self.cmd_peek_ready(conn_id),
            Command::PeekDelayed => self.cmd_peek_delayed(conn_id),
            Command::PeekBuried => self.cmd_peek_buried(conn_id),
            Command::Kick { bound } => self.cmd_kick(conn_id, bound),
            Command::KickJob { id } => self.cmd_kick_job(id),
            Command::StatsJob { id } => self.cmd_stats_job(id),
            Command::StatsTube { tube } => self.cmd_stats_tube(&tube),
            Command::Stats => self.cmd_stats(),
            Command::ListTubes => self.cmd_list_tubes(),
            Command::ListTubeUsed => self.cmd_list_tube_used(conn_id),
            Command::ListTubesWatched => self.cmd_list_tubes_watched(conn_id),
            Command::PauseTube { tube, delay } => self.cmd_pause_tube(&tube, delay),
            Command::FlushTube { tube } => self.cmd_flush_tube(&tube),
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
        idempotency_key: Option<String>,
        group: Option<String>,
        after_group: Option<String>,
        concurrency_key: Option<String>,
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

        // Idempotency dedup: if key already exists for a live job, return original ID
        if let Some(ref key) = idempotency_key
            && let Some(tube) = self.tubes.get(&tube_name)
            && let Some(&existing_id) = tube.idempotency_keys.get(key)
            && self.jobs.contains_key(&existing_id)
        {
            return Response::Inserted(existing_id);
        }

        // WAL: check space reservation
        if let Some(wal) = &self.wal {
            // Estimate record size for reservation check
            let est_size = crate::wal::estimate_full_job_size_raw(
                &tube_name,
                body.len(),
                &idempotency_key,
                &group,
                &after_group,
                &concurrency_key,
            );
            if !wal.reserve_put(est_size) {
                return Response::OutOfMemory;
            }
        }

        let id = self.next_job_id;
        self.next_job_id += 1;

        let job = Job::new(
            id,
            pri,
            Duration::from_secs(delay as u64),
            Duration::from_secs(ttr as u64),
            body,
            tube_name.clone(),
        );

        self.jobs.insert(id, job);

        // Set extension fields
        if let Some(job) = self.jobs.get_mut(&id) {
            job.idempotency_key = idempotency_key;
            job.group = group;
            job.after_group = after_group;
            job.concurrency_key = concurrency_key;
        }

        // Register idempotency key in tube index
        if let Some(job) = self.jobs.get(&id)
            && let Some(ref key) = job.idempotency_key
            && let Some(tube) = self.tubes.get_mut(&tube_name)
        {
            tube.idempotency_keys.insert(key.clone(), id);
        }

        // Track group membership
        let group_name = self.jobs.get(&id).and_then(|j| j.group.clone());
        if let Some(ref grp) = group_name {
            let gs = self.groups.entry(grp.clone()).or_insert_with(GroupState::new);
            gs.pending += 1;
        }

        // Check if this is an after-group job that should be held
        let after_group_name = self.jobs.get(&id).and_then(|j| j.after_group.clone());
        let hold_for_group = if let Some(ref ag) = after_group_name {
            let gs = self.groups.entry(ag.clone()).or_insert_with(GroupState::new);
            !gs.is_complete()
        } else {
            false
        };

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
        } else if hold_for_group {
            // Hold this after-job: mark as delayed with no deadline (held indefinitely)
            if let Some(job) = self.jobs.get_mut(&id) {
                job.state = JobState::Delayed;
                job.deadline_at = None;
            }
            // Add to group's waiting list (will be promoted when group completes)
            if let Some(ref ag) = after_group_name {
                if let Some(gs) = self.groups.get_mut(ag) {
                    gs.waiting_jobs.push(id);
                }
            }
        } else {
            if let Some(job) = self.jobs.get(&id) {
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

        self.stats.total_jobs_ct += 1;
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.total_jobs_ct += 1;
        }

        // WAL: write put record
        self.wal_write_put(id);

        self.process_queue();

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

    fn cmd_reserve(&mut self, conn_id: u64, timeout: Option<u32>) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_worker();
        }

        // Check deadline_soon
        if self.conn_deadline_soon(conn_id) && !self.conn_has_ready_job(conn_id) {
            return Response::DeadlineSoon;
        }

        // Weighted mode: pick a tube by weight
        let conn = self.conns.get(&conn_id);
        let reserve_mode = conn.map(|c| c.reserve_mode).unwrap_or(ReserveMode::Fifo);

        if reserve_mode == ReserveMode::Weighted {
            if let Some(job_id) = self.select_weighted_job(conn_id) {
                return self.do_reserve(conn_id, job_id);
            }
        }

        // FIFO mode: try to find a ready job from watched tubes
        if let Some(job_id) = self.find_ready_job_for_conn(conn_id) {
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
                if let Some(j) = self.jobs.get(&id) {
                    if j.priority < URGENT_THRESHOLD {
                        self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
                        if let Some(tube) = self.tubes.get_mut(&tube_name) {
                            tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
                        }
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
        let job = match self.jobs.get(&job_id) {
            Some(j) => j,
            None => return Response::NotFound,
        };
        let tube_name = job.tube_name.clone();
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.ready.remove_by_id(job_id);
        }
        self.ready_ct = self.ready_ct.saturating_sub(1);
        if let Some(j) = self.jobs.get(&job_id) {
            if j.priority < URGENT_THRESHOLD {
                self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
                }
            }
        }

        self.do_reserve_inner(conn_id, job_id)
    }

    fn do_reserve_inner(&mut self, conn_id: u64, job_id: u64) -> Response {
        let ttr = self
            .jobs
            .get(&job_id)
            .map(|j| j.ttr)
            .unwrap_or(Duration::from_secs(1));
        let body = self
            .jobs
            .get(&job_id)
            .map(|j| j.body.clone())
            .unwrap_or_default();

        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.state = JobState::Reserved;
            job.reserver_id = Some(conn_id);
            job.reserved_at = Some(Instant::now());
            job.deadline_at = Some(Instant::now() + ttr);
            job.reserve_ct += 1;
        }
        self.acquire_concurrency_key(job_id);
        if let Some(job) = self.jobs.get(&job_id) {
            if let Some(tube) = self.tubes.get_mut(&job.tube_name) {
                tube.stat.reserved_ct += 1;
                tube.stat.total_reserve_ct += 1;
            }
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
            _ => Response::BadFormat,
        }
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

        match state {
            JobState::Reserved => {
                // Must be reserved by this connection
                if job.reserver_id != Some(conn_id) {
                    return Response::NotFound;
                }
                self.release_concurrency_key(id);
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    conn.reserved_jobs.retain(|&jid| jid != id);
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

            if state == JobState::Reserved {
                if let Some(ra) = reserved_at {
                    let secs = Instant::now().duration_since(ra).as_secs_f64();
                    tube.stat.processing_time_samples += 1;
                    if tube.stat.processing_time_samples == 1 {
                        tube.stat.processing_time_ewma = secs;
                    } else {
                        const ALPHA: f64 = 0.1;
                        tube.stat.processing_time_ewma =
                            ALPHA * secs + (1.0 - ALPHA) * tube.stat.processing_time_ewma;
                    }
                    tube.stat.processing_time_min =
                        Some(tube.stat.processing_time_min.map_or(secs, |m| m.min(secs)));
                    tube.stat.processing_time_max =
                        Some(tube.stat.processing_time_max.map_or(secs, |m| m.max(secs)));
                }
            }
        }
        self.stats.total_delete_ct += 1;

        // Remove idempotency key from tube index
        if let Some(job) = self.jobs.get(&id)
            && let Some(ref key) = job.idempotency_key
            && let Some(tube) = self.tubes.get_mut(&tube_name)
        {
            tube.idempotency_keys.remove(key);
        }

        // Group tracking: decrement pending count and check completion
        let group_name = self.jobs.get(&id).and_then(|j| j.group.clone());
        if let Some(ref grp) = group_name {
            if let Some(gs) = self.groups.get_mut(grp) {
                gs.pending = gs.pending.saturating_sub(1);
                if state == JobState::Buried {
                    gs.buried = gs.buried.saturating_sub(1);
                }
            }
        }

        // WAL: write delete state change
        self.wal_write_state_change(id, None, 0, Duration::ZERO);

        self.jobs.remove(&id);

        // Check if any group completed and promote waiting after-jobs
        if let Some(ref grp) = group_name {
            self.check_group_completion(grp);
        }

        Response::Deleted
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

        // Remove reserved jobs from owning connections
        for &id in &reserved_ids {
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
                if let Some(ref grp) = job.group {
                    if let Some(gs) = self.groups.get_mut(grp) {
                        gs.pending = gs.pending.saturating_sub(1);
                        if job.state == JobState::Buried {
                            gs.buried = gs.buried.saturating_sub(1);
                        }
                        if !affected_groups.contains(grp) {
                            affected_groups.push(grp.clone());
                        }
                    }
                }
                // Also remove held after-jobs from group waiting lists
                if let Some(ref ag) = job.after_group {
                    if let Some(gs) = self.groups.get_mut(ag) {
                        gs.waiting_jobs.retain(|&jid| jid != id);
                    }
                }
            }
        }

        // WAL: write delete for each job, then remove from jobs map
        for &id in &job_ids {
            self.wal_write_state_change(id, None, 0, Duration::ZERO);
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
        self.wal_write_state_change(id, Some(wal_state), pri, Duration::from_secs(delay as u64));

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
        if let Some(job) = self.jobs.get(&id) {
            if let Some(ref grp) = job.group {
                if let Some(gs) = self.groups.get_mut(grp) {
                    gs.buried += 1;
                }
            }
        }

        // WAL: write bury state change
        self.wal_write_state_change(id, Some(JobState::Buried), pri, Duration::ZERO);

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
            if was_watching {
                if let Some(t) = self.tubes.get_mut(tube) {
                    t.watching_ct = t.watching_ct.saturating_sub(1);
                }
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
        if let Some(tube) = self.tubes.get(tube_name) {
            if let Some(&(_, job_id)) = tube.ready.peek() {
                if let Some(job) = self.jobs.get(&job_id) {
                    return Response::Found {
                        id: job_id,
                        body: job.body.clone(),
                    };
                }
            }
        }
        Response::NotFound
    }

    fn cmd_peek_delayed(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name) {
            if let Some(&(_, job_id)) = tube.delay.peek() {
                if let Some(job) = self.jobs.get(&job_id) {
                    return Response::Found {
                        id: job_id,
                        body: job.body.clone(),
                    };
                }
            }
        }
        Response::NotFound
    }

    fn cmd_peek_buried(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name) {
            if let Some(&job_id) = tube.buried.front() {
                if let Some(job) = self.jobs.get(&job_id) {
                    return Response::Found {
                        id: job_id,
                        body: job.body.clone(),
                    };
                }
            }
        }
        Response::NotFound
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
                if let Some(job) = self.jobs.get(&job_id) {
                    if let Some(ref grp) = job.group {
                        if let Some(gs) = self.groups.get_mut(grp) {
                            gs.buried = gs.buried.saturating_sub(1);
                        }
                    }
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
                self.wal_write_state_change(job_id, Some(JobState::Ready), 0, Duration::ZERO);
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
                self.wal_write_state_change(job_id, Some(JobState::Ready), 0, Duration::ZERO);
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
                if let Some(job) = self.jobs.get(&id) {
                    if let Some(ref grp) = job.group {
                        if let Some(gs) = self.groups.get_mut(grp) {
                            gs.buried = gs.buried.saturating_sub(1);
                        }
                    }
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
        self.wal_write_state_change(id, Some(JobState::Ready), 0, Duration::ZERO);

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
             group: {}\n\
             after-group: {}\n\
             concurrency-key: {}\n",
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
            job.idempotency_key.as_deref().unwrap_or(""),
            job.group.as_deref().unwrap_or(""),
            job.after_group.as_deref().unwrap_or(""),
            job.concurrency_key.as_deref().unwrap_or(""),
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
             processing-time-ewma: {:.6}\n\
             processing-time-min: {:.6}\n\
             processing-time-max: {:.6}\n\
             processing-time-samples: {}\n",
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
            tube.stat.processing_time_ewma,
            tube.stat.processing_time_min.unwrap_or(0.0),
            tube.stat.processing_time_max.unwrap_or(0.0),
            tube.stat.processing_time_samples,
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats(&self) -> Response {
        let delayed_ct: usize = self.tubes.values().map(|t| t.delay.len()).sum();
        let uptime = Instant::now().duration_since(self.started_at).as_secs();

        // rusage stats
        let (rusage_utime, rusage_stime) = {
            let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
            let utime = format!("{}.{:06}", usage.ru_utime.tv_sec, usage.ru_utime.tv_usec);
            let stime = format!("{}.{:06}", usage.ru_stime.tv_sec, usage.ru_stime.tv_usec);
            (utime, stime)
        };

        // WAL stats
        let (binlog_oldest, binlog_current, binlog_max_size) = match &self.wal {
            Some(wal) => (wal.oldest_seq(), wal.current_seq(), wal.max_file_size()),
            None => (0, 0, 0),
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
             version: \"{}\"\n\
             rusage-utime: {}\n\
             rusage-stime: {}\n\
             uptime: {}\n\
             binlog-oldest-index: {}\n\
             binlog-current-index: {}\n\
             binlog-records-migrated: 0\n\
             binlog-records-written: 0\n\
             binlog-max-size: {}\n\
             current-concurrency-keys: {}\n\
             draining: {}\n\
             id: {}\n\
             hostname: {}\n\
             os: {}\n\
             platform: {}\n",
            self.stats.urgent_ct,
            self.ready_ct,
            self.stats.reserved_ct,
            delayed_ct,
            self.stats.buried_ct,
            self.stats.op_ct[1],  // OP_PUT
            self.stats.op_ct[2],  // OP_PEEKJOB
            self.stats.op_ct[18], // OP_PEEK_READY
            self.stats.op_ct[19], // OP_PEEK_DELAYED
            self.stats.op_ct[10], // OP_PEEK_BURIED
            self.stats.op_ct[3],  // OP_RESERVE
            self.stats.op_ct[20], // OP_RESERVE_TIMEOUT
            self.stats.op_ct[4],  // OP_DELETE
            self.stats.op_ct[5],  // OP_RELEASE
            self.stats.op_ct[11], // OP_USE
            self.stats.op_ct[12], // OP_WATCH
            self.stats.op_ct[13], // OP_IGNORE
            self.stats.op_ct[6],  // OP_BURY
            self.stats.op_ct[7],  // OP_KICK
            self.stats.op_ct[21], // OP_TOUCH
            self.stats.op_ct[8],  // OP_STATS
            self.stats.op_ct[9],  // OP_STATSJOB
            self.stats.op_ct[17], // OP_STATS_TUBE
            self.stats.op_ct[14], // OP_LIST_TUBES
            self.stats.op_ct[15], // OP_LIST_TUBE_USED
            self.stats.op_ct[16], // OP_LIST_TUBES_WATCHED
            self.stats.op_ct[23], // OP_PAUSE_TUBE
            self.stats.op_ct[26], // OP_RESERVE_MODE
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
            uptime,
            binlog_oldest,
            binlog_current,
            binlog_max_size,
            self.concurrency_keys.len(),
            if self.drain_mode { "true" } else { "false" },
            self.instance_id,
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

    fn conn_deadline_soon(&self, conn_id: u64) -> bool {
        let conn = match self.conns.get(&conn_id) {
            Some(c) => c,
            None => return false,
        };
        let now = Instant::now();
        let margin = Duration::from_secs(1);
        for &job_id in &conn.reserved_jobs {
            if let Some(job) = self.jobs.get(&job_id) {
                if let Some(deadline) = job.deadline_at {
                    if deadline <= now + margin {
                        return true;
                    }
                }
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
            if let Some(job) = self.jobs.get_mut(job_id) {
                if job.state == JobState::Delayed && job.deadline_at.is_none() {
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

        // First pass: collect indices of timed-out and fulfillable waiters
        let mut timed_out_indices = Vec::new();
        let mut fulfill_indices = Vec::new();

        for (i, waiter) in self.waiters.iter().enumerate() {
            if let Some(deadline) = waiter.deadline {
                if now >= deadline {
                    timed_out_indices.push(i);
                    continue;
                }
            }
            // Check if there's a ready job for this connection
            if self.find_ready_job_for_conn_inner(waiter.conn_id).is_some() {
                fulfill_indices.push(i);
            }
        }

        // Remove from back to front to preserve indices
        let mut all_remove: Vec<usize> = timed_out_indices
            .iter()
            .chain(fulfill_indices.iter())
            .cloned()
            .collect();
        all_remove.sort_unstable();
        all_remove.dedup();

        let mut removed: Vec<(WaitingReserve, bool)> = Vec::new(); // (waiter, should_fulfill)
        for &i in all_remove.iter().rev() {
            let waiter = self.waiters.remove(i);
            let should_fulfill = fulfill_indices.contains(&i);
            removed.push((waiter, should_fulfill));
        }
        removed.reverse();

        for (waiter, should_fulfill) in removed {
            if should_fulfill {
                if let Some(job_id) = self.find_ready_job_for_conn_inner(waiter.conn_id) {
                    let resp = self.do_reserve(waiter.conn_id, job_id);
                    let _ = waiter.reply_tx.send(resp);
                } else {
                    let _ = waiter.reply_tx.send(Response::TimedOut);
                }
            } else {
                let _ = waiter.reply_tx.send(Response::TimedOut);
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
    ) {
        if let Some(wal) = self.wal.as_mut() {
            if let Some(mut job) = self.jobs.remove(&job_id) {
                if let Err(e) = wal.write_state_change(&mut job, state, pri, delay) {
                    tracing::error!("WAL write_state_change error: {}, disabling WAL", e);
                    self.wal = None;
                }
                self.jobs.insert(job_id, job);
            }
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
                        if let Some(ag) = self.jobs.get(&job_id).and_then(|j| j.after_group.clone()) {
                            if let Some(gs) = self.groups.get_mut(&ag) {
                                gs.waiting_jobs.push(job_id);
                            }
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
            if let Some(unpause_at) = tube.unpause_at {
                if now >= unpause_at {
                    tube.pause = Duration::ZERO;
                    tube.unpause_at = None;
                }
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

        // Check waiting connection timeouts
        let now2 = Instant::now();
        // Collect indices of timed-out waiters
        let timed_out: Vec<usize> = self
            .waiters
            .iter()
            .enumerate()
            .filter(|(_, w)| w.deadline.map(|d| now2 >= d).unwrap_or(false))
            .map(|(i, _)| i)
            .collect();

        // Remove from back to front
        let mut expired = Vec::new();
        for &i in timed_out.iter().rev() {
            expired.push(self.waiters.remove(i));
        }

        // Now send responses (no longer borrowing self.waiters)
        for waiter in expired {
            if self.conn_deadline_soon(waiter.conn_id) {
                let _ = waiter.reply_tx.send(Response::DeadlineSoon);
            } else {
                let _ = waiter.reply_tx.send(Response::TimedOut);
            }
        }

        // Try to fulfill remaining waiters with newly ready jobs
        self.process_queue();

        // WAL maintenance (GC, sync)
        if let Some(wal) = self.wal.as_mut() {
            let migrate_ids = wal.maintain();
            for job_id in migrate_ids {
                if self.jobs.contains_key(&job_id) {
                    // Re-write the full job record for compaction
                    if let Some(wal) = self.wal.as_mut() {
                        if let Some(mut job) = self.jobs.remove(&job_id) {
                            if let Err(e) = wal.write_put(&mut job) {
                                tracing::error!("WAL compaction write error: {}, disabling WAL", e);
                                self.wal = None;
                                self.jobs.insert(job_id, job);
                                break;
                            }
                            self.jobs.insert(job_id, job);
                        }
                    }
                }
            }
        }
    }

    /// Restore jobs from WAL replay into server state.
    fn restore_jobs(&mut self, jobs: HashMap<u64, Job>, next_job_id: u64) {
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

            // Register idempotency key in tube index
            if let Some(ref key) = idempotency_key
                && let Some(tube) = self.tubes.get_mut(&tube_name)
            {
                tube.idempotency_keys.insert(key.clone(), id);
            }

            // Rebuild group state
            if let Some(ref grp) = group {
                let gs = self.groups.entry(grp.clone()).or_insert_with(GroupState::new);
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
                    if let Some(job) = self.jobs.get(&job_id) {
                        if job.state == JobState::Ready {
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
                    }
                    // Add to group waiting list
                    let gs = self.groups.entry(ag).or_insert_with(GroupState::new);
                    gs.waiting_jobs.push(job_id);
                }
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
) -> io::Result<()> {
    let listener = TcpListener::bind((addr, port)).await?;
    tracing::info!("listening on {}:{}", addr, port);

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
    run_with_listener(listener, max_job_size, wal_path).await
}

pub async fn run_with_listener(
    listener: TcpListener,
    max_job_size: u32,
    wal_dir: Option<&Path>,
) -> io::Result<()> {
    let (engine_tx, mut engine_rx) = mpsc::channel::<EngineMsg>(1024);
    let mut state = ServerState::new(max_job_size);

    // WAL: open and replay if configured
    if let Some(dir) = wal_dir {
        let mut wal = Wal::open(dir, None)?;
        let (jobs, next_id) = wal.replay()?;
        let job_count = jobs.len();
        state.restore_jobs(jobs, next_id);
        state.wal = Some(wal);
        tracing::info!("WAL: replayed {} jobs from {:?}", job_count, dir);
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
                            if is_reserve && matches!(resp, Response::TimedOut) {
                                if timeout != Some(0) {
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
                            }

                            let _ = reply_tx.send(resp);
                        }
                        EnginePayload::Disconnect => {
                            state.unregister_conn(msg.conn_id);
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
    });

    // Accept loop
    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::debug!("accepted connection from {}", peer);

        let tx = engine_tx.clone();

        tokio::spawn(async move {
            handle_connection(socket, tx, max_job_size).await;
        });
    }
}

/// Atomic counter for connection IDs (simpler than engine round-trip).
static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

async fn handle_connection(
    socket: tokio::net::TcpStream,
    engine_tx: mpsc::Sender<EngineMsg>,
    max_job_size: u32,
) {
    let conn_id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line_buf = String::new();

    loop {
        line_buf.clear();
        let _n = match reader.read_line(&mut line_buf).await {
            Ok(0) => break, // EOF
            Ok(n) => n,
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
                let _ = writer.write_all(&resp.serialize()).await;
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
            let mut body_buf = vec![0u8; body_size + 2]; // +2 for \r\n
            match reader.read_exact(&mut body_buf).await {
                Ok(_) => {
                    // Verify trailing \r\n
                    if body_buf[body_size] != b'\r' || body_buf[body_size + 1] != b'\n' {
                        let _ = writer.write_all(&Response::ExpectedCrlf.serialize()).await;
                        continue;
                    }
                    body_buf.truncate(body_size);

                    if body_size > max_job_size as usize {
                        let _ = writer.write_all(&Response::JobTooBig.serialize()).await;
                        continue;
                    }

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
                if writer.write_all(&resp.serialize()).await.is_err() {
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
        ServerState::new(65535)
    }

    fn register(state: &mut ServerState) -> u64 {
        state.register_conn()
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
            idempotency_key: Some("mykey".into()),
            group: Some("grp1".into()),
            after_group: Some("grp0".into()),
            concurrency_key: Some("con1".into()),
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
}
