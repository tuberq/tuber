use std::collections::HashMap;
use std::io;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{ConnState, ReserveMode, WatchedTube};
use crate::job::{Job, JobState, URGENT_THRESHOLD};
use crate::protocol::{self, Command, Response};
use crate::tube::Tube;

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

/// All server state, owned by the engine task.
struct ServerState {
    jobs: HashMap<u64, Job>,
    tubes: HashMap<String, Tube>,
    conns: HashMap<u64, ConnState>,
    next_job_id: u64,
    next_conn_id: u64,
    max_job_size: u32,
    drain_mode: bool,
    ready_ct: u64,
    started_at: Instant,
    stats: GlobalStats,
    /// Connections waiting for a job via reserve.
    waiters: Vec<WaitingReserve>,
}

impl ServerState {
    fn new(max_job_size: u32) -> Self {
        let mut tubes = HashMap::new();
        tubes.insert("default".to_string(), Tube::new("default"));

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
            stats: GlobalStats::default(),
            waiters: Vec::new(),
        }
    }

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

    fn handle_command(
        &mut self,
        conn_id: u64,
        cmd: Command,
        body: Option<Vec<u8>>,
    ) -> Response {
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
            Command::Put { pri, delay, ttr, bytes } => {
                self.cmd_put(conn_id, pri, delay, ttr, bytes, body)
            }
            Command::Use { tube } => self.cmd_use(conn_id, &tube),
            Command::Reserve => self.cmd_reserve(conn_id, None),
            Command::ReserveWithTimeout { timeout } => {
                self.cmd_reserve(conn_id, Some(timeout))
            }
            Command::ReserveJob { id } => self.cmd_reserve_job(conn_id, id),
            Command::ReserveMode { mode } => self.cmd_reserve_mode(conn_id, &mode),
            Command::Delete { id } => self.cmd_delete(conn_id, id),
            Command::Release { id, pri, delay } => {
                self.cmd_release(conn_id, id, pri, delay)
            }
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
            Command::Quit => Response::Deleted, // handled at connection level
        }
    }

    // --- Command implementations ---

    fn cmd_put(
        &mut self,
        conn_id: u64,
        pri: u32,
        delay: u32,
        ttr: u32,
        _bytes: u32,
        body: Option<Vec<u8>>,
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

        let tube_name = self.conns.get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());

        self.ensure_tube(&tube_name);

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
        let ttr = self.jobs.get(&job_id).map(|j| j.ttr).unwrap_or(Duration::from_secs(1));
        let body = self.jobs.get(&job_id).map(|j| j.body.clone()).unwrap_or_default();

        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.state = JobState::Reserved;
            job.reserver_id = Some(conn_id);
            job.deadline_at = Some(Instant::now() + ttr);
            job.reserve_ct += 1;
        }
        if let Some(job) = self.jobs.get(&job_id) {
            if let Some(tube) = self.tubes.get_mut(&job.tube_name) {
                tube.stat.reserved_ct += 1;
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

        match state {
            JobState::Reserved => {
                // Must be reserved by this connection
                if job.reserver_id != Some(conn_id) {
                    return Response::NotFound;
                }
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
        }
        self.stats.total_delete_ct += 1;
        self.jobs.remove(&id);

        Response::Deleted
    }

    fn cmd_release(
        &mut self,
        conn_id: u64,
        id: u64,
        pri: u32,
        delay: u32,
    ) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state != JobState::Reserved || job.reserver_id != Some(conn_id) {
            return Response::NotFound;
        }

        let tube_name = job.tube_name.clone();

        // Remove from reserved
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
            job.deadline_at = None;
            job.bury_ct += 1;
        }

        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.buried.push_back(id);
            tube.stat.buried_ct += 1;
        }
        self.stats.buried_ct += 1;

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
            Some(job) => Response::Found { id, body: job.body.clone() },
            None => Response::NotFound,
        }
    }

    fn cmd_peek_ready(&self, conn_id: u64) -> Response {
        let tube_name = self.conns.get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name) {
            if let Some(&(_, job_id)) = tube.ready.peek() {
                if let Some(job) = self.jobs.get(&job_id) {
                    return Response::Found { id: job_id, body: job.body.clone() };
                }
            }
        }
        Response::NotFound
    }

    fn cmd_peek_delayed(&self, conn_id: u64) -> Response {
        let tube_name = self.conns.get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name) {
            if let Some(&(_, job_id)) = tube.delay.peek() {
                if let Some(job) = self.jobs.get(&job_id) {
                    return Response::Found { id: job_id, body: job.body.clone() };
                }
            }
        }
        Response::NotFound
    }

    fn cmd_peek_buried(&self, conn_id: u64) -> Response {
        let tube_name = self.conns.get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name) {
            if let Some(&job_id) = tube.buried.front() {
                if let Some(job) = self.jobs.get(&job_id) {
                    return Response::Found { id: job_id, body: job.body.clone() };
                }
            }
        }
        Response::NotFound
    }

    fn cmd_kick(&mut self, conn_id: u64, bound: u32) -> Response {
        let tube_name = self.conns.get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());

        let mut kicked = 0u32;

        // Kick buried first, then delayed
        let has_buried = self.tubes.get(&tube_name)
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
            JobState::Reserved | JobState::Delayed => {
                job.deadline_at
                    .map(|d| {
                        if d > now { d.duration_since(now).as_secs() as i64 }
                        else { 0 }
                    })
                    .unwrap_or(0)
            }
            _ => 0,
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
             file: 0\n\
             reserves: {}\n\
             timeouts: {}\n\
             releases: {}\n\
             buries: {}\n\
             kicks: {}\n",
            job.id,
            job.tube_name,
            job.state.as_str(),
            job.priority,
            age,
            job.delay.as_secs(),
            job.ttr.as_secs(),
            time_left,
            job.reserve_ct,
            job.timeout_ct,
            job.release_ct,
            job.bury_ct,
            job.kick_ct,
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats_tube(&self, tube_name: &str) -> Response {
        let tube = match self.tubes.get(tube_name) {
            Some(t) => t,
            None => return Response::NotFound,
        };

        let pause_time_left = tube.unpause_at
            .map(|u| {
                let now = Instant::now();
                if u > now { u.duration_since(now).as_secs() as i64 }
                else { 0 }
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
             pause-time-left: {}\n",
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
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats(&self) -> Response {
        let delayed_ct: usize = self.tubes.values().map(|t| t.delay.len()).sum();
        let uptime = Instant::now().duration_since(self.started_at).as_secs();

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
             version: \"beanstalkd-rs 0.1.0\"\n\
             uptime: {}\n\
             draining: {}\n",
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
            uptime,
            if self.drain_mode { "true" } else { "false" },
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
        let tube = self.conns.get(&conn_id)
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
        let mut best: Option<(u32, u64, u64)> = None; // (pri, job_id, job_id)

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if let Some(&((pri, jid), _)) = tube.ready.peek() {
                    match &best {
                        None => best = Some((pri, jid, jid)),
                        Some((bp, bid, _)) => {
                            if pri < *bp || (pri == *bp && jid < *bid) {
                                best = Some((pri, jid, jid));
                            }
                        }
                    }
                }
            }
        }

        best.map(|(_, _, id)| id)
    }

    fn select_weighted_job(&self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;

        // Sum weights of tubes with ready, unpaused jobs
        let mut total_weight: u32 = 0;
        let mut candidates: Vec<(u64, u32)> = Vec::new(); // (job_id, weight)

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() || !tube.has_ready() {
                    continue;
                }
                if let Some(&(_, job_id)) = tube.ready.peek() {
                    total_weight += w.weight;
                    candidates.push((job_id, w.weight));
                }
            }
        }

        if total_weight == 0 {
            return None;
        }

        // Simple deterministic selection for now (use random in production)
        // We'll use a basic pseudo-random based on current time nanos
        let r = (Instant::now().elapsed().subsec_nanos() as u32) % total_weight;
        let mut cumulative = 0u32;
        for (job_id, weight) in &candidates {
            cumulative += weight;
            if r < cumulative {
                return Some(*job_id);
            }
        }

        candidates.last().map(|(id, _)| *id)
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
        let mut all_remove: Vec<usize> = timed_out_indices.iter().chain(fulfill_indices.iter()).cloned().collect();
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

    /// Same as find_ready_job_for_conn but doesn't need &self only - uses inner state directly.
    fn find_ready_job_for_conn_inner(&self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;
        let mut best: Option<(u32, u64)> = None;

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if let Some(&((pri, jid), _)) = tube.ready.peek() {
                    match &best {
                        None => best = Some((pri, jid)),
                        Some((bp, bid)) => {
                            if pri < *bp || (pri == *bp && jid < *bid) {
                                best = Some((pri, jid));
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

    /// Tick: promote delayed jobs, expire TTR, unpause tubes.
    fn tick(&mut self) {
        let now = Instant::now();

        // Promote delayed jobs to ready
        let tube_names: Vec<String> = self.tubes.keys().cloned().collect();
        for tube_name in &tube_names {
            loop {
                let should_promote = self.tubes.get(tube_name)
                    .and_then(|t| t.delay.peek().map(|&((deadline, _), _)| deadline <= now))
                    .unwrap_or(false);

                if !should_promote {
                    break;
                }

                let job_id = self.tubes.get_mut(tube_name)
                    .and_then(|t| t.delay.pop().map(|(_, id)| id));

                if let Some(job_id) = job_id {
                    if let Some(job) = self.jobs.get_mut(&job_id) {
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
                conn.reserved_jobs.iter()
                    .filter(|&&jid| {
                        self.jobs.get(&jid)
                            .and_then(|j| j.deadline_at)
                            .map(|d| now >= d)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            };

            for job_id in expired_jobs {
                // Remove from connection's reserved list
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
                    job.deadline_at = None;
                    let key = job.ready_key();
                    let tube_name = job.tube_name.clone();
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
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
        let timed_out: Vec<usize> = self.waiters.iter().enumerate()
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
    }
}

/// Start the beanstalkd server.
pub async fn run(addr: &str, port: u16, max_job_size: u32) -> io::Result<()> {
    let listener = TcpListener::bind((addr, port)).await?;
    tracing::info!("listening on {}:{}", addr, port);

    let (engine_tx, mut engine_rx) = mpsc::channel::<EngineMsg>(1024);
    let mut state = ServerState::new(max_job_size);

    // Engine task
    let engine_handle = tokio::spawn(async move {
        let mut tick_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                Some(msg) = engine_rx.recv() => {
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
            handle_connection(socket, tx).await;
        });
    }
}

/// Atomic counter for connection IDs (simpler than engine round-trip).
static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

async fn handle_connection(
    socket: tokio::net::TcpStream,
    engine_tx: mpsc::Sender<EngineMsg>,
) {
    let conn_id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line_buf = String::new();

    loop {
        line_buf.clear();
        let n = match reader.read_line(&mut line_buf).await {
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

                    if body_size > 65535 {
                        // Check against configured limit (TODO: get from engine)
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
        let _ = engine_tx.send(EngineMsg {
            conn_id,
            payload: EnginePayload::Command {
                cmd,
                body,
                reply_tx,
            },
        }).await;

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
    let _ = engine_tx.send(EngineMsg {
        conn_id,
        payload: EnginePayload::Disconnect,
    }).await;
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

    #[test]
    fn test_put_and_reserve() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(c, Command::Put {
            pri: 0, delay: 0, ttr: 10, bytes: 5,
        }, Some(b"hello".to_vec()));
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

        s.handle_command(c, Command::Put { pri: 5, delay: 0, ttr: 10, bytes: 1 }, Some(b"a".to_vec()));
        s.handle_command(c, Command::Put { pri: 1, delay: 0, ttr: 10, bytes: 1 }, Some(b"b".to_vec()));
        s.handle_command(c, Command::Put { pri: 3, delay: 0, ttr: 10, bytes: 1 }, Some(b"c".to_vec()));

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

        s.handle_command(c, Command::Put { pri: 1, delay: 0, ttr: 10, bytes: 1 }, Some(b"a".to_vec()));
        s.handle_command(c, Command::Put { pri: 1, delay: 0, ttr: 10, bytes: 1 }, Some(b"b".to_vec()));
        s.handle_command(c, Command::Put { pri: 1, delay: 0, ttr: 10, bytes: 1 }, Some(b"c".to_vec()));

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

        s.handle_command(c, Command::Put { pri: 0, delay: 0, ttr: 10, bytes: 1 }, Some(b"x".to_vec()));
        let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
        assert!(matches!(resp, Response::Deleted));

        let resp = s.handle_command(c, Command::Peek { id: 1 }, None);
        assert!(matches!(resp, Response::NotFound));
    }

    #[test]
    fn test_bury_and_kick() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, Command::Put { pri: 0, delay: 0, ttr: 10, bytes: 1 }, Some(b"x".to_vec()));
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
        s.handle_command(producer, Command::Use { tube: "emails".into() }, None);
        s.handle_command(producer, Command::Put { pri: 0, delay: 0, ttr: 10, bytes: 5 }, Some(b"hello".to_vec()));

        // Consumer watches "emails", reserve should find it
        s.handle_command(consumer, Command::Watch { tube: "emails".into(), weight: 1 }, None);
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

        s.handle_command(c, Command::Put { pri: 5, delay: 0, ttr: 10, bytes: 1 }, Some(b"x".to_vec()));
        let resp = s.handle_command(c, Command::Reserve, None);
        let id = match resp { Response::Reserved { id, .. } => id, _ => panic!() };

        let resp = s.handle_command(c, Command::Release { id, pri: 0, delay: 0 }, None);
        assert!(matches!(resp, Response::Released));

        // Should be back in ready with new priority
        let resp = s.handle_command(c, Command::PeekReady, None);
        assert!(matches!(resp, Response::Found { .. }));
    }

    #[test]
    fn test_touch() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, Command::Put { pri: 0, delay: 0, ttr: 10, bytes: 1 }, Some(b"x".to_vec()));
        let resp = s.handle_command(c, Command::Reserve, None);
        let id = match resp { Response::Reserved { id, .. } => id, _ => panic!() };

        let resp = s.handle_command(c, Command::Touch { id }, None);
        assert!(matches!(resp, Response::Touched));
    }

    #[test]
    fn test_drain_mode() {
        let mut s = make_state();
        let c = register(&mut s);
        s.drain_mode = true;

        let resp = s.handle_command(c, Command::Put { pri: 0, delay: 0, ttr: 10, bytes: 1 }, Some(b"x".to_vec()));
        assert!(matches!(resp, Response::Draining));
    }

    #[test]
    fn test_close_releases_jobs() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, Command::Put { pri: 0, delay: 0, ttr: 10, bytes: 1 }, Some(b"x".to_vec()));
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

        let resp = s.handle_command(c, Command::ReserveMode { mode: "weighted".into() }, None);
        assert!(matches!(resp, Response::Using(m) if m == "weighted"));

        let resp = s.handle_command(c, Command::ReserveMode { mode: "fifo".into() }, None);
        assert!(matches!(resp, Response::Using(m) if m == "fifo"));

        let resp = s.handle_command(c, Command::ReserveMode { mode: "invalid".into() }, None);
        assert!(matches!(resp, Response::BadFormat));
    }

    #[test]
    fn test_watch_and_ignore() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(c, Command::Watch { tube: "foo".into(), weight: 1 }, None);
        assert!(matches!(resp, Response::Watching(2)));

        let resp = s.handle_command(c, Command::Ignore { tube: "default".into() }, None);
        assert!(matches!(resp, Response::Watching(1)));

        // Can't ignore last tube
        let resp = s.handle_command(c, Command::Ignore { tube: "foo".into() }, None);
        assert!(matches!(resp, Response::NotIgnored));
    }

    #[test]
    fn test_kick_job_buried() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, Command::Put { pri: 0, delay: 0, ttr: 10, bytes: 1 }, Some(b"x".to_vec()));
        s.handle_command(c, Command::Reserve, None);
        s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

        let resp = s.handle_command(c, Command::KickJob { id: 1 }, None);
        assert!(matches!(resp, Response::KickedOne));
    }

    #[test]
    fn test_pause_tube() {
        let mut s = make_state();
        let c = register(&mut s);

        let resp = s.handle_command(c, Command::PauseTube { tube: "default".into(), delay: 60 }, None);
        assert!(matches!(resp, Response::Paused));

        assert!(s.tubes.get("default").unwrap().is_paused());
    }

    #[test]
    fn test_stats_job() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, Command::Put { pri: 100, delay: 0, ttr: 60, bytes: 5 }, Some(b"hello".to_vec()));

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
    fn test_list_tubes() {
        let mut s = make_state();
        let c = register(&mut s);

        s.handle_command(c, Command::Use { tube: "emails".into() }, None);

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
