use super::*;

impl ServerState {
    /// Tick: promote delayed jobs, expire TTR, unpause tubes.
    pub(super) fn tick(&mut self) {
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
                        .and_then(|j| j.after_group())
                        .and_then(|ag| self.groups.get(ag))
                        .map(|gs| !gs.is_complete())
                        .unwrap_or(false);

                    if hold_for_group {
                        // Keep as delayed with no deadline; add to group waiting list
                        if let Some(job) = self.jobs.get_mut(&job_id) {
                            job.deadline_at = None;
                        }
                        if let Some(ag) = self.jobs.get(&job_id).and_then(|j| j.after_group().cloned())
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
                let mut requeue = None;
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.timeout_ct += 1;
                    job.state = JobState::Ready;
                    job.reserver_id = None;
                    job.reserved_at = None;
                    job.deadline_at = None;
                    requeue = Some((job.tube_name.clone(), job.ready_key()));
                }
                if let Some((tube_name, key)) = requeue {
                    self.ensure_tube(&tube_name);
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
                let pri = self.job_pri(job_id);
                self.wal_write_state_change(
                    job_id,
                    Some(JobState::Ready),
                    pri,
                    Duration::ZERO,
                    0,
                    StateChangeReason::Timeout,
                );
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
            let waiter = self.remove_waiter_at(i);
            // deliver_waiter_failure sends DEADLINE_SOON here (the connection is
            // within the TTR safety margin) for batch and single waiters alike.
            self.deliver_waiter_failure(waiter, now);
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

        let mut expired = Vec::new();
        for &i in timed_out.iter().rev() {
            expired.push(self.remove_waiter_at(i));
        }

        for waiter in expired {
            self.deliver_waiter_failure(waiter, now);
        }

        // Try to fulfill remaining waiters with newly ready jobs
        self.process_queue();

        // Clean up expired idempotency cooldowns (with memory accounting).
        let sys_now = SystemTime::now();
        let expired: Vec<(String, String)> = self
            .tubes
            .iter()
            .flat_map(|(tube_name, tube)| {
                tube.idempotency_cooldowns
                    .iter()
                    .filter(|(_, (_, expiry))| *expiry <= sys_now)
                    .map(|(k, _)| (tube_name.clone(), k.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        for (tube_name, key) in expired {
            self.remove_tombstone(&tube_name, &key);
        }

        // Memory-accounting drift detector.
        //
        // When the live set (jobs + tombstones) is empty, `total_job_bytes`
        // must also be zero. Non-zero means a missed decrement somewhere,
        // which would eventually produce bogus OUT_OF_MEMORY responses on
        // an empty queue. Log loudly (so operators see it), bump a counter
        // (so they can alert on `rate(tuber_accounting_drift_events_total
        // [5m]) > 0`), and self-heal by resetting — a permanent leak is
        // worse than a one-off warning.
        if self.total_job_bytes != 0
            && self.jobs.is_empty()
            && self
                .tubes
                .values()
                .all(|t| t.idempotency_cooldowns.is_empty())
        {
            tracing::warn!(
                "memory accounting drift: total_job_bytes={} with no live \
                 jobs or tombstones — resetting to 0. This is a tuber bug; \
                 please report it.",
                self.total_job_bytes,
            );
            self.stats.accounting_drift_events += 1;
            self.total_job_bytes = 0;
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
                .filter(|(_, job)| job.wal_seq() == Some(target_seq))
                .map(|(id, _)| *id)
                .take(count)
                .collect();

            for job_id in migrate_ids {
                if let (Some(wal), Some(job)) = (self.wal.as_mut(), self.jobs.get_mut(&job_id)) {
                    if let Err(e) = wal.write_put(job) {
                        tracing::error!("WAL compaction write error: {}, disabling WAL", e);
                        self.wal = None;
                        break;
                    }
                    // A compaction FullJob only carries created_at_epoch, so a
                    // release/kick-to-delayed job would replay its delay from
                    // creation and fire early (the mirror of the v7 fix). Re-
                    // assert Delayed with the *remaining* delay stamped at now,
                    // so the v7 StateChange arm reconstructs the real deadline.
                    // Skips held after-group jobs (Delayed, no deadline), which
                    // the group logic re-holds on restore regardless.
                    if job.state == JobState::Delayed
                        && let Some(deadline) = job.deadline_at
                    {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        let pri = job.priority;
                        if let Err(e) = wal.write_state_change(
                            job,
                            Some(JobState::Delayed),
                            pri,
                            remaining,
                            0,
                            StateChangeReason::None,
                        ) {
                            tracing::error!(
                                "WAL compaction delay re-assert error: {}, disabling WAL",
                                e
                            );
                            self.wal = None;
                            break;
                        }
                    }
                    wal.record_migration();
                }
            }
        }

        // After-group-held jobs (state Delayed, no deadline) live only in
        // GroupState::waiting_jobs — their tube looks idle but must survive
        // GC, or group completion would promote them into a deleted tube.
        let held_tubes: std::collections::HashSet<&String> = self
            .groups
            .values()
            .flat_map(|gs| gs.waiting_jobs.iter())
            .filter_map(|id| self.jobs.get(id))
            .filter(|j| j.state == JobState::Delayed && j.deadline_at.is_none())
            .map(|j| &j.tube_name)
            .collect();

        // New connections default to "use default" + "watch default", so always keep it
        self.tubes
            .retain(|name, tube| name == "default" || !tube.is_idle() || held_tubes.contains(name));
    }

    /// Restore jobs from WAL replay into server state.
    pub(super) fn restore_jobs(
        &mut self,
        jobs: HashMap<u64, Job>,
        next_job_id: u64,
        tombstones: Vec<IdpTombstone>,
    ) {
        self.next_job_id = next_job_id;

        // Adopt the replayed map wholesale instead of re-inserting job by
        // job — the per-job path keeps two full job tables alive at once,
        // which at millions of jobs is hundreds of MB of avoidable peak
        // RSS during startup (when the process is most OOM-prone). The
        // index pass below only needs ids plus small cloned fields.
        let ids: Vec<u64> = jobs.keys().copied().collect();
        for job in jobs.values() {
            self.total_job_bytes = self
                .total_job_bytes
                .saturating_add(Self::job_memory_cost(job));
        }
        self.jobs = jobs;

        // Collect after_group job IDs for a second pass
        let mut after_group_jobs: Vec<u64> = Vec::new();

        for id in ids {
            let Some(job) = self.jobs.get(&id) else {
                continue;
            };
            let tube_name = job.tube_name.clone();
            let state = job.state;
            let pri = job.priority;
            let ready_key = job.ready_key();
            let deadline_at = job.deadline_at;
            let delay = job.delay;
            let idempotency_key = job.idempotency_key().cloned();
            let group = job.group().cloned();
            let after_group = job.after_group().cloned();

            self.ensure_tube(&tube_name);

            match state {
                JobState::Ready => {
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.ready.insert(ready_key, id);
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
                    let deadline = deadline_at.unwrap_or_else(|| Instant::now() + delay);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.delay.insert((deadline, id), id);
                    }
                    self.stats.total_jobs_ct += 1;
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.total_jobs_ct += 1;
                    }
                }
                JobState::Buried => {
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
                JobState::Reserved => unreachable!(
                    "WAL deserialization translates Reserved -> Ready before restore_jobs"
                ),
            }

            // Register concurrency limit
            if let Some(job) = self.jobs.get(&id) {
                if let Some((key, limit)) = job.concurrency_key() {
                    let entry = self.concurrency_limits.entry(key.clone()).or_insert(0);
                    *entry = (*entry).max(*limit);
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
            let ag = self.jobs.get(&job_id).and_then(|j| j.after_group().cloned());
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
            self.insert_tombstone(
                &tombstone.tube_name,
                tombstone.key,
                tombstone.job_id,
                tombstone.expires_at,
            );
        }
    }
}
