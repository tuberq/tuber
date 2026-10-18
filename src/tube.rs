use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::heap::IndexHeap;
use crate::job::{Job, JobState};

/// Ready heap key: (priority, job_id). Lower priority = higher urgency.
pub type ReadyKey = (u32, u64);

/// Delay heap key: (deadline_at, job_id). Instants are Copy + Ord.
pub type DelayKey = (Instant, u64);

#[derive(Debug, Default, Clone)]
pub struct TubeStats {
    pub urgent_ct: u64,
    pub buried_ct: u64,
    /// `aft:` jobs their group holds: on its waiting list, in no queue.
    pub held_ct: u64,
    pub reserved_ct: u64,
    pub pause_ct: u64,
    pub total_delete_ct: u64,
    pub total_jobs_ct: u64,

    // Throughput counters
    pub total_reserve_ct: u64,
    pub total_timeout_ct: u64,
    pub total_bury_ct: u64,

    // Processing time stats
    pub processing_time_ewma: f64,
    pub processing_time_min: Option<f64>,
    pub processing_time_max: Option<f64>,
    pub processing_time_samples: u64,

    // Dual EWMA: fast (<100ms) vs slow (>=100ms)
    pub processing_time_ewma_fast: f64,
    pub processing_time_samples_fast: u64,
    pub processing_time_ewma_slow: f64,
    pub processing_time_samples_slow: u64,

    pub processing_time_ring_fast: VecDeque<f64>,
    pub processing_time_ring_slow: VecDeque<f64>,

    // Queue time (put-to-reserve)
    pub queue_time_ewma: f64,
    pub queue_time_min: Option<f64>,
    pub queue_time_max: Option<f64>,
    pub queue_time_samples: u64,

    pub idp_dedups: IdpDedups,
}

/// `idp:` dedups, by the state of the job the put matched. Hits on a buried
/// job are the ones to alert on: nothing was queued, and nothing runs until
/// the job is kicked.
#[derive(Debug, Default, Clone, Copy)]
pub struct IdpDedups {
    pub ready: u64,
    pub reserved: u64,
    pub delayed: u64,
    pub buried: u64,
    /// Hits on a cooldown tombstone, whose job is already deleted.
    pub deleted: u64,
}

impl IdpDedups {
    /// Count one dedup against a job in `state`; `None` for a tombstone.
    pub fn count(&mut self, state: Option<JobState>) {
        let ct = match state {
            Some(JobState::Ready) => &mut self.ready,
            Some(JobState::Reserved) => &mut self.reserved,
            Some(JobState::Delayed) => &mut self.delayed,
            Some(JobState::Buried) => &mut self.buried,
            None => &mut self.deleted,
        };
        *ct += 1;
    }

    pub fn total(&self) -> u64 {
        self.ready + self.reserved + self.delayed + self.buried + self.deleted
    }
}

const RING_CAPACITY: usize = 1000;

impl TubeStats {

    pub fn update_ewma(ewma: &mut f64, samples: &mut u64, value: f64, alpha: f64) {
        *samples += 1;
        if *samples == 1 {
            *ewma = value;
        } else {
            *ewma = alpha * value + (1.0 - alpha) * *ewma;
        }
    }

    pub fn record_timing(
        ewma: &mut f64, samples: &mut u64,
        min: &mut Option<f64>, max: &mut Option<f64>,
        value: f64, alpha: f64,
    ) {
        Self::update_ewma(ewma, samples, value, alpha);
        *min = Some(min.map_or(value, |m| m.min(value)));
        *max = Some(max.map_or(value, |m| m.max(value)));
    }

    fn push_ring(ring: &mut VecDeque<f64>, value: f64) {
        if ring.len() >= RING_CAPACITY {
            ring.pop_front();
        }
        ring.push_back(value);
    }

    pub fn record_fast_sample(&mut self, secs: f64) {
        Self::push_ring(&mut self.processing_time_ring_fast, secs);
    }

    pub fn record_slow_sample(&mut self, secs: f64) {
        Self::push_ring(&mut self.processing_time_ring_slow, secs);
    }

    pub fn bury_rate(&self) -> f64 {
        if self.total_reserve_ct > 0 {
            self.total_bury_ct as f64 / self.total_reserve_ct as f64
        } else {
            0.0
        }
    }

    /// Compute p50/p95/p99 over all recent samples — the fast (<100ms) and slow
    /// (>=100ms) rings combined. Using the slow ring alone whenever it held any
    /// sample skewed every percentile toward the slow tail: a single 200ms job
    /// among a thousand 10ms jobs would report p50=200ms. (Each ring keeps its
    /// own recent window, so a heavily bimodal tube weights the two modes by
    /// window occupancy rather than raw counts — an acceptable approximation.)
    pub fn percentiles(&self) -> (f64, f64, f64) {
        let total = self.processing_time_ring_fast.len() + self.processing_time_ring_slow.len();
        if total == 0 {
            return (0.0, 0.0, 0.0);
        }
        let mut sorted: Vec<f64> = Vec::with_capacity(total);
        sorted.extend(self.processing_time_ring_fast.iter().copied());
        sorted.extend(self.processing_time_ring_slow.iter().copied());
        sorted.sort_unstable_by(f64::total_cmp);
        let len = sorted.len();
        let p = |pct: f64| -> f64 {
            let idx = ((pct / 100.0) * (len - 1) as f64).round() as usize;
            sorted[idx.min(len - 1)]
        };
        (p(50.0), p(95.0), p(99.0))
    }
}

/// A tube's ready jobs, filed so a reserve can step past a blocked `con:` key
/// in one move instead of one job at a time.
///
/// Jobs without a key sit in `free`. Jobs with one sit in `keyed`, grouped by
/// key, and each key's best job also sits in `heads`. Whether a key is blocked
/// depends only on the key (its reservations against its limit), so when a
/// key's head is blocked, every job behind it is too. A reserve walks `heads`
/// rather than jobs, and so passes at most one entry per blocked key. A key is
/// blocked only while it holds reservations, so that's bounded by the jobs in
/// flight, however deep any one key's backlog grows.
///
/// `heads` keeps blocked keys as well as unblocked ones. A key's slots are
/// shared by every tube, so holding only unblocked heads would mean updating
/// each tube with that key's jobs whenever a slot filled or freed; checking at
/// reserve time keeps the queue self-contained.
#[derive(Debug, Default)]
pub struct ReadyQueue {
    /// Ready jobs without a `con:` key.
    free: IndexHeap<ReadyKey>,
    /// Ready jobs with a `con:` key, ordered by key and then ready key, so
    /// each key's jobs are one range with its best job first.
    keyed: BTreeSet<(Arc<str>, ReadyKey)>,
    /// Each keyed job's entry in `keyed`, so a job can be removed by id.
    key_of: HashMap<u64, (Arc<str>, ReadyKey)>,
    /// One allocation per key in `keyed`, shared by all of that key's jobs.
    keys: HashSet<Arc<str>>,
    /// The best job of each key in `keyed`, whether the key is blocked or not.
    heads: IndexHeap<ReadyKey>,
}

impl ReadyQueue {
    pub fn len(&self) -> usize {
        self.free.len() + self.key_of.len()
    }

    pub fn is_empty(&self) -> bool {
        self.free.is_empty() && self.key_of.is_empty()
    }

    /// Give back the room a burst left behind ([`crate::shrink`]).
    pub fn shrink_if_sparse(&mut self) {
        self.free.shrink_if_sparse();
        self.heads.shrink_if_sparse();
        crate::shrink::shrink_if_sparse(&mut self.key_of);
        crate::shrink::shrink_if_sparse(&mut self.keys);
    }

    /// Entries the queue's largest part has room for without growing.
    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        [self.free.capacity(), self.heads.capacity(), self.key_of.capacity(), self.keys.capacity()]
            .into_iter()
            .max()
            .unwrap()
    }

    /// Every job in the queue, in no particular order.
    pub fn ids(&self) -> Vec<u64> {
        let mut ids = self.free.ids();
        ids.extend(self.key_of.keys());
        ids
    }

    /// The best ready job, blocked or not: what `peek-ready` shows.
    pub fn peek(&self) -> Option<(ReadyKey, u64)> {
        self.free
            .peek()
            .into_iter()
            .chain(self.heads.peek())
            .copied()
            .min()
    }

    /// Queue `job` under its ready key, filed by its `con:` key if it has
    /// one. A job already queued is left where it is.
    pub fn insert(&mut self, job: &Job) {
        let (rk, id) = (job.ready_key(), job.id);
        let Some((con, _)) = job.concurrency_key() else {
            if !self.key_of.contains_key(&id) {
                self.free.insert(rk, id);
            }
            return;
        };
        if self.free.contains(id) || self.key_of.contains_key(&id) {
            return;
        }
        let key = match self.keys.get(con.as_str()) {
            Some(key) => key.clone(),
            None => {
                let key: Arc<str> = Arc::from(con.as_str());
                self.keys.insert(key.clone());
                key
            }
        };
        match self.head_of(&key) {
            Some(head) if head < rk => {}
            Some(head) => {
                self.heads.remove_by_id(head.1);
                self.heads.insert(rk, id);
            }
            None => {
                self.heads.insert(rk, id);
            }
        }
        self.keyed.insert((key.clone(), rk));
        self.key_of.insert(id, (key, rk));
    }

    pub fn remove_by_id(&mut self, id: u64) -> Option<(ReadyKey, u64)> {
        if let Some(entry) = self.free.remove_by_id(id) {
            return Some(entry);
        }
        let (key, rk) = self.key_of.remove(&id)?;
        self.keyed.remove(&(key.clone(), rk));
        if self.heads.remove_by_id(id).is_some() {
            match self.head_of(&key) {
                Some(next) => {
                    self.heads.insert(next, next.1);
                }
                None => {
                    self.keys.remove(&key);
                }
            }
        }
        Some((rk, id))
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// The best job whose `con:` key, if any, isn't blocked. `is_blocked` is
    /// asked about the head of each key in turn, best first, until one is
    /// free or the best untagged job comes ahead of the rest.
    pub fn best_unblocked(&self, is_blocked: impl Fn(u64) -> bool) -> Option<(ReadyKey, u64)> {
        let free = self.free.peek().copied();
        let heads = self.heads.entries();
        let Some(&top) = heads.first() else {
            return free;
        };
        if free.is_some_and(|f| f < top) {
            return free;
        }
        if !is_blocked(top.1) {
            return Some(top);
        }

        // Best-first walk of the heads heap: an entry's children are the only
        // candidates for next best that aren't already pending.
        let mut pending: BinaryHeap<Reverse<((ReadyKey, u64), usize)>> = BinaryHeap::new();
        let push_children = |pending: &mut BinaryHeap<_>, idx: usize| {
            for child in [idx * 2 + 1, idx * 2 + 2] {
                if let Some(&entry) = heads.get(child) {
                    pending.push(Reverse((entry, child)));
                }
            }
        };
        push_children(&mut pending, 0);
        while let Some(Reverse((entry, idx))) = pending.pop() {
            if free.is_some_and(|f| f < entry) {
                break;
            }
            if !is_blocked(entry.1) {
                return Some(entry);
            }
            push_children(&mut pending, idx);
        }
        free
    }

    /// The best job filed under `key`.
    fn head_of(&self, key: &Arc<str>) -> Option<ReadyKey> {
        self.keyed
            .range((key.clone(), (0, 0))..)
            .next()
            .filter(|(k, _)| k == key)
            .map(|&(_, rk)| rk)
    }

    /// Panic unless the indexes agree: `key_of` and `keyed` hold the same
    /// jobs, `heads` holds exactly each key's best job, and `keys` exactly
    /// the keys present.
    #[cfg(test)]
    pub fn assert_consistent(&self) {
        let mut from_key_of: Vec<_> = self.key_of.values().cloned().collect();
        from_key_of.sort();
        let from_keyed: Vec<_> = self.keyed.iter().cloned().collect();
        assert_eq!(from_key_of, from_keyed, "key_of and keyed disagree");
        assert!(self.key_of.iter().all(|(&id, (_, rk))| rk.1 == id));
        assert!(
            self.free
                .ids()
                .iter()
                .all(|id| !self.key_of.contains_key(id))
        );

        let mut best: HashMap<&Arc<str>, ReadyKey> = HashMap::new();
        for (key, rk) in &self.keyed {
            best.entry(key).or_insert(*rk);
        }
        let mut want: Vec<_> = best.values().copied().collect();
        want.sort();
        let mut got: Vec<_> = self.heads.entries().iter().map(|&(rk, _)| rk).collect();
        got.sort();
        assert_eq!(got, want, "heads are not each key's best job");
        assert_eq!(self.keys.len(), best.len(), "keys holds a key with no jobs");
        assert!(best.keys().all(|k| self.keys.contains(*k)));
    }
}

/// An `idp:` key's cooldown after its job was deleted: a put with the key
/// is answered with `job_id` until `expires_at`.
#[derive(Debug, Clone, Copy)]
pub struct Cooldown {
    pub job_id: u64,
    pub expires_at: SystemTime,
    /// Its WAL tombstone record, which holds a reference on its segment
    /// until the cooldown ends. `None` without persistence.
    pub record: Option<crate::wal::RecordRef>,
}

#[derive(Debug)]
pub struct Tube {
    /// Shared with every job in the tube via `Job.tube_name` — the
    /// single allocation behind all those `Arc<str>` clones.
    pub name: Arc<str>,
    pub ready: ReadyQueue,
    pub delay: IndexHeap<DelayKey>,
    pub buried: VecDeque<u64>,
    pub stat: TubeStats,
    pub idempotency_keys: HashMap<String, u64>,
    /// Keyed by the same `Arc` as the key's entry in `ServerState::cooldown_expiry`.
    pub idempotency_cooldowns: HashMap<Arc<str>, Cooldown>,
    pub using_ct: u32,
    pub watching_ct: u32,
    pub pause: Duration,
    pub unpause_at: Option<Instant>,
    /// The parked reserves watching this tube, by arrival (`seq`), so the
    /// first is the one a job here should go to.
    pub waiters: BTreeSet<u64>,
}

impl Tube {
    pub fn new(name: &str) -> Self {
        Tube {
            name: Arc::from(name),
            ready: ReadyQueue::default(),
            delay: IndexHeap::new(),
            buried: VecDeque::new(),
            idempotency_keys: HashMap::new(),
            idempotency_cooldowns: HashMap::new(),
            stat: TubeStats::default(),
            using_ct: 0,
            watching_ct: 0,
            pause: Duration::ZERO,
            unpause_at: None,
            waiters: BTreeSet::new(),
        }
    }

    pub fn is_paused(&self) -> bool {
        if let Some(unpause_at) = self.unpause_at {
            Instant::now() < unpause_at
        } else {
            false
        }
    }

    pub fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    pub fn has_delayed(&self) -> bool {
        !self.delay.is_empty()
    }

    pub fn has_buried(&self) -> bool {
        !self.buried.is_empty()
    }

    /// Returns true if the tube is completely empty and unused.
    ///
    /// Reserved jobs live in `conn.reserved_jobs`, not in any tube
    /// structure, so `reserved_ct` is the only thing keeping a tube with
    /// in-flight jobs alive — without it, idle GC would delete the tube
    /// and release/bury/TTR-expiry would re-enqueue into a void.
    ///
    /// An unexpired pause also holds the tube open. The pause lives
    /// nowhere but this struct, so reaping a drained-but-paused tube
    /// silently discards it: the operator's "stop this tube while we
    /// investigate" survives only as long as the last job does, and work
    /// arriving afterwards is reservable immediately. Keeping it costs
    /// nothing unbounded — `unpause_at` expires on its own, and the next
    /// tick reaps the tube then. Deliberate divergence from beanstalkd,
    /// which frees the tube on its refcount without consulting the pause.
    /// Checked last so the clock read only happens for a tube that is
    /// otherwise idle.
    /// Give back the room a burst of jobs left behind ([`crate::shrink`]).
    pub fn shrink_if_sparse(&mut self) {
        self.ready.shrink_if_sparse();
        self.delay.shrink_if_sparse();
        crate::shrink::shrink_if_sparse(&mut self.buried);
        crate::shrink::shrink_if_sparse(&mut self.idempotency_keys);
        crate::shrink::shrink_if_sparse(&mut self.idempotency_cooldowns);
    }

    pub fn is_idle(&self) -> bool {
        self.ready.is_empty()
            && self.delay.is_empty()
            && self.buried.is_empty()
            && self.waiters.is_empty()
            && self.stat.reserved_ct == 0
            && self.idempotency_keys.is_empty()
            && self.idempotency_cooldowns.is_empty()
            && self.using_ct == 0
            && self.watching_ct == 0
            && !self.is_paused()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobExt;

    #[test]
    fn test_tube_new() {
        let t = Tube::new("default");
        assert_eq!(t.name.as_ref(), "default");
        assert!(t.ready.is_empty());
        assert!(t.delay.is_empty());
        assert!(t.buried.is_empty());
        assert!(t.waiters.is_empty());
        assert!(!t.is_paused());
        assert!(t.is_idle());
    }

    #[test]
    fn test_percentiles_combine_fast_and_slow_rings() {
        let mut s = TubeStats::default();
        // Many fast samples and a single slow outlier. The median must reflect
        // the fast bulk, not be dragged to the slow tail (the slow-ring-only bug).
        for _ in 0..999 {
            s.record_fast_sample(0.010);
        }
        s.record_slow_sample(0.200);

        let (p50, _p95, p99) = s.percentiles();
        assert!(
            (p50 - 0.010).abs() < 1e-9,
            "p50 should reflect the fast bulk, got {p50}"
        );
        assert!(p99 >= p50, "percentiles must stay ordered");
    }

    #[test]
    fn test_percentiles_empty_is_zero() {
        let s = TubeStats::default();
        assert_eq!(s.percentiles(), (0.0, 0.0, 0.0));
    }

    // Adapted from cttest_ms_append -- tests watch set add
    #[test]
    fn test_watch_set_add() {
        let watch: Vec<String> = vec!["tube1".into(), "tube2".into()];
        assert_eq!(watch.len(), 2);
        assert!(watch.contains(&"tube1".to_string()));
        assert!(watch.contains(&"tube2".to_string()));
    }

    // Adapted from cttest_ms_remove
    #[test]
    fn test_watch_set_remove() {
        let mut watch: Vec<String> = Vec::new();
        watch.push("tube1".into());
        watch.push("tube2".into());
        watch.push("tube3".into());

        watch.retain(|t| t != "tube2");
        assert_eq!(watch.len(), 2);
        assert!(!watch.contains(&"tube2".to_string()));
    }

    // Adapted from cttest_ms_contains
    #[test]
    fn test_watch_set_contains() {
        let watch: Vec<String> = vec!["tube1".into()];
        assert!(watch.contains(&"tube1".to_string()));
        assert!(!watch.contains(&"tube2".to_string()));
    }

    // Adapted from cttest_ms_clear_empty
    #[test]
    fn test_watch_set_clear() {
        let mut watch: Vec<String> = Vec::new();
        watch.clear(); // clearing empty is fine
        assert_eq!(watch.len(), 0);

        watch.push("tube1".into());
        watch.clear();
        assert_eq!(watch.len(), 0);
    }

    // Adapted from cttest_ms_take
    #[test]
    fn test_watch_set_take() {
        let mut watch: VecDeque<String> = VecDeque::new();
        watch.push_back("tube1".into());
        watch.push_back("tube2".into());

        let taken = watch.pop_front();
        assert_eq!(taken.as_deref(), Some("tube1"));
        assert_eq!(watch.len(), 1);
    }

    // Adapted from cttest_ms_take_sequence
    #[test]
    fn test_watch_set_take_sequence() {
        let mut watch: VecDeque<String> = VecDeque::new();
        watch.push_back("a".into());
        watch.push_back("b".into());
        watch.push_back("c".into());

        assert_eq!(watch.pop_front().as_deref(), Some("a"));
        assert_eq!(watch.pop_front().as_deref(), Some("b"));
        assert_eq!(watch.pop_front().as_deref(), Some("c"));
        assert!(watch.is_empty());
    }

    #[test]
    fn test_tube_stats_defaults() {
        let stats = TubeStats::default();
        assert_eq!(stats.total_reserve_ct, 0);
        assert_eq!(stats.total_timeout_ct, 0);
        assert_eq!(stats.total_bury_ct, 0);
        assert_eq!(stats.processing_time_samples, 0);
        assert_eq!(stats.processing_time_ewma, 0.0);
        assert!(stats.processing_time_min.is_none());
        assert!(stats.processing_time_max.is_none());
    }

    fn job(id: u64, pri: u32, con: Option<&str>) -> Job {
        let mut job = Job::new(
            id,
            pri,
            Duration::ZERO,
            Duration::from_secs(60),
            Vec::new(),
            "test",
        );
        if let Some(key) = con {
            job.set_ext(JobExt {
                concurrency_key: Some((key.to_string(), 1)),
                ..Default::default()
            });
        }
        job
    }

    #[test]
    fn test_tube_ready_heap() {
        let mut t = Tube::new("test");
        // Insert jobs with different priorities
        t.ready.insert(&job(1, 5, None));
        t.ready.insert(&job(2, 1, None));
        t.ready.insert(&job(3, 3, None));

        assert!(t.has_ready());
        assert_eq!(t.ready.len(), 3);

        // Priority 1 comes first (job id 2)
        assert_eq!(t.ready.peek(), Some(((1, 2), 2)));
    }

    #[test]
    fn test_ready_queue_heads_follow_inserts_and_removals() {
        let mut q = ReadyQueue::default();
        q.insert(&job(1, 5, Some("a")));
        q.insert(&job(2, 3, Some("a"))); // better than job 1: becomes a's head
        q.insert(&job(3, 9, Some("a")));
        q.insert(&job(4, 4, None));
        q.assert_consistent();
        assert_eq!(q.len(), 4);
        assert_eq!(q.peek(), Some(((3, 2), 2)));

        // Removing the head promotes the key's next best job.
        assert_eq!(q.remove_by_id(2), Some(((3, 2), 2)));
        q.assert_consistent();
        assert_eq!(q.peek(), Some(((4, 4), 4)));
        assert_eq!(q.best_unblocked(|_| false), Some(((4, 4), 4)));

        // Removing a job behind the head leaves the head alone.
        assert_eq!(q.remove_by_id(3), Some(((9, 3), 3)));
        q.assert_consistent();
        assert_eq!(q.remove_by_id(3), None);

        // The key's last job frees the key.
        q.remove_by_id(1);
        q.assert_consistent();
        assert!(q.keys.is_empty());
        assert_eq!(q.ids(), vec![4]);

        // A job already queued is not queued twice.
        q.insert(&job(4, 0, None));
        assert_eq!(q.len(), 1);
        assert_eq!(q.peek(), Some(((4, 4), 4)));

        q.clear();
        assert!(q.is_empty());
        assert_eq!(q.peek(), None);
    }

    #[test]
    fn test_best_unblocked_skips_a_blocked_key_in_one_step() {
        let mut q = ReadyQueue::default();
        for id in 1..=1000 {
            q.insert(&job(id, 0, Some("a")));
        }
        q.insert(&job(1001, 0, Some("b")));
        q.insert(&job(1002, 0, None));
        q.assert_consistent();

        let asked = std::cell::Cell::new(0);
        let blocked = |id: u64| {
            asked.set(asked.get() + 1);
            id <= 1000
        };
        assert_eq!(q.best_unblocked(blocked), Some(((0, 1001), 1001)));
        assert_eq!(asked.get(), 2, "a's head, then b's head: not a's backlog");

        // peek-ready still shows the blocked job at the top.
        assert_eq!(q.peek(), Some(((0, 1), 1)));
        // With every key blocked, the untagged job is next.
        assert_eq!(q.best_unblocked(|id| id <= 1001), Some(((0, 1002), 1002)));
        // And with nothing unblocked at all, there is nothing to reserve.
        q.remove_by_id(1002);
        assert_eq!(q.best_unblocked(|_| true), None);
    }

    #[test]
    fn test_best_unblocked_prefers_a_better_untagged_job() {
        let mut q = ReadyQueue::default();
        q.insert(&job(1, 5, Some("a")));
        q.insert(&job(2, 1, None));
        // The untagged job wins without consulting the key at all.
        assert_eq!(
            q.best_unblocked(|_| panic!("no key needed")),
            Some(((1, 2), 2))
        );
    }

    #[test]
    fn test_tube_with_reserved_jobs_not_idle() {
        let mut t = Tube::new("test");
        assert!(t.is_idle());
        t.stat.reserved_ct = 1;
        assert!(!t.is_idle());
        t.stat.reserved_ct = 0;
        assert!(t.is_idle());
    }

    #[test]
    fn test_tube_pause() {
        let mut t = Tube::new("test");
        assert!(!t.is_paused());

        t.pause = Duration::from_secs(60);
        t.unpause_at = Some(Instant::now() + Duration::from_secs(60));
        assert!(t.is_paused());
    }

    /// The idle reaper is the only caller of `is_idle`, so a drained but
    /// still-paused tube must report non-idle — otherwise the pause is
    /// collected along with the tube and work arriving next is reservable
    /// at once.
    #[test]
    fn test_drained_but_paused_tube_is_not_idle() {
        let mut t = Tube::new("test");
        assert!(t.is_idle(), "empty unpaused tube is idle");

        t.pause = Duration::from_secs(60);
        t.unpause_at = Some(Instant::now() + Duration::from_secs(60));
        assert!(!t.is_idle(), "a live pause holds a drained tube open");

        // An expired pause must not pin the tube forever.
        t.unpause_at = Some(Instant::now() - Duration::from_secs(1));
        assert!(!t.is_paused());
        assert!(t.is_idle(), "expired pause releases the tube to the reaper");
    }
}
