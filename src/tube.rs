use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime};

use crate::heap::IndexHeap;

/// Ready heap key: (priority, job_id). Lower priority = higher urgency.
pub type ReadyKey = (u32, u64);

/// Delay heap key: (deadline_at, job_id). Instants are Copy + Ord.
pub type DelayKey = (Instant, u64);

#[derive(Debug, Default, Clone)]
pub struct TubeStats {
    pub urgent_ct: u64,
    pub waiting_ct: u64,
    pub buried_ct: u64,
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

    pub processing_time_ring_slow: VecDeque<f64>,

    // Queue time (put-to-reserve)
    pub queue_time_ewma: f64,
    pub queue_time_min: Option<f64>,
    pub queue_time_max: Option<f64>,
    pub queue_time_samples: u64,
}

const SLOW_RING_CAPACITY: usize = 1000;

impl TubeStats {
    pub fn update_ewma(ewma: &mut f64, samples: &mut u64, value: f64, alpha: f64) {
        *samples += 1;
        if *samples == 1 {
            *ewma = value;
        } else {
            *ewma = alpha * value + (1.0 - alpha) * *ewma;
        }
    }

    pub fn record_slow_sample(&mut self, secs: f64) {
        if self.processing_time_ring_slow.len() >= SLOW_RING_CAPACITY {
            self.processing_time_ring_slow.pop_front();
        }
        self.processing_time_ring_slow.push_back(secs);
    }

    pub fn bury_rate(&self) -> f64 {
        if self.total_reserve_ct > 0 {
            self.total_bury_ct as f64 / self.total_reserve_ct as f64
        } else {
            0.0
        }
    }

    pub fn slow_percentiles(&self) -> (f64, f64, f64) {
        if self.processing_time_ring_slow.is_empty() {
            return (0.0, 0.0, 0.0);
        }
        let mut sorted: Vec<f64> = self.processing_time_ring_slow.iter().copied().collect();
        sorted.sort_unstable_by(f64::total_cmp);
        let len = sorted.len();
        let p = |pct: f64| -> f64 {
            let idx = ((pct / 100.0) * (len - 1) as f64).round() as usize;
            sorted[idx.min(len - 1)]
        };
        (p(50.0), p(95.0), p(99.0))
    }
}

#[derive(Debug)]
pub struct Tube {
    pub name: String,
    pub ready: IndexHeap<ReadyKey>,
    pub delay: IndexHeap<DelayKey>,
    pub buried: VecDeque<u64>,
    pub waiting_conns: Vec<u64>,
    pub stat: TubeStats,
    pub idempotency_keys: HashMap<String, u64>,
    pub idempotency_cooldowns: HashMap<String, (u64, SystemTime)>,
    pub using_ct: u32,
    pub watching_ct: u32,
    pub pause: Duration,
    pub unpause_at: Option<Instant>,
}

impl Tube {
    pub fn new(name: &str) -> Self {
        Tube {
            name: name.to_string(),
            ready: IndexHeap::new(),
            delay: IndexHeap::new(),
            buried: VecDeque::new(),
            waiting_conns: Vec::new(),
            idempotency_keys: HashMap::new(),
            idempotency_cooldowns: HashMap::new(),
            stat: TubeStats::default(),
            using_ct: 0,
            watching_ct: 0,
            pause: Duration::ZERO,
            unpause_at: None,
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
    pub fn is_idle(&self) -> bool {
        self.ready.is_empty()
            && self.delay.is_empty()
            && self.buried.is_empty()
            && self.waiting_conns.is_empty()
            && self.idempotency_keys.is_empty()
            && self.idempotency_cooldowns.is_empty()
            && self.using_ct == 0
            && self.watching_ct == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tube_new() {
        let t = Tube::new("default");
        assert_eq!(t.name, "default");
        assert!(t.ready.is_empty());
        assert!(t.delay.is_empty());
        assert!(t.buried.is_empty());
        assert!(t.waiting_conns.is_empty());
        assert!(!t.is_paused());
        assert!(t.is_idle());
    }

    // Adapted from cttest_ms_append -- tests watch set add
    #[test]
    fn test_watch_set_add() {
        let mut watch: Vec<String> = Vec::new();
        watch.push("tube1".into());
        watch.push("tube2".into());
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
        let mut watch: Vec<String> = Vec::new();
        watch.push("tube1".into());
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

    #[test]
    fn test_tube_ready_heap() {
        let mut t = Tube::new("test");
        // Insert jobs with different priorities
        t.ready.insert((5, 1), 1);
        t.ready.insert((1, 2), 2);
        t.ready.insert((3, 3), 3);

        assert!(t.has_ready());
        assert_eq!(t.ready.len(), 3);

        // Pop should give us priority 1 first (job id 2)
        let (key, id) = t.ready.pop().unwrap();
        assert_eq!(key.0, 1);
        assert_eq!(id, 2);
    }

    #[test]
    fn test_tube_pause() {
        let mut t = Tube::new("test");
        assert!(!t.is_paused());

        t.pause = Duration::from_secs(60);
        t.unpause_at = Some(Instant::now() + Duration::from_secs(60));
        assert!(t.is_paused());
    }
}
