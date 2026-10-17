use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

/// Connection type bitmask flags.
pub const CONN_TYPE_PRODUCER: u8 = 1;
pub const CONN_TYPE_WORKER: u8 = 2;
pub const CONN_TYPE_WAITING: u8 = 4;

pub const MAX_TUBE_WEIGHT: u32 = 9999;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveMode {
    Fifo,
    Weighted,
    WeightedFair,
}

/// A watched tube entry with its weight for weighted reserve mode.
#[derive(Debug, Clone)]
pub struct WatchedTube {
    pub name: String,
    pub weight: u32,
}

/// Engine-side connection state. The actual TCP socket is handled by
/// a separate tokio task; this struct tracks the logical state.
#[derive(Debug)]
pub struct ConnState {
    pub id: u64,
    pub use_tube: String,
    /// The tubes watched, keyed by when each was watched, so they keep that
    /// order (`list-tubes-watched` reports it). See [`ConnState::watched`].
    watched: BTreeMap<u64, WatchedTube>,
    /// Each watched tube's key in `watched`, so `watch` and `ignore` find a
    /// tube without walking the list. Walking it made watching N tubes one
    /// at a time take O(N²) time: 41 s for 200,000.
    watched_at: HashMap<String, u64>,
    next_watch_key: u64,
    pub reserve_mode: ReserveMode,
    pub conn_type: u8,
    pub reserved_jobs: Vec<u64>,
    /// Cached *lower bound* on the deadline of every job in `reserved_jobs`.
    ///
    /// Invariant: `Some(m)` implies `m <= job.deadline_at` for all held jobs.
    /// `None` means "unknown". It is deliberately a bound and not an exact
    /// minimum, which is what makes it cheap to maintain: the only event that
    /// can lower a held job's deadline is reserving a *new* job, so that is the
    /// single site obliged to update it (`do_reserve_inner`). Removing a job or
    /// touching one can only raise the true minimum, leaving the bound merely
    /// stale-low — which costs an unnecessary scan, never a missed expiry, and
    /// self-heals the next time `tick` rescans.
    ///
    /// Lets `tick` skip an idle connection and `conn_deadline_soon` answer in
    /// O(1) instead of walking every reserved job; both were O(n) per call,
    /// which `reserve-batch` turned from trivial into the dominant cost.
    pub min_deadline: Option<Instant>,
    /// A schedule sync this connection has begun and not yet committed or
    /// aborted. Dropped with the connection, which aborts it.
    pub schedule_sync: Option<crate::schedule::Staging>,
}

impl ConnState {
    pub fn new(id: u64) -> Self {
        let mut conn = ConnState {
            id,
            use_tube: "default".to_string(),
            watched: BTreeMap::new(),
            watched_at: HashMap::new(),
            next_watch_key: 0,
            reserve_mode: ReserveMode::Fifo,
            conn_type: 0,
            reserved_jobs: Vec::new(),
            min_deadline: None,
            schedule_sync: None,
        };
        conn.watch("default", 1);
        conn
    }

    pub fn is_producer(&self) -> bool {
        self.conn_type & CONN_TYPE_PRODUCER != 0
    }

    pub fn is_worker(&self) -> bool {
        self.conn_type & CONN_TYPE_WORKER != 0
    }

    pub fn is_waiting(&self) -> bool {
        self.conn_type & CONN_TYPE_WAITING != 0
    }

    pub fn set_producer(&mut self) {
        self.conn_type |= CONN_TYPE_PRODUCER;
    }

    pub fn set_worker(&mut self) {
        self.conn_type |= CONN_TYPE_WORKER;
    }

    /// The tubes this connection watches, in the order it watched them.
    pub fn watched(&self) -> impl ExactSizeIterator<Item = &WatchedTube> {
        self.watched.values()
    }

    pub fn watches_tube(&self, name: &str) -> bool {
        self.watched_at.contains_key(name)
    }

    /// Watch `name` with `weight`, or set its weight if it is watched
    /// already. True if it wasn't watched before.
    pub fn watch(&mut self, name: &str, weight: u32) -> bool {
        if let Some(w) = self
            .watched_at
            .get(name)
            .and_then(|key| self.watched.get_mut(key))
        {
            w.weight = weight;
            return false;
        }
        let key = self.next_watch_key;
        self.next_watch_key += 1;
        let name = name.to_string();
        self.watched_at.insert(name.clone(), key);
        self.watched.insert(key, WatchedTube { name, weight });
        true
    }

    /// Stop watching `name`. True if it was watched.
    pub fn ignore(&mut self, name: &str) -> bool {
        match self.watched_at.remove(name) {
            Some(key) => self.watched.remove(&key).is_some(),
            None => false,
        }
    }
}
