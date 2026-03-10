/// Connection type bitmask flags.
pub const CONN_TYPE_PRODUCER: u8 = 1;
pub const CONN_TYPE_WORKER: u8 = 2;
pub const CONN_TYPE_WAITING: u8 = 4;

pub const MAX_TUBE_WEIGHT: u32 = 9999;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveMode {
    Fifo,
    Weighted,
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
    pub watched: Vec<WatchedTube>,
    pub reserve_mode: ReserveMode,
    pub conn_type: u8,
    pub reserved_jobs: Vec<u64>,
}

impl ConnState {
    pub fn new(id: u64) -> Self {
        ConnState {
            id,
            use_tube: "default".to_string(),
            watched: vec![WatchedTube {
                name: "default".to_string(),
                weight: 1,
            }],
            reserve_mode: ReserveMode::Fifo,
            conn_type: 0,
            reserved_jobs: Vec::new(),
        }
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

    pub fn watches_tube(&self, name: &str) -> bool {
        self.watched.iter().any(|w| w.name == name)
    }
}
