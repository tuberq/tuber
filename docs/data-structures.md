# Data Structures

## Job

```rust
pub enum JobState { Ready, Reserved, Delayed, Buried }

pub struct Job {
    // Identity
    pub id: u64,
    pub tube_name: String,

    // Core properties
    pub priority: u32,
    pub delay: Duration,
    pub ttr: Duration,
    pub body: Vec<u8>,
    pub state: JobState,

    // Timestamps
    pub created_at: Instant,
    pub deadline_at: Option<Instant>,

    // Who has this job reserved (connection ID)
    pub reserver_id: Option<u64>,

    // Counters
    pub reserve_ct: u32,
    pub timeout_ct: u32,
    pub release_ct: u32,
    pub bury_ct: u32,
    pub kick_ct: u32,
}
```

Jobs are stored centrally in `HashMap<u64, Job>` and referenced by ID everywhere
else. This is the Rust equivalent of the C version's global hash table with
pointer-based references.

## IndexHeap

Custom min-heap with O(log n) insert, remove-min, and remove-by-ID.

```rust
pub struct IndexHeap<K: Ord> {
    data: Vec<(K, u64)>,           // (sort_key, id)
    positions: HashMap<u64, usize>, // id -> index in data
}
```

Used for:
- **Tube ready heap**: key = `(priority, job_id)` -- lowest priority first, FIFO within same priority
- **Tube delay heap**: key = `(deadline_nanos, job_id)` -- soonest deadline first

The C version uses a `setpos` callback to track positions; we use an internal
HashMap instead, which is cleaner.

## Tube

```rust
pub struct Tube {
    pub name: String,
    pub ready: IndexHeap<(u32, u64)>,
    pub delay: IndexHeap<(i64, u64)>,
    pub buried: VecDeque<u64>,
    pub waiting_conns: Vec<u64>,
    pub stat: TubeStats,
    pub using_ct: u32,
    pub watching_ct: u32,
    pub pause: Duration,
    pub unpause_at: Option<Instant>,
}
```

## ConnState (engine-side connection tracking)

```rust
pub struct ConnState {
    pub id: u64,
    pub use_tube: String,
    pub watch_tubes: Vec<(String, u32)>,    // (name, weight)
    pub reserve_mode: ReserveMode,
    pub conn_type: u8,                       // PRODUCER | WORKER | WAITING
    pub reserved_jobs: Vec<u64>,
    pub response_tx: mpsc::Sender<Response>,
    pub waiting_tx: Option<oneshot::Sender<Response>>,
}
```

The `waiting_tx` is the key mechanism for blocking reserve: when no job is
available, the engine stores the oneshot sender. When a matching job arrives
(via put or kick), `process_queue()` pops the sender and delivers the response.

## ServerState

```rust
pub struct ServerState {
    pub jobs: HashMap<u64, Job>,
    pub tubes: HashMap<String, Tube>,
    pub conns: HashMap<u64, ConnState>,
    pub next_job_id: u64,
    pub next_conn_id: u64,
    pub max_job_size: u32,
    pub drain_mode: bool,
    pub started_at: Instant,
    pub stats: GlobalStats,
}
```

All state lives here, owned by the single engine task. No `Arc`, no `Mutex`.
