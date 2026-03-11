# Implementation Plan

## Phases

### Phase 1: Foundation Data Structures (no networking)

**Step 1.1: `src/heap.rs` -- Custom IndexHeap**

Port `heap.c`. Rust's `std::collections::BinaryHeap` lacks remove-by-index,
which is needed for deleting/kicking jobs from tube heaps. Implement:

```rust
pub struct IndexHeap<K: Ord> {
    data: Vec<(K, u64)>,            // (sort_key, id)
    positions: HashMap<u64, usize>,  // id -> index in data
}
```

Methods: `insert`, `remove_by_id`, `peek`, `pop`, `len`, `is_empty`.
The `positions` map replaces the C `setpos` callback.

Write unit tests matching all 6 C heap tests.

**Step 1.2: `src/job.rs` -- Job struct and state machine**

Define `JobState` enum, `Job` struct with all fields. Constants:
`MAX_TUBE_NAME_LEN = 200`, `URGENT_THRESHOLD = 1024`,
`JOB_DATA_SIZE_LIMIT_DEFAULT = 65535`.

No global state -- jobs stored in `ServerState` HashMap.

Write unit tests matching all 8 C job tests.

**Step 1.3: `src/tube.rs` -- Tube struct**

Define `Tube` with `IndexHeap` for ready/delay, `VecDeque<u64>` for buried,
`Vec<u64>` for waiting connections. Include `TubeStats`, pause fields.

Write unit tests adapted from the 6 C multiset tests.

---

### Phase 2: Engine Core (server state, no networking)

**Step 2.1: `src/server.rs` -- ServerState**

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

Implement core operations as methods:
- `make_job()`, `enqueue_job()` -- put path
- `reserve_job()`, `process_queue()` -- reserve path (FIFO and weighted)
- `delete_job()`, `release_job()`, `bury_job()`, `touch_job()`
- `kick_jobs()`, `kick_job()` -- kick buried/delayed
- `tick()` -- promote delayed jobs, expire TTR, unpause tubes

**Step 2.2: `src/conn.rs` -- ConnState**

```rust
pub struct ConnState {
    pub id: u64,
    pub use_tube: String,
    pub watch_tubes: Vec<(String, u32)>,  // (name, weight)
    pub reserve_mode: ReserveMode,
    pub conn_type: u8,
    pub reserved_jobs: Vec<u64>,
    pub response_tx: mpsc::Sender<Response>,
    pub waiting_tx: Option<oneshot::Sender<Response>>,
}
```

Weighted reserve mode is first-class: `process_queue()` checks the connection's
`reserve_mode` and either takes from the first watched tube with a ready job
(FIFO) or selects proportionally based on weights (weighted).

---

### Phase 3: Networking and Protocol

**Step 3.1: `src/protocol.rs` -- Command parsing and response formatting**

Define `Command` and `Response` enums covering all commands:
- put, reserve, reserve-with-timeout, reserve-job, delete, release, bury,
  touch, watch (with optional weight), ignore, use, kick, kick-job,
  peek/peek-ready/peek-delayed/peek-buried, stats/stats-job/stats-tube,
  list-tubes/list-tube-used/list-tubes-watched, pause-tube, reserve-mode, quit

Tube name validation: `A-Za-z0-9` plus `-+/;.$_()`, max 200 chars, first
char cannot be `-`.

Parse `put` body separately (read exact byte count + trailing `\r\n`).

Must be wire-compatible with existing beanstalkd clients.

**Step 3.2: `src/server.rs` -- TCP listener and connection tasks**

The `run()` function:
1. Bind `TcpListener`
2. Spawn engine task (owns `ServerState`, reads from command channel)
3. Accept loop: spawn a connection task per TCP connection

Connection task:
1. Read commands via `BufReader` (scan for `\r\n`)
2. For `put`: read body bytes after command line
3. Send `(Command, conn_id, response_tx)` to engine channel
4. Await response, write to socket

Engine task loop:
```rust
loop {
    tokio::select! {
        Some(msg) = cmd_rx.recv() => {
            state.handle_command(msg);
        }
        _ = tick_interval.tick() => {
            state.tick();
        }
    }
}
```

Tick interval: compute from soonest deadline (delayed job promotion, reserved
job TTR expiry, waiting connection timeout, tube unpause).

**Step 3.3: Protocol edge cases**

- `\r\n` line terminator everywhere
- `put` body includes trailing `\r\n` (not counted in body size)
- YAML stats output with `---\n` header
- Time values in protocol are seconds; internal use `Duration`/`Instant`
- Minimum TTR enforced at 1 second

---

### Phase 4: WAL Persistence

**Step 4.1: `src/wal.rs` -- Write-ahead log**

```rust
pub struct Wal {
    pub dir: PathBuf,
    pub filesize: usize,
    pub want_sync: bool,
    pub sync_rate: Duration,
    pub files: VecDeque<WalFile>,
    pub next_seq: i32,
    pub reserved: i64,
    pub alive: i64,
    pub last_sync: Instant,
}
```

Binary format must be compatible with C beanstalkd `Walver = 7` for migration.
On-disk record: `[4-byte namelen][name bytes][Jobrec struct][body bytes]`.

Use `#[repr(C)]` for `Jobrec` to match C struct layout exactly.

WAL hooks into: put (write record), delete (invalidation record), bury/release
(update record), tick (maintenance, fsync). Startup replays log to restore state.

**Step 4.2: Directory locking and file management**

`flock`-based directory locking. File rotation at size limit. Compaction
(garbage collection) when files have enough dead records. fsync on timer.

---

### Phase 5: Polish and Compatibility

- Signal handling: SIGUSR1 for drain mode (`tokio::signal`)
- `-V` verbose flag (multiple levels)
- `-u` user flag (drop privileges)
- Instance ID generation (random hex)
- `rusage` stats (`libc::getrusage`)
- Unix socket support (`-l unix:/path`)
- Exact YAML stats format matching C output

---

## Dependency Order

```
heap.rs          (no deps)
job.rs           (no deps)
tube.rs          (heap.rs)
conn.rs          (job.rs, tube.rs)
protocol.rs      (job.rs)
server.rs        (all above)
wal.rs           (job.rs, server.rs)
```

## Crate Dependencies

```toml
tokio = { version = "1", features = ["full"] }   # async runtime
bytes = "1"                                        # byte buffer utilities
clap = { version = "4", features = ["derive"] }    # CLI parsing
tracing = "0.1"                                    # structured logging
tracing-subscriber = "0.3"                         # log output
rand = "0.8"                                       # weighted tube selection
```

## Key Design Decisions

1. **Single engine task** -- all state owned by one task, no Arc/Mutex
2. **Jobs referenced by u64 ID** -- no Rc/RefCell, clean ownership
3. **Custom IndexHeap** -- std BinaryHeap lacks remove-by-index
4. **HashMap for job storage** -- replaces C custom hash table
5. **VecDeque for buried lists** -- replaces C doubly-linked lists
6. **Weighted reserve first-class** -- built into process_queue() from day one
7. **WAL deferred** -- core queue works without persistence
8. **Wire-compatible** -- existing beanstalkd clients work unmodified
