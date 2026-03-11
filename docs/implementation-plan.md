# Implementation Plan

## Phases

### Phase 1: Foundation Data Structures (no networking) -- DONE

**Step 1.1: `src/heap.rs` -- Custom IndexHeap** -- DONE

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

Write unit tests matching all 6 C heap tests. -- DONE (6 tests)

**Step 1.2: `src/job.rs` -- Job struct and state machine** -- DONE

Define `JobState` enum, `Job` struct with all fields. Constants:
`MAX_TUBE_NAME_LEN = 200`, `URGENT_THRESHOLD = 1024`,
`JOB_DATA_SIZE_LIMIT_DEFAULT = 65535`.

No global state -- jobs stored in `ServerState` HashMap.

Write unit tests matching all 8 C job tests. -- DONE (8 tests)

**Step 1.3: `src/tube.rs` -- Tube struct** -- DONE

Define `Tube` with `IndexHeap` for ready/delay, `VecDeque<u64>` for buried,
`Vec<u64>` for waiting connections. Include `TubeStats`, pause fields.

Write unit tests adapted from the 6 C multiset tests. -- DONE (9 tests)

---

### Phase 2: Engine Core (server state, no networking) -- DONE

**Step 2.1: `src/server.rs` -- ServerState** -- DONE

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
- `make_job()`, `enqueue_job()` -- put path -- DONE
- `reserve_job()`, `process_queue()` -- reserve path (FIFO and weighted) -- DONE
- `delete_job()`, `release_job()`, `bury_job()`, `touch_job()` -- DONE
- `kick_jobs()`, `kick_job()` -- kick buried/delayed -- DONE
- `tick()` -- promote delayed jobs, expire TTR, unpause tubes -- DONE

Engine unit tests: 16 tests covering put/reserve, priority ordering, FIFO,
delete, bury/kick, multi-tube, release, touch, drain mode, close releases jobs,
weighted reserve, watch/ignore, kick-job, pause-tube, stats-job, list-tubes.

**Step 2.2: `src/conn.rs` -- ConnState** -- DONE

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

### Phase 3: Networking and Protocol -- DONE

**Step 3.1: `src/protocol.rs` -- Command parsing and response formatting** -- DONE

Define `Command` and `Response` enums covering all commands:
- put, reserve, reserve-with-timeout, reserve-job, delete, release, bury,
  touch, watch (with optional weight), ignore, use, kick, kick-job,
  peek/peek-ready/peek-delayed/peek-buried, stats/stats-job/stats-tube,
  list-tubes/list-tube-used/list-tubes-watched, pause-tube, reserve-mode, quit

Tube name validation: `A-Za-z0-9` plus `-+/;.$_()`, max 200 chars, first
char cannot be `-`.

Parse `put` body separately (read exact byte count + trailing `\r\n`).

Must be wire-compatible with existing beanstalkd clients.

Protocol unit tests: 25 tests covering all command parsing, response
serialization, tube name validation, and edge cases.

**Step 3.2: `src/server.rs` -- TCP listener and connection tasks** -- DONE

The `run()` function:
1. Bind `TcpListener`
2. Spawn engine task (owns `ServerState`, reads from command channel)
3. Accept loop: spawn a connection task per TCP connection

Also provides `run_with_listener()` which accepts a pre-bound `TcpListener`
for testability (bind port 0, get OS-assigned port).

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

**Step 3.3: Protocol edge cases** -- DONE

- `\r\n` line terminator everywhere
- `put` body includes trailing `\r\n` (not counted in body size)
- YAML stats output with `---\n` header
- Time values in protocol are seconds; internal use `Duration`/`Instant`
- Minimum TTR enforced at 1 second

**Step 3.4: `src/lib.rs` -- Library crate for integration tests** -- DONE

Created `src/lib.rs` with public module re-exports so that integration tests
in `tests/` can access server internals. `src/main.rs` uses
`beanstalkd_rs::server` instead of inline `mod` declarations.

**Step 3.5: `tests/integration.rs` -- TCP integration tests** -- DONE

56 integration tests ported from the C `testserv.c`, verifying wire-compatibility
over real TCP connections. Tests organized in three batches:

- **Batch 1 -- Basic protocol** (30 tests): unknown command, peek variants,
  touch/bury/kick-job bad format, delete variants, release variants, tube names,
  two-commands-in-one-packet, job size limits, priority edge cases, list-tubes,
  reserve-mode, watch with weight.

- **Batch 2 -- Job lifecycle** (12 tests): peek-delayed, peek-buried-kick,
  kickjob buried/delayed, stats-job/stats-tube format, TTR large/small,
  reserve-job (ready/delayed/buried/already-reserved).

- **Batch 3 -- Multi-connection & timing** (14 tests): multi-tube producer/consumer,
  reserve timeout two connections, close/quit releases job, delete reserved by other,
  delayed-to-ready transition, pause/unpause tube, TTR deadline-soon,
  weighted reserve empty tube, weighted reserve distribution.

---

### Phase 4: WAL Persistence -- DONE

**Step 4.1: `src/job.rs` -- WAL persistence fields** -- DONE

Added three fields to `Job` struct:
- `wal_file_seq: Option<u64>` -- which WAL file holds latest record
- `wal_used: usize` -- bytes used by this job in WAL (for ref tracking)
- `created_at_epoch: u64` -- seconds since UNIX epoch (for persistence;
  `Instant` is meaningless across restarts)

**Step 4.2: `src/wal.rs` -- Serialization layer** -- DONE

Rust-native WAL format with CRC32 checksums. No C binary compatibility.

On-disk format:
- **File header** (8 bytes): `"TWAL"` magic + version u32 LE = 1
- **FullJob record** (type 0x01): job_id + payload (priority, delay, ttr,
  epoch, state, counters, tube name, body, extension fields) + CRC32
- **StateChange record** (type 0x02): job_id + new_state + new_priority +
  new_delay_nanos + CRC32 (fixed 30 bytes)

State encoding: Ready=0, Reserved=1, Delayed=2, Buried=3, Deleted=0xFF.
Extension fields use `option_string` encoding (u16 len, 0 = None).

Pure serialization functions with unit tests:
- `serialize_full_job()` / `serialize_state_change()`
- `deserialize_record()` with CRC validation
- `estimate_full_job_size()` / `estimate_full_job_size_raw()`
- `write_header()` / `read_header()`

10 unit tests: roundtrip serialization, CRC corruption detection,
option_string encoding, state encoding, header validation, size estimation,
write+replay.

**Step 4.3: `src/wal.rs` -- File management and space reservation** -- DONE

```rust
pub struct Wal {
    dir: PathBuf,
    max_file_size: usize,       // default 10MB
    files: VecDeque<WalFile>,
    next_seq: u64,
    reserved_bytes: u64,        // global reservation for future deletes
    alive_bytes: u64,
    lock_fd: Option<File>,      // held for lifetime (flock)
}
```

- Directory locking via raw `libc::flock` (LOCK_EX | LOCK_NB)
- Segmented log files named `binlog.NNNNNN`
- Auto-rotation when current file exceeds `max_file_size`
- State changes allowed to slightly overflow file size (eliminates
  per-file balancing)

Space reservation (simplified from C's per-file balance/balancerest):
- Single global `reserved_bytes` counter
- On `put`: check free space >= record_size + STATE_CHANGE_RECORD_SIZE,
  return false (→ OUT_OF_MEMORY) if insufficient
- On `delete`/state change: always write, decrement `reserved_bytes`
- Preserves property: full disk can still make forward progress

**Step 4.4: `src/wal.rs` -- Replay and GC** -- DONE

`replay()` reads all binlog files in sequence order:
1. Validate header (magic + version)
2. Read records, validate CRC (skip bad with warning)
3. FullJob records: create/update job (Reserved → Ready on replay)
4. StateChange records: update state or remove (Deleted)
5. Track `refs` per file, `wal_file_seq` per job
6. After replay: create new writable file, return (jobs, next_job_id)

Replay details:
- `deadline_at` reconstructed: Delayed jobs get `Instant::now() + delay`
- `reserver_id` always `None` (reservations don't survive restart)

GC: remove head files with `refs == 0` (never the current writable file).
`maintain()` runs GC + periodic fsync on each tick.

**Step 4.5: `src/server.rs` -- ServerState integration** -- DONE

Added `wal: Option<Wal>` to `ServerState`.

Helper methods:
- `wal_write_put(job_id)` -- serialize full job, write to WAL
- `wal_write_state_change(job_id, state, pri, delay)` -- serialize state change

On WAL I/O error: log, set `self.wal = None` (disable WAL, keep serving).

Integration points in command handlers:
- `cmd_put`: check `reserve_put()` → `OutOfMemory`, then `wal_write_put()`
- `cmd_delete`: `wal_write_state_change(Deleted)` before removing from jobs map
- `cmd_bury`: `wal_write_state_change(Buried)`
- `cmd_release`: `wal_write_state_change(Ready or Delayed)`
- `cmd_kick` / `cmd_kick_job`: `wal_write_state_change(Ready)`
- `tick()`: call `wal.maintain()` for GC and fsync

Updated signatures:
- `run(addr, port, max_job_size, wal_dir: Option<&str>)`
- `run_with_listener(listener, max_job_size, wal_dir: Option<&Path>)`

Startup: if `wal_dir` is Some, open WAL, replay, restore jobs into
ServerState (insert into jobs map, enqueue into tube heaps by state).

Also fixed engine shutdown: `engine_rx.recv()` now breaks on `None` (all
senders dropped) so the engine task exits cleanly when the accept loop
is aborted.

**Step 4.6: `tests/binlog.rs` -- WAL integration tests** -- DONE

10 integration tests, each starting a server with WAL in a tempdir,
performing operations, restarting, and verifying persistence:

1. `test_binlog_basic` -- put, restart, peek finds job, delete works
2. `test_binlog_bury` -- put, reserve, bury, restart, peek-buried finds it
3. `test_binlog_read` -- put to named tube, release with new pri, restart, verify
4. `test_binlog_size_limit` -- put many jobs, verify WAL files created, restart
5. `test_binlog_allocation` -- put 96, delete all, verify no crash
6. `test_binlog_reserved_replayed_as_ready` -- reserved job replays as ready
7. `test_binlog_delayed_job` -- delayed job survives restart
8. `test_binlog_extension_fields` -- idp/grp/aft/con survive restart
9. `test_binlog_put_delete_restart` -- only surviving jobs restored, next ID correct
10. `test_binlog_kick_restart` -- kick state change survives restart

---

### Phase 5: Polish and Compatibility -- NOT STARTED

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
heap.rs          (no deps)          -- DONE
job.rs           (no deps)          -- DONE
tube.rs          (heap.rs)          -- DONE
conn.rs          (job.rs, tube.rs)  -- DONE
protocol.rs      (job.rs)           -- DONE
server.rs        (all above)        -- DONE
lib.rs           (all modules)      -- DONE
wal.rs           (job.rs, server.rs) -- DONE
```

## Crate Dependencies

```toml
tokio = { version = "1", features = ["full"] }   # async runtime
bytes = "1"                                        # byte buffer utilities
clap = { version = "4", features = ["derive"] }    # CLI parsing
tracing = "0.1"                                    # structured logging
tracing-subscriber = "0.3"                         # log output
crc32fast = "1"                                    # WAL record checksums
libc = "0.2"                                       # flock for WAL directory locking
```

Dev dependencies:
```toml
tempfile = "3"                                     # temp directories for WAL tests
```

Note: `rand` was originally planned for weighted tube selection but was not
needed -- a simple xorshift PRNG using server uptime as seed provides
sufficient randomness without an external dependency.

## Key Design Decisions

1. **Single engine task** -- all state owned by one task, no Arc/Mutex
2. **Jobs referenced by u64 ID** -- no Rc/RefCell, clean ownership
3. **Custom IndexHeap** -- std BinaryHeap lacks remove-by-index
4. **HashMap for job storage** -- replaces C custom hash table
5. **VecDeque for buried lists** -- replaces C doubly-linked lists
6. **Weighted reserve first-class** -- built into process_queue() from day one
7. **WAL deferred** -- core queue works without persistence
8. **Wire-compatible** -- existing beanstalkd clients work unmodified
9. **`run_with_listener()` for testability** -- tests bind port 0 and pass in the listener
10. **`max_job_size` passed to connection handler** -- avoids hardcoded limit

## Test Summary

| Module | Unit Tests | Integration Tests |
|--------|-----------|-------------------|
| heap.rs | 6 | -- |
| job.rs | 8 | -- |
| tube.rs | 9 | -- |
| protocol.rs | 25 | -- |
| server.rs | 16 | -- |
| wal.rs | 10 | -- |
| integration | -- | 56 |
| binlog | -- | 10 |
| **Total** | **74** | **66** |
| **Grand Total** | | **140** |

Note: `cargo test` reports 87 unit tests (some modules have additional
tests beyond the C reference) plus 72 integration tests = **159 total**.
