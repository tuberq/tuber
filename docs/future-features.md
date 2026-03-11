# Future Features & Improvements

Ideas sourced from open issues and pain points in the original [beanstalkd](https://github.com/beanstalkd/beanstalkd).

## New Protocol Commands

### `flush-tube <tube>`

([#25](https://github.com/beanstalkd/beanstalkd/issues/25)) — Atomically drain all jobs (ready, delayed, buried) from a tube.

```
flush-tube <tube>\r\n
```

Response: `FLUSHED <count>\r\n`

### `list-jobs <tube> [state]`

([#45](https://github.com/beanstalkd/beanstalkd/issues/45)) — Enumerate jobs in a tube, optionally filtered by state (ready, delayed, buried, reserved). Replaces the limited `peek-buried` / `peek-ready` / `peek-delayed` which only return one job each.

```
list-jobs <tube> [ready|delayed|buried|reserved]\r\n
```

### Bulk Retrieval

([#427](https://github.com/beanstalkd/beanstalkd/issues/427)) — Reserve/peek multiple jobs at once. Needs a concrete command design (e.g. `reserve-batch <count>`, `peek-batch <tube> <state> <count>`).

## `put` Parameter Extensions

Optional trailing parameters on `put`, using a `prefix:value` convention. All are order-independent and backwards compatible — existing clients simply omit them.

```
put <pri> <delay> <ttr> <bytes> [idp:<key>] [grp:<id>] [aft:<id>] [con:<key>]\r\n
```

**Key constraints:** All key values follow the same rules as tube names: 1–200 characters, same valid character set.

**Immutability:** All prefix keys are set at creation and cannot be changed on `release`. Priority and delay remain the only mutable operational knobs.

| Tag | Phase | Purpose |
|---------|-------|-------------------|
| `idp:` | put time | "Does this job already exist?" |
| `grp:` | delete time | "Decrement group counter" |
| `aft:` | state transition | "Is my group done yet?" |
| `con:` | reserve time | "Is this resource available?" |

They compose freely. For example, `grp:batch-1 con:grumpy-api` means three jobs in a group will process sequentially through the grumpy API — the `aft:` job fires when all three are deleted. Each feature just does its own bookkeeping at its own phase.

### `idp:` — Idempotency Keys

Prevent duplicate jobs on retry. Scoped per-tube.

```
put 100 0 60 11 idp:my-key\r\n
```

- Key lives as long as the job; removed on delete
- Second put with same key returns `INSERTED <original-id>` (not an error — truly idempotent)
- Consider: tombstone with TTL for post-completion idempotency

### `grp:` and `aft:` — Job Groups (Fan-out/Fan-in)

Lightweight server-assisted orchestration: one job spawns many children, then a final cleanup job runs after all children complete.

```
# Tag child jobs with a group
put 100 0 60 11 grp:batch-42\r\n

# Final job — held until all jobs in batch-42 are deleted
put 100 0 60 14 aft:batch-42\r\n

# Chaining — after job for group A, member of group B
put 100 0 60 14 aft:batch-a grp:batch-b\r\n
```

**Server state:** `HashMap<String, GroupState>` tracking pending count and waiting jobs.

**Design decisions:**

- Group is implicitly created on first tagged `put`
- If `aft:<id>` is used with no existing children (pending == 0), the job fires immediately. To build DAG structure before adding children, use `delay` on the `aft:` job to hold it while children are added
- Multiple `after` jobs per group are allowed
- Buried jobs block group completion (buried = something went wrong, don't run cleanup)
- Adding more jobs to a group after an `after` job is allowed — counter just increments
- **Chaining:** combine `aft:` and `grp:` on the same job to build DAG-style orchestration with no extra mechanism
- **Cycles** are the client's problem — server just holds waiting jobs indefinitely (same as infinite delay). No deadlock risk for the server

### `con:` — Concurrency Keys

Referenced in the feature table but not yet designed in detail. Intent: limit concurrent reserved jobs sharing a key (e.g. one-at-a-time per API endpoint).

## Operational Improvements

### Job Processing Duration Tracking

Add `reserved_at: Option<Instant>` to `Job`. Set on reserve, clear on release/bury. On `delete` (successful completion), compute processing time (`reserved_at.elapsed()`) and total latency (`created_at` to deletion). Expose "time-reserved-so-far" via `stats-job` while a job is still reserved. Feed completed durations into per-tube aggregate stats.

### Per-Tube Throughput Counters

Extend `TubeStats` with additional counters incremented at state transition points:

- `total_reserve_ct` — reserves from this tube
- `total_timeout_ct` — TTR timeouts
- `total_bury_ct` — total buries

These are simple `+= 1` additions at existing transition points in `server.rs`.

### Per-Tube Processing Time Stats (EWMA)

Track min/max/average processing time per tube using an exponentially weighted moving average. Updated on each successful delete:

```rust
ewma = alpha * sample + (1.0 - alpha) * ewma
```

No allocations or sliding windows — just a single `f64` field per tube, updated on each job completion. Provides tube-level throughput insight with negligible overhead.

### Command Counters

Wire up the existing `cmd-put`, `cmd-reserve`, `cmd-delete`, etc. fields in the `stats` command output. Each command handler increments its counter on entry. Useful for operational debugging (e.g. many reserves but few deletes = stuck workers).

### Enhanced `stats-tube` Output

([#37](https://github.com/beanstalkd/beanstalkd/issues/37)) — Add tube latency, oldest job age, and the new processing time stats (EWMA, min, max) and throughput counters to `stats-tube` output. Data already available from job creation timestamps and the new per-tube tracking fields.

### Prometheus Metrics Endpoint

Expose a `/metrics` HTTP endpoint in standard Prometheus text format for dashboard/Grafana integration.

- Enabled via optional flag: `-m <port>` (e.g. `-m 9100`)
- Serves on a separate HTTP port from the main beanstalkd protocol port
- Uses `metrics` + `metrics-exporter-prometheus` Rust crates
- Data already available internally from existing `stats` tracking

**Key metrics to expose:**

- `tuber_jobs_total{state}` — gauge per state (ready, reserved, buried, delayed)
- `tuber_tube_jobs{tube, state}` — gauge per tube per state
- `tuber_cmd_total{cmd}` — counter per command type (put, reserve, delete, etc.)
- `tuber_connections` — current connection count
- `tuber_uptime_seconds` — server uptime

**Alternative considered:** sidecar exporter that connects via the text protocol and scrapes `stats`/`stats-tube`. Zero server changes but an extra process to deploy. Built-in is better for a greenfield Rust implementation.

## Known Pain Points (C version) That Rust Helps With

- **Monotonic time** ([#166](https://github.com/beanstalkd/beanstalkd/issues/166)) — `tokio::time::Instant` avoids DST/clock-skew issues
- **Memory safety** — Rust's ownership model eliminates the crash and leak bugs reported in multiple issues
- **Structured logging** ([#378](https://github.com/beanstalkd/beanstalkd/issues/378)) — already covered by `tracing`

## Known Bugs to Avoid

- **File descriptor exhaustion** ([#361](https://github.com/beanstalkd/beanstalkd/issues/361)) — connections hang when server runs out of FDs
- **Binlog growth** ([#622](https://github.com/beanstalkd/beanstalkd/issues/622)) — binlogs grow unbounded when delaying jobs
- **fsync audit** ([#477](https://github.com/beanstalkd/beanstalkd/issues/477)) — ensure correct fsync usage in write-ahead log

## Rejected Ideas

- ~~**Reserve by ID / put with ID** ([#379](https://github.com/beanstalkd/beanstalkd/issues/379))~~ — superseded by `idp:` keys
- ~~**Wait command** ([#560](https://github.com/beanstalkd/beanstalkd/issues/560))~~ — superseded by `aft:` groups
- ~~**Tube size limits** ([#56](https://github.com/beanstalkd/beanstalkd/issues/56))~~ — conflicts with ephemeral tube design
