# Future Features & Improvements

Ideas sourced from open issues and pain points in the original [beanstalkd](https://github.com/beanstalkd/beanstalkd).

## Done

### `flush-tube <tube>`

([#25](https://github.com/beanstalkd/beanstalkd/issues/25)) — Atomically drain all jobs (ready, delayed, buried) from a tube.

```
flush-tube <tube>\r\n
```

Response: `FLUSHED <count>\r\n`

### `put` Parameter Extensions

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

Limit concurrent reserved jobs sharing a key (e.g. one-at-a-time per API endpoint). Jobs with a `con:` key are skipped during reserve if another job with the same key is already reserved.

### `reserve-mode <default|weighted>`

Switch between priority-first and weighted-random reserve strategies. Used with optional weight parameter on `watch`.

### Job Processing Duration Tracking

`reserved_at: Option<Instant>` on `Job`. Set on reserve, cleared on release/bury. On `delete`, computes processing time and total latency. Feeds into per-tube aggregate stats.

### Per-Tube Throughput Counters

`TubeStats` tracks `total_reserve_ct`, `total_timeout_ct`, and `total_bury_ct`, incremented at state transition points.

### Per-Tube Processing Time Stats (EWMA)

Tracks min/max/average processing time per tube using an exponentially weighted moving average. Updated on each successful delete. Extended with dual EWMA (fast/slow split at 100ms), percentiles (p50/p95/p99 from last 1000 slow samples), and bury rate.

### Enhanced `stats-tube` Output

([#37](https://github.com/beanstalkd/beanstalkd/issues/37)) — Includes processing time stats (EWMA, min, max, samples), dual EWMA (fast/slow), percentiles, bury rate, and throughput counters.

### Prometheus Metrics Endpoint

`/metrics` HTTP endpoint in Prometheus text format. Enabled via `--metrics-port <port>`. Exposes job gauges, tube stats, connection count, and uptime.

### Idempotency Tombstones

Post-completion idempotency via tombstone with TTL (`idp:key:N`) — prevents re-insertion of a recently completed job for N seconds after deletion. Tombstones are persisted in the WAL and restored on replay.

## Partially Done

### Command Counters

Structure exists (`op_ct` array in `GlobalStats`) and is wired into `stats` output, but counters are not yet incremented in command handlers.

### WAL Fsync Mode

Done: `--wal-sync-interval` (env `TUBER_WAL_SYNC_INTERVAL`) exposes the fsync cadence. `0` means per-write (strongest durability); a positive duration bounds how much committed state can be lost on crash. Writes are buffered through a 64 KiB `BufWriter` so the syscall count stays low in all modes.

## Future

### `list-jobs <tube> [state]`

([#45](https://github.com/beanstalkd/beanstalkd/issues/45)) — Enumerate jobs in a tube, optionally filtered by state (ready, delayed, buried, reserved). Replaces the limited `peek-buried` / `peek-ready` / `peek-delayed` which only return one job each.

```
list-jobs <tube> [ready|delayed|buried|reserved]\r\n
```

### Bulk Retrieval

([#427](https://github.com/beanstalkd/beanstalkd/issues/427)) — Reserve/peek multiple jobs at once. Needs a concrete command design (e.g. `reserve-batch <count>`, `peek-batch <tube> <state> <count>`).

### zstd Body Compression (Server-Side)

Compress job bodies in memory and in the WAL using zstd. Decompress transparently on `reserve`/`peek`. Invisible to clients — the protocol stays plain text.

- zstd is fast enough (>1 GB/s compress, >3 GB/s decompress) to add negligible latency
- JSON/text bodies (the common case) typically compress 3–5x
- Reduces both memory footprint and WAL disk usage
- Skip compression for small bodies (e.g. <64 bytes) where overhead exceeds savings
- Could be a server flag: `--compress` or `--compress-min-size <bytes>`

### OIDC / Bearer Token Authentication

Add optional authentication to the text protocol via an `AUTH` command:

```
AUTH bearer <token>\r\n
```

Response: `OK\r\n` or `UNAUTHORIZED\r\n`

**Design:**

- Per-connection auth — validate once at connect, all commands return `UNAUTHORIZED` until `AUTH` succeeds
- Server validates JWTs locally using the OIDC provider's JWKS (fetched at startup, refreshed periodically)
- No per-command round-trip to the IdP — just signature + claims checks (`exp`, `iss`, `aud`)
- Rust crates: `jsonwebtoken` for validation, `reqwest` for JWKS fetching
- Server flags: `--oidc-issuer <url>` and `--oidc-audience <aud>`

**Optional authorization via JWT scopes/claims:**

| Scope | Capabilities |
|---|---|
| `tuber:producer` | `use`, `put` |
| `tuber:worker` | `watch`, `reserve`, `delete`, `release`, `bury`, `touch` |
| `tuber:admin` | `stats`, `flush-tube`, `kick`, `pause-tube` |

Maps naturally onto the existing connection type bitmask (`CONN_TYPE_PRODUCER`, `CONN_TYPE_WORKER`).

**Open questions:**

- Token revocation mid-session: re-validate on a timer, or just let tokens expire naturally?
- Should unauthenticated mode remain the default (auth only when `--oidc-issuer` is set)?

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
