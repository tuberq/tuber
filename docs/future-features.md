# Future Features & Improvements

Ideas sourced from open issues and pain points in the original [beanstalkd](https://github.com/beanstalkd/beanstalkd).

## Feature Requests from Original Project

- **`flush-tube`** ([#25](https://github.com/beanstalkd/beanstalkd/issues/25)) — atomically drain all jobs (ready, delayed, buried) from a tube. Responds `FLUSHED <count>\r\n`
- **List existing jobs** ([#45](https://github.com/beanstalkd/beanstalkd/issues/45)) — enumerate jobs in a tube
- **Bulk job retrieval** ([#427](https://github.com/beanstalkd/beanstalkd/issues/427)) — reserve/peek multiple jobs at once
- **Reserve by ID / put with ID** ([#379](https://github.com/beanstalkd/beanstalkd/issues/379)) — custom job identifiers
- ~~**Wait command** ([#560](https://github.com/beanstalkd/beanstalkd/issues/560))~~ — superseded by `aft:` groups, which achieve the same thing without blocking a connection
- **Tube size limits** ([#56](https://github.com/beanstalkd/beanstalkd/issues/56)) — cap the number of jobs per tube
- **Tube latency / oldest job age** ([#37](https://github.com/beanstalkd/beanstalkd/issues/37)) — better observability

## beanstalkd-rs Extensions

### Idempotency Keys

Optional 5th parameter on `put` to prevent duplicate jobs on retry:

```
put <pri> <delay> <ttr> <bytes> idp:<key>\r\n
```

- Uses the same `prefix:value` convention as `grp:` and `aft:` — all optional params are order-independent
- Backwards compatible — existing clients omit the key
- Scoped per-tube
- Key lives as long as the job; removed on delete
- Second put with same key returns `INSERTED <original-id>` (not an error — truly idempotent)
- Consider: tombstone with TTL for post-completion idempotency

### Job Groups (Fan-out/Fan-in Orchestration)

Lightweight server-assisted orchestration for the common pattern: one job spawns many children, then a final cleanup job runs after all children complete.

**Protocol:** No new commands — optional trailing parameters on `put`:

```
put <pri> <delay> <ttr> <bytes> [idp:<key>] [grp:<id>] [aft:<id>]\r\n

# Tag child jobs with a group
put 100 0 60 11 grp:batch-42\r\n

# Final job — held until all jobs in batch-42 are deleted
put 100 0 60 14 aft:batch-42\r\n

# Chaining — after job for group A, member of group B
put 100 0 60 14 aft:batch-a grp:batch-b\r\n

# All optional params together (any order)
put 100 0 60 14 idp:my-key grp:batch-42 aft:batch-a\r\n
```

**Server state:** `HashMap<String, GroupState>` tracking pending count and waiting jobs.

**Design decisions:**

- Group is implicitly created on first tagged `put`
- If `aft:<id>` is used with no existing children (pending == 0), the job fires immediately
- Multiple `after` jobs per group are allowed
- Buried jobs block group completion (buried = something went wrong, don't run cleanup)
- Adding more jobs to a group after an `after` job is allowed — counter just increments
- **Chaining:** combine `aft:` and `grp:` on the same job to build DAG-style orchestration with no extra mechanism
- **Cycles** are the client's problem — server just holds waiting jobs indefinitely (same as infinite delay). No deadlock risk for the server.

### Prometheus Metrics Endpoint

Expose a `/metrics` HTTP endpoint in standard Prometheus text format for dashboard/Grafana integration.

- Enabled via optional flag: `-m <port>` (e.g. `-m 9100`)
- Serves on a separate HTTP port from the main beanstalkd protocol port
- Uses `metrics` + `metrics-exporter-prometheus` Rust crates
- Data already available internally from existing `stats` tracking

**Key metrics to expose:**

- `beanstalkd_jobs_total{state}` — gauge per state (ready, reserved, buried, delayed)
- `beanstalkd_tube_jobs{tube, state}` — gauge per tube per state
- `beanstalkd_cmd_total{cmd}` — counter per command type (put, reserve, delete, etc.)
- `beanstalkd_connections` — current connection count
- `beanstalkd_uptime_seconds` — server uptime

**Alternative considered:** sidecar exporter that connects via the text protocol and scrapes `stats`/`stats-tube`. Zero server changes but an extra process to deploy. Built-in is better for a greenfield Rust implementation.

## Known Pain Points (C version) That Rust Helps With

- **Monotonic time** ([#166](https://github.com/beanstalkd/beanstalkd/issues/166)) — `tokio::time::Instant` avoids DST/clock-skew issues
- **Memory safety** — Rust's ownership model eliminates the crash and leak bugs reported in multiple issues
- **Structured logging** ([#378](https://github.com/beanstalkd/beanstalkd/issues/378)) — already covered by `tracing`

## Known Bugs to Avoid

- **File descriptor exhaustion** ([#361](https://github.com/beanstalkd/beanstalkd/issues/361)) — connections hang when server runs out of FDs
- **Binlog growth** ([#622](https://github.com/beanstalkd/beanstalkd/issues/622)) — binlogs grow unbounded when delaying jobs
- **fsync audit** ([#477](https://github.com/beanstalkd/beanstalkd/issues/477)) — ensure correct fsync usage in write-ahead log
