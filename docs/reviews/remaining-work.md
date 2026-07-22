# Remaining work from the 2026-06-11 full-app review

Context: a four-agent review of the whole codebase found ~30 issues. The
critical/high tier was fixed in commits `853532a`, `c8e13d2`, `a002293`,
`8297981` (tube GC stranding, reserve-path job loss, Prometheus exposition,
WAL GC sync-ordering, directory fsyncs, zero-byte TOAST segment repair).
Line numbers below are approximate — re-grep before editing.

## High value, do first

1. **`tuber work` double-execution** (`src/cmd_work.rs:58-71`)
   - No `touch` while a job's shell command runs: any job whose runtime
     exceeds TTR is timed out server-side and re-run by another worker.
     Spawn a touch loop (e.g. every ttr/2) while the child runs.
   - Delete/bury responses are discarded — `NOT_FOUND` after a TTR
     timeout is logged as success. Check responses; log loudly.
   - Watch/ignore responses unchecked (`:44`) — a failed `watch` leaves the
     worker executing jobs from `default` instead of the intended tube.
   - Lifecycle: workers die permanently on any I/O error and `run()` then
     exits 0 (`:21-31`); add reconnect/backoff and a non-zero exit on total
     failure. Ctrl-C kills the in-flight child (job gets *buried* on the
     non-zero exit instead of released); SIGTERM not handled at all.

2. **Protocol desync on bad `put` extension tags** (`src/protocol.rs:325-374`,
   `src/server.rs:~3790`)
   - Tag validation (`idp:`/`grp:`/`aft:`/`con:`) fails *after* `<bytes>` is
     parsed, but the body is never drained, so the client's own payload is
     parsed as commands (e.g. a body containing `delete 42\r\n` executes).
   - Fix shape: on BAD_FORMAT where `bytes` is known, drain body + CRLF like
     the JOB_TOO_BIG path (`src/server.rs:~3804-3816`) already does.

3. **`TUBER_SYNC_INTERVAL` env var breaks in-memory startup** (`src/main.rs:74-83`, `:222-230`)
   - clap `requires = "binlog_dir"` fires on env-provided values, so
     `TUBER_SYNC_INTERVAL=50ms tuber server` refuses to start without `-b`.
     Verified live. Worse: the `TUBER_WAL_SYNC_INTERVAL` deprecation shim
     copies the old var into the new one before parsing, so legacy
     deployments with it exported can't run in-memory at all.
   - Fix: drop the `requires` and validate after parse (warn or error only
     when the flag was explicitly set *and* persistence is off).

4. **`concurrency_limits` unbounded leak** (`src/server.rs:~1008`, cleanup at `:~593`)
   - Every `con:<key>` put registers a limit; removal only happens when an
     *acquired* count drops to 0. Jobs deleted/flushed while never reserved
     leak the entry forever. Unbounded distinct keys → unbounded memory.
   - Related semantics bug: mixed limits take `max()`, so `con:api:1`
     exclusivity is violated by a sibling `con:api:5`; limit also re-derives
     order-dependently after the count hits 0. Decide policy (min wins? per-
     job limit?) before fixing the leak.

## Medium — durability/correctness polish

5. **TOAST body headers have no checksum** (`src/body_store.rs:~829-848`)
   - One flipped `len` mid-segment silently drops every later body in the
     segment, and if it's the current segment, new appends overwrite live
     bodies. The reserved 4 bytes in the 20-byte body header can hold a
     header CRC. Needs a TOAST version bump or backward-compatible scheme
     (0 = no checksum).

6. **Sealed-segment WAL corruption truncates destructively** (`src/wal.rs:~1321-1336`)
   - Any record error in *any* file `set_len`s at the bad offset. Right for
     the live tail; irreversible loss for older sealed segments (dropped
     delete records resurrect jobs). Only auto-truncate the final segment;
     quarantine/warn for sealed ones. Also adjust `bytes_written`/
     `total_disk_bytes` after truncation (budget overcount).

7. **Crash mid-compaction double-count** (`src/body_store.rs:~202-240`)
   - If the process dies after migration fsync but before `remove_file`,
     restart scans both copies: global + per-segment `live_bytes` double-
     counted, and the index overwrite doesn't decrement the superseded
     segment's `live_bytes`, pinning it below the compaction threshold
     forever. Fix: on duplicate body_id during scan, decrement the older
     segment's live_bytes and don't double-count globally.

8. **Idempotency tombstones don't survive WAL GC + restart** (`src/wal.rs:~1258-1273`)
   - Replay rebuilds a tombstone only if the FullJob record (carrying
     tube+key) still exists, but the delete decrefs that file so GC can
     unlink it before the TTL expires. Encode tube+key in the delete record
     (WAL version bump) or pin the FullJob ref until tombstone expiry.

9. **Strict-mode ack ordering** (`src/server.rs:~2818`, doc at `:~3417`)
   - `process_queue` sends `RESERVED` to waiters synchronously inside
     command handling, i.e. *before* the batch fsync — violating strict
     mode's "ack ⇒ on disk" for reserve acks (the doc comment claims
     otherwise). Also `sync_wal()` failure still drains pending `INSERTED`
     acks (`:~3644`) — should fail them with INTERNAL_ERROR.

10. **`flush-tube` misses after-group-held jobs** (`src/server.rs:~1502-1514`)
    - Held `aft:` jobs live only in `GroupState::waiting_jobs`, so they
      survive the flush while the idp map is cleared. Also empty
      `GroupState` entries for `aft:`-only names leak (`:~1030-1038`).

11. **`urgent_ct` drift on idp priority upgrade** (`src/server.rs:~896-901`)
    - Increment happens regardless of job state; delayed/buried dup-upgrades
      double-count when later promoted/kicked. Only adjust when Ready.

## Lower priority

- Parked `reserve` never notices client disconnect (`src/server.rs:~3852`) —
  select over reply_rx + read-half closure.
- `process_queue` is O(all tubes) per fulfilled waiter with String clones
  (`src/server.rs:~2790`) — hot path, client-inflatable.
- Delayed jobs restart their full delay on WAL replay (`src/wal.rs:~533`) —
  use `created_at_epoch` to bound remaining delay.
- v3/v4 WAL migration re-runs every restart (`src/server.rs:~3125`) — rewrite
  migrated jobs via `wal.write_put` post-replay to converge in one restart.
- `pause-tube` prefix matching accepts `pause-tubedefault` (`src/protocol.rs:~285`).
- CLI `put` default priority is 0, most urgent (`src/main.rs:~151`) — convention is ~1024.
- client.rs: no timeouts anywhere; `reserve_with_timeout` panics on malformed
  response (`src/client.rs:~80`); trailing CRLF after bodies never verified.
- Per-connection `resp_buf` pins the largest body's capacity forever
  (`src/server.rs:~3755`).
- `drain` is an unauthenticated client command affecting all tubes.
- Over-long/non-UTF-8 lines close the connection instead of BAD_FORMAT +
  resync (beanstalkd divergence).
- O(all jobs) scans in `peek-reserved` / `stats-group` / flush reserved scan —
  consider a per-tube reserved index.
- metrics HTTP: no read timeout, unbounded `read_line`, N+2 serial round
  trips per scrape; binds to the main listener's IP (0.0.0.0 default).

## Docs / housekeeping

- ~~CLAUDE.md says "WAL version: 5" but code writes v6~~ — fixed; CLAUDE.md
  and `src/wal.rs` both say v7 now. Keep them in sync when the version bumps.
- Test-coverage gaps worth closing: kill-9 test around WAL compaction+GC,
  TTR expiry waking a blocked reserve on another connection, end-to-end
  background TOAST compaction (live ratio < 0.5 → compacts → survives
  restart), CLI layer (`client.rs`, `cmd_*.rs`) has zero tests.
- Untracked files predating this work: `autoresearch-throughput.md` and
  `review_options.md` — decide whether to commit or ignore.
  (`docs/batch-limits.md` and `docs/reviews/` have since been committed.)
