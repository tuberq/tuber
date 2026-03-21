# Changes

## v0.3.7

**Fix WAL state change ref counting causing data loss after GC**

Non-delete state changes (bury/release/kick) incorrectly moved a job's WAL file reference from the FullJob record's file to the StateChange record's file. This allowed GC to delete the file containing the only FullJob, causing silent job loss on WAL replay. A subtle ref-counting bug — the data looked fine at runtime, but jobs vanished after a restart.

## v0.3.6

**Simplify WAL compaction code**

Cleanup pass on the WAL compaction implementation introduced in v0.3.4.

## v0.3.5

**Fix global command counters always showing zero**

Global stats counters (`cmd-put`, `cmd-reserve`, `cmd-delete`, etc.) were never incremented, causing tuber-tui throughput graphs to always show 0. Added `op_ct` increments for all protocol commands and replaced magic number indices with named constants.

## v0.3.4

**Implement WAL compaction**

The WAL grew unboundedly because compaction was never implemented — `maintain()` returned an empty Vec. Ported beanstalkd's self-regulating waste-ratio strategy: compute `waste = (total_space - alive) / alive`, and when ratio >= 2, migrate live jobs from the oldest file to the current file. More waste means more jobs moved per tick. Without this, any long-running server with persistence enabled would eventually exhaust disk.

## v0.3.3

**Include 'tuber' in stats version string**

Allows clients (like tuber-tui) to detect they're connected to tuber rather than vanilla beanstalkd.

## v0.3.2

**Fix per-tube reserved count not decremented on disconnect**

When a connection disconnected, reserved jobs were correctly released back to the ready queue, but the per-tube `reserved_ct` stat was not decremented. This caused `stats-tube` to show ghost reserved jobs that didn't actually exist — misleading for monitoring and debugging.

## v0.3.1

**Add `peek-reserved` command**

New tuber extension to inspect the oldest reserved job in the current tube. Useful for debugging ghost reservations from dead connections.

## v0.3.0

**Add jemalloc allocator, RSS and WAL disk stats**

Switch global allocator to jemalloc for better performance under job alloc/dealloc churn. Added `rusage-maxrss`, `binlog-file-count`, `binlog-total-bytes` to stats output and corresponding Prometheus metrics gauges.

## v0.2.11

**Add extension feature interaction tests and WAL/rusage stats**

Added integration tests for cross-cutting extension interactions: cross-tube concurrency, group + idempotency dedup, delete after-job while group pending, release-with-delay + concurrency. Exposed rusage and WAL disk stats in stats and Prometheus output.

## v0.2.10

**Add `binlog-enabled` field to stats output**

## v0.2.9

**Log version number on startup**

## v0.2.8

**Add `undrain` command**

Complement to `drain` — allows restoring normal operation without a server restart.

## v0.2.7

**Add `drain` command and logging**

Graceful drain mode: reject new `put` commands while allowing workers to finish processing existing jobs. Useful for planned maintenance.

## v0.2.6

**Add `reserve-mode` weighted random strategy**

New `reserve-mode weighted` command allows connections to reserve jobs from watched tubes using a weighted-random strategy instead of strict priority ordering. Tubes can be assigned weights via `watch <tube> <weight>`.

## v0.2.5

**Performance optimizations to close gap with beanstalkd**

- Enable `TCP_NODELAY` on accepted connections (matching beanstalkd)
- Skip `process_queue()` on put when no waiters exist
- Replace O(n) tube name validation with O(1) match expression
- Reduce redundant HashMap lookups in hot paths (put, reserve, delete)
- Use `swap_remove` instead of `retain` for reserved_jobs deletion
- Add `serialize_into()` to reuse write buffer across responses

## v0.2.4

**Add `delete-batch` command**

Bulk delete up to 1000 jobs in a single round-trip. Complementary to `reserve-batch` — clients can now reserve and delete in bulk.

## v0.2.3

**Fix DoS vulnerabilities**

- Cap command line reads at `MAX_LINE_LEN` (891 bytes), preventing unbounded memory growth from clients sending data without newlines
- Check body size against `max_job_size` before allocating the buffer, preventing OOM from malicious `put` commands claiming huge body sizes
- Added 10 fuzz/hardening integration tests

## v0.2.2

**Docker multi-arch builds, CI fixes**

Added multi-arch Docker image builds and updated CI actions for Node.js 24 compatibility.

## v0.2.1

**Add `reserve-batch` command, fix tube name validation**

Bulk reserve up to 100 jobs in a single round-trip. Fixed tube name validation to reject names containing spaces.

## v0.2.0

**Idempotency TTL cooldowns, concurrency limits, WAL tombstones**

Major extension release: idempotency keys with configurable TTL cooldown, concurrency key enforcement for single-job-at-a-time reservations, and WAL tombstone persistence for delete records. Reordered WAL payload fields and added WAL format spec.

## v0.1.x

**Proactive `DEADLINE_SOON`, `stats-group`, graceful shutdown**

Wake waiting clients proactively before their reservation deadline expires. Added `stats-group` command for inspecting job group state. Idempotency state included in responses. Graceful shutdown on SIGTERM.

**Job groups and concurrency keys**

Added `grp:`, `aft:`, and `con:` extension tags on `put` for job orchestration — group tracking with pending/buried counts, after-group dependencies for DAG-style workflows, and concurrency keys for mutual exclusion.

**Fix `process_queue` bug with competing waiters**

Competing waiters on a tube were incorrectly timed out when another waiter received a job. The root cause: the wrong waiter was being removed from the waiting list.

**WAL corruption recovery**

Replaced `unwrap()` calls in WAL deserialization with proper error propagation, allowing the server to recover from partial/corrupt WAL files instead of panicking on startup.

**Core beanstalkd implementation**

Initial Rust implementation of the beanstalkd protocol: TCP server, all standard commands, write-ahead log for persistence, binary min-heap for ready/delay queues, per-connection state management, and integration test suite.
