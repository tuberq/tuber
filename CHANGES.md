# Changes

## v0.13.0

**A paused tube no longer loses its pause when it drains.** `pause-tube maint 300` followed by the tube emptying discarded the pause within one 100 ms tick, and work arriving afterwards was reservable immediately — with nothing logged and nothing in `stats-tube` to show it had happened, because the tube itself was gone.

The idle reaper collects any non-`default` tube that is empty and unused, and `Tube::is_idle()` checked ready/delay/buried/waiting/reserved/idempotency/using/watching — everything except the pause, which lives nowhere but the `Tube` struct. So the pause survived exactly as long as the last job did. That breaks the ordinary shape of the feature: pause a tube while you investigate, let in-flight work finish, and the pause silently expires at the moment it starts mattering. A side effect of the same gap: `pause-tube` on an already-drained tube returns `NOT_FOUND`, since the tube was reaped before the command arrived.

`is_idle()` now treats an unexpired pause as non-idle. Nothing grows without bound — `unpause_at` expires on its own and the next tick reaps the tube then — and the check is last in the `&&` chain, so the clock read only happens for a tube that is otherwise idle.

This diverges from beanstalkd, which refcounts tubes and frees them without consulting the pause (`tube_dref` → `tube_free` → `ms_remove`); verified against 1.13, which drops a drained tube so eagerly that `pause-tube` on it fails outright. Reported from tuber-tui, where a paused-but-drained tube kept vanishing from the display.

Not fixed, and worth knowing for client authors: the reaper still means a tube can disappear between a client's `list-tubes` and its follow-up `stats-tube <name>`, which then answers `NOT_FOUND`. That is inherent to reaping — it is a TOCTOU across two commands, present in beanstalkd for the same reason — so any client that iterates tubes must tolerate `NOT_FOUND` per tube rather than failing the whole poll.

**Idle connections can now be pruned**, with `--conn-idle-timeout <secs>` (off by default).

The connection ceiling below bounds the damage from a client that opens connections and never closes them — storage keeps its descriptors and the process survives — but nothing reclaimed, so those slots stayed held until the client disconnected and everyone else stayed locked out.

Pruning needs care rather than a plain timeout, because the connections most worth keeping are the silent ones. **A worker parked in `reserve` sends nothing at all** — that is the normal steady state for a worker pool on a quiet queue — and **a worker running jobs it reserved is silent for as long as the work takes**. A "no traffic for N seconds" sweep would close exactly those, releasing in-flight jobs back to the queue and presenting as random worker deaths whenever the queue went quiet.

So the decision is split. The connection's own task owns the timer and closes only after the engine confirms the connection holds nothing (not parked in `reserve`, no reserved jobs). Putting the timer in the task rather than in the engine tick makes a parked `reserve` safe structurally rather than by predicate — that task is blocked awaiting its reply, not reading, so its timer cannot fire — and it catches the case an engine-side sweep would miss entirely: a connection that has never sent a command has no `ConnState` at all, since registration is lazy, which makes connect-and-send-nothing both the cheapest way to hold a slot and invisible to anything walking the connection table.

Worth recording for anyone who reaches for it later: the predicate that suggests itself, `!ConnState::is_waiting()`, would not have worked. `CONN_TYPE_WAITING` is defined but never set anywhere, so `is_waiting()` is always false. The authoritative source for "parked in reserve" is the engine's waiter list.

The deadline runs from the last *completed command* rather than the last byte, and covers the `put` body read and the two refused-body drain paths as well as the command line. Each of those was otherwise a bypass: a client could stay immortal by dribbling one byte per period, or by sending a single `put` header and then going quiet. Pruning closes with no reply — the client isn't awaiting one, and an unsolicited line would be read as the response to whatever it sends next — so clients see a clean EOF.

New in `stats` and `/metrics`: `connections-pruned` and `conn-idle-timeout` (`tuber_connections_pruned_total`, `tuber_conn_idle_timeout_seconds`).

Off by default because the period is a judgement about your clients, not about tuber. The awkward case is not the worker pool — those are protected — but a long-lived producer that puts a job rarely: a connection held open for an hourly job is indistinguishable from an abandoned socket. Sizing guidance in [Connection & file-descriptor lifecycle](docs/connection-lifecycle.md).

**Connection ceiling derived from the file-descriptor budget**, with `--max-connections` to override.

There was no cap on concurrent connections, so a flood could take every descriptor in the process. That matters beyond connection availability: TOAST holds one open fd per ~64 MiB segment for the life of the process, so connections and job storage draw on the same pool, and connections winning means failed puts and unreadable bodies.

The ceiling is therefore derived rather than configured — `fd_soft_limit − (toast_segments + wal_files) − 64` — and the storage term is republished every engine tick, so it tracks storage growth instead of being a guess frozen at startup. Verified: writing ~260 MiB of 1 MiB bodies grew TOAST from 0 to 5 segments and lowered the ceiling by exactly 5. Connections yield 1:1 to storage, which is the right priority — a refused connection is a client retry.

Over the ceiling, the socket is accepted (the kernel gives no way to decline) and closed immediately with no reply. No reply is deliberate: the client has sent nothing yet, so an unsolicited line would be read as the response to whatever command it sends next. `--max-connections 0` disables the cap, as does an unreadable `getrlimit`.

New in `stats` and `/metrics`: `fd-soft-limit`, `fd-storage-used`, `fd-connections-used`, `max-connections`, `connections-refused` (`tuber_fd_*` and `tuber_connections_refused_total`). Alert on the *ratio* of used to max, not the raw connection count — the ceiling moves as the body store grows, so a comfortable count can reach the limit without the client population changing.

Full details, including `ulimit -n` sizing that accounts for TOAST, in [Connection & file-descriptor lifecycle](docs/connection-lifecycle.md).

**Running out of file descriptors no longer kills the server.** `accept()` errors were propagated out of the accept loop with `?`, so a single `EMFILE` ended `serve()`, returned to `main`, and exited the process. Reproduced with `ulimit -n 64`: 55 connections were enough to take the server down with `server error: Too many open files (os error 24)`, requiring a restart and a WAL replay.

Accept failures are now non-fatal. `EMFILE`/`ENFILE` back off for 50 ms before retrying — the pending connection keeps the listener readable, so an immediate retry would spin at 100% CPU for the duration of the pressure — and the warning is rate-limited to once every 5 s so a sustained outage doesn't flood the log at the retry rate. Other accept errors, which are properties of a single connection attempt, are logged and skipped.

Verified after the change: the server stays up under a sustained flood, sits at 0.3% CPU while fully exhausted, and recovers on its own once descriptors free up.

This is the backstop rather than the first line of defence — with the connection ceiling above, the cap normally refuses connections long before the process reaches EMFILE. It still matters when the ceiling is disabled (`--max-connections 0`), when `getrlimit` is unreadable, or when the soft limit is lowered at runtime.

**A `put` whose declared `<bytes>` is shorter than the body sent now closes the connection** after replying `EXPECTED_CRLF`, instead of resuming the command loop.

A short declared length desynchronises the stream. The server reads exactly `<bytes>` + 2 and finds no trailer; the excess is an unknown length by definition, so there is no way to locate the next command boundary. beanstalkd replies and keeps parsing, which means the remainder of the *body* gets interpreted as commands — verified before the fix, where `put 0 0 60 2\r\nAAAAA\r\ndelete 999\r\n` executed the delete, and the same shape ran `list-tubes`.

That is a command-injection path for any client that miscounts bytes: JavaScript `.length` counts UTF-16 code units, Ruby `String#length` is not `#bytesize`, Python `str` is not `bytes`. Multi-byte UTF-8 in an attacker-supplied payload makes declared < actual, and everything after an embedded `\r\n` becomes the command stream — `delete <id>`, `flush-tube`, `pause-tube`. The excess cannot be drained (its length is exactly what the client got wrong), so closing is the only safe recovery. It matches what the same code path already did for an over-long command line, and HTTP/1.1 practice on framing errors.

Unchanged, and verified: the stored body is always exactly `<bytes>` long, excess data never enters a job body, and a put that fails the trailer check stores nothing. Over-declaring is also unaffected — the server blocks for the remaining bytes and consumes whatever arrives as body, never executing it.

Client authors should treat `EXPECTED_CRLF` as fatal and reconnect rather than retrying on the same connection. Documented under [`put`](docs/protocol.md) in the protocol reference.

**Default `--max-job-size` raised to 1 MiB** (from beanstalkd's 65535), and **`--max-jobs-size` now defaults to 1 GiB** instead of unlimited.

The per-job bump is overdue: since bodies moved into TOAST, a resident job costs ~512 B of metadata regardless of body size, so the 64 KiB ceiling stopped tracking any real resource. 1 MiB matches where the rest of the ecosystem landed — Kafka's `message.max.bytes` and NATS' `max_payload` are both 1 MiB.

It is deliberately not larger, because the body store fixed the *steady-state* cost and not the *peak* one. A body is still materialised in RAM in full twice in its life: on `put` the server allocates the declared length and reads the body off the socket before the engine or any budget sees it, and on `reserve`/`peek` it is read back out of TOAST into a fresh buffer for the wire. Real peak exposure is `max-job-size × concurrent puts and reserves`, and disk-backed bodies don't help with any of that. The README now documents this, with measurements, under [Sizing `--max-job-size`](README.md#sizing---max-job-size).

Worth knowing for capacity planning: the cost tracks bytes genuinely in flight, not connection count or declared size. Measured at 500 connections with `-z 1m`, idle connections cost ~10 KB each (an 8 KiB read buffer, independent of `--max-job-size`) and 500 stalled `put` headers declaring 1 MiB apiece added 0.6 MiB total — the body buffer is allocated zeroed, so it stays reserved address space until bytes actually arrive. Sending those same 500 bodies for real costs the full 509 MiB. Slow clients are cheap; concurrency is what you budget for.

Raising the per-job cap 16× while the total memory budget defaulted to unlimited would have been a straight 16× increase in how far a default-configured server can run before the OOM killer arrives, so the two changes ship together: `--max-jobs-size` now defaults to 1 GiB and gives you `OUT_OF_MEMORY` backpressure out of the box. With persistence on that's roughly 2M resident jobs, since only metadata counts. Pass `--max-jobs-size 0` for the old unlimited behaviour — 0 was already the "unlimited" sentinel `stats` reported for an unset budget.

Operator notes:

- **Upgrading with a large existing binlog and no explicit `--max-jobs-size`:** the budget is enforced after replay, so a WAL whose live set exceeds 1 GiB in memory will now refuse to start with a diagnostic naming the actual figure rather than silently replaying. Raise the flag or pass `0`.
- Payloads between 64 KiB and 1 MiB that used to be rejected with `JOB_TOO_BIG` are now accepted. If you were relying on the old cap as incidental backpressure, set `-z 65535` explicitly.
- The startup log line now always reports `max-jobs-size`, printing `unlimited` when opted out — previously the field was simply absent when unset, which is ambiguous now that a budget is the default.
- Both flags accept the usual suffixes via env var too (`TUBER_MAX_JOB_SIZE=1m`, `TUBER_MAX_JOBS_SIZE=512m`).

**Startup TOAST-reclamation logs no longer read as crash damage.** The stranded-body line was a `WARN` blaming "a partial put that didn't survive crash", which misdescribed the usual cause and put an alarming line in front of operators on ordinary restarts.

TOAST has no on-disk deletion tombstone: a runtime delete drops the index entry and the bytes stay in the segment until compaction rewrites it. Since `BodyStore::open` rebuilds the index by *scanning* the segment files, every deleted-but-uncompacted body reappears as live on the next start and has to be re-reclaimed. Bodies the retained WAL still names go to the orphan pass; bodies whose WAL segment has already been reclaimed have no reference left anywhere and land in the stranded sweep. Both counts therefore track delete volume since the last compaction, not crashes.

They get large when a segment never rotates. `compaction_candidate` skips the current write segment, so a store that has only ever filled one segment accumulates every dead body until the segment fills or a restart forces the issue — a real deployment showed 109,470 stranded bodies against 81 live ones in a single 39 MiB segment. That reclaim also force-rotates and compacts, so the pass usually frees most of the store's disk in one go; the line now reports how much.

The genuine leak — `write_body` succeeded, the WAL write failed — still lands in the same counter and is indistinguishable from delete garbage on disk, so `reclaimed-stranded-bodies` rate>0 was never the alertable signal the metric help and `docs/wal-format.md` claimed. Both now point at the WAL write error the leak always logs, which is the one signal that discriminates.

**Startup TOAST reclamation now covers both passes and skips segments not worth rewriting.** Two fixes to the same step, which pull in opposite directions on I/O and roughly cancel.

The orphan pass deleted its ids straight out of the index, so the stranded scan — which derives its segment list by walking that index — could never see which segments had lost bytes. Unless a stranded body happened to share a segment with an orphan, the orphan bytes stayed on disk. That's more common than it sounds: orphans skew recent (the WAL must still hold both the `FullJob` and the delete record) and therefore cluster in the current write segment, while strandeds skew old and sit in sealed ones. The waste was bounded at roughly one segment, but it counted against `--max-storage-bytes`, so a restart near the cap could bounce puts with `OUT_OF_STORAGE` until compaction caught up. The pass now hands its segments forward via `delete_many_tracking_segments` and both share one compaction step. The counters stay separate.

In the other direction, that compaction was unconditional: one stranded body in a 64 MiB segment that was 99% live triggered a full rewrite of it, at startup, before the listener was up. It now applies `COMPACTION_LIVE_RATIO_THRESHOLD` — the same bar `compaction_candidate` uses on the background tick — and the forced rotation of the current segment happens only when that segment actually qualifies, so a restart loop no longer mints a segment per boot.

One consequence worth knowing: garbage in a segment above the threshold is now re-counted on every restart. The bodies leave the index each time, the bytes stay, the next scan re-finds them. A stable `reclaimed-stranded-bodies` across boots is therefore expected, not a leak signature — which is why the alerting advice above moved to the WAL write error.

## v0.12.1

**`pause-tube <tube> 0` now unpauses immediately.** It is the documented unpause idiom — what `tuber-cli pause <t> --delay 0` and tuber-tui's `u` key both send — but a zero delay was floored to a full second, so the tube stayed genuinely paused for that second and `stats-tube` reported `pause: 1`.

The floor was a porting slip rather than a deliberate guard. beanstalkd does floor zero to 1 ("Always pause for a positive amount of time, to make sure that waiting clients wake up when the deadline arrives"), but at that point its delay is already in *nanoseconds* — the floor is 1 ns, i.e. resume now. tuber applied the same `max(1)` in seconds, 10⁹× too coarse.

That also explains the mismatched pair reported by clients: `pause` is the stored duration (1 s) while `pause-time-left` computes `unpause_at − now` ≈ 999 ms and truncates to 0. One bug, two readings.

Zero now clears the pause outright instead of using a sentinel — tuber doesn't need beanstalkd's nonzero trick, since its 100 ms tick sweeps unpauses unconditionally and the paused check reads the clock live. `pause-tube` also now runs the ready queue on that path, which it never did: without it a blocked `reserve` waited for the next tick, trading 1000 ms for up to 100 ms rather than being served at once.

`put` and `release` with `delay 0` were never affected — both skip the delay heap entirely on zero. The TTR floor (`ttr 0` → 1 s) is real beanstalkd behaviour and is unchanged.

Note for consumers: `pause` still lags a pause *expiring* by up to one tick, since the tick is what clears it. `pause-time-left` is computed live and remains the only field that answers "paused right now".

## v0.12.0

**`touch-all`** — a one-command heartbeat for the jobs a connection holds, and the missing middle of the batch lifecycle. `reserve-batch` and `delete-batch` already existed; keeping a batch alive was still one `touch` per job.

The need is structural rather than cosmetic. `reserve-batch` starts the TTR clock on all N jobs at the same instant, but a worker processes them serially — so the tail of a large batch expires and silently returns to the queue unless the worker heartbeats. That was N round-trips for something the server can answer in one.

```
touch-all
TOUCHED_ALL 600
```

It takes no id list, because it doesn't need one. The engine already tracks the reserved set per connection, and every exit path — `delete`, `release`, `bury`, TTR timeout, `flush-tube`, disconnect — removes the job from it. A job the worker has already returned is simply absent, so there are no stale ids to reject, no ownership check, and no partial-failure reply to design. Each job keeps its own ttr: deadlines are extended individually, never levelled onto a common value.

The count is the useful part. A worker knows how many jobs it thinks it holds; `TOUCHED_ALL <n>` tells it how many it *actually* holds. A drop means jobs hit their TTR and went back to the queue while it was working — possibly already running elsewhere — which is otherwise invisible, since nothing pushes a "you lost job X" notification. A batch worker heartbeating `600 … 600 … 598` has just learned its TTR is too tight, for free.

`TOUCHED_ALL` is a distinct verb rather than an overloaded `TOUCHED <n>`, so a pipelining client can frame the response stream without lookahead — matching `RESERVED_BATCH` and `DELETED_BATCH`.

**TTR bookkeeping is now O(1) per connection.** Two scans that were trivial when a connection held one job became the dominant cost once `reserve-batch` let it hold a thousand:

- `tick` walked every reserved job of every connection, as scattered hash lookups, every 100 ms.
- `conn_deadline_soon` walked every reserved job on *every* reserve.

Both are now a single comparison against a cached per-connection minimum deadline. Unlike the ready and delay queues this needs no heap: the cache is deliberately a *lower bound* rather than an exact minimum, which is what makes it cheap to maintain. The only event that can lower a held job's deadline is reserving a new job, so that is the single site obliged to update it. Removing a job or touching one can only raise the true minimum, leaving the bound merely stale-low — costing an unnecessary scan, never a missed expiry — and it self-heals the next time `tick` rescans.

No WAL or on-disk format change; `touch` is in-memory and journals nothing. Existing clients are unaffected.

## v0.11.1

**Live memory stats.** `stats` gains five fields reporting real allocator memory, so you can watch actual footprint instead of inferring it. The two memory numbers that already existed are both misleading for this: `rusage-maxrss` is a peak high-water mark that never falls, and `current-jobs-size` is a backpressure estimate (`512 B/job + body`) that can't reflect the real struct footprint — deleting a million jobs moves neither in a way that tracks RSS.

The new fields, read live via `tikv-jemalloc-ctl` (the jemalloc epoch is advanced per `stats` call to refresh them):

- **`mem-allocated-bytes`** — live bytes in use; what the job set actually costs, and the number that drops when you delete jobs.
- **`mem-resident-bytes`** — physical pages jemalloc holds (≈ its share of RSS).
- **`mem-retained-bytes`** — address space held back from the OS but unused: freed-then-kept pages plus fragmentation. This is the memory a delete run frees on paper but doesn't return; `resident − allocated` is the slack.
- **`mem-active-bytes`** — bytes in active pages.
- **`current-rss-bytes`** — live process RSS (`/proc/self/statm` on Linux; falls back to jemalloc's resident estimate elsewhere), complementing the peak-only `rusage-maxrss`.

Surfaced in the `stats` protocol response — so `tuber stats`, a raw `stats` command, and the Prometheus endpoint (new `tuber_mem_*` / `tuber_rss_bytes` gauges, since the metrics server parses the same response) all report them. The standalone `tuber-cli` binary has a fixed field schema and needs its own update to display them.

Adds two crates (`tikv-jemalloc-ctl` + `paste`), pairing with the `tikv-jemallocator` allocator already in use. WAL/protocol formats unchanged.

## v0.11.0

**Replay memory and crash-recovery pass** — peak RSS during WAL replay drops 77%, and two replay bugs that could resurrect or reorder jobs are fixed. WAL format is unchanged from v0.10.0: upgrade in place, no migration, no `--migrate-wal`.

Replay was the worst possible place to be memory-hungry — it runs at startup, exactly when a crash-looping server is most likely to be OOM-killed. Observed in production: a 1.68M-job WAL that fit in ~2.5 GB steady-state could not replay inside a 4 GB container.

Peak RSS restoring an identical 500,000-job WAL, across the series:

| Change | Peak RSS |
| --- | --- |
| v0.10.0 baseline | 990 MB |
| Replay stops holding three copies of the job table | 599 MB |
| `Job` slimmed 336 → 216 bytes | 410 MB |
| Tube names interned as `Arc<str>` | 394 MB |
| Job table stores `Box<Job>` | 229 MB |

Memory:

- **Replay no longer holds up to three full copies of the job table at once.** Missing-body jobs are reaped in place rather than rebuilt via `into_iter().collect()` (which kept two full tables alive simultaneously), and `restore_jobs` adopts the replayed map wholesale, building tube indexes from an id list instead of re-inserting every job into a second table.
- **`Job` shrank from 336 to 208 bytes.** The four extension keys (`idp:`/`grp:`/`aft:`/`con:` put tags) inlined 112 bytes of `Option`s that are almost always all `None`; they now live behind an `Option<Box<JobExt>>`, so tagged jobs allocate the box and plain jobs pay only the 8-byte pointer. Access moves to accessors (`job.group()` etc.), and `set_ext()` never stores an all-None box, so untagged jobs allocate nothing on replay. WAL tracking fields (`wal_file_seq`/`wal_used`) shrank 24 → 8 bytes; they are never serialized and are rebuilt on every replay.
- **Tube names are interned.** Every job owned a private `String` copy of its tube's name — 1.68M copies of `"detailer"` in the production backlog, each a separate heap allocation, re-allocated on every `tube_name.clone()` in the put/TTR/restore paths. `Tube.name` is now the canonical `Arc<str>` and jobs share it, so hot-path clones are refcount bumps rather than mallocs. Replay interns through a small cache, so a replayed job set holds one allocation per tube rather than per job.
- **The job table stores `Box<Job>`.** Slots are now a 16-byte `(u64, Box<Job>)` pair, so the 12–50% of slots a hash table keeps empty and the transient double-table spike during a rehash both shrink ~13x — a spike of hundreds of MB at millions of jobs, hit exactly when the queue is growing fastest. Replay moves the deserializer's existing `Box<Job>` straight into the table instead of copying the struct out of it. Cost is one pointer deref per lookup and one alloc per put; measured put throughput is unchanged (25.2k vs 25.5k puts/sec).

Crash recovery:

- **Truncated older WAL segments are quarantined again, not deleted.** Replay had been deleting *any* sub-header segment with an INFO log. Only the newest segment can be a benign creation-crash artifact; an older sub-header segment was once fsynced past its header, so a short read means a filesystem fault truncated records — and a lost delete record resurrects jobs. Older segments now take the quarantine path (`.corrupt` sidecar preserved, ERROR logged), while the newest keeps the quiet removal that fixes the crash-loop artifact below.
- **Buried job order survives a restart.** `restore_jobs` rebuilt each tube's buried FIFO in `HashMap` iteration order, so after a restart `kick` returned arbitrary jobs instead of the oldest buries, breaking beanstalkd FIFO semantics. Replay now returns bury order (last bury event per still-buried job, in WAL order) and `restore_jobs` queues buried jobs in it.
- **No more one `.corrupt` file per restart under a crash loop.** The segment created at the end of replay had an unflushed header, so any kill before the first sync tick left a 0-byte file that the next start quarantined with an ERROR. Headers are now fsynced at creation, and recordless newest segments are removed with an INFO line instead.

Observability:

- **Panics are reported through `tracing`.** A panic inside a per-connection tokio task unwinds that task alone: the server keeps serving, `JoinHandle` swallows the error, and the default hook's unstructured stderr line was the only trace it left — easy for a log pipeline to miss. A global panic hook now emits the payload, location, and thread as a `tracing::error!` before chaining to the default hook, covering connection tasks, the WAL/TOAST background tasks, and the metrics server without any of them opting in. Alerting can key on it like any other server error.

Internal:

- `adopt_jobs()` joins `insert_job`/`take_job` as the documented accounting boundary, so a future `job_memory_cost` change can't silently skip the restore path and drift `total_job_bytes` — which would surface as spurious `OUT_OF_MEMORY` after a restart.
- Sentry was evaluated and rejected: its HTTP transport pulls 86–116 crates (47 → 133/163 built), out of proportion for this tree — the same reasoning that has `metrics.rs` hand-rolling an HTTP server rather than depending on hyper. Rationale recorded in CLAUDE.md.

## v0.10.0

**Durability & protocol correctness pass (WAL v7)** — from a three-part server/protocol/persistence review.

Server & protocol fixes:

- **`con:` concurrency limits no longer leak.** The per-key limit is now registered lazily on first reserve instead of eagerly at `put`, so a key whose jobs are deleted before ever being reserved no longer strands a stale (and, via the `.max` rule, higher) limit that would let later same-key jobs run wider than intended, nor accumulate unique keys in the map forever.
- **`flush-tube` now sweeps `aft:`-held jobs.** After-group jobs held for a group's completion live only in the group's waiting list, not the ready/delay/buried heaps, so a flush missed them and group completion later promoted the survivor back into the "flushed" tube. Sweeping them (plus reserved jobs) covers every job state, which also makes the blanket idempotency-key clear correct — no live job is left behind to lose its key.
- **Strict durability (`--sync-interval 0`) no longer leaks a job before its fsync.** A parked consumer woken by a `put` was answered `RESERVED` immediately, before the group-commit fsync; a crash in that window left the consumer holding a job that didn't survive replay. Woken-waiter acks are now deferred through the same fsync. On an fsync *failure*, deferred acks return `INTERNAL_ERROR` rather than a false success.
- **Unreadable TOAST bodies auto-bury instead of wedging a tube.** A bit-rotted body at the head of the ready heap failed every reserve; the job is now buried (and the bury persisted) so the tube keeps flowing and the job is preserved for inspection.
- `bury` wakes parked waiters when it frees a concurrency slot (matching delete/release); a non-UTF-8 command line is answered with `UNKNOWN_COMMAND` instead of dropping the connection; an idempotent re-put priority upgrade only bumps `current-jobs-urgent` for Ready jobs.

Persistence:

- **WAL v7 — delayed jobs replay with their remaining delay.** State-change records gained a change-time timestamp so a released/kicked-to-delayed job no longer resets to its full delay on every restart (a 1h-delay job restarted at t=59m would previously wait another full hour). Initial delayed puts use `created_at_epoch`. WAL compaction re-asserts delayed jobs with their remaining delay so a compaction rewrite can't make one fire early. Reads v3–v7; pre-v7 records fall back to the full delay.
- **Corrupt WAL segments are quarantined, not destroyed.** An unreadable or bad-header segment is renamed to `<name>.corrupt` instead of being skipped and later GC-unlinked (a transient EIO would otherwise permanently drop its live jobs); a mid-segment corruption is copied aside before the file is truncated (which would otherwise destroy the valid records — including deletes — after the bad one, resurrecting deleted jobs). Both failure modes become operator-recoverable.
- **Storage budget survives a WAL disable.** `--max-storage-bytes` now keys off the body store rather than the WAL, so a transient WAL error that disables the WAL (while TOAST stays attached) no longer silently removes the disk cap.
- Torn TOAST tails are truncated at scan time so stray bytes can't be parsed as a phantom body; a startup/rotation warning fires when the open TOAST segment count nears the process fd limit.

Client & operational:

- The client rejects whitespace/CRLF in tube names and `idp:`/`grp:`/`aft:`/`con:` values before sending (protocol-injection guard), and `reserve-with-timeout` guards a truncated reply instead of panicking.
- The Prometheus metrics HTTP read is bounded by a timeout and per-line size cap; `-z`/`--max-job-size` is rejected above the 1 GiB protocol limit; the deprecated-env-var alias is applied before the async runtime starts (`set_var` soundness); per-tube processing-time percentiles combine the fast and slow sample rings instead of skewing toward the slow tail.

## v0.9.1

**`flush-tube`/`flush-buried`: `FLUSHED 0` for an absent tube**

Both commands previously returned `NOT_FOUND` when the named tube did not exist, copying beanstalkd's named-admin convention (`pause-tube`/`stats-tube`). But they are tuber extensions, not beanstalkd commands, so they aren't bound to that convention. An absent tube now yields `FLUSHED 0` — a bulk delete against a tube holding nothing has correctly deleted nothing. It's the idempotent, more ergonomic answer, and it's stable against the idle-tube reaper, which would otherwise make `NOT_FOUND` appear racily once a drained tube is reclaimed. A tube that exists but has nothing to flush already returned `FLUSHED 0`; only the absent-tube case changes. The beanstalkd-compatible commands are unchanged.

## v0.9.0

**`flush-buried <tube>`: bulk-delete buried jobs**

A new protocol command that deletes every buried job in a tube, leaving ready, delayed, and reserved jobs untouched, and returns `FLUSHED <count>`. It fills the gap between `flush-tube` (deletes everything) and `kick` (only revives buried jobs) — the operator move for clearing a poison-pill backlog without disturbing live work.

`cmd_flush_buried` drains the tube's buried queue and applies the same per-job teardown as `delete`: idempotency tombstone, group pending/buried decrement, after-group waiter removal, and WAL delete. External bodies are released in a single batched `BodyStore` call, mirroring `flush-tube`. Stats counters are derived from the jobs actually removed (not the drained-list length), so a buried/jobs desync can't skew them. Buried jobs hold no concurrency slot — it's released at bury time — so no waiter needs waking. Covered by integration tests and a WAL-replay durability test.

## v0.8.1

**`reserve-batch`: deliver `DEADLINE_SOON` to blocking waiters; surface body-store faults**

Two follow-up fixes to the v0.8.0 long-poll timeout:

- **`DEADLINE_SOON` is no longer swallowed.** A connection holding a near-TTR reserved job while blocked on a positive-timeout `reserve-batch` was being woken with an empty `RESERVED_BATCH` instead of `DEADLINE_SOON`, silently dropping the signal to service its expiring job. `DEADLINE_SOON` is a connection-scoped out-of-band interrupt, not an answer to the reserve, so `deliver_waiter_failure` now checks deadline-soon before the batch short-circuit and returns it for batch and single waiters alike — matching `reserve-with-timeout`. Normal batch timeouts (no reserved jobs held) still return `RESERVED_BATCH 0`.
- **Body-store read failures are surfaced, not masked.** `collect_batch` broke on any non-`Reserved` reserve result and returned the jobs gathered so far, so a TOAST read failure on the head-of-line job looked identical to an empty queue — a batch client would poll forever, never learning of the fault, while a single reserve returns `INTERNAL_ERROR`. `collect_batch` now also returns the terminal `INTERNAL_ERROR`; both call sites surface it when zero jobs were collected. A non-empty partial batch still wins — the unreadable job stays ready and resurfaces on a later reserve — matching the single-reserve path.

## v0.8.0

**`reserve-batch` optional long-poll timeout**

`reserve-batch` was non-blocking: an empty queue returned `RESERVED_BATCH 0` immediately, so clients busy-looped polling for work. The command now accepts an optional timeout — `reserve-batch <count> [timeout]`. With no timeout (or `timeout 0`) it stays non-blocking. With a positive timeout it long-polls on the existing waiter machinery: it blocks only while 0 jobs are available, then drains whatever is ready (up to `<count>`) the instant the first job arrives, or replies `RESERVED_BATCH 0` on timeout. The response shape is uniform — never `TIMED_OUT` — so use the timeout form to stop clients hot-looping on empty polls.

`WaitingReserve` carries the batch count; `cmd_reserve_batch` parks a waiter when empty with a positive timeout, and `process_queue` drains a batch on wakeup. New shared helpers `collect_batch` and `deliver_waiter_failure` are reused across `process_queue` and the maintenance tick. Covered by protocol grammar tests and integration tests for the non-blocking, timeout-0, immediate-when-ready, timeout-empty, blocks-then-wakes, and waiter-accounting cases.

## v0.7.2

**Worker, protocol, and startup bug fixes** (from the 2026-06-11 full-app review)

- **`tuber work` no longer double-executes jobs.** The worker now touches a job every ~ttr/2 while its shell command runs, so a job whose runtime exceeds its TTR is no longer timed out server-side and re-run by another worker. delete/bury responses are checked (a `NOT_FOUND` after a TTR loss is logged loudly instead of as success), `watch`/`ignore` are validated (a failed subscription refuses to run rather than silently draining `default`), and the worker lifecycle is hardened: reconnect with exponential backoff, non-zero exit on total failure, SIGTERM handling, and in-flight jobs are *released* (not buried) on graceful shutdown. Adds client `touch`/`release`/`stats_job`.

- **`put` with a bad extension tag can no longer smuggle commands.** Tag validation (`idp:`/`grp:`/`aft:`/`con:`) fails after the declared `<bytes>` is parsed, but the body wasn't drained on `BAD_FORMAT`, so a client payload like `delete 1\r\n` was parsed and executed as a command. The server now drains the declared body (plus CRLF) on any rejected `put` line, mirroring the existing `JOB_TOO_BIG` path. Covered by a targeted regression test and a deterministic fuzz test.

- **Persistence env vars no longer block in-memory startup.** clap `requires = "binlog_dir"` fired on env-provided values, so `TUBER_SYNC_INTERVAL=… tuber server` (no `-b`) refused to start; the legacy `TUBER_WAL_SYNC_INTERVAL` shim made it worse. `--sync-interval`, `--max-storage-bytes`, and `--migrate-wal` are now validated after parse — ignored (with a warning) in-memory, while the genuine "persistence requires a disk budget" constraint is still enforced. Adds the first CLI-layer tests (`tests/cli.rs`).

## v0.7.1

**Three-tier body storage cleanup**

Follow-up to v0.7.0. Behaviour unchanged; the three-tier path is now expressed more directly. Net 18 fewer lines across `src/`.

- New `job::should_externalize(body_len, body_store_enabled)` is the single source of truth for the placement decision; the put-path WAL-size estimate and the body-ref construction both call it instead of repeating the threshold check.
- `serialize_full_job` and `fetch_body` route `Tiny` and `Heap` through `BodyRef::as_inline_bytes()` so only one inline branch exists in each. The post-shortcut `External` case uses a `let … else` instead of an exhaustive match with `unreachable!()` arms.
- `estimate_full_job_size_raw` takes `Option<&str>` for the four optional-key fields instead of `&Option<String>` / `&Option<(String, u32)>` — drops a `String::clone()` per put on the WAL size-estimate hot path.
- Legacy-WAL migration loop uses `if len > HEAP_INLINE_MAX && let Some(bytes) = body.take_inline()` so the size guard is the only filter — `take_inline` returns `None` for `External` naturally, no separate `matches!()`-then-`expect()`.
- `take_inline` on `Tiny` clears `len` in place instead of re-emitting a zeroed 23-byte placeholder.

## v0.7.0

**Three-tier body storage: small bodies skip TOAST**

Empirical: a 1 M-put flood of 64-byte jobs against `tuber server` produced multi-second pauses lining up exactly with `TOAST segment compacted` log lines. TOAST compaction takes the body-store mutex per migrated body, and `write_body` on the put path needs the same mutex — so puts queue behind the migration. Small bodies also pay a structural tax even when no compaction is running: 20-byte segment-header + ~32-byte in-RAM index entry + a positioned disk read on every reserve, for a 64-byte payload that already fits in the unused half of the `BodyRef` enum slot.

The fix is a three-tier `BodyRef`:

- `Tiny { len: u8, bytes: [u8; 23] }` — bodies ≤ 23 bytes inline directly in the enum slot. Zero heap allocation. Sized so a binary UUID (16 B) or a `u64` decimal (≤ 20 B) fits.
- `Heap(Vec<u8>)` — bodies 24 – 256 bytes still inline in the WAL FullJob record, but on the heap. No body-store traffic.
- `External(BodyId)` — bodies > 256 bytes go through TOAST as before. This is where TOAST's per-body overhead amortises against the body itself.

Result on the reproducer above: zero `TOAST segment compacted` events during the run; put throughput stays flat with no multi-second gaps.

**WAL format v5 → v6.** The FullJob record's trailing `body_id (u64)` becomes a 1-byte `body_kind` discriminant followed by either inline `len + bytes` (kind=0) or a `BodyId` (kind=1). The discriminant is per-record, so the runtime threshold is a constant, not a format commitment — changing `HEAP_INLINE_MAX` later doesn't require another version bump. v5 reads still work (every v5 body is External); v3/v4 reads still work with `--migrate-wal`, but only bodies > 256 B get pushed into TOAST during migration — smaller ones are rewritten as inline v6 records and stay out of the body store.

No configuration change required. The two thresholds (23 B and 256 B) are compile-time constants chosen from structural reality (enum-slot size; TOAST per-body fixed overhead amortisation point) rather than tuning surface — no new flag.

The earlier "always external when persistence is on" rationale (see `docs/wal-body-split.md`) wasn't wrong about reserve-side cost, but missed the put-side cost of TOAST itself for very small bodies under contention.

## v0.6.4

**`stats-group` per-state member counters**

`stats-group <name>` now returns `ready`, `reserved`, `delayed`, `buried`, and `waiting-jobs` in place of the old `pending`/`buried`/`complete` triplet — bringing the response in line with the rest of the stats family and giving operators an immediate view of where a group's members are stuck without crossing to `stats-job`. Counters are computed by scanning live jobs at query time, so state-transition sites don't need new bookkeeping. Field names are intentionally bare (no `current-jobs-` prefix); `stats-group` is a tuber extension with no beanstalkd-client compatibility surface to preserve.

Note: this is a breaking change for any tool that parses the `stats-group` YAML for `pending` or `complete`. The deferred `grp:<name>:<ttl>` cooldown design that would have added `cooldown-remaining` is filed under `docs/future/grp-ttl-cooldown.md` and not implemented in this release.

## v0.6.3

**Startup visibility: WAL/TOAST replay progress logs**

A 20 GB WAL replay used to be a silent multi-minute window between the "WAL: replaying N bytes" line and the post-replay summary. Now both `Wal::replay` and `BodyStore::open` emit segment-boundary progress lines, time-throttled to one every ~5 s, showing bytes processed, percent done, and a live job/body count:

```text
TOAST: scanning body store at "/var/lib/tuber/binlog/toast"
TOAST scan: segment 87/320, 5.43 GiB / 19.84 GiB (27%), 412384 bodies indexed
WAL: replaying 1247 segments / 12.18 GiB from "/var/lib/tuber/binlog" (not yet accepting connections)
WAL replay: segment 412/1247, 4.01 GiB / 12.18 GiB (32%), 891204 jobs so far
…
WAL: replayed 2480915 jobs and 1289 idempotency tombstones from "/var/lib/tuber/binlog"
```

Also documents the v0.4.2 readiness contract in the README: the beanstalk TCP listener is bound only after replay completes, so the existing `Dockerfile` HEALTHCHECK (`tuber stats`) is an honest readiness probe — fails during replay, succeeds after. No new HTTP endpoint; doing so would re-introduce the false-healthy window v0.4.2 closed.

## v0.6.2

**`stats-tube` head-of-queue ages for starvation triage**

Adds `oldest-ready-id`, `oldest-ready-age`, `oldest-delayed-age`, and `oldest-buried-age` to `stats-tube`. The age values come from the heap roots (and `buried.front()`), which match the next-to-serve job — exactly what you want when diagnosing "is this tube draining or starving?" without having to chain `peek-ready` + `stats-job`. All ages are seconds; `oldest-ready-id` is `0` when the bucket is empty.

## v0.6.1

**TOAST compactor throughput, short flags**

- TOAST compactor now drains multiple sealed segments per tick under a single wall-clock budget instead of one per tick, so backlog catches up faster on a busy server.
- Structured `tracing` fields and leaner comments in the compaction loop.
- Short flags: `-s` for `--max-storage-bytes`, `-i` for `--sync-interval`.
- README rewrite that explains the WAL/TOAST split and why it matters.

## v0.6.0

**WAL/TOAST split: external body store with combined disk budget**

The WAL was carrying job bodies inline, which made compaction expensive (whole bodies copied between segments) and made the disk footprint unpredictable for large jobs. v0.6.0 splits storage in two: the WAL holds metadata and references, and bodies live in a separate append-only TOAST store under `<binlog_dir>/toast/`. Compaction now moves bytes only when bodies are actually dead.

- **WAL v5** — `FullJob` records carry a `BodyId` reference instead of inline bytes; `StateChange` records unchanged. Reader still accepts v3/v4; on first replay legacy inline bodies are migrated into TOAST. `--migrate-wal` opt-in for explicit pre-v5 conversion.
- **TOAST format v1** — append-only `body.NNNNNN` segments under `<binlog_dir>/toast/`. Per-body header is `body_id u64 + len u32 + crc32 u32 + reserved u32`; file header is `"TBOD" + version + reserved`. 64 MiB default segment size. Concurrent reads, durable rotation.
- **Sync ordering** — every WAL fsync is preceded by a TOAST fsync. A crash mid-sync leaves orphan bodies, never dangling references; orphans are detected on replay and reclaimed.
- **`--max-storage-bytes` (mandatory with `-b`)** — caps WAL + TOAST combined. Puts return `OUT_OF_STORAGE` once the budget minus a one-segment WAL reserve (proportional, ≤10 MiB) would be exceeded; state changes (delete/release/bury/kick/touch) always succeed. New flag joins `--max-jobs-size` (in-RAM cap, returns `OUT_OF_MEMORY`).
- **`--sync-interval` (renamed from `--wal-sync-interval`)** — drives both stores. Old name kept as a hidden alias.
- **Group commit** — WAL+TOAST fsyncs are amortised across the engine's drained batch instead of fsyncing per write. Strict/relaxed sync modes split out so relaxed mode skips the drain.
- **Crash-recovery hardening at startup**:
  - Reap WAL-references-missing-TOAST jobs (FullJob points to a body that doesn't exist).
  - Reclaim stranded TOAST bodies (TOAST has bytes that no live WAL job points to).
  - Reclaim orphan bodies left by interrupted syncs.
- **Compaction safety** — TOAST drops CRC-failed bodies and continues instead of aborting; migrated bodies are fsynced before the old segment is unlinked.
- **Stats + Prometheus** — TOAST gauges (segment count, total bytes, alive bytes, live ratio) and counters (`reclaimed-orphan-bodies`, `reclaimed-stranded-bodies`, `recovered-missing-bodies`, `toast-bodies-migrated-total`, `toast-bodies-dropped-corrupted`).
- **Tracing on the read path** — `fetch_body` emits structured fields for triage when a body lookup fails.

## v0.5.7

**Concurrency-keyed reserve throughput on large queues**

Three compounding bottlenecks made flood-style workloads (many ready jobs sharing a small set of saturated `con:` keys) crawl at single-digit ops/sec on a 100K-entry tube. Measured: 16-worker `flood_con` on 100K jobs went from ~5 jobs/sec to ~2,860 jobs/sec.

- `cmd_delete` now calls `process_queue` when a concurrency key is released and waiters exist (mirrors `cmd_release`). Previously a parked waiter only woke on the next tick.
- `process_queue` precomputes the per-tube unblocked top once per outer pass and uses it for the cheap waiter-eligibility check. Drops per-delete cost from O(W × N) to O(N + W) when many waiters share the same watched tube.
- `find_best_unblocked_ready` slow-path traversal is now capped at 256 nodes (new `FIND_UNBLOCKED_MAX_VISITS`). With saturated con: keys blocking many top jobs, the cap stops scans walking a 100K-entry heap on every reserve; missed jobs are picked up on the next event-driven re-check.
- Replaced the slow path's `collect+sort+linear-scan` with a lazy in-heap-order traversal via an auxiliary `BinaryHeap`.
- Tests: `test_cmd_delete_wakes_concurrency_waiter` (unit), `test_find_unblocked_returns_smallest_unblocked` (unit), `test_many_waiters_concurrency_keyed_drain` (integration, 32 waiters × 2,500 jobs).

**Body storage abstraction (Phase 2 placeholder)**

- Introduce `BodyRef::Inline(Vec<u8>)` / `BodyRef::External(BodyId)` enum to decouple `Job.body` from inline storage. `External` is reserved for a future external body store (TOAST) and is currently never constructed; access panics with a clear message.
- Replace dead `JobState::Reserved` WAL replay arm with `unreachable!()`.

## v0.5.6

**Container health check**

- Dockerfile `HEALTHCHECK` now invokes `tuber stats` so orchestrators (Docker, Kubernetes, Compose) can detect a hung server.

## v0.5.5

**Fix WAL pre-flight check blocking restarts**

- Remove the WAL on-disk size pre-flight check that compared raw binlog bytes against `--max-jobs-size`. In production, WAL bloat from tombstones, deleted records, and multi-file overhead made the on-disk size ~1.4× larger than the live in-memory set, blocking restarts even at 41% memory capacity. The post-replay check (added in 0.5.4) is the sole enforcement and uses the actual in-memory `total_job_bytes`.

## v0.5.4

**Bug fixes for stats, WAL replay, group state, and shutdown**

- `--max-jobs-size` is now exactly enforced after WAL replay. A post-replay check compares `total_job_bytes` against `--max-jobs-size` and aborts with a distinct diagnostic message naming the in-memory size.
- Fix `reserve-job` bypassing `after_group` dependencies; add tests for `waiting_ct` and WAL priority preservation.
- Await engine task on shutdown to prevent WAL data loss.
- Simplify SIGTERM WAL test; tidy shutdown comments.
- Extract `GroupState::remove_waiting_job` helper.
- Fix deleting held after-job leaking its ID in `GroupState::waiting_jobs`.
- Fix per-tube `waiting_ct` leak on disconnect; extract `job_pri` helper.
- Fix WAL replay resetting job priority to 0 for kicks, timeouts, and reserves.
- Remove `tube.waiting_conns` dead state; unify waiter removal.
- Fix `waiting_ct` stats never being updated.

## v0.5.3

**Configurable WAL durability + buffered writes**

- New `--wal-sync-interval` flag (env `TUBER_WAL_SYNC_INTERVAL`, default `100ms`)
  controls how often the WAL is `fsync`ed. `0` syncs on every write before
  acknowledging the client (strongest durability, slowest). Positive values
  bound how much committed state can be lost on crash. On clean shutdown the
  tail is always flushed and synced regardless of the interval.
- WAL writes now pass through a 64 KiB userland `BufWriter`, cutting syscall
  count under load without changing durability guarantees (every fsync path
  flushes the buffer first).
- When `--wal-sync-interval` is shorter than the engine's 100 ms tick, the
  tick shortens to match so fsync cadence isn't capped by maintenance cadence.

## v0.5.2

**Fix WAL replay losing per-job counters**

After a restart, `stats-job` showed `reserves: 0`, `buries: 0`, `kicks: 0` for every job — even jobs that had been reserved, buried, and kicked multiple times. The counters (`reserve_ct`, `bury_ct`, `timeout_ct`, `release_ct`, `kick_ct`) were only persisted in the FullJob WAL record written at `put` time (when all counters are 0). Subsequent state transitions wrote StateChange records that carried state/priority/delay but no counter data, so replay restored counters to zero.

Additionally, `reserve` and `timeout` transitions never wrote to the WAL at all, so their counters were lost even within a single WAL file's lifetime.

**Fix:** StateChange records now include a 1-byte "reason" field identifying the transition type (reserve, release, bury, kick, timeout). On replay, the appropriate counter is incremented based on this reason. WAL writes are also added for reserve and timeout transitions that previously had none.

- WAL version bumped from 3 to 4. New tuber reads both v3 and v4 files — v3 StateChange records replay with reason=None (counters stay at 0, no worse than before). Old tuber rejects v4 files cleanly via version check.
- StateChange record size grows from 38 to 39 bytes (+1 byte for reason).

## v0.5.1

**Env var support for all server flags**

Every `server` subcommand option can now be set via a `TUBER_*` environment variable.

## v0.5.0

**Memory budget, startup readiness, env var config**

- **`--max-jobs-size` memory budget** — new flag (and `TUBER_MAX_JOBS_SIZE` env var) that caps the total in-memory footprint of all jobs. When the budget is full, `put` returns `OUT_OF_MEMORY` — an explicit backpressure signal instead of a silent OOM kill. Workers can always reserve, release, bury, kick, and delete at capacity. Each job costs `body_len + 512` bytes against the budget; idempotency tombstones cost `key_len + 128`. Accepts human-readable suffixes (`2g`, `500M`, `100k`). Default is unlimited for backward compatibility.
- **Replay pre-check** — if the WAL on disk is larger than `--max-jobs-size`, tuber aborts at startup with a diagnostic error naming both sizes and what to do, instead of OOMing mid-replay.
- **Accounting drift detector** — when the queue drains to empty, tuber verifies the internal byte counter is zero. If not, it logs a warning, bumps an `accounting-drift-events` counter (exposed in stats and Prometheus), and self-heals by resetting. Every increment is a bug worth reporting.
- **Stats and metrics** — new `current-jobs-size`, `max-jobs-size`, and `accounting-drift-events` fields in `stats` output, plus Prometheus gauges `tuber_jobs_size_bytes`, `tuber_jobs_size_limit_bytes`, and counter `tuber_accounting_drift_events_total`.
- **`--max-job-size` suffix support** — the existing per-job body size limit (`-z`) now accepts the same human-readable suffixes (`-z 64k`, `-z 1m`). Backward compatible — raw integers still work.
- **Env var support for all server flags** — every `server` subcommand option can now be set via a `TUBER_*` environment variable (`TUBER_LISTEN`, `TUBER_PORT`, `TUBER_BINLOG_DIR`, `TUBER_MAX_JOB_SIZE`, `TUBER_MAX_JOBS_SIZE`, `TUBER_VERBOSE`, `TUBER_METRICS_PORT`, `TUBER_NAME`). Useful for Docker Compose and Kubernetes deployments where env vars are easier than command arrays.
- **Startup log shows full config** — the startup INFO line now includes `max-job-size`, `max-jobs-size` (if set), `binlog` path (if set), and `metrics` address (if set), so operators can verify the running config from `docker compose logs` without connecting.

## v0.4.2

**Replay WAL before binding listener**

The TCP listener was bound before WAL replay, so the accept port was reachable while the server was still loading the binlog into memory. TCP health checks would pass during replay even though no command could be served — and if replay OOMed, monitors saw a healthy port during the brief window between restart and next OOM. Now `run()` replays the WAL first, then binds. TCP reachability honestly reflects readiness. Also logs the binlog size before replay begins via `Wal::total_disk_bytes()`.

## v0.4.1

**Add floor and min samples to weighted-fair selection**

## v0.4.0

**Weighted-fair reserve mode**

New `reserve-mode weighted-fair` that allocates worker time proportional to tube weights, adjusted for processing time. Prevents slow tubes from starving fast ones.

## v0.3.15

**IDP priority upgrade on duplicate put**

When a duplicate idempotency put arrives with a higher priority (lower number), the existing job's priority is now upgraded instead of being silently ignored. Only upgrades are allowed — never downgrades — to prevent flapping. Works for all job states (ready, reserved, delayed, buried); for non-ready jobs the new priority takes effect on the next state transition. The response includes the new priority when an upgrade occurs: `INSERTED <id> <state> <pri>`.

## v0.3.14

**Percentile fallback, record_timing helper, EWMA_ALPHA cleanup**

Percentiles now fall back to fast-job samples when no slow samples exist, so tubes where all jobs complete in < 100ms get useful p50/p95/p99 instead of zeros. Also extracted `record_timing()` and `push_ring()` helpers to reduce duplication, and hoisted `EWMA_ALPHA` to module level.

## v0.3.13

**Add queue-time (time-in-queue) stats to `stats-tube`**

Tracks how long jobs wait from `put` to `reserve` via EWMA, min, max, and sample count. Growing queue time indicates you need more workers. Exposed in `stats-tube` and Prometheus `/metrics`.

## v0.3.12

**Enhanced processing time stats: dual EWMA, percentiles, and bury rate**

`stats-tube` now exposes bimodal-aware processing time tracking:
- **Dual EWMA** — jobs are split at a 100ms threshold into fast (e.g. idempotent exits) and slow (real work) buckets, each with its own EWMA.
- **Percentiles** — p50/p95/p99 computed from the last 1000 slow-job samples.
- **Bury rate** — `total-buries / total-reserves` for quick failure monitoring.
- All new fields are also exposed via the Prometheus `/metrics` endpoint.

The existing `processing-time-ewma` field is unchanged for backwards compatibility.

## v0.3.11

**Restore concurrency limits from WAL on restart**

`restore_jobs()` was not populating `concurrency_limits` during WAL replay, so after restart `is_concurrency_blocked()` would default to limit 1 instead of the configured limit.

## v0.3.10

**Add `--name` flag and `TUBER_NAME` env for instance naming**

New `--name` flag (and `TUBER_NAME` environment variable) to label server instances. The name appears in stats YAML output, startup log, and Prometheus `tuber_info` gauge with name/version/id labels.

## v0.3.9

**Reap idle tubes during maintenance tick**

Empty non-default tubes are now removed during the periodic maintenance tick when they have no jobs, no watchers, and no active connections — matching beanstalkd's cleanup behavior.

## v0.3.8

**Fix WAL `reserved_bytes` leak causing spurious `OUT_OF_MEMORY`**

Compaction migrations called `write_put()` which unconditionally incremented `reserved_bytes`. Long-lived idle jobs migrated repeatedly inflated the counter until `reserve_put()` rejected new puts. Fixed by only reserving for new jobs. Also simplified `reserve_put()` to match beanstalkd: the WAL creates files on demand, so the only constraint is that a record fits in one file.

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
