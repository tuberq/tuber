# WAL/TOAST split — consolidated review findings

Four independent reviews of the `feature/body-wal-split` branch:

- **[C]** `claude.md` — Claude Opus 4.7 (1M context)
- **[D]** `dsv4.md` — DeepSeek v4
- **[G]** `glm.md` — GLM-5.1
- **[K]** `kimi.md` — Kimi (OpenCode)

All four agree the implementation is faithful to the design doc, well-commented, and ready to merge after addressing the items below. Tests pass (432/0/1). The bugs are small; the operational and observability gaps are the bigger story.

Each finding tags which reviewer(s) raised it. Where reviewers disagree, both views are noted.

---

## 1. Bugs / Correctness

### 1.1 `--max-storage-bytes` is not mandatory when `-b` is set [D, G]

**Severity: high** — design-doc violation with real footgun potential.

The doc (line 23) says it's mandatory; the implementation makes it `Option<u64>` and `storage_limit_exceeded()` returns `false` unconditionally when absent (`server.rs:374-377`). Operators who enable persistence without thinking about the budget get unbounded TOAST growth — exactly the foot-shape the doc set out to prevent.

**Fix:** validate at startup; refuse to start with `-b` but no `--max-storage-bytes`.

### 1.2 `reserve_put` warning logs `OUT_OF_MEMORY` for a record-too-big condition [D]

**Severity: low** — misleading log message, not a behavioural bug.

`server.rs:921` logs `"WAL: OUT_OF_MEMORY — job record... exceeds max file size"`. The actual condition is "this single record won't fit in any WAL segment," which is unrelated to memory or storage budgets. The client gets `OUT_OF_MEMORY` back, which is also wrong post-TOAST.

**Fix:** rename the log + return a more accurate response (or at minimum update the log message).

### 1.3 Body leaks if `wal_write_put` fails after `write_body` succeeds [C]

**Severity: low** — disk leak, not corruption; recoverable on next manual cleanup.

`cmd_put` lands the body in TOAST first (`server.rs:935`), then writes the WAL record. If the WAL write fails (`server.rs:2807-2813`), the WAL is disabled but the orphan stays on disk forever — the replay-orphan path only fires for explicit deletes, not for puts that never finished. If WAL writes are failing you have bigger issues, but a `bs.delete(body_id)` in the error branch is cheap insurance.

---

## 2. Crash safety / Durability

### 2.1 Compaction has no fsync between migration and unlink [G]

**Severity: medium** — narrow but real durability window.

`compact_segment` writes migrated bodies via `pwrite`, then unlinks the old segment file. There's no `sync_data` on the new segment before the unlink. A crash after unlink but before the kernel writes the new data through would lose those bodies. The periodic sync interval narrows the window but doesn't close it.

**Fix:** `bs.fsync()` (or sync just the affected segment) after the migration loop, before unlinking.

### 2.2 Compaction crash between migration and unlink leaves duplicate entries [G]

**Severity: low** — works correctly by accident; needs a comment.

If the server crashes between writing the migrated body and unlinking the old segment, the same `BodyId` exists in two segments on restart. The header-scan rebuild iterates segments in seq order and `index.insert` keeps the last one, which happens to be the new (correct) location. Correct behaviour, but it's implicit. Worth a comment in `BodyStore::open` so future maintainers don't break it.

### 2.3 `flush-tube` WAL failure leaves in-flight deletes unjournalled [K]

**Severity: low** — known limitation of the disable-on-error model.

If `wal_write_state_change` fails mid-batch and disables the WAL (`self.wal = None`), the rest of the flush continues deleting from memory and freeing TOAST bodies, but those deletes won't survive a crash. Acceptable for `flush-tube` (destructive command), but worth a code comment near the loop.

---

## 3. Performance / async correctness

### 3.1 TOAST compaction blocks the async runtime [K]

**Severity: medium-high** — only review to flag this; worth taking seriously.

The compaction task (`server.rs:3139-3179`) runs synchronous file I/O (`read_exact_at`, `write_all_at`, `fs::remove_file`) directly inside `tokio::spawn`, with no `spawn_blocking`. The inline comment claims "well under 100 ms" — that assumes fast SSD and typical body sizes. On HDD/NAS, large bodies, or under CPU pressure this could stall the runtime for seconds.

**Fix:** wrap `compact_segment` in `tokio::task::spawn_blocking`, or at minimum instrument with tracing spans so operators can see stalls.

### 3.2 Per-body mutex acquire in `compact_segment` [C]

**Severity: low** — could starve concurrent puts under heavy load.

Migration takes the inner mutex once per body (`body_store.rs:531`). A 64 MiB segment of 50 KiB bodies = ~1300 acquires per compaction. Each is brief but the cumulative effect is non-trivial. Batching N bodies per lock would help; measure first.

### 3.3 `flush-tube` does double `take_job` [K]

**Severity: low** — inefficiency, not a bug.

`wal_write_state_change` internally `take_jobs` + serialises + re-inserts, then the flush loop `take_jobs` *again* to delete from memory and collect the BodyId. Extra hashmap churn. Batch the WAL state changes and memory removal more tightly.

### 3.4 `write_body` burns a `BodyId` on rotation failure; uses SeqCst unnecessarily [C]

**Severity: trivial** — slow u64 leak under sustained errors; over-strict atomic ordering.

`next_body_id.fetch_add(1, SeqCst)` runs unconditionally before append (`body_store.rs:225`). On ENOSPC/EMFILE the id is burned. Switch to `Relaxed` (other atomics on the same struct already use it) and consider only consuming the id after a successful append.

### 3.5 Storage budget check is inherently racy [G]

**Severity: trivial** — known limitation; should be documented.

`total_bytes` updates use `Ordering::Relaxed`, sometimes inside the inner lock and sometimes outside. Two concurrent puts could both pass the budget check before either writes. Acceptable (overage bounded by one body per concurrent put), but mark as intentional.

---

## 4. Operator experience / Ergonomics

### 4.1 WAL reserve floor of 10 MiB is too generous for tight budgets [C]

**Severity: medium** — surprising on small budgets.

`storage_limit_exceeded` reserves `max(wal.max_file_size(), DEFAULT_MAX_FILE_SIZE)` = ≥10 MiB unconditionally (`server.rs:392`). A state-change record is 39 bytes, so 10 MiB = headroom for ~270k deletes. Anyone trying a 5 MiB budget gets `OUT_OF_STORAGE` on the first put. Bound the reserve to `STATE_CHANGE_RECORD_SIZE * N` (some plausible per-rotation churn ceiling) instead.

### 4.2 `storage_limit_exceeded` counts pre-replay orphans [K]

**Severity: low** — self-healing but surprising.

`bs.total_bytes()` includes orphan bytes that haven't been reclaimed yet. After a crash with many uncompacted deletes, a server can refuse puts immediately on restart until replay's `delete_many` runs. Self-corrects within a tick but visible in the logs.

### 4.3 Background compaction runs even with no budget [C]

**Severity: trivial** — counter-intuitive but cheap.

The compaction task spawns whenever `body_store.is_some()`, including when `--max-storage-bytes` is unset. Cheap (5s scan + mutex acquire), but operators who deliberately disabled the budget will wonder why something is still scanning. Document that compaction triggers off live-ratio, not budget.

### 4.4 Compaction skips entire segment on a single corrupted body [G]

**Severity: medium** — one bit-rot blocks all reclamation for that segment.

`compact_segment` re-verifies CRC when reading from the old segment. A `BadCrc` on any body fails the whole compaction, so the entire segment stays on disk indefinitely. Consider logging + skipping the corrupted body (treat it as deleted) so the rest of the segment can be reclaimed.

### 4.5 No startup warning for WAL-referenced bodies missing from TOAST [G]

**Severity: low** — observability gap.

If WAL references a `BodyId` that's not in TOAST (corruption, manual intervention), the job fails at reserve time with `InternalError`. Startup-time validation (or at least a counted warning) would catch this much earlier.

### 4.6 `fetch_body` returns opaque `InternalError` on missing/corrupt bodies [K]

**Severity: low** — client can't distinguish causes; tracing exists but is sparse.

`tracing::error!` logs the body_id but not the job_id or segment. Add structured tracing fields so operators can correlate failures.

### 4.7 Orphan cleanup is only triggered on startup or incidentally by compaction [D]

**Severity: low** — works in practice, fragile in principle.

Orphan bodies have no index entries → contribute to dead bytes → segment eventually drops below the 0.5 threshold and gets compacted. But if all segments stay above 0.5, orphan bytes persist until the next restart. Acceptable but worth noting.

### 4.8 `Job::new` initialises body as `Inline` then the caller overwrites [G]

**Severity: trivial** — minor smell.

When persistence is on, `Inline` is a transient state that should never reach the WAL serialiser (which `unreachable!()`s on it). Either accept this (it preserves a single Job constructor signature across modes) or split into two constructors. Probably leave it.

### 4.9 `BodyLocation.offset` is the body-data offset, requiring subtraction to read the header [G]

**Severity: trivial** — footgun for future maintainers.

`read_body` reads the header at `offset - BODY_HEADER_SIZE`. Storing the header offset and computing the data offset on demand would be cleaner.

---

## 5. Documentation

### 5.1 Design doc says v3→v4; implementation shipped v5 [D, G]

`docs/wal-body-split.md` line 71 says "Bump WAL version (v3 → v4)". Reality: `WAL_VERSION = 5` (`wal.rs:27`). v4 was consumed by the `StateChangeReason` byte. Update the doc to v3→v5 and explain the v4 step.

### 5.2 `docs/wal-format.md` only documents v3 [D]

The format spec ends at v3. Missing:
- v4 reason byte in StateChange records (payload_len 21→22)
- v5 BodyId replacement of inline body bytes in FullJob records
- TOAST on-disk format (TBOD magic, body record layout, segment lifecycle)

This is the user-facing format doc — anyone debugging a binlog needs all three.

### 5.3 Document `flush-tube` WAL-failure behaviour [K]

Add a code comment near the flush loop noting that a WAL failure mid-batch leaves the rest of the deletes un-journalled (acceptable for a destructive command).

### 5.4 Document compaction crash-safety invariants [G]

Comment in `BodyStore::open` explaining why the header-scan order makes duplicate entries (from a crashed compaction) resolve correctly.

### 5.5 Document storage-budget race as intentional [G]

Comment in `storage_limit_exceeded` noting the Relaxed-atomic race is bounded by concurrent-puts × body-size and is acceptable.

---

## 6. Test coverage gaps

Several reviewers flagged the same theme: unit tests are good, integration coverage is thin.

### 6.1 No end-to-end WAL+TOAST integration tests [K, C]

Specifically missing:

- Put → restart → reserve, confirming the body round-trips through TOAST. *(Partial coverage in `tests/binlog.rs` — the `toast_*` tests cover this but only for happy paths.)*
- Crash between WAL fsync and `BodyStore::delete` → restart → confirm orphan reclaimed end-to-end. (Unit-tested at the WAL layer; no server-level test.)
- TOAST segment rotation across a server restart. (Header scan within one segment is tested; rotation + restart is not.)
- `--max-storage-bytes` enforcement with both WAL and TOAST growing concurrently.
- Concurrent put + compaction + reserve stress.

### 6.2 No test for concurrent put vs. compaction on a non-current segment [C]

`compact_current_segment_is_a_noop` covers the wrong race. The interesting case is a put landing in segment N while compaction migrates bodies from segment N-2.

---

## 7. Metrics & observability

### 7.1 No metric for orphan bodies reclaimed on replay [K]

A counter would let operators see how often crash-recovery is reclaiming leaked space — useful signal for unhealthy nodes.

### 7.2 Enhance `fetch_body` error tracing [K]

Add `job_id`, `body_id`, `segment`, `offset` to the tracing event so operators can debug corruption.

---

## 8. Code quality

### 8.1 Pre-existing clippy `doc_lazy_continuation` warnings on `main.rs:92-94` [C]

Unrelated to this branch but live in a touched file. Trivial to fix.

---

## What everyone agreed was well done

Cross-cited strengths across all four reviews:

- **TOAST-then-WAL fsync ordering** — consistently enforced at every WAL fsync path; the durability invariant is documented at every call site.
- **Logical `BodyId` indirection** — paying off immediately; compaction reorganises bodies without touching WAL records.
- **Always-external when persistence is on** — no threshold, single code path, `unreachable!()` in the v5 serialiser enforces it.
- **Orphan filtering on replay** — `HashSet` excludes bodies whose ids got re-used in the same WAL.
- **Compaction stale-entry guard** — only commits the index swap when the snapshot still matches; correct concurrency model.
- **Self-healing WAL truncation** on corrupt records prevents repeated warnings.
- **`--migrate-wal` opt-in** for legacy formats — explicit operator decision before any in-place conversion.
- **WAL version sniffing before TOAST creation** — refusal is side-effect-free.
- **Memory accounting** correctly excludes bodies that live in TOAST from the in-RAM budget.
- **Bulk `delete_many`** in `flush-tube` avoids per-job mutex contention.
- **Lock-free atomic counters** keep stats and budget checks off the BodyStore inner mutex.
- **Comprehensive Prometheus metrics** for TOAST (total/live bytes, segments, compactions, bodies migrated).

---

## Suggested priority for follow-up work

Working backwards from "what would I fix first" to "what can wait":

### Must-fix before/just-after merge

1. **Make `--max-storage-bytes` mandatory with `-b`** (1.1) — design-doc violation, real production footgun.
2. **Wrap compaction in `spawn_blocking` or instrument it** (3.1) — could stall the runtime under realistic workloads.
3. **Fix the misleading `OUT_OF_MEMORY` log + response in `reserve_put`** (1.2) — confusing failure mode for operators.

### Should-fix soon

4. **fsync after compaction migration, before unlink** (2.1) — closes a real durability window.
5. **Update `docs/wal-format.md`** with v4, v5, and TOAST format (5.2) — anyone debugging a binlog is currently in the dark.
6. **Update design doc to say v3→v5** (5.1).
7. **Add server-level integration tests** covering crash-restart, rotation-restart, and concurrent put+compact (6.1, 6.2).
8. **Bound the WAL reserve to plausible state-change churn** (4.1) — fixes the small-budget footgun.
9. **Skip + log corrupted bodies in compaction** (4.4) — one bit-rot shouldn't block segment reclamation.

### Nice-to-have / opportunistic

10. Drop the orphan body in `cmd_put`'s WAL-failure branch (1.3).
11. `delete_many` for `flush-tube` failure-mid-batch comment (2.3).
12. Remove the double `take_job` in `flush-tube` (3.3).
13. Switch `next_body_id` to `Relaxed`, only consume on success (3.4).
14. Document the storage-budget race + compaction-restart duplicate handling (5.4, 5.5).
15. Add orphan-reclaimed metric + better `fetch_body` tracing (7.1, 7.2).
16. Fix clippy doc warnings on `main.rs` (8.1).
17. Startup warning for WAL-referenced bodies missing from TOAST (4.5).
18. Consider whether `BodyLocation.offset` should be the header offset (4.9).
