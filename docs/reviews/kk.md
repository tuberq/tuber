


     Here is my review of the WAL + TOAST body-split feature, based on the design document (docs/wal-body-split.md) and the implementation across wal.rs,
     body_store.rs, job.rs, server.rs, metrics.rs, protocol.rs, and main.rs.

     ---

     Overall Assessment: Well-Executed

     The design is principled, and the implementation faithfully follows it. The core decisions — logical BodyId indirection, TOAST-then-WAL sync ordering,
     always-external bodies under persistence, unified --max-storage-bytes budget, and no compression — are all sound and correctly reflected in the code.

     ---

     Design → Code Alignment (What's Right)

     ┌────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────┬───────┐
     │Design Doc Decision                 │Code Reality                                                                                             │Verdict│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │WAL v5 carries BodyId instead of    │serialize_full_job() panics on BodyRef::Inline; deserialize_full_job reads body_id for v5+ and inline    │✅     │
     │inline body                         │bytes for v3/v4.                                                                                         │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │TOAST-then-WAL sync ordering        │Wal::pre_sync_body_store() fsyncs TOAST before every WAL fsync path (rotate_if_needed, maintain,         │✅     │
     │                                    │flush_and_sync, sync_per_write).                                                                         │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Logical addressing (BodyId →        │BodyStore maintains HashMap<BodyId, BodyLocation>; index rebuilt by scanning segment headers on open.    │✅     │
     │BodyLocation)                       │                                                                                                         │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Always-external when persistence is │cmd_put writes body via bs.write_body() before WAL, producing BodyRef::External.                         │✅     │
     │on                                  │                                                                                                         │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Two operating modes (no -b = in-    │body_store is Option<Arc<BodyStore>>; None = inline mode, Some = TOAST mode. Clean enum dispatch         │✅     │
     │memory, -b = WAL+TOAST)             │everywhere.                                                                                              │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Unified --max-storage-bytes with    │storage_limit_exceeded() uses wal_bytes + toast_bytes + body_len + BODY_OVERHEAD + wal_reserve where     │✅     │
     │WAL reserve                         │wal_reserve = max(wal.max_file_size(), DEFAULT_MAX_FILE_SIZE).                                           │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │OUT_OF_STORAGE for disk budget,     │Response::OutOfStorage returned by storage_limit_exceeded; OutOfMemory still used for in-RAM --max-jobs- │✅     │
     │OUT_OF_MEMORY for RAM               │size.                                                                                                    │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Orphan body cleanup on replay       │wal.replay() returns orphan_bodies: Vec<BodyId>; server calls body_store.delete_many(&orphan_bodies).    │✅     │
     │                                    │                                                                                                         │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Pre-v5 inline → TOAST migration     │Replay loop uses job.body.take_inline() and body_store.write_body() + fsync before proceeding.           │✅     │
     │                                    │                                                                                                         │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Single --sync-interval              │CLI has --sync-interval with hidden alias --wal-sync-interval; TUBER_WAL_SYNC_INTERVAL env var migrated  │✅     │
     │                                    │with deprecation warning.                                                                                │Correct│
     ├────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
     │Compaction threshold 0.5            │COMPACTION_LIVE_RATIO_THRESHOLD = 0.5; compaction_candidate() picks most-wasted sealed segment.          │✅     │
     │                                    │                                                                                                         │Correct│
     └───     
     ---

     Edge Cases & Risk Areas

     1. TOAST Compaction Blocks the Async Runtime
     Severity: Medium

     The compaction task (server.rs:3139-3179) runs synchronous file I/O (read_exact_at, write_all_at, fs::remove_file) directly inside a tokio::spawn task
     without spawn_blocking:
     tokio::spawn(async move {
         loop {
             // ...
             match bs.compact_segment(seq) { /* 64 MiB of pwrites */ }
         }
     })
     The inline comment claims this is "well under 100 ms," but that assumes fast SSD and typical body sizes. On:
     - HDDs or network-attached storage
     - Very large bodies (the design doc targets 10–100 KB, but the limit is 1GB)
     - High CPU contention during CRC verification

     … this could block the async runtime for seconds. Recommendation: wrap compact_segment in tokio::task::spawn_blocking inside the compaction task, or at
     least instrument with tracing spans so operators notice stalls.

     ---

     2. Double take_job in flush-tube
     Severity: Low (inefficiency)

     cmd_flush_tube calls wal_write_state_change(id, ...) which internally take_jobs, serializes, and re-inserts the job. Then the flush loop immediately
     take_jobs again to remove it permanently and collect its BodyId. This is harmless but wastes hashmap churn. Recommendation: batch WAL state changes and
     memory removal more tightly.

     ---

     3. WAL Failure Mid-Flush-Tube Leaves Deletes Unjournal-ed
     Severity: Low (known limitation)

     If wal_write_state_change fails and disables the WAL (self.wal = None), the rest of the flush-tube batch continues deleting from memory and freeing
     TOAST bodies, but those deletes won't be on disk. A crash would resurrect them on replay. This is acceptable for flush-tube (it's documented as
     destructive), but worth a code comment.

     ---

     4. storage_limit_exceeded Counts Pre-Replay Orphans
     Severity: Low

     After a crash, orphan TOAST bodies are not deleted until wal.replay() completes and delete_many runs. But storage_limit_exceeded checks bs.total_bytes()
      which includes orphans. So a server that crashed with many deleted (but not yet compacted) jobs could refuse new puts immediately on restart, even
     though replay will soon reclaim them. This is self-healing but could surprise operators.

     ---

     5. fetch_body Returns InternalError on Missing/Corrupt Bodies
     Severity: Medium (observability gap)

     If BodyStore::read_body returns NotFound or BadCrc, fetch_body returns None, which surfaces as Response::InternalError to the client. The server logs
     the error with tracing::error!, but the client can't distinguish "server bug" from "body was corrupted/deleted." This is a hard failure for reserve/
     peek.

     Recommendation: Consider whether NotFound deserves its own response (e.g., InternalError is fine, but add more structured tracing with body_id, job_id,
     and segment info for debugging).

     ---

     Testing Gaps

     There are no integration tests that exercise WAL + TOAST together end-to-end. The unit tests in:
     - wal.rs — test serialization, replay, compaction target, GC (with synthetic BodyRef::External)
     - body_store.rs — test write/read/rotate/delete/compaction in isolation

     …but nothing tests:
     1. A put that writes to TOAST → WAL → survives restart → reserve reads from TOAST
     2. A crash between TOAST write and WAL fsync producing orphans that are cleaned on replay
     3. max_storage_bytes enforcement with both WAL and TOAST growing
     4. Pre-v5 inline body migration through a full server restart cycle
     5. Concurrent puts + compaction + reserve stress behavior

     Recommendation: Add at least one integration test in tests/ (or an existing integration test module) that spins up a full ServerState with both WAL and
     BodyStore on a temp directory, exercises put/reserve/delete, crashes by reopening, and asserts body readability.

     ---

     Metrics & Observability

     The Prometheus metrics are comprehensive. Notable wins:
     - tuber_toast_total_bytes, tuber_toast_live_bytes, tuber_toast_segments
     - tuber_toast_compactions_total, tuber_toast_bodies_migrated_total
     - tuber_max_storage_bytes

     One gap: no metric for orphan bodies detected/reclaimed on replay. This would help operators understand whether their crash-recovery path is regularly
     cleaning up leaked space.

     ---

     Format & Versioning

     - WAL v5 writes BodyId reference. v3/v4 still readable.
     - TOAST v1 with TBOD magic. Simple and extensible.
     - The BodyRef enum keeps Inline for no-persistence mode, maintaining a single Job struct shape — exactly as the design doc intended.

     ---

     Concrete Recommendations

     1. Use spawn_blocking for TOAST compaction to prevent async runtime stalls.
     2. Add a replay-orphans metric (counter of bodies reclaimed during replay).
     3. Add integration tests that exercise the WAL+TOAST boundary together.
     4. Document the flush-tube WAL-failure behavior in a code comment.
     5. Enhance fetch_body error tracing with job_id, body_id, and segment/offset context for debugging corruption.

     ---

     Bottom Line

     This is a clean, well-reasoned implementation of a non-trivial architectural change. The hard problems (sync ordering, orphan cleanup, backward
     compatibility, logical addressing) are solved correctly. The remaining issues are operational polish (blocking I/O in async context, testing coverage,
     metrics gaps) rather than design flaws. Ship it after addressing the spawn_blocking concern and adding at least one end-to-end integration test.