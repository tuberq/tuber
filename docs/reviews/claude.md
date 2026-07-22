# Review: WAL/TOAST split (`feature/body-wal-split`)

Reviewer: Claude (Opus 4.7, 1M context)
Scope: design doc `docs/wal-body-split.md` and the implementation across `src/body_store.rs`, `src/wal.rs`, `src/server.rs`, `src/job.rs`, `src/main.rs`, `src/metrics.rs`, `tests/binlog.rs`.

## Verdict

A careful, well-executed implementation that tracks the design doc closely. Code is well-commented, tests are thorough (432 pass, 1 ignored), `cargo clippy` is clean on this branch's new code. I'd merge it; the items below are mostly polish or follow-ups, not blockers.

## Design adherence — what works

- **Two-store layout, logical addressing, no threshold.** `BodyId` (`src/job.rs:13`) is monotonic and never reused; `BodyLocation` (`src/body_store.rs:101`) is RAM-only and rebuilt by header scan on startup. The `BodyRef::Inline | External` enum (`src/job.rs:18`) keeps one Job shape across modes; the put hot-path always picks `External` when persistence is on (`src/server.rs:935`).
- **Sync ordering is consistently enforced.** Every WAL fsync path calls `pre_sync_body_store()` first: `write_put` / `write_state_change` when `sync_interval==0` (`src/wal.rs:946,1016`), `rotate_if_needed` (`src/wal.rs:915`), `maintain` (`src/wal.rs:1294`), `flush_and_sync` (`src/wal.rs:1315`). The TOAST-then-WAL invariant is documented at every call site, not just in the design doc.
- **Orphan recovery on replay.** `Wal::replay` returns body IDs whose owning job was deleted (`src/wal.rs:1180`), then filters out re-created ids via a `HashSet` (`src/wal.rs:1251`). `build_state` calls `body_store.delete_many` to reclaim them (`src/server.rs:2948`).
- **Background compaction.** Tokio task scans every 5s for segments below the 0.5 live-ratio threshold (`src/server.rs:3134-3179`); migration uses a stale-entry guard so a concurrent delete or migrator can't resurrect a body (`src/body_store.rs:536`).
- **Truncated-tail recovery.** `scan_segment` stops at the first incomplete record, treating the tail as not-present so the next write resumes there (`src/body_store.rs:671-680`). Tested.
- **Lock-free counters on hot paths.** `total_bytes` / `live_bytes` / `segment_count` are atomics so the per-put budget check and per-poll stats path don't grab the inner mutex (`src/body_store.rs:127-149`).
- **`--migrate-wal` opt-in for legacy.** Pre-v5 WAL records refuse to start without an explicit decision, with a clear error message naming the version and the flag (`src/server.rs:2907-2922`). Tested both directions (refusal, migration, no-op on already-current).
- **`OUT_OF_STORAGE` distinct from `OUT_OF_MEMORY`.** Wired through `protocol::Response` (`src/protocol.rs:121,181`) and surfaced from `cmd_put` only when the projected on-disk footprint plus the WAL reserve would exceed `--max-storage-bytes`. State-change records bypass the cap so an operator can always drain a wedged queue.
- **Documentation.** `CLAUDE.md` updated with the new module, the persistence model, the sync ordering, the disk-budget semantics, and the version constants.

## Concerns worth following up

None are blockers; the implementation is shippable as-is. Each point names a specific file/line and either a fix or a question.

### A. WAL reserve floor is generous to a fault

`storage_limit_exceeded` reserves `max(wal.max_file_size(), DEFAULT_MAX_FILE_SIZE)` — i.e. ≥10 MiB unconditionally (`src/server.rs:392`). The comment says small custom segment sizes "must still leave room for state-change churn," but a state-change record is 39 bytes; 10 MiB reserve = headroom for ~270k deletes. Anyone trying a tiny budget (say 5 MiB on a dev box) will get `OUT_OF_STORAGE` before the first put. A bounded reserve like `STATE_CHANGE_RECORD_SIZE * N` (some plausible per-rotation churn ceiling) would be more proportional.

### B. `write_body` allocates a `BodyId` before append succeeds

`next_body_id.fetch_add(1, SeqCst)` runs unconditionally (`src/body_store.rs:225`). On rotation failure (ENOSPC, EMFILE), the id is burned. Not catastrophic — restart rebuilds the index correctly because the burned id never appears on disk — but a slow u64 leak under sustained errors. Also, `SeqCst` is stronger than needed for monotonic-unique; `Relaxed` would suffice (the other atomics on the same struct already use it).

### C. Body leaks if `wal_write_put` fails post-`write_body`

In `cmd_put`, body lands in TOAST first (`src/server.rs:935`), then WAL. If the WAL write fails (`src/server.rs:2807-2813`), the WAL is disabled but the orphan stays on disk forever — the orphan-detection path on replay only sees deletes, not no-shows. Consider calling `bs.delete(body_id)` in that error branch. Realistically: if WAL writes are failing you have bigger problems, but a `tracing::warn!` plus the cleanup is cheap.

### D. Compaction runs unconditionally when persistence is on

The background task spawns whenever `body_store.is_some()` (`src/server.rs:3136`), even with `--max-storage-bytes` unset. Cheap (one mutex acquire + scan per 5s) but counter-intuitive when an operator has deliberately disabled the budget. Doc-only fix: note in `--max-storage-bytes` help that compaction is independent of the budget — it triggers off live-ratio, full stop.

### E. Per-body lock acquire in `compact_segment`

Migration takes the inner mutex once per body (`src/body_store.rs:531`). For a 64 MiB segment of 50 KiB bodies that's ~1300 acquires per compaction. Each is brief, but could starve concurrent puts under heavy load. Worth measuring before optimising; the obvious next step is batching N bodies per lock acquisition.

### F. Pre-existing clippy warnings on `src/main.rs:92-94`

Two `doc_lazy_continuation` warnings on `--max-jobs-size` doc paragraphs that are unrelated to this branch but live in a file you touched. Trivial to clean up while here so the branch leaves clippy fully clean.

### G. Test coverage gaps (opportunistic, not regressions)

- No server-level test covers crash *between* WAL fsync and `BodyStore::delete` to confirm end-to-end orphan reclamation. Only the WAL unit test covers it (`test_replay_returns_orphan_body_ids_for_deleted_jobs`).
- No test exercises TOAST segment rotation across a server restart. Header-scan rebuild within one segment is tested; rotation + restart is not.
- No concurrent put-vs-compaction test on a non-current segment. `compact_current_segment_is_a_noop` covers a different race.

## Test results

- `cargo build` — clean.
- `cargo test` — 432 passed, 1 ignored, 0 failed across all suites.
- `cargo clippy` — clean except for the two pre-existing doc warnings noted in (F).
