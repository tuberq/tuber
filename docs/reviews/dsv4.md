# WAL + TOAST Split Review

## Structure & Coherence

The split is cleanly architected. `body_store.rs` (1012 lines) owns all TOAST logic; `wal.rs` (2714 lines) handles WAL v3/v4/v5 serialization and replay; `job.rs` has `BodyId` + `BodyRef`; `server.rs` connects them with the put/reserve/delete/flush-tube paths and the background compactor. The code is well-commented, especially the concurrency invariants (TOAST-then-WAL fsync ordering, stale-entry guard during compaction, orphan handling).

## Issues Found

### 1. `--max-storage-bytes` is not mandatory (design doc violation)

The design doc (line 23) says: *"--max-storage-bytes is mandatory when -b is set."* The implementation makes it `Option<u64>` with no enforcement. When absent, `storage_limit_exceeded()` returns `false` unconditionally (`server.rs:374-377`), meaning the budget fence is a no-op. The doc's rationale ("unbounded TOAST is exactly the kind of footgun...") is valid — this should be enforced at startup via a validation error if `-b` is set without `--max-storage-bytes`.

### 2. WAL version jump: v3→v5, not v3→v4 (doc mismatch)

The design doc (line 71) says: *"Bump WAL version (v3 → v4)."* The implementation uses `WAL_VERSION = 5` (`wal.rs:27`). v4 was consumed by the `StateChangeReason` byte addition (line 22-23). The design doc should be updated to say v3→v5, and it should explain that v4 was an intermediate step.

### 3. `docs/wal-format.md` only documents v3

The format spec (`docs/wal-format.md:1`) declares "Version 3" with a version history table ending at v3. It doesn't document:

- v4: the added reason byte in StateChange records (payload_len 21→22)
- v5: the `BodyId` replacement of inline body bytes in FullJob records

The TOAST on-disk format (TBOD magic, body record layout, segment lifecycle) is entirely undocumented in this file. This is the user-facing format doc — if someone needs to debug a binlog, they're in the dark.

### 4. `reserve_put` warning log still says OUT_OF_MEMORY

At `server.rs:921`, the warning message is `"WAL: OUT_OF_MEMORY — job record... exceeds max file size"`. This isn't a storage-budget issue — it's a record-too-large-for-a-WAL-segment issue. The log message is misleading in a TOAST world.

### 5. No orphan cleanup outside startup

The design doc (line 51) says orphan bodies are "Cleaned up on next compaction or detected on startup." The startup detection path exists (`wal.replay()` returns `orphan_bodies`, `build_state()` calls `delete_many`). The compaction path for orphans relies on the fact that orphan bodies have no index entries → dead bytes → segment drops below the 0.5 threshold. This is correct but *incidental* — compaction doesn't explicitly detect orphans. If all segments stay above 0.5 live ratio, orphan bytes persist until restart. This is acceptable in practice but worth noting.

## What's Well Done

- **TOAST-then-WAL sync ordering** is correct at every call site: `pre_sync_body_store()` before every WAL fsync in `write_put`, `write_state_change`, `maintain`, and `flush_and_sync`.
- **Orphan filtering** on replay (`wal.rs:1251-1258`) correctly excludes re-created jobs (same id, re-put in same WAL).
- **Compaction stale-entry guard** (`body_store.rs:536-539`) prevents double-migration or resurrection of deleted bodies.
- **Bulk delete in flush-tube** (`server.rs:1559-1570`) batches `delete_many` to avoid BodyStore mutex contention per job.
- **Self-healing truncation** on corrupt WAL records (`wal.rs:1231-1236`) truncates the file at the corruption point so the problem doesn't repeat on every restart.
- **Migration gate** (`server.rs:2904-2923`) requires `--migrate-wal` to be explicit before converting v3/v4 inline bodies into TOAST — good safety.
- **Memory accounting** (`server.rs:806-810`) correctly accounts `body_in_ram = 0` when BodyStore is present, so body bytes don't double-count toward the in-memory budget.
- **WAL version sniffing** (`wal::min_format_version`) runs before the TOAST dir is created, preventing side-effects on refusal.

## Recommendations

1. Enforce `--max-storage-bytes` mandatory when `-b` is set.
2. Update `docs/wal-format.md` to document v4, v5, and the TOAST on-disk format (TBOD magic, body record layout).
3. Update the design doc to mention v5 (not v3→v4).
4. Fix the `reserve_put` warning message to say "record too large" instead of "OUT_OF_MEMORY".
5. Consider adding a startup-time check: if `max_storage_bytes` exists but `wal.total_disk_bytes + body_store.total_bytes > max_storage_bytes`, refuse to start with a clear error. Currently the budget is only enforced on new puts — an operator reducing the budget and restarting gets no warning.
