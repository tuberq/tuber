# GLM-5.1 Review: WAL + TOAST Body Split

Review of `docs/wal-body-split.md` against the implementation in `src/body_store.rs`, `src/wal.rs`, `src/job.rs`, `src/protocol.rs`, `src/server.rs`, and `src/main.rs`.

## Faithfulness to Design

The implementation tracks the design doc closely. Every major decision in the doc is reflected in code:

| Design Decision | Implemented? | Notes |
|---|---|---|
| BodyRef enum (Inline/External) | Yes | Exactly as specified |
| Always-External when `-b` set (no threshold) | Yes | `unreachable!()` in WAL v5 serializer enforces this |
| Logical BodyId addressing | Yes | `BodyId(u64)`, monotonic, never reused |
| In-RAM `HashMap<BodyId, BodyLocation>` index | Yes | Rebuilt from header scan on startup |
| TOAST segment format | Yes | 16-byte file header + 20-byte body headers + raw bytes, matches spec |
| Single `--sync-interval` | Yes | `--wal-sync-interval` is a deprecated alias |
| TOAST-then-WAL fsync ordering | Yes | `pre_sync_body_store()` called before every WAL fsync |
| Combined `--max-storage-bytes` budget | Yes | WAL + TOAST checked together |
| WAL reserve (one segment) | Yes | ~10 MiB reserve in `storage_limit_exceeded()` |
| OUT_OF_STORAGE response | Yes | Distinct from OUT_OF_MEMORY |
| No compression | Yes | Zero compression code |
| Compaction at live ratio < 0.5 | Yes | Background tokio task every 5s |
| Inline-to-External migration on replay | Yes | v3/v4 Inline bodies migrated before server starts |
| Orphan body reclamation on startup | Yes | WAL replay collects orphans, `delete_many` cleans them |

## Design Doc Accuracy Issues

Minor discrepancies between the doc and what was actually built:

1. **Doc says WAL version v3→v4.** Implementation went straight to v5 (reads v3, v4, v5; writes v5). The doc should be updated to reflect this.

2. **Doc says `--max-storage-bytes` is mandatory with `-b`.** Need to verify this is enforced at the CLI level — if not, it's a gap worth closing.

3. **`Job::new()` initializes `body` as `Inline`, then the caller overwrites to `External`.** This works but is a minor smell — the design doc implies Inline is a first-class variant, while in practice with `-b` it's a transient state that should never reach the WAL.

## Design Concerns

### 1. Compaction crash safety: duplicate entries on restart

When `compact_segment` migrates bodies and then unlinks the old file, a crash after migration but before unlink means the same body exists in two segments. On restart, the index scan will find both — the later entry in `open()` will overwrite the earlier one (since segments are scanned in seq order and `index.insert` is used). This happens to be correct (the new location is the right one), but it's implicit rather than documented. Worth a comment.

### 2. Compaction fsync gap

`compact_segment` does not fsync the new segment after migration. Bodies are written via `pwrite` but there's no `sync_data` before the old file is unlinked. A crash after unlink but before the new data is durable would lose those bodies. This is partially mitigated by the periodic sync interval, but there's a window.

### 3. Missing-body detection at startup

If the WAL references a body that TOAST doesn't know about (corruption or manual tampering), the job will fail at reserve time with a `NotFound` error returned as `InternalError`. This is arguably correct but silent — a startup warning about WAL-referenced bodies missing from TOAST would aid debugging.

### 4. Storage budget check is inherently racy

`total_bytes` is updated with `Ordering::Relaxed` atomics, sometimes inside the inner lock and sometimes outside it. Two concurrent puts could both pass the budget check before either writes. This is acceptable for a queue (the overage is bounded by one body per concurrent put), but should be documented as intentional.

### 5. Corrupted body blocks entire segment compaction

Compaction re-verifies CRC when reading from the old segment. If a body has bit-rotted, compaction fails with `BadCrc` and the entire segment compaction aborts. One corrupted body blocks reclamation of the entire segment. Consider logging and skipping corrupted bodies (treating them as deleted) rather than failing the whole compaction.

### 6. `BodyLocation.offset` is a footgun

The offset stored in the index is the body *data* offset (after the 20-byte header), so `read_body` must read the header at `offset - BODY_HEADER_SIZE`. This works but is a footgun for future maintainers. Storing the header offset would be cleaner and avoid the subtraction.

## What the Doc Gets Right

- **The "always external, no threshold" argument** is well-reasoned and the implementation confirms it — one code path, no branching, simpler mental model.
- **The TOAST-then-WAL ordering argument** is correct and well-implemented. The orphan-vs-dangling-reference tradeoff is the right one.
- **The combined budget with WAL reserve** prevents the deadlock the doc describes (TOAST fills budget → can't write delete WAL records → can't free TOAST space).
- **The logical BodyId indirection** is already paying off — compaction can relocate bodies without touching WAL records.

## Summary

The implementation is a faithful, well-tested realization of the design doc. The main gaps are: (1) the doc references WAL v4 but implementation shipped v5, (2) compaction has a fsync gap between body migration and old file unlink, and (3) corrupted bodies during compaction will block segment reclamation entirely rather than being skipped. The design doc itself is clear and well-argued — the "always-external" and "TOAST-then-WAL" decisions are particularly sound.
