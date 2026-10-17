# WAL + TOAST Binary Format Specification

WAL version 9 + TOAST version 2 — Tuber persistence layer.

## Overview

When persistence is enabled (`-b <dir>`), tuber maintains two on-disk stores
side by side:

- **WAL** (`binlog.NNNNNN`) — metadata records (FullJob carrying either an
  inline body or a `BodyId` reference, StateChange for
  delete/release/bury/kick/timeout). Replays on startup to rebuild the
  in-memory job map.
- **TOAST** (`toast/body.NNNNNN`) — append-only body segments holding raw job
  payload bytes, addressed by `BodyId`. Holds only bodies large enough to
  amortise its per-body fixed overhead (currently > 256 B); smaller bodies
  live inline inside the WAL FullJob record. The in-memory `BodyId →
  BodyLocation` index is reconstructed on startup by scanning segment
  headers.

```
wal-dir/
  lock              # flock — prevents concurrent access
  binlog.000001     # oldest WAL segment
  binlog.000002
  binlog.000003     # current writable WAL segment
  toast/
    body.000000     # oldest TOAST segment
    body.000001     # current writable TOAST segment
```

All multi-byte integers in both stores are **little-endian**.

The two stores are kept consistent by the **TOAST-then-WAL fsync ordering**:
every WAL fsync is preceded by a TOAST fsync. A crash mid-sync therefore
leaves orphan bodies (TOAST has bytes, WAL doesn't reference them) — wasted
space, never dangling references. Orphans are reclaimed on the next replay.

---

# Part 1 — WAL format

## File Header

```
 0                   1                   2
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  'T'  'W'  'A'  'L'  |     version (u32)     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      flags (u32)      |
+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Size | Field     | Value                                      |
|--------|------|-----------|--------------------------------------------|
| 0      | 4    | magic     | `TWAL` (`0x5457414C`)                      |
| 4      | 4    | version   | `8` (u32 LE) — current; v3–v7 still readable |
| 8      | 4    | flags     | `0` — reserved for future use              |

**Total: 12 bytes**

The `flags` field is reserved for forward compatibility. Currently written as
zero and ignored on read.

## Record Types

Five record types follow the header: FullJob and StateChange, plus
Tombstone and IdMark from v8 and ToastMark from v9. All share a common
envelope:

```
+-----------+------------------+------------------+---------+----------+
| type (u8) |   job_id (u64)   | payload_len (u32)|  payload | crc (u32)|
+-----------+------------------+------------------+---------+----------+
     1              8                  4            variable      4
```

The CRC32 (`crc32fast`) covers everything from the type byte through the end
of the payload — the CRC itself is excluded.

### 0x01 — FullJob (v6)

Written on `put`. Contains the complete job metadata plus a body section
that is either inline bytes or a `BodyId` reference into the TOAST store.
Per-record `body_kind` discriminant decides which — see "Body section"
below.

```
+------+----------+-------------+-----------------------------------+-----+
| 0x01 | job_id   | payload_len |            payload                | crc |
|  1B  |   8B     |     4B      |          variable                 | 4B  |
+------+----------+-------------+-----------------------------------+-----+
```

**Payload layout (v6)** — field order matches `serialize_full_job`:

```
+------------------------------------------------------------------+
| priority (u32)                                                    |  4
+------------------------------------------------------------------+
| delay_nanos (u64)                                                 |  8
+------------------------------------------------------------------+
| ttr_nanos (u64)                                                   |  8
+------------------------------------------------------------------+
| created_at_epoch_secs (u64)                                       |  8
+------------------------------------------------------------------+
| state (u8)                                                        |  1
+------------------------------------------------------------------+
| reserve_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| timeout_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| release_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| bury_ct (u32)                                                     |  4
+------------------------------------------------------------------+
| kick_ct (u32)                                                     |  4
+------------------------------------------------------------------+
| tube_name_len (u16) | tube_name (bytes)                          |  2+N
+------------------------------------------------------------------+
| idempotency_key          (option_string)                          |  2+N
+------------------------------------------------------------------+
| idempotency_ttl (u32)                                             |  4
+------------------------------------------------------------------+
| group                    (option_string)                          |  2+N
+------------------------------------------------------------------+
| after_group              (option_string)                          |  2+N
+------------------------------------------------------------------+
| concurrency_key          (option_string)                          |  2+N
+------------------------------------------------------------------+
| concurrency_limit (u32)                                           |  4
+------------------------------------------------------------------+
| body_kind (u8)                                                    |  1     <-- v6
+------------------------------------------------------------------+
| body section (variant — see below)                                |  variable
+------------------------------------------------------------------+
```

**Body section (v6):**

- `body_kind = 0x00` — Inline. Followed by `body_len (u32 LE)` and `body_len`
  raw body bytes. The runtime materialises this as `BodyRef::Tiny` for very
  small bodies (≤ 23 B, no heap allocation) or `BodyRef::Heap` (heap-allocated
  `Vec<u8>`) for larger ones. The on-disk format is the same either way — the
  tier boundary is a runtime choice carried in code, not in the format.
- `body_kind = 0x01` — External. Followed by `body_id (u64 LE)`. The body
  bytes themselves live in TOAST and are read at reserve/peek time using
  this id. Written before v9, and read by every version since v6.
- `body_kind = 0x02` — External with CRC (v9+). Followed by `body_id (u64
  LE)` and `crc32 (u32 LE)`: the body's CRC32, the same value its TOAST
  record header holds. Startup compares the two for every job, from the
  TOAST index, without reading a body; a reserve or peek checks the record
  header against it before sending the bytes. A record whose own CRCs agree
  but whose CRC isn't this one holds another body's bytes, and its job is
  reaped as if the body were missing. Compaction copies bodies unchanged,
  so the value holds wherever a body moves.

The decision of which kind to write happens at `put`-time based on body
length and whether a body store is configured. Bodies up to
`HEAP_INLINE_MAX` (256 B by default) stay inline; larger ones go external
when a body store is available, otherwise they remain inline.

This is what changed in v6: v5 unconditionally serialised a `body_id (u64)`
trailer (the body always lived in TOAST). v6 introduces the discriminant
so small bodies ride inside the WAL record itself, eliminating TOAST
overhead and compaction contention for small-body workloads.

### 0x02 — StateChange (v4+)

Written on `delete`, `bury`, `release`, `kick`, `timeout`. Fixed-size
payload (22 bytes in v4+; v3 was 21 bytes — see version history).

```
+------+----------+-------------+----------------------+-----+
| 0x02 | job_id   | payload_len |       payload        | crc |
|  1B  |   8B     |  4B (= 22) |        22B           | 4B  |
+------+----------+-------------+----------------------+-----+
                                 |                      |
                                 v                      |
                   +-------+-----+-----------+--------+----+
                   | state | pri | delay_ns  | expiry | rsn |
                   |  1B   | 4B  |    8B     |   8B   | 1B  |
                   +-------+-----+-----------+--------+-----+
```

| Offset* | Size | Field              | Notes                                   |
|---------|------|--------------------|-----------------------------------------|
| 0       | 1    | state              | New state, or `0xFF` for deleted        |
| 1       | 4    | new_priority       | Updated priority (u32)                  |
| 5       | 8    | new_delay_nanos    | Updated delay in nanoseconds (u64)      |
| 13      | 8    | expiry_epoch_secs  | Idempotency tombstone expiry (u64), 0 = none |
| 21      | 1    | reason             | StateChangeReason (see below) — v4+ only |

*Offsets relative to payload start.

**Total record size: 39 bytes** (1 + 8 + 4 + 22 + 4)

The `reason` byte was added in v4 so replay can reconstruct per-job
counters (`reserve_ct`, `release_ct`, `bury_ct`, `kick_ct`, `timeout_ct`).
v3 records replay with `reason = None` and don't increment counters.

v7 appended `change_epoch_secs` (u64) to the payload (30 bytes, record
47 bytes): the wall-clock second the change was applied, so a delayed
job replays with its remaining delay. From v8 a delete writes
`expiry_epoch_secs = 0`; a cooldown gets a Tombstone record of its own.

### 0x03 — Tombstone (v8+)

An `idp:` cooldown, whole: once a job with `idp:<key>:<ttl>` is deleted, a
put with the same key in the same tube is answered with the deleted job's
id until the cooldown expires. `job_id` is the deleted job's id.

| Offset* | Size | Field              | Notes                                   |
|---------|------|--------------------|-----------------------------------------|
| 0       | 8    | expires_epoch_secs | UNIX second the cooldown ends (u64); `0` clears it |
| 8       | 2    | tube_len           | u16                                     |
| 10      | N    | tube               | UTF-8                                   |
| 10+N    | 2    | key_len            | u16                                     |
| 12+N    | M    | key                | UTF-8                                   |

A record supersedes every earlier one for the same tube and key. While its
cooldown lasts it holds a reference on its segment, so GC keeps the
segment, and compaction rewrites it into the current segment as it does
live jobs. A clearing record (`expires_epoch_secs = 0`, written by
`flush-tube`) holds no reference: it only matters while an earlier record
for the key survives, and GC collects segments oldest first.

### 0x04 — IdMark (v8+)

The first record of every segment, written as the segment is created. Its
`job_id` field holds the highest job id assigned so far; the payload is
empty (`payload_len = 0`, record 17 bytes). GC never removes the newest
segment, so replay always finds the mark, and job ids never go backwards
when older segments are collected.

### 0x05 — ToastMark (v9+)

The TOAST recovery point: how far the body store was durable when the
record was written. `job_id` holds the TOAST segment seq and the payload
the offset in it (`u64 LE`, `payload_len = 8`, record 25 bytes): every
segment before `seq`, and `seq` up to `offset`, had been fsynced.

`Wal::sync` writes one after the TOAST fsync that precedes every WAL
fsync, into the batch that fsync makes durable, whenever the point has
moved. Every new segment repeats the latest after its IdMark, so GC never
takes the last one. Compaction moves bodies and fsyncs them with no WAL
write of its own, so the tick marks the WAL dirty when the body store's
fsyncs have passed the last mark, and the backstop sync records a new one.

At startup the latest mark is read from the newest WAL segment back
(`Wal::read_toast_mark`) before TOAST is opened, and decides how much of
the newest TOAST segment a crash could have torn: see TOAST Crash Safety.
Replay itself ignores marks. A mark can lag safely; a stale one only
widens the stretch treated as possibly torn. Once TOAST is open, its end
is the point the replay's new segment opens with, superseding the old one.
Design and alternatives: `docs/toast-recovery-point.md`.

## Encoding Details

### State Encoding

| Value  | State      |
|--------|------------|
| `0x00` | Ready      |
| `0x01` | Reserved   |
| `0x02` | Delayed    |
| `0x03` | Buried     |
| `0xFF` | Deleted    |

### StateChangeReason Encoding (v4+)

| Value | Reason   | Counter incremented on replay |
|-------|----------|------------------------------|
| `0`   | None     | (none)                       |
| `1`   | Reserve  | `reserve_ct`                 |
| `2`   | Release  | `release_ct`                 |
| `3`   | Bury     | `bury_ct`                    |
| `4`   | Kick     | `kick_ct`                    |
| `5`   | Timeout  | `timeout_ct`                 |

### option_string

Variable-length nullable string, used for extension fields.

```
+----------------+-----------------+
| len (u16 LE)   | bytes (UTF-8)   |
+----------------+-----------------+
```

- `len = 0` → None (no bytes follow)
- `len > 0` → Some(string), `len` bytes of UTF-8 follow

### Idempotency Tombstones

From v8 each cooldown is a Tombstone record (above). Before v8, the delete
StateChange's `expiry_epoch_secs` carried the expiry and replay took the
key from the job's FullJob record, so a cooldown was lost once GC
collected the segment holding that record or the delete itself. Replay
still rebuilds such cooldowns from v7-and-earlier records, and startup
rewrites each as a Tombstone record and syncs it.

## Replay Semantics

On startup, files are read in sequence order. For each record:

- **FullJob (v6)**: Insert or replace in the job map. The `body_kind`
  discriminant decides: kind `0x00` → inline bytes materialised as
  `BodyRef::Tiny` (≤ 23 B) or `BodyRef::Heap`; kind `0x01` →
  `BodyRef::External(BodyId, None)` whose bytes are looked up from TOAST
  at reserve/peek time; kind `0x02` (v9) → the same with the body's CRC.
  A job without one takes its CRC from the TOAST index after replay.
- **FullJob (v5)**: `body` is set to `BodyRef::External(BodyId)` — every
  v5 body lives in TOAST.
- **FullJob (v3/v4)**: Inline body bytes are read directly into the
  appropriate tier (`Tiny`/`Heap`). The post-replay migration step
  (see "Legacy migration" below) pushes only bodies larger than
  `HEAP_INLINE_MAX` into TOAST; smaller bodies stay inline and get
  rewritten as v6 inline records on the next put. v3/v4 reads require
  the `--migrate-wal` flag.
- **StateChange (delete)**: Remove from job map. If `expiry_epoch_secs > now`
  (pre-v8) and the job's FullJob record survives, restore its cooldown.
  Track the body's `BodyId` as a candidate orphan for the post-replay
  reclaim step.
- **Tombstone (v8)**: Replaces any cooldown for the same tube and key. If
  `expires_epoch_secs > now` the cooldown is restored and the record's
  segment gains a reference; otherwise (expired, or cleared) the key is
  free.
- **IdMark (v8)**: Raises the next job id to at least `job_id + 1`.
- **ToastMark (v9)**: Ignored; read before replay by `read_toast_mark`.
- **StateChange (other)**: Update the job's state, priority, and delay.
  Increment the counter named by `reason` (v4+).

State adjustments during replay:

- **Reserved → Ready** — reservations don't survive restarts
- **Delayed** — `deadline_at` reconstructed as `Instant::now() + delay`
- `reserver_id` always set to `None`

### Orphan body reclamation

After replay, the WAL returns a list of `BodyId`s belonging to jobs whose
StateChange-delete records survived but whose owning job is no longer in
the live set. Replay filters out any id that has been re-used by a re-put
within the same WAL (same job_id, fresh body) so live bodies are never
collected. The remaining ids are passed to `BodyStore::delete_many` to
reclaim disk space — this is what makes a crash between TOAST fsync and
runtime `BodyStore::delete` self-healing.

### Legacy migration (`--migrate-wal`)

When the newest WAL segment predates the current version, the server
refuses to start unless `--migrate-wal` is set, since older tuber
versions can't read the segments the new one writes. Only the newest
segment counts: a started server opens a current-version segment, so
older segments left behind (until compaction or GC empties them) don't
ask for the flag again. From v5 on nothing
is converted. For v3/v4, with the flag, the replay
path lifts inline body bytes that exceed `HEAP_INLINE_MAX` from v3/v4
FullJob records into TOAST (assigning fresh `BodyId`s), fsyncs TOAST, and
proceeds. Smaller v3/v4 inline bodies stay inline and will be rewritten
as v6 inline records the next time their owning job's state changes
(or on a subsequent put of a successor). Subsequent fresh writes are v6.

The migration is one-way and in-place. Back up the WAL directory first if
you want a rollback option, and roll back by restoring it: tuber 0.15
quarantines v8 segments as `.corrupt` and starts without them.

### Corruption handling

A crash can only damage the **newest** segment: it cuts the last writes
short. A corrupt or truncated record there ends processing of that file;
the valid records before it are kept, a copy of the whole segment is kept
as `<segment>.corrupt`, and the file is truncated at the bad offset so the
next restart doesn't trip on it again. A newest segment shorter than its
header (a crash between creating it and fsyncing the header) holds no
records and is removed.

Damage anywhere else stops the server from starting (the TOAST body
store follows the same rule; see its Crash Safety section):

- a corrupt or truncated record, or a bad header, in an **older**
  segment. Rotation fsyncs a segment before creating the next one, and
  creating a segment fsyncs its header, so a crash can't explain it.
  Records there were durable.
- any segment, newest included, that can't be read (EIO, permissions).

Replaying past either would lose the changes recorded there. A lost
delete brings its job back, and a lost put loses its job. The error
names the segment and the damage, and nothing on disk is changed. Restore the WAL
directory from a backup, or start with `--wal-skip-corrupt` (env
`TUBER_WAL_SKIP_CORRUPT`) to replay what remains. A segment with a bad
header or a read error is then renamed to `<segment>.corrupt`, and one
with a corrupt record is copied there and truncated at the bad offset, so
a later start doesn't need the flag again. If the copy can't be written
(a full disk, say), the server refuses to start, saying why, and leaves
the segment whole, since it is then the only place the records past the
damage are; once the copy can be written, the same flag gets past it. A
torn tail in the newest segment is truncated either way, since appends
continue past it.

## Durability

Writes pass through a 64 KiB userland `BufWriter` to amortise syscall
overhead. `fsync` cadence is controlled by `--sync-interval` (env
`TUBER_SYNC_INTERVAL`, default `100ms`). The legacy `--wal-sync-interval`
flag is accepted as a hidden alias for backward compatibility.

The same interval drives both WAL and TOAST fsyncs. On every sync tick,
TOAST is fsynced *first*, then WAL — see the ordering rationale in the
overview.

- **`--sync-interval 0`** — TOAST and WAL are flushed and `fsync`'d on every
  put/state-change *before* the server acknowledges the client. Zero
  ack-vs-durable window; the engine task blocks on fsync, so throughput
  drops sharply.
- **`--sync-interval <duration>`** (e.g. `100ms`, `1s`) — writes are buffered
  and `fsync` runs at most once per interval, plus once on segment rotation
  and once on clean shutdown. Up to `interval` of acknowledged records can
  be lost on a crash. On clean shutdown the tail is always synced regardless
  of interval.

There is no "never fsync" mode: if you don't care about durability at all,
don't pass `-b`/`--binlog-dir`. Setting a very large interval (e.g. `24h`)
approximates that behaviour while still syncing on clean shutdown.

When `sync_interval` is shorter than the engine's 100 ms tick, the tick
shortens to match so fsyncs don't back up behind tick cadence.

## File Management

### Segmentation and Rotation

WAL files rotate when the current segment reaches `max_file_size` (default
10 MiB). The current file is fsynced before a new segment is created.

StateChange records are allowed to slightly exceed the size limit — this
avoids per-file balancing complexity while keeping segments roughly bounded.

### Garbage Collection

Each WAL file tracks a reference count (`refs`): the live jobs whose most
recent FullJob record is in that file, plus the live cooldowns whose
Tombstone record is. On each server tick, head files with `refs == 0`
are deleted, oldest first. The current writable file is never removed;
each opens with an IdMark, which keeps the job-id high-water mark, and
then the latest ToastMark.

### WAL compaction

When the dead-to-live ratio across all WAL files reaches 2:1, the engine
migrates jobs out of the oldest file into the current one (re-writing
their FullJob records) at a rate of `ratio` jobs per tick, and the same
number of live cooldowns (re-writing their Tombstone records). This frees
the oldest file for GC. Self-regulating: more waste = more migration.

### Storage budget

`--max-storage-bytes` is **mandatory** when `-b` is set. It caps the
combined WAL + TOAST footprint. Puts return `OUT_OF_STORAGE` once the
projected footprint plus a one-WAL-segment reserve would exceed the
budget. State changes (delete, release, bury, kick, touch) bypass the
cap entirely so an operator can always drain a wedged queue.

The WAL reserve guarantees there's always room to journal the deletes
that would free TOAST space — without it, a TOAST-full state would
deadlock.

### Directory Locking

The WAL directory is exclusively locked via `flock(2)` on a `lock` file.
This prevents multiple tuber instances from writing to the same WAL.

## Version History

| Version | Header Size | Changes |
|---------|-------------|---------|
| 1       | 8 bytes     | Initial format: magic + version. |
| 2       | 12 bytes    | Added `flags` (u32). Added `expiry_epoch_secs` to StateChange for idempotency tombstones. Added `concurrency_limit` and `idempotency_ttl` to FullJob payload. |
| 3       | 12 bytes    | Reordered FullJob payload: body moved to end, idempotency_ttl grouped with idempotency_key, concurrency_limit grouped with concurrency_key. |
| 4       | 12 bytes    | Added `reason` (u8) byte to StateChange payload (21 → 22 bytes), enabling per-job counter reconstruction on replay. |
| 5       | 12 bytes    | Replaced inline body bytes (`body_len u32` + raw bytes) in FullJob with `body_id (u64)` reference. Bodies live in TOAST. v3/v4 reads still supported via `--migrate-wal`. |
| 6       | 12 bytes    | FullJob body field becomes a 1-byte `body_kind` discriminant followed by inline (`body_len u32` + bytes) or external (`body_id u64`). Small bodies stay inline; only larger bodies go to TOAST. Per-record kind decouples the on-disk format from the runtime threshold. v3–v5 reads still supported (v3/v4 via `--migrate-wal`). |
| 7       | 12 bytes    | StateChange payload gains `change_epoch_secs` (u64), so a delayed job replays with its remaining delay. |
| 8       | 12 bytes    | Tombstone (0x03) and IdMark (0x04) records. A cooldown no longer depends on its job's FullJob record surviving GC, and job ids never go backwards. Deletes write `expiry_epoch_secs = 0`. Startup on an older WAL needs `--migrate-wal`; an older tuber can't read v8 segments. |
| 9       | 12 bytes    | ToastMark (0x05) records the TOAST recovery point, so startup tells a torn TOAST tail from corruption. FullJob body kind 0x02 carries the TOAST body's CRC32. Startup on an older WAL needs `--migrate-wal`. An older tuber sets a v9 newest segment aside as `.corrupt` and starts without it, and refuses an older one. The header's `flags` word is still written as 0 and ignored on read. |

---

# Part 2 — TOAST format

The TOAST ("The Oversized-Attribute Storage Technique") store holds raw
job body bytes, addressed by monotonically-increasing `BodyId`. WAL
records reference body ids; the in-memory `BodyId → BodyLocation` index
maps each id to a `(segment_seq, offset, len)` triple. The index is
**not** persisted — it's rebuilt on startup by scanning the file header
and per-body record headers in each segment file (body bytes themselves
are skipped during scan, so recovery cost is bounded by job count, not
body volume).

## Files

```
toast/
  body.000000     # oldest TOAST segment
  body.000001
  body.000002     # current writable segment
```

Files are named `body.NNNNNN` (zero-padded sequence number). New segments
are created when the current one would exceed the segment size limit
(default 64 MiB). Segments are sealed and `fsync`'d before rotation; a
freshly-created segment is also `fsync`'d after writing its file header.

## File Header

```
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| 'T'  'B'  'O'  'D' | version  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         reserved (u64)         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Size | Field    | Value                            |
|--------|------|----------|----------------------------------|
| 0      | 4    | magic    | `TBOD` (`0x54424F44`)            |
| 4      | 4    | version  | `2` (u32 LE); `1` is still read  |
| 8      | 8    | reserved | `0` — for future use             |

**Total: 16 bytes**

## Body Records

After the file header, each body is laid out as:

```
+------------------------------------+
| body_id (u64)                       |  8
+------------------------------------+
| len (u32)                           |  4
+------------------------------------+
| crc32 (u32)                         |  4
+------------------------------------+
| header_crc32 (u32)                  |  4
+------------------------------------+
| body bytes (len)                    |  N
+------------------------------------+
```

**Header total: 20 bytes** + `len` body bytes.

- `body_id` — the logical id stored in the WAL's FullJob record.
- `len` — body byte count (does not include the header).
- `crc32` — `crc32fast` over the body bytes only.
- `header_crc32` — `crc32fast` over the 16 header bytes before it
  (`body_id`, `len`, `crc32`). Version 1 segments have zero here and are
  read without the check.

The body CRC is verified on every read. Compaction also re-verifies it
when copying bodies to a new segment, so silent disk corruption surfaces
loudly rather than silently propagating. The header CRC is verified by
the startup scan, so a damaged id or length is caught there instead of
being misread (see Crash Safety).

## In-RAM Index

The store maintains:

```rust
HashMap<BodyId, BodyLocation { seq, offset, len }>
```

- `seq` — the segment sequence number where the body lives.
- `offset` — byte offset of the **body data** within the file (i.e. the
  body header sits at `offset - 20`).
- `len` — body byte count (mirrors the header field for fast budget
  accounting).

The index is reconstructed on startup by `BodyStore::open`: each segment
is opened, its file header is validated, and its body record headers are
walked sequentially. Body bytes are skipped during scan; only header
bytes are read.

## Compaction

A background tokio task scans every 5 seconds for sealed segments whose
**live ratio** (`live_bytes / total_bytes`, where `live_bytes` excludes
deleted bodies) has dropped below `0.5`. The most-wasted qualifying
segment is compacted: its surviving bodies are migrated into the current
write segment (with a stale-entry guard so concurrent deletes can't
resurrect a body), then the old segment file is unlinked.

Outstanding `Arc<File>` handles held by in-flight readers keep the file
descriptor valid through the unlink (POSIX semantics) — that's what makes
unlink-during-read safe.

Compaction works precisely *because* the WAL references logical
`BodyId`s, not physical locations: the index update is the only
synchronisation point.

## Crash Safety

- **TOAST-then-WAL fsync ordering** (see overview) — a WAL record never
  becomes durable before the body it references. A crash leaves orphan
  bodies, never dangling references.
- **Torn tail recovery** — a crash can only cut short what was written
  after the last completed fsync. Rotation fsyncs a segment before
  creating the next, and fsyncs the new segment's file header, so that
  is in the newest segment, and from the WAL's TOAST recovery point on
  (its latest ToastMark; all of the newest segment without one, as
  before WAL v9, and none of it when the mark names a later segment,
  since deleted). There, `BodyStore::open` stops at the first partial
  record, failed header CRC or length running past the end of the file,
  and truncates the segment at it. The next write resumes at that point.

  The newest segment is not just recent writes: compaction appends the
  live bodies of older segments to it, then unlinks them. Without the
  recovery point a bad header in the middle of it was taken for a torn
  tail, and every body after it was truncated away, durable or not.
- **Damage** — the same findings before the recovery point in the
  newest segment, anywhere in an older segment, or a segment with a bad
  file header, mean bodies that were durable are gone, and the server
  refuses to start, leaving the files alone. So does a newest segment
  that ends before the recovery point; with `--wal-skip-corrupt` it
  starts without the bodies that were past its end. With `--wal-skip-corrupt`
  it starts with the bodies before the damage and keeps a copy of the
  segment as `<segment>.corrupt` (or moves it there when its file header
  is bad). The segment is then truncated at the damage, so a later
  start doesn't need the flag again. If the copy can't be written, the
  server refuses to start and leaves the segment whole, since the bodies
  past the damage may still be recoverable from it. Jobs whose
  bodies are lost are reaped as missing-body jobs (below).
- **Version 1 segments** — read without the header check. A version 1
  newest segment is not appended to; the first write starts a version 2
  segment.
- **Orphan reclamation** — bodies whose owning jobs were deleted before
  the runtime `BodyStore::delete` could run are detected during WAL
  replay and dropped via `BodyStore::delete_many`.
- **Compaction restart** — if a crash happens between writing the
  migrated body and unlinking the old segment, the same `BodyId` exists
  in two segments. On restart the header scan iterates segments in seq
  order; the new (later) location wins via `index.insert`. Subsequent
  compaction reclaims the duplicate.
- **Missing-body reaping** — TOAST-then-WAL ordering rules out the
  WAL→missing-body direction *for clean crashes*, but disk corruption,
  manual file removal, and the corruption-drop path in `compact_segment`
  can all leave WAL FullJob records pointing at body ids that no longer
  exist in the index, or (v9) at a record whose CRC isn't the one the
  FullJob carries. On startup, every such job is reaped (logged with
  `body_id` / `tube` / `state`, deleted from the live set, and journalled
  as a state-change-delete so the warning doesn't re-fire). The reaped
  count is surfaced as `recovered-missing-bodies` in `stats`. A
  missing-body job that survived to runtime would `INTERNAL_ERROR` every
  reserve attempt against it — startup reaping prevents that poison-pill
  state.

  **Operational note:** `rm -rf <wal-dir>/toast/` (or any other
  catastrophic loss of TOAST while the WAL survives) reaps **every**
  job — there's nothing to serve their bodies from. This is honest:
  the bodies are gone, so the jobs are. Move the WAL aside too if you
  want a clean restart with no recovery noise.
- **Stranded-body reclamation** — symmetric to missing-body reaping.
  On startup, the reclamation walks the TOAST index and drops any
  `BodyId` that no live job points at. Counted as
  `reclaimed-stranded-bodies`.

  It then compacts the affected segments so the bytes physically leave
  disk, but only those that have gone sparse (live ratio below
  `COMPACTION_LIVE_RATIO_THRESHOLD`, the same bar
  `compaction_candidate` applies on the background tick). Compaction
  copies every *live* body out before unlinking, so rewriting a
  mostly-live segment to reclaim a sliver is the wrong trade — and
  startup, before the listener is up, is the worst moment to make it.
  A qualifying segment that is still the current write target gets a
  forced rotation first, since neither this pass nor the background
  tick can compact the segment being appended to; the rotation costs
  one 16-byte file header, and is skipped when the segment doesn't
  qualify so a restart loop can't mint a segment per boot.

  The orphan pass (`reclaimed-orphan-bodies`) hands its segments to
  this step via `delete_many_tracking_segments`. Its ids are already
  out of the index by then, so the scan above cannot rediscover their
  segments — without the hand-off those bytes would wait for the
  segment to seal on its own. One compaction step covers both passes;
  the counters stay separate.

  **This is normally routine, not a failure signal.** TOAST has no
  on-disk deletion tombstone: `BodyStore::delete` drops the index
  entry and decrements the segment's live-bytes counter, but the
  bytes stay in the segment until compaction rewrites it. Because
  `BodyStore::open` rebuilds the index by *scanning* the segment
  files, every deleted-but-uncompacted body reappears as live on the
  next start. Ones the retained WAL still names are re-killed by the
  orphan pass (`reclaimed-orphan-bodies`); ones whose WAL segment has
  already been reclaimed have no reference left anywhere and land
  here. So the count tracks delete volume since the last compaction
  of the affected segments.

  It gets large when a segment never rotates: `compaction_candidate`
  skips the current write segment, so a store that has only ever
  filled one segment (default 64 MiB) accumulates every dead body
  until either the segment fills or a restart triggers this pass —
  which force-rotates and compacts, so the reclaim usually frees most
  of the store's disk in one go.

  A nonzero count also repeats across restarts when the garbage sits
  in a segment above the compaction threshold: the bodies leave the
  index each time but the bytes stay, so the next scan re-finds them.
  That's working as intended — the segment isn't worth rewriting yet —
  but it means a *stable* count is not by itself evidence of a leak.

  The genuine leak lands here too: `cmd_put` writes TOAST first, then
  WAL, so a WAL write that fails after `write_body` succeeded leaves
  a body with no reference. On disk it is indistinguishable from
  delete garbage — see the alerting note in the matrix below.

### TOAST/WAL write-failure matrix for one put

`cmd_put` writes TOAST first (it needs the `BodyId` to build the WAL
record), then WAL. The fsync ordering at sync time is the same: TOAST
fsync runs before WAL fsync. Each cell shows what's left on disk and
which startup path cleans it up.

|   | TOAST ✓ | TOAST ✗ |
|---|---------|---------|
| **WAL ✓** | Happy path. Body durable, WAL durable. Client acked (immediately in relaxed mode, after sync in strict). | *Unreachable* — WAL is only attempted after `write_body` succeeds. |
| **WAL ✗** | The leak. Body in TOAST, no WAL reference. WAL gets dropped and puts are refused with `INTERNAL_ERROR` until restart. On restart: stranded-body reclamation drops the index entry and force-compacts the segment. Counted as `reclaimed-stranded-bodies` — but so is ordinary delete garbage, so `rate>0` is *not* an alertable signal on its own (see above). The one signal that discriminates is the WAL write error itself: the leak always logs one and drops the WAL, and delete garbage never does. Alert on that — `tuber_binlog_failed` goes to 1 when it happens. A count that grows restart over restart is corroborating, not diagnostic — threshold-gated compaction can carry the same garbage across several boots. | *Unreachable* — `write_body` errored first; cmd_put returned `INTERNAL_ERROR` and never touched WAL. Equivalent to TOAST ✗ alone. No state, no leak. |

The complementary direction — WAL has a FullJob but TOAST is missing
the body (segment header bit-rot, manual `rm`, the corruption-drop
path in `compact_segment`) — is handled by the missing-body reaping
described above. Counted as `recovered-missing-bodies`.

The two paths are independent. A single restart can produce nonzero
counts on both if multiple distinct failures happened.

## TOAST Version History

| Version | Changes |
|---------|---------|
| 1       | Initial format: 16-byte file header + 20-byte body record headers + raw bytes. CRC32 over body bytes only. |
| 2       | The body header's last 4 bytes, reserved in v1, hold `header_crc32` over the 16 bytes before them, checked by the startup scan. v1 segments are still read. An older tuber refuses to start on a v2 segment (`TOAST bad version: 2`). |
