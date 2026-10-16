# TOAST recovery point — telling a torn tail from corruption

> **Status: accepted, 2026-10-13.** Options A and E1 ship together in WAL
> v9. F is the next step if salvage or startup time ever matters. B, C, D and
> E2 are recorded so the evaluation isn't redone.

## Context

The pre-launch review's H4: corrupt the length of body 3 of 10 in a TOAST
segment and restart. Eight of the ten jobs are gone.

0.16.0 added a header CRC and stopped truncating older segments, but the
review's repro has a single segment, which is also the newest. The startup
scan treats any damage in the newest segment as a torn tail
(`scan_segment`, `body_store.rs`): it truncates there, keeps no `.corrupt`
copy, and logs a WARN. The WAL replay then reaps every job whose body went
with it, journalling a delete for each, so the loss is permanent.

The rule "only the newest segment can be torn" confuses position with
durability. A crash can only tear bytes written after the last completed
fsync. Everything before that was durable, and damage there is corruption.
The newest segment is not just the recent writes either: TOAST compaction
appends the live bodies of old segments to it (`migrate_body`), then unlinks
the old segment. So the newest segment can hold bodies whose WAL records
have been durable for weeks, and truncating it destroys their only copy.

The WAL has the same blind spot. Its compaction rewrites live jobs from the
oldest segment into the newest, and damage mid-way through the newest
segment is truncated (with a `.corrupt` copy, at least).

What's needed is the durable boundary: where the last fsync reached.

## Decision

**A: record the TOAST recovery point in the WAL.** Every WAL fsync is
already preceded by a TOAST fsync (`Wal::pre_sync_body_store`). Once that
returns, the WAL appends a `ToastMark { seq, offset }` record to the batch it
is about to fsync, naming how far TOAST was durable. Every new WAL segment
repeats the latest mark after its id mark, so GC never takes the last one.

At startup the latest mark is read from the newest WAL segments before TOAST
is opened. Then:

- Damage in the newest TOAST segment **before** the mark is corruption. It
  is handled like damage in an older segment: refuse to start, or with
  `--wal-skip-corrupt` keep a `.corrupt` copy, cut the segment at the
  damage and carry on with the bodies before it.
- Damage **after** the mark is a torn tail and is truncated quietly, as
  before.
- A newest segment shorter than the mark lost bodies the WAL vouched for,
  but what is left isn't damaged. Startup carries on, and replay reaps
  each job whose body is missing, logged as lost data and counted in
  `recovered-missing-bodies`, as for a body lost any other way. Refusing
  instead is left open.
- A newest segment older than the mark's segment: the mark's segment is
  gone. Usually the compactor deleted it, since an old-format newest
  segment takes no new bodies and is emptied once its own are dead;
  otherwise it was lost. Either way the segment now newest was sealed
  before the mark's segment was created, so none of it can be torn, and
  damage anywhere in it is handled like damage in an older segment.
- No mark (a WAL from before v9, or an empty one): the old rule, truncate.

A mark can lag safely. A stale one only widens the stretch treated as
possibly torn; it can never call a torn byte durable. So marks need no
fsync of their own, and losing the newest ones in a WAL torn tail is fine.

**E1: the WAL carries each TOAST body's CRC.** A `FullJob` that references a
TOAST body stores the body's CRC32 beside its `BodyId` (body kind 2). The
CRC is the one `write_body` already computes and stores in the body's
header, so it costs 4 bytes per record and no hashing. Compaction copies
bodies byte for byte and reuses the header, so the value stays right
wherever the body moves: nothing has to track compaction.

- The startup scan already reads every body header, so the index keeps the
  header CRC (it fits in `BodyLocation`'s padding). Replay compares it with
  the WAL's for every job without reading a body. A mismatch means TOAST
  holds an intact body that isn't the one the WAL wrote — a misdirected
  write, or a segment from another backup — and the job is reaped as if its
  body were missing.
- Jobs replayed from older records (body kind 1) take the CRC from the
  index, so every live job has one after startup.
- A reserve checks the record's header CRC against the WAL's before the
  bytes go out.

Together: the WAL says which bodies must be durable (A) and exactly which
bytes each must be (E1). TOAST has to produce them.

Both need WAL v9. Without the bump, 0.16 would take a mark or a kind-2
body for a corrupt record: it would refuse in an older segment, but in the
newest it would truncate there, quietly losing everything after the first
mark. With it, 0.16 sees a bad header instead. That is no clean refusal
either: a newest segment with a bad header is set aside as `.corrupt` and
startup carries on without it, and only an older one (every start adds a
segment) stops it. But it is loud, and the segment is kept whole. Rolling
back means restoring a backup taken before the upgrade, as for v8. The
replication generation number needs no bump: it goes in the segment
header's `flags` word, which every version writes as 0 and ignores on
read.

## Options considered

| Option | What | Format change | Verdict |
|---|---|---|---|
| **A** | TOAST recovery point recorded in the WAL | WAL v9 | **yes** |
| B | Recovery point in a checkpoint file beside TOAST | none | fallback |
| C | Scan forward past damage for a valid header | none | no |
| D | Per-segment salt in the header CRC; salvage past damage | TOAST v3 | no; F does it better |
| **E1** | Body CRC in the `FullJob` record | WAL v9 | **yes** |
| E2 | Body location (segment, offset) in the `FullJob` record | WAL | no |
| F | Hint files: a durable TOAST index written when a segment seals | new file | later |

### Why not B

A Kafka-style `toast/recovery-point` file, rewritten every few seconds and
at rotation and shutdown (tmp, fsync, rename), needs no format change and
nothing at startup reordered. It would have caught the review's repro. But
it ties TOAST to nothing in the WAL, and the last few seconds of the newest
segment stay unjudged. It is the fallback if a format change is ever
unwelcome.

### Why not C

Treat damage in the newest segment as corruption if a header that passes
its CRC follows it. Unsafe on its own: the kernel writes back pages in any
order, so after a power cut a later page can be on disk when an earlier one
isn't. C would refuse to start after an ordinary power loss. Past a
recovery point it adds nothing A doesn't already say.

### Why not D

Put a random salt in the TOAST file header's 8 reserved bytes and fold it
into each header CRC. Salvage under `--wal-skip-corrupt` can then search
forward byte by byte for the next record and keep bodies 4–10 instead of
losing them:

- Chance matches: a 64 MiB segment has ~64M offsets, so a 32-bit header CRC
  alone would find a false header in about 1.5% of full scans. Checking the
  body CRC as well makes this negligible even without the salt.
- Crafted bodies: a producer controls body bytes and can embed a valid
  unsalted header. If damage lands just before such a body, an unsalted
  search accepts it and can point a lost job's `body_id` at bytes the
  producer chose. The salt never leaves the server, so this can't be forged.

It changes what a checked field means, so it needs TOAST v3, and it only
improves an operator's last resort. F makes salvage exact without searching.
It does not help with stray bytes after a truncation (the comment in
`scan_segment`): those were written under the same salt.

### Why not E2

With locations in the WAL, TOAST compaction has to tell the WAL where each
body went. Either:

- **Relocation records:** copy, fsync TOAST, write and fsync
  `SegmentCompacted { old_seq, [(id, new_offset)] }`, then unlink. Crash-safe
  at every step, and WAL compaction rewriting a job's record makes its
  relocations garbage. But TOAST compaction is a background task today, and
  it would have to go through the engine thread that owns the WAL.
- **Tie it to WAL compaction,** as RocksDB's BlobDB does: move the body when
  WAL compaction rewrites its job's record. Free relocation, but TOAST
  compaction then follows the WAL's oldest-segment-first order instead of
  choosing segments under 0.5 live, and the two contend.

Both drag the WAL into TOAST compaction. F gets the benefits — no startup
scan, exact salvage — without it.

### F, later

Bitcask's hint files. When a TOAST segment seals, or compaction writes one,
write a footer or sidecar listing `(id, offset, len, crc)` with its own CRC.
Startup reads hints instead of walking every header, and salvage in a
damaged sealed segment knows where every record starts. The WAL doesn't
change, TOAST compaction stays separate, and only the newest, unsealed
segment is still scanned — which A already covers.

## Prior art

| System | How it tells a torn tail from corruption |
|---|---|
| Kafka | `recovery-point-offset-checkpoint` per log dir. Below it is trusted; above it is re-validated and truncated at the first bad batch. The checkpoint lags safely. |
| BookKeeper | Journal (WAL) plus entry logs (data), tuber's split. `lastMark` records the journal position up to which entry logs are flushed. |
| WiscKey | Key-value separation. The value log's durable head is stored in the LSM. |
| RocksDB BlobDB | Per-record CRCs in blob files; a blob file enters the MANIFEST only once synced. |
| PostgreSQL | WAL records chained by `xl_prev` with CRCs; `minRecoveryPoint` in the control file. |
| SQLite WAL | Per-generation salt and a checksum chained across frames, so stale bytes never read as valid. |
| etcd | CRC chained across records and files; zero-filled preallocated segments, so a torn write leaves zero sectors. Anything else is a hard error. |
| RocksDB WAL | Recovery modes: tolerate a corrupt tail, absolute consistency, point-in-time, skip corrupt records. |
| ZFS | Parent block pointers carry the child's checksum. E1 is this, one level deep. |

`RWF_ATOMIC` (Linux 6.11+) doesn't help: it needs O_DIRECT and hardware
support, covers 16–64 KiB at most against bodies up to 1 MiB, and can't tell
corruption from a torn write.
