# Replication and fast failover (design notes)

Status: design discussion, not implemented. Captured 2026-07-25.

Goal: a single host does the work, but the WAL and TOAST are available on
another host and cutover is fast. The model borrowed throughout is Postgres
streaming replication — logical record shipping, replay on the standby,
generation numbers (PG "timelines") for split-brain detection.

## The WAL is already a complete logical replication log

The starting worry was that extension metadata (`idp:`/`grp:`/`aft:`/`con:`)
and per-job counters might not replicate cleanly. Inspection says otherwise —
everything durable is already in the record formats:

- `FullJob` records carry idempotency key + TTL, group, after-group,
  concurrency key + limit, and the `reserve_ct`/`timeout_ct` counters
  (src/wal.rs, v7 payload layout).
- Reserves are logged: every reserve writes a `StateChange` with
  `StateChangeReason::Reserve` (src/server.rs, cmd_reserve path), and replay
  increments the per-job counters per record.
- Idempotency tombstones are embedded in delete `StateChange` records
  (`expiry_epoch_secs`) and recovered on replay with expiry honored.
- Delayed jobs replay with their *remaining* delay (v7 `change_epoch_secs`).

What deliberately does not survive replay, and why that's correct for
failover:

- **Reservation leases.** Replay maps `Reserved → Ready` and clears
  `reserver_id`. On failover the worker's TCP connection died with the
  primary, so the lease is void and the job must be redeliverable. Same
  at-least-once contract TTR already imposes; workers see a mass TTR expiry
  (and `touch-all`'s count-drift detection surfaces it to batch workers).
- **Connection state** (use/watch/reserve-mode/weights, waiting lists) —
  dies with the connections; clients re-issue `use`/`watch` on reconnect.
- **Tube pause** — `pause`/`unpause_at` are in-memory only. A paused tube
  unpauses on failover. Honest gap; could become a WAL record type if it
  matters.
- **Cumulative tube/global stats** — reset on failover. Cosmetic.
- **The last `--sync-interval` window (~100 ms) of un-fsynced records** —
  already accepted for local crash; replication adds shipping lag on top.

## Three tiers

**Tier 0 — cold standby, ordered file mirroring, zero code changes.**
Both stores are append-only segment files; a dumb tailer/rsync works.
Cutover = start tuber on the standby, normal replay runs. Replay time is
bounded: `--max-storage-bytes` caps WAL + TOAST and compaction keeps the
live ratio up. Mirror ordering must respect the same invariant
`pre_sync_body_store` enforces locally: **copy TOAST before WAL, deletions
last** (orphan bodies on the standby are safe; dangling references are not).
Concretely: rsync `toast/` first, then `binlog.*`, then a separate delete
pass. A torn tail on the live segment is safe — replay truncates and
preserves a `.corrupt` copy.

**Tier 1 — warm standby with continuous replay.** A replica mode: start
against the mirrored dir, replay, keep tailing new records into in-memory
state, don't serve clients (or stats only). Promotion is a signal.
Near-instant cutover; main change is making `Wal::replay` resumable.

**Tier 2 — streaming replication** (the rest of this document). Best RPO,
most code.

## Streaming design: one stream, logical records, bodies inline

Physical byte-mirroring of segment files is the wrong shape: TOAST
compaction rewrites live bodies into new segments and deletes old ones, so
physical shipping must replicate compaction events and re-ships bodies that
already crossed the wire; the replica can never compact independently.

Logical shipping instead: the stream carries the WAL record sequence —
`FullJob` records *with body bytes inlined*, `StateChange` records as-is.
The replica applies each record through the same path a local operation
takes: append the body to *its own* TOAST, get its own `BodyId`, write its
own WAL record, update in-memory state. Files diverge byte-wise; logical
state is identical. `BodyId`s are internal references, never exposed to
clients, so divergence is harmless.

Why logical wins:

- **Bodies cross the wire exactly once**, at put time. Compaction stays a
  local concern on each side.
- **The TOAST-before-WAL sync-ordering invariant comes for free** — the
  body is *in* the record, so the replica cannot apply a reference before
  it has the bytes.
- **The wire format already exists.** v3/v4 records are inline-body
  `FullJob` records, and replay already migrates inline bodies into TOAST.
  The replication apply loop is that migration move running continuously.

Mechanics:

- **Tap point:** tee logical records into a channel where
  `wal_write_state_change` / the FullJob write happens. The sender inlines
  body bytes for FullJob records.
- **Positions (LSN equivalent):** `(segment seq, byte offset)` on the
  primary's WAL. Every streamed record is tagged with its position. The
  replica acks applied positions — lag monitoring now, optional synchronous
  ack mode later ("don't ack the put until the replica acked the record").
- **Catch-up:** a replica reconnecting with a position gets a file-based
  scan — the primary reads its own WAL from that position, resolves bodies
  via the TOAST index, inlines, sends. Same record shape as the live tee;
  the replica can't tell the difference.
- **Stream content is the WAL, verbatim, in both phases.** Catch-up streams
  mostly FullJobs but also whatever StateChanges survive in live segments —
  do not filter to puts only, or counters and buried/delayed states come out
  wrong for jobs whose state changed after their FullJob was written.
  `touch` appears in neither phase because it never enters the WAL (nothing
  durable changes; leases void at promotion anyway). Reserve records are
  ~40 bytes — ship them; "stream = WAL" is a better invariant than "WAL
  minus some record types".

## Bootstrap is catch-up from position 0

PG needs base backups because its WAL is a truncated history. Tuber's WAL
is log-structured *storage*: segments are deleted only when every record in
them is dead (superseded jobs are rewritten as migration FullJobs in the
tail — the `decref_file` bookkeeping). Replaying the current segment set
from the oldest live segment reconstructs complete state, so **the WAL is
permanently its own base backup**. A new replica joins with position 0 and
gets the standard catch-up scan starting at the oldest segment. One
mechanism; bootstrap is its degenerate case. It also ships less than a file
copy would: only live bodies are resolved and sent, never
dead-but-uncompacted garbage.

Details that make the catch-up → live handoff clean:

- **Position-based dedup on the replica** (drop records ≤ last applied
  position). Subscribe to the live tee *first*, then scan files up to where
  the subscription began; overlap is harmless by construction. Needed
  because records are not idempotent to reapply (a duplicate Reserve would
  double-increment `reserve_ct`).
- **Pin the segment being scanned** (refcount hold). A segment deleted
  before the scanner reaches it is fine — its live jobs were migrated into
  tail FullJobs, and apply already skips StateChanges for unknown job ids.
  Body lookups during the scan go through the BodyStore index, not raw
  offsets, so TOAST compaction moving a body mid-scan resolves to the new
  location.
- **A replica joining at position 0 wipes its local WAL/TOAST first.**
  Leftover state is from an old generation; the handshake forces the full
  resync.

## Cutover and generations

The generation number (PG timeline) bumps as the first durable act of an
**explicit promotion step** — never as a side effect of the first client
write. Bump-on-write leaves a window where a promoted-but-idle node still
identifies as generation N, which is exactly the ambiguity generations
exist to kill.

Cutover sequence:

1. **Fence the old primary first.** Kill the process, pull the VIP, or
   firewall the port — before promotion, not after. Generations *detect*
   divergence; fencing *prevents* it. v1 fencing is an operator action or
   health-check script; consensus does not belong inside tuber.
2. **Promote the replica.** Stop the apply loop, drain the stream buffer,
   durably record generation N+1 — a small sidecar file (timeline-history
   equivalent) plus a stamp in the header of the next WAL segment opened.
   Only after that fsync does the client listener open. Crash-during-
   promotion is then unambiguous: restart replays local WAL, finds N+1,
   knows it is a primary.
3. **No state-conversion step, by design.** The replica applies the stream
   with replay semantics throughout (Reserve bumps `reserve_ct`, job stays
   Ready), so at promotion every job is already in its post-failover state.
   Jobs mid-reservation on the dead primary are immediately reservable —
   leases void, at-least-once, same as a local restart.
4. **Clients follow the VIP/DNS flip**, reconnect, re-issue
   `use`/`watch`/`reserve-mode`. Indistinguishable from a server restart to
   standard beanstalkd clients.
5. **The old primary rejoins as a replica.** Handshake with generation N
   against a gen-N+1 primary is refused; it must wipe and bootstrap from
   position 0.

The invariant underneath: **generation N+1 must be durable on the new
primary before any client write lands.** Fencing style, VIP mechanics, and
salvage are swappable per deployment; that ordering is what guarantees two
divergent WALs are distinguishable after the fact.

## RPO and the salvage tool

Async shipping means records the old primary fsynced but never shipped
(last ~sync-interval plus stream lag) are lost from the cluster on
failover. Acked puts inside that window can be lost — document it plainly.

Tuber-specific improvement: before wiping, a rejoin tool can scan the old
primary's WAL tail *past* the replica's last-acked position and re-put the
orphaned jobs against the new primary. For jobs carrying `idp:` keys the
salvage is safely idempotent — the new primary's tombstones and dedup
window absorb overlap — so the effective RPO for idempotent producers
becomes "nothing lost unless both disks die."

## Recommended path

Start with Tier 0 (an ops script, not a feature), measure real replay time
at production job counts, and only build the warm-standby / streaming modes
if that number is too slow. The storage budget caps replay time even for
pathological backlogs.
