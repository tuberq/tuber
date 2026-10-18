# Body I/O off the engine task

> **Status: proposed, not scheduled (2026-10-18).** It only matters once
> operators raise `-z` well past its 1 MiB default. Written down so the
> design, and the reasons the obvious version doesn't work, aren't worked out
> again.

## Context

Every command runs on one engine task (`ServerState`), so anything slow there
delays every client. With `-b`, bodies over 256 bytes (`HEAP_INLINE_MAX`) live
in TOAST, and both copies between RAM and TOAST happen on the engine:

- **Put:** the connection task reads the body into a `Vec`, which is fine
  because that's off the engine, and hands it over. `put_job` then computes
  its CRC and writes it to TOAST (`BodyStore::write_body_with_crc`), holding
  the body store's lock for the write. Then it has to reach disk. Either the
  next sync fsyncs it on the engine, or the next TOAST rotation does:
  `Inner::rotate` seals the previous segment with an fsync, under the lock, in
  whichever put triggers it.
- **Reserve and peek:** `fetch_body` reads the whole body and checks its CRC
  before the job is dequeued (`do_reserve`, `cmd_reserve_job`,
  `found_or_error`).
  - `reserve-batch` reads up to 1,000 bodies in one engine step.
  - A put that wakes a parked reserve reads its body back the same way
    (`serve_waiters`).

At the default 1 MiB this costs milliseconds. At `-z 1g`, the most
`JOB_DATA_SIZE_LIMIT_MAX` allows, every other client waits while each large
put or reserve moves a gigabyte through the page cache. Then they wait again
for its fsync, for as long as the disk takes. None of this has been measured.

## Proposal

The connection task does the I/O for large bodies. There are two thresholds,
both constants set by measurement:

- **Reads** move off the engine from around 64 KiB. The only extra cost is a
  hop to the blocking pool.
- **Writes** move off the engine from several MiB. Each one creates two
  segment files and costs five fsyncs (see below). Next to a gigabyte that's
  nothing, but it would swamp a 100 KiB put.

Below them nothing changes: small puts keep their single round trip to the
engine.

There's no protocol change and no on-disk format change, so a tuber from
before this change reads everything it writes.

### Put

1. The connection task reads the body as it does today.
2. It asks the engine to admit the put (`PutAdmit`: tube, length, tags). The
   engine runs the checks that don't need the body: draining, a failed WAL,
   `--max-jobs-size`, `--max-storage-bytes`, and the `idp:` dedup. Then it
   either:
   - answers the put outright, with `INSERTED <id> <state>` for a duplicate
     or with the refusal; or
   - admits it, adding its length to a pending-bytes count that the storage
     check includes. That stops several uploads in flight from overshooting
     the budget between them.
3. The connection task writes the body to TOAST on the blocking pool and
   makes it durable (see below).
4. It sends `PutInsert` with the body id and CRC.
   - The engine runs `put_job` on the already-written body and drops the
     pending bytes.
   - It repeats every check, because things may have changed since step 2:
     a duplicate put may have landed, or a drain begun. The storage check
     first takes out this put's own pending bytes, since its body is now
     counted in TOAST's total.
   - If the put is refused at this point, the engine deletes the body.

If a connection drops between steps 2 and 4, `unregister_conn` drops its
pending bytes and deletes its body if it was written. A crash in between
leaves an orphan body, which startup already reclaims.

The cost is two engine round trips instead of one for a large put, plus that
body's own fsync before the reply. In relaxed mode the fsync is new latency
for that client, though nobody else waits for it. It also makes the body
durable before `INSERTED`, which relaxed mode didn't promise.

### Where the body goes in TOAST

Moving `write_body_with_crc` to another task isn't enough on its own:

- **The lock:** appends happen under the body store's mutex. The engine's own
  TOAST calls would queue behind a gigabyte `pwrite`, so the stall would just
  move to the lock. Those calls are the fsync before each WAL sync reading
  its point, small puts' appends, and read lookups.
- **Writing outside the lock into the current segment:** the TOAST recovery
  point assumes the write segment is filled in order. A region that is still
  being written, or was abandoned by a crash, leaves a hole before bodies the
  WAL may already reference. Startup would either cut those bodies off with
  the torn tail or refuse to start, depending on which side of the recovery
  point the hole fell.

So a large body gets a segment of its own:

1. **Under the lock:** take the next segment number `N` for the body, then
   rotate the write segment to `N+1`. The write segment has to stay the
   newest. If `N` were newest after a crash, the write segment's torn tail
   would count as damage in an older segment, and that stops startup.
2. **Off the lock:** write the file header and body record to `body.N.tmp`,
   fsync it, rename it to `body.N`, and fsync the directory.
   - A crash before the rename leaves only the `.tmp`, which startup deletes.
   - Older tubers leave the `.tmp` alone, since they only match `body.`
     followed by digits (`BodyStore::open_with_recovery_point`).
3. **Under the lock:** register segment `N` as sealed, holding one live body,
   and index the body id.

The result is an ordinary v2 segment holding one body:
- The engine's sync never has to flush it.
- The recovery point and its startup rules are untouched, since they only
  concern the write segment.
- Compaction never moves the body, because its segment is all live. Once the
  job is deleted, compaction removes the file.

Today, a body larger than the 64 MiB segment size already ends up alone in
its segment, as a side effect of rotation. This design makes that deliberate
for every body above the write threshold, and moves the write off the lock.

The costs:
- **Five fsyncs per body.** Two are for the body itself: the `.tmp` file, and
  the directory after the rename. The other three are the rotation's: the
  previous write segment, the new one's header, and the directory again.
- **A fresh write segment behind each large body.** A burst of large puts
  creates near-empty segments, which compaction later merges. Until then they
  hold more open file descriptors (`warn_if_fd_pressure`).

Both costs are why the write threshold sits in MiB.

### Reserve and peek

The engine does everything it does today except the read. It dequeues the
job, marks it reserved and writes the state change. Then it replies with a
handle instead of the bytes: the segment's `Arc<File>`, offset, length and
CRC, taken under the lock. Segments are append-only and an open file
survives being unlinked, so the handle stays good if compaction moves the
body or the job is flushed in the meantime.

The connection task reads the body on the blocking pool and checks the CRC.
Then it writes the response as header, body and trailer, rather than first
copying the body into `resp_buf`. That halves peak RAM.

**An unreadable body** is now found after the job is reserved. Today the
engine reads before dequeuing, so it can bury the job and keep the tube
flowing (`bury_unreadable_ready`). Instead, the connection answers
`INTERNAL_ERROR`, as it does today, and tells the engine, which buries the
job from reserved. The outcome is the same. `reserve-job` currently leaves
the job where it was when the read fails. Under this design it would bury
the job too, which is a decision still to make.

**`reserve-batch`** reads all its bodies before writing anything.
`RESERVED_BATCH <n>` goes out first, so a failure partway through couldn't be
taken back. Batches keep today's RAM use, but off the engine.

## What it doesn't fix

- **The WAL fsync stays on the engine.** That covers strict mode's per-batch
  fsync and a schedule commit writing its whole set, which can be up to
  8 MiB. Moving the sync to its own thread is a separate change.
- **Rotation's fsync of the previous segment** stays under the lock, on
  whichever put triggers it. That segment now only holds small bodies,
  though.
- **Schedule fires** still write their bodies to TOAST on the engine. The set
  cap bounds their size.
- **The whole body is still held in RAM** on the way in and on the way out.

## Follow-ups

- **Stream bodies out.** Once the connection task has the handle, it can send
  the body in chunks, so a reserve costs one chunk of RAM. The catch is the
  CRC, which only resolves once the body has been sent. The options are to
  read the body twice (the second read comes from the page cache) or to close
  the connection on a mismatch.
- **Stream bodies in.** `put` declares `<bytes>` up front, so the connection
  task could create `body.N.tmp` first and write chunks as they arrive. With
  one segment per body, an abandoned upload is just a `.tmp` to delete, so
  this needs no format change either. Two things remain:
  - Admission has to come before the body is read, so step 2 moves ahead of
    step 1.
  - The trailing `\r\n` is only checked at the end.
- **Cap the body bytes in flight across connections.** With `-b`,
  `--max-jobs-size` doesn't count bodies, so N concurrent large puts hold N
  bodies in RAM unchecked.

## Testing

Start with tests that fail today:
- Hold a large body's TOAST write with a test hook that blocks it until
  released, and check that another connection's `stats` still answers
  meanwhile. Today it can't, because the engine is waiting on the write.
- Do the same for a reserve's read.

Then the crash cases:
- A crash before the rename leaves only the `.tmp`. Startup deletes it, and
  the put was never acknowledged.
- A crash after the rename but before `PutInsert` leaves an orphan, which
  startup reclaims.
- A tuber from before this change starts on a directory this one wrote.
