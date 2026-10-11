---
name: tuber
description: Interact with a Tuber or beanstalkd work queue server using tuber-cli (preferred), the tuber binary's put/work/tubes subcommands, or raw protocol over nc (netcat). Use when tasks involve job queues, background workers, running queued shell commands, or beanstalkd protocol commands.
---

# Tuber / Beanstalkd Work Queue Client

Two command-line tools talk to a tuber server:

| Tool | Use it for |
| --- | --- |
| `tuber-cli` | One-shot protocol commands with JSON output: inspect, put, delete, kick, flush, pause, drain. The default choice. |
| `tuber` (the server binary) | `tuber work` to *run* jobs as shell commands, `tuber put` to enqueue one job per stdin line, `tuber tubes` for a per-tube count table. |

Drop to raw protocol over `nc` only for the connection-scoped commands listed
under [Raw protocol](#raw-protocol-fallback), or when neither tool is installed.

```bash
command -v tuber-cli tuber
```

## Server address

`tuber-cli`, `tuber put/work/stats/tubes` and `tuber-tui` all find the server
the same way: the `-a`/`--addr` flag, else the first non-empty of `TUBER_URL`,
`TUBER_ADDR`, `BEANSTALKD_URL`, else `localhost:11300`. The Ruby gem reads
`TUBER_URL` too, so setting it once points every client on the box at the same
server. Despite the name it takes a bare address, not a URL: `host:port`,
`host` (port defaults to 11300), or `:port` (host defaults to localhost).

## tuber-cli basics

```
-a, --addr <ADDR>      Server address (see above)
-f, --format <FORMAT>  Output format: json (default) or text
```

Output is pretty-printed JSON on stdout. Stats keys are snake_case
(`current_jobs_ready`), not the protocol's hyphenated names — use those with
`jq`. A failure prints the server's reply as `Error: NOT_FOUND`,
`Error: TIMED_OUT`, `Error: DRAINING`, … on stderr and exits 1.

---

## Inspecting state

```bash
# Server-wide statistics
tuber-cli stats

# List all tubes (names only)
tuber-cli list-tubes

# One line per tube with its counts -- the quickest overview
tuber tubes
#   default: ready=4 reserved=0 delayed=0 buried=0
#   emails: ready=16 reserved=2 delayed=0 buried=1

# Tube statistics
tuber-cli stats-tube default

# Peek at a specific job by ID
tuber-cli peek 42

# Peek at next ready/buried/delayed job in a tube
tuber-cli peek-ready --tube series
tuber-cli peek-buried --tube series
tuber-cli peek-delayed --tube series

# Job statistics (state, pri, age, TTR, reserves, timeouts, etc.)
tuber-cli stats-job 42

# Group statistics (debug why aft: jobs aren't running)
tuber-cli stats-group batch-1
```

To see what a worker would get next, use `peek-ready`, not `reserve` — a
reserve really takes the job (bumping its `reserves` count) and hands it back
when the process exits.

## Producing jobs

```bash
# Put a job into the default tube
tuber-cli put "hello world"

# Put into a specific tube with options (defaults: priority 0, delay 0, ttr 120)
tuber-cli put --tube emails --priority 0 --delay 0 --ttr 120 "send user@example.com"

# Put from stdin -- ALL of stdin becomes ONE job body
echo '{"user": "alice"}' | tuber-cli put --tube notifications

# Bulk put: `tuber put` makes ONE JOB PER LINE (blank lines skipped)
printf 'resize 1.jpg\nresize 2.jpg\n' | tuber put -t thumbs
cat jobs.txt | tuber put -t batch
```

`tuber put` takes `-t` tube, `-p`/`--pri`, `-d`/`--delay`, `--ttr` (default
**60**, not 120), the tag flags below, and `-a`. It prints the raw protocol
reply per job (`INSERTED 7`, or `INSERTED 4 READY` for a dedup). A rejected
put prints its error reply (`JOB_TOO_BIG`, `DRAINING`, `OUT_OF_MEMORY`, …) in
that line's place; the rest of stdin is still put, and the run then exits 1
with `error: N of M puts rejected` on stderr. A dedup is not a rejection.

### Tuber extensions

Tuber adds four optional tags to `put`:

| Tag | `tuber-cli put` | `tuber put` | Effect |
| --- | --- | --- | --- |
| `idp:<key>[:<ttl>]` | `--idempotent <key>` `[--idempotent-ttl <n>]` | `-i <key>[:<ttl>]` | Drop the put if a job with this key is already in the tube |
| `grp:<name>` | `--group <name>` | `-g <name>` | Add the job to a group (fan-out) |
| `aft:<name>` | `--after <name>` | `--aft <name>` | Hold the job until every job in the group is deleted (fan-in) |
| `con:<key>[:<n>]` | `--concurrency <key>[:<n>]` | `-c <key>[:<n>]` | At most `n` (default 1) jobs with this key reserved at once |

```bash
# Idempotent put -- drops the put if a job with this key exists in the tube
tuber-cli put --tube emails --idempotent welcome-user-42 "send welcome"

# Keep deduping for 24h AFTER the job is deleted -- e.g. the same cron firing
# on several hosts: whichever enqueues first wins
tuber-cli put --tube reports --idempotent nightly-report --idempotent-ttl 86400 "./report.sh"

# Job groups + after-group dependencies (fan-out/fan-in)
tuber-cli put --tube work --group batch-1 "shard 1"
tuber-cli put --tube work --group batch-1 "shard 2"
tuber-cli put --tube work --after batch-1 "cleanup"

# Concurrency key -- one user-42 job at a time; or up to 3 with user-42:3
tuber-cli put --tube work --concurrency user-42 "resize avatar"
tuber-cli put --tube work --concurrency user-42:3 "resize avatar"
```

Without a TTL the idempotency key is freed the moment its job is deleted. A
`con:` key's other jobs stay ready but are hidden from `reserve` until a
reservation ends (delete, release, bury, TTR timeout, or disconnect).

A deduplicated put is **not an error**. It reports the job already there:

```json
{ "id": 1, "duplicate": true, "state": "READY" }
```

`id` is the *existing* job's id and nothing was enqueued. `state` is where that
job sits — `READY`, `RESERVED`, `DELAYED`, `BURIED`, or `DELETED` during an
`--idempotent-ttl` cooldown. A fresh insert returns just `{ "id": 7 }`, so
branch on the `duplicate` key. A duplicate with a more urgent (lower)
priority ratchets the existing job's priority down — never up; `stats-job`
shows the result.

> **Colons in keys are a trap.** Keys and group names allow letters, digits
> and `- + / ; . $ _ ( )` — no colon. In `idp:` and `con:` the **last** colon
> separates the key from its number, so `--idempotent series:123` means key
> `series` with TTL 123, *not* key `series:123`, and `--idempotent user:abc`
> is `BAD_FORMAT`. Prefer dashes or dots: `--idempotent series-123`.

## Managing jobs

```bash
# Delete a job by ID (NOT_FOUND if another connection has it reserved)
tuber-cli delete 42

# Delete multiple jobs by ID -- exits 1 if any were not found
tuber-cli delete-batch 1 2 3 4 5
# {"deleted": 3, "not_found": 2}

# Kick buried/delayed jobs back to ready (bound defaults to 1)
tuber-cli kick 10 --tube emails

# Kick one specific job
tuber-cli kick-job 42

# Reserve -- see the trap below: these hand the job straight back on exit
tuber-cli reserve --tube emails --timeout 5      # exits 1 with TIMED_OUT if empty
tuber-cli reserve-batch 10 --tube emails --timeout 5   # {"jobs": [], "reserved": 0} if empty
tuber-cli reserve-job 42                         # one specific job, ignoring tube and watch list
```

There is no `tuber-cli bury`: a bury has to come from the connection holding
the reservation (see the trap below). To bury a job, use the `nc` recipe under
[Raw protocol](#raw-protocol-fallback).

## Server and tube control

```bash
# Pause a tube for 60 seconds (0 = unpause)
tuber-cli pause emails --delay 60

# Flush all jobs from a tube
tuber-cli flush-tube mytube

# Flush only the buried jobs from a tube (ready/delayed/reserved untouched)
tuber-cli flush-buried mytube

# Drain mode: reject new puts with DRAINING, let in-flight work finish.
# Server-wide and persistent -- survives until undrain. Also sent by SIGUSR1.
tuber-cli drain
tuber-cli undrain

# Need a throwaway server to test against? In-memory, nothing persisted.
tuber server -l 127.0.0.1 -p 11301 &
```

---

## Running jobs: `tuber work`

`tuber work` is the shell's way to actually *consume* jobs. Each job body is
run with `sh -c`, so a job is a shell command:

```bash
tuber put -t thumbs "convert in.jpg -resize 200x200 out.jpg"

tuber work -t thumbs              # one worker
tuber work -t thumbs -j 4         # four workers, each on its own connection
timeout 300 tuber work -t thumbs  # bounded run: work for 5 minutes, then stop cleanly
```

| The command… | The job is… |
| --- | --- |
| exits 0 | deleted |
| exits non-zero | buried (look with `peek-buried`, retry with `kick`) |
| is still running | touched every ttr/2, so long commands keep their reservation |
| is running when the worker gets SIGINT/SIGTERM | killed, and the job released to ready |

- **It runs until signalled** — it does not exit when the tube is empty. From
  an agent, run it in the background or under `timeout`; `timeout`'s SIGTERM
  is the clean shutdown, so the in-flight job is released, not lost.
- The command's own stdout/stderr pass straight through. The worker's log
  lines (`worker 0: job 6 completed`, `worker 0: job 7 failed (exit 3), buried`)
  go to stderr, and are safe to pipe: `tuber work 2>&1 | grep -m1 completed`
  leaves the worker running after `grep` exits.
- **Don't pipe a worker's stdout into something that exits early** (`| head`).
  The jobs inherit that stdout, so once the reader is gone any job that prints
  dies on the closed pipe — and is buried as a failure.
- `-t` watches that one tube only (it ignores `default`).
- Dropped connections are retried with backoff (1s, doubling to 30s); a worker
  gives up after 10 consecutive failed connects.
- **Anyone who can put to the tube can run commands on the worker host.** Only
  point `tuber work` at tubes whose producers you trust.

`tuber work` reserves, runs, and deletes or buries on one connection, so the
trap below doesn't apply to it.

---

## The connection-scoped trap

**A job reservation belongs to the connection that made it, and each
`tuber-cli` invocation is a new connection that closes on exit.** So nothing
can follow `tuber-cli reserve` to finish the job: the reservation dies with the
process, and the job goes **straight back to ready** the moment `tuber-cli`
exits (its `reserves` count still went up). The same
holds for an `nc` session — when `nc` exits, everything it reserved is
released. Commands that only make sense inside one live session:

| Command | Why it can't be a one-shot |
| --- | --- |
| `bury`, `release`, `touch` | Require the job reserved *by this connection* — from a fresh one they can only answer `NOT_FOUND` |
| `touch-all` | Heartbeats this connection's reserved set — a fresh one holds nothing, so it always answers `TOUCHED_ALL 0` |
| `watch` / `ignore` | Mutate this connection's watch list |
| `reserve-mode` | Sets `fifo`/`weighted` for this connection |
| `peek-reserved` | Peeks a job this connection reserved |
| `list-tube-used`, `list-tubes-watched` | Report this connection's state |

`tuber-cli reserve` / `reserve-batch` / `reserve-job` are therefore for
*inspection* — seeing what a worker would get. To actually consume a job,
reserve and finish it on one connection: `tuber work` for shell-command jobs,
otherwise `tuber-lib`'s `TuberClient`, a beanstalkd client library, or a single
`printf`/`nc` session.

---

## Raw protocol fallback

Tuber (and beanstalkd) speak a line-based text protocol over TCP (default port
11300). All commands are `\r\n` terminated. Use `printf` over `echo -e` for
multi-command sessions — a single `nc` invocation is a single connection, which
is what makes reserve/bury/touch compose.

```bash
# Reserve, work, delete -- all on one connection
printf 'watch emails\r\nignore default\r\nreserve-with-timeout 5\r\n' | nc -w 6 localhost 11300

# Bury one specific job: reserve it and bury it on the same connection
printf 'reserve-job 42\r\nbury 42 0\r\n' | nc -w 2 localhost 11300
# Response: RESERVED 42 <bytes> + body, then BURIED

# put <pri> <delay> <ttr> <bytes> [tags...]\r\n<body>\r\n
# bytes must be the EXACT byte length of the body, or you get BAD_FORMAT /
# EXPECTED_CRLF. This is the main reason to prefer tuber-cli.
printf 'use emails\r\nput 0 0 120 21\r\nsend user@example.com\r\n' | nc -w 2 localhost 11300
# Response: INSERTED <id>

# Extension tags ride on the put line
printf 'put 0 0 60 5 idp:unique-key\r\nhello\r\n' | nc -w 2 localhost 11300
printf 'put 0 0 60 5 grp:batch-1\r\nhello\r\n' | nc -w 2 localhost 11300
printf 'put 0 0 60 7 aft:batch-1\r\ncleanup\r\n' | nc -w 2 localhost 11300
printf 'put 0 0 60 5 con:user-42:3\r\nhello\r\n' | nc -w 2 localhost 11300
```

### Connection-scoped commands

These have no working `tuber-cli` equivalent by design (see the trap above):

```bash
# Bury a job this connection reserved
bury <id> <pri>\r\n                   # BURIED | NOT_FOUND

# Release a reserved job back to ready (or delayed)
release <id> <pri> <delay>\r\n        # RELEASED | BURIED | NOT_FOUND

# Reset the TTR timer on one reserved job
touch <id>\r\n                        # TOUCHED | NOT_FOUND

# Heartbeat EVERY job this connection holds -- the reserve-batch keep-alive.
# reserve-batch starts the TTR clock on all N jobs at once but you process them
# serially, so long batches need this or the tail expires. Takes no ids: jobs
# already deleted/released/buried aren't in your reserved set.
touch-all\r\n                         # TOUCHED_ALL <n>

# Watch set and reserve strategy
watch <tube> [weight]\r\n             # WATCHING <count>
ignore <tube>\r\n                     # WATCHING <count> | NOT_IGNORED
reserve-mode fifo|weighted\r\n        # USING <mode> | BAD_FORMAT

# This connection's state
peek-reserved\r\n                     # FOUND <id> <bytes> | NOT_FOUND
list-tube-used\r\n                    # USING <tube>
list-tubes-watched\r\n                # OK <bytes> + YAML list

quit\r\n                              # close the connection
```

`TOUCHED_ALL <n>` is worth watching: `n` is how many jobs you *actually* still
hold. Lower than expected means some hit their TTR and went back to the queue —
possibly already running elsewhere — and nothing else reports that. A batch
worker heartbeating `600 … 600 … 598` has just learned its TTR is too tight.

### Batch commands

```bash
# Up to 1000 per call
reserve-batch <count>\r\n             # RESERVED_BATCH <n>, then n x RESERVED
delete-batch <id> <id> ...\r\n        # DELETED_BATCH <deleted> <not_found>

# Blocking batch reserve: long-poll up to 30s, then drain what's ready
# (avoids hot-looping on an empty queue). Returns RESERVED_BATCH 0 on timeout,
# or DEADLINE_SOON if one of your reserved jobs is about to hit its TTR.
reserve-batch 5 30\r\n
```

## Tips

- **Prefer tuber-cli** — exact byte counts and connection handling are the two
  things that go wrong with raw `nc`.
- **Shell-command jobs? Use `tuber put` + `tuber work`** — no client code, and
  the reserve/run/delete-or-bury happens on one connection.
- **One connection per unit of work** when reserving. Reserve and delete/bury
  in the same session or the reservation evaporates.
- **Default tube** is `default` — no `use`/`watch` needed for it.
- **`use` vs `watch`**: producers `use` a tube (where puts go), consumers
  `watch` tubes (where reserves come from). They're independent sets.
- **TTR matters** — a reserved job auto-returns to ready after TTR expires.
  Long jobs should `touch` (`tuber work` does it for you); batch workers should
  `touch-all`.
- **Job IDs** are sequential integers starting from 1.
