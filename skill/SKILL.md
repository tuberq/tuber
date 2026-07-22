---
name: tuber
description: Interact with a Tuber or beanstalkd work queue server using tuber-cli (preferred) or echo and nc (netcat). Use when tasks involve job queues, background workers, or beanstalkd protocol commands.
---

# Tuber / Beanstalkd Work Queue Client

Use `tuber-cli` — it handles byte counting, connection management, and outputs
structured JSON. Drop to raw protocol over `nc` only for the connection-scoped
commands listed under [Raw protocol](#raw-protocol-fallback), or when
`tuber-cli` isn't installed.

```bash
command -v tuber-cli
```

## Global options

```
-a, --addr <ADDR>      Server address (default: localhost:11300)
-f, --format <FORMAT>  Output format: json (default) or text
```

The address can also be set via the `TUBER_ADDR` environment variable (the
`--addr` flag overrides it). Accepts `host:port`, `host` (port defaults to
11300), or `:port` (host defaults to localhost). Both `tuber-cli` and
`tuber-tui` honor it.

---

## Inspecting state

```bash
# Server-wide statistics
tuber-cli stats

# List all tubes
tuber-cli list-tubes

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

## Producing jobs

```bash
# Put a job into the default tube
tuber-cli put "hello world"

# Put into a specific tube with options
tuber-cli put --tube emails --priority 0 --delay 0 --ttr 120 "send user@example.com"

# Put from stdin (pipe or heredoc)
echo '{"user": "alice"}' | tuber-cli put --tube notifications
```

### Tuber extensions

Tuber adds four optional tags to `put`. Each has a flag:

```bash
# Idempotent put -- drops the put if a live job with this key exists in the tube
tuber-cli put --tube emails --idempotent welcome-user-42 "send welcome"

# ...with a TTL: the key stays reserved for 300s
tuber-cli put --tube emails --idempotent welcome-user-42 --idempotent-ttl 300 "send welcome"

# Job groups + after-group dependencies (fan-out/fan-in)
tuber-cli put --tube work --group batch-1 "shard 1"
tuber-cli put --tube work --group batch-1 "shard 2"
tuber-cli put --tube work --after batch-1 "cleanup"

# Concurrency key -- limit how many jobs with this key run at once
tuber-cli put --tube work --concurrency user-42 "resize avatar"
```

A deduplicated put is **not an error**. It reports the job already there:

```json
{ "id": 1, "duplicate": true, "state": "READY" }
```

`id` is the *existing* job's id and nothing was enqueued. A fresh insert
returns just `{ "id": 7 }`, so branch on the `duplicate` key.

> **Colons in `idp:` keys are a trap.** On the wire the format is
> `idp:<key>` or `idp:<key>:<ttl>`, and the **last** colon separates key from
> TTL — so `idp:series:123` means key `series` with TTL 123, *not* key
> `series:123`. Prefer dashes or dots: `--idempotent series-123`.

## Managing jobs

```bash
# Reserve the next available job (returns id + body as JSON)
tuber-cli reserve --timeout 5

# Reserve up to N jobs at once; --timeout long-polls for at least one
tuber-cli reserve-batch 10 --tube emails --timeout 5

# Reserve one specific job by ID, ignoring tube and watch list
tuber-cli reserve-job 42

# Delete a job by ID
tuber-cli delete 42

# Delete multiple jobs by ID
tuber-cli delete-batch 1 2 3 4 5

# Bury a reserved job
tuber-cli bury 42

# Kick buried/delayed jobs back to ready
tuber-cli kick 10 --tube emails

# Kick one specific job
tuber-cli kick-job 42
```

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
```

---

## The connection-scoped trap

**A job reservation belongs to the connection that made it, and each
`tuber-cli` invocation is a new connection that closes on exit.** So this does
*not* work:

```bash
tuber-cli reserve --timeout 5     # reserves job 42... then exits
tuber-cli bury 42                 # Error: NOT_FOUND -- different connection
```

The reserve is dropped when the process exits, and the job returns to ready
once its TTR elapses. Commands that only make sense inside one live session:

| Command | Why it can't be a one-shot |
| --- | --- |
| `release`, `touch` | Require the job reserved *by this connection* |
| `touch-all` | Heartbeats this connection's reserved set — a fresh one holds nothing, so it always answers `TOUCHED_ALL 0` |
| `watch` / `ignore` | Mutate this connection's watch list |
| `reserve-mode` | Sets `fifo`/`weighted` for this connection |
| `peek-reserved` | Peeks a job this connection reserved |
| `list-tube-used`, `list-tubes-watched` | Report this connection's state |

`tuber-cli reserve` / `reserve-batch` / `reserve-job` are therefore for
*inspection* — seeing what a worker would get. To actually consume a job,
reserve and finish it on one connection: use `tuber-lib`'s `TuberClient`, a
beanstalkd client library, or a single `printf`/`nc` session.

---

## Raw protocol fallback

Tuber (and beanstalkd) speak a line-based text protocol over TCP (default port
11300). All commands are `\r\n` terminated. Use `printf` over `echo -e` for
multi-command sessions — a single `nc` invocation is a single connection, which
is what makes reserve/bury/touch compose.

```bash
# Reserve, work, delete -- all on one connection
printf 'watch emails\r\nignore default\r\nreserve-with-timeout 5\r\n' | nc -w 6 localhost 11300

# put <pri> <delay> <ttr> <bytes> [tags...]\r\n<body>\r\n
# bytes must be the EXACT byte length of the body, or you get BAD_FORMAT /
# EXPECTED_CRLF. This is the main reason to prefer tuber-cli.
printf 'use emails\r\nput 0 0 120 21\r\nsend user@example.com\r\n' | nc -w 2 localhost 11300
# Response: INSERTED <id>

# Extension tags ride on the put line
printf 'put 0 0 60 5 idp:unique-key\r\nhello\r\n' | nc -w 2 localhost 11300
printf 'put 0 0 60 5 grp:batch-1\r\nhello\r\n' | nc -w 2 localhost 11300
printf 'put 0 0 60 7 aft:batch-1\r\ncleanup\r\n' | nc -w 2 localhost 11300
printf 'put 0 0 60 5 con:user-42\r\nhello\r\n' | nc -w 2 localhost 11300
```

### Connection-scoped commands

These have no `tuber-cli` equivalent by design (see the trap above):

```bash
# Release a reserved job back to ready (or delayed)
release <id> <pri> <delay>\r\n        # RELEASED | BURIED | NOT_FOUND

# Reset the TTR timer on one reserved job
touch <id>\r\n                        # TOUCHED | NOT_FOUND

# Heartbeat EVERY job this connection holds; takes no ids -- the reserve-batch
# keep-alive (TTR starts on all N jobs at once, but processing is serial).
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
- **One connection per unit of work** when reserving. Reserve and delete/bury
  in the same session or the reservation evaporates.
- **Default tube** is `default` — no `use`/`watch` needed for it.
- **`use` vs `watch`**: producers `use` a tube (where puts go), consumers
  `watch` tubes (where reserves come from). They're independent sets.
- **TTR matters** — a reserved job auto-returns to ready after TTR expires.
  Long jobs should `touch`; batch workers should `touch-all`.
- **Job IDs** are sequential integers starting from 1.
