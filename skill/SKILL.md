---
name: Tuber / Beanstalkd Client
description: Interact with a Tuber or beanstalkd work queue server using echo and nc (netcat). Use when tasks involve job queues, background workers, or beanstalkd protocol commands.
---

# Tuber / Beanstalkd Work Queue Client

Tuber is a Rust rewrite of beanstalkd, a simple and fast work queue. Both use the same text protocol over TCP (default port 11300). All commands are `\r\n` terminated.

## Connecting

```bash
# Interactive session (type commands, see responses)
nc localhost 11300

# One-shot command
echo -e "stats\r\n" | nc localhost 11300

# Multiple commands in one session (use printf for precise control)
printf "use mytube\r\nput 0 0 60 5\r\nhello\r\n" | nc localhost 11300
```

## Core Workflow

The typical pattern is: **put** jobs into a tube, **reserve** them from another connection, **delete** when done.

### Put a Job

```bash
# put <priority> <delay> <ttr> <bytes>\r\n<body>\r\n
# - priority: 0-4294967295 (lower = more urgent)
# - delay: seconds before job becomes ready (0 = immediate)
# - ttr: time-to-run in seconds (job auto-releases if worker doesn't finish in time)
# - bytes: exact byte length of the body

echo -e "put 0 0 60 5\r\nhello\r\n" | nc localhost 11300
# Response: INSERTED <id>

# Put into a specific tube (default tube is "default")
printf "use emails\r\nput 0 0 120 19\r\nsend user@example.com\r\n" | nc localhost 11300
# Response: USING emails\r\nINSERTED <id>

# Delayed job (becomes ready after 30 seconds)
echo -e "put 0 30 60 5\r\nhello\r\n" | nc localhost 11300

# Low priority job (higher number = lower priority)
echo -e "put 1000 0 60 5\r\nhello\r\n" | nc localhost 11300
```

### Reserve a Job (Claim for Processing)

```bash
# Reserve blocks until a job is available
echo -e "reserve\r\n" | nc localhost 11300
# Response: RESERVED <id> <bytes>\r\n<body>

# Reserve with timeout (0 = return immediately if nothing available)
echo -e "reserve-with-timeout 5\r\n" | nc localhost 11300
# Response: RESERVED <id> <bytes>\r\n<body>
# Or: TIMED_OUT

# Reserve from a specific tube
printf "watch emails\r\nreserve-with-timeout 0\r\n" | nc localhost 11300
```

### Delete a Job (Mark Complete)

```bash
echo -e "delete <id>\r\n" | nc localhost 11300
# Response: DELETED
```

### Release a Job (Return to Queue)

```bash
# release <id> <new-priority> <delay>
echo -e "release <id> 0 0\r\n" | nc localhost 11300
# Response: RELEASED
```

### Bury a Job (Set Aside for Later Inspection)

```bash
echo -e "bury <id> 0\r\n" | nc localhost 11300
# Response: BURIED

# Later, kick buried jobs back to ready
echo -e "kick 10\r\n" | nc localhost 11300
# Response: KICKED <count>
```

## Complete Worker Example (Bash)

```bash
#!/bin/bash
# Simple worker that reserves and processes jobs from "tasks" tube
HOST=localhost
PORT=11300

while true; do
  # Reserve a job with 30s timeout
  RESPONSE=$(printf "watch tasks\r\nreserve-with-timeout 30\r\n" | nc $HOST $PORT)

  if echo "$RESPONSE" | grep -q "^RESERVED"; then
    JOB_ID=$(echo "$RESPONSE" | grep "^RESERVED" | awk '{print $2}')
    BODY=$(echo "$RESPONSE" | tail -1)

    echo "Processing job $JOB_ID: $BODY"
    # ... do work with $BODY ...

    # Delete on success
    echo -e "delete $JOB_ID\r\n" | nc $HOST $PORT
  fi
done
```

## Tube Management

```bash
# Switch tube for producing jobs
echo -e "use mytube\r\n" | nc localhost 11300
# Response: USING mytube

# Watch a tube for consuming jobs
echo -e "watch mytube\r\n" | nc localhost 11300
# Response: WATCHING <count>

# Stop watching a tube
echo -e "ignore default\r\n" | nc localhost 11300
# Response: WATCHING <count>

# List all tubes
echo -e "list-tubes\r\n" | nc localhost 11300
# Response: OK <bytes>\r\n---\r\n- default\r\n- mytube\r\n

# Delete all jobs in a tube (tuber extension)
echo -e "flush-tube mytube\r\n" | nc localhost 11300
# Response: FLUSHED <count>
```

## Inspecting Jobs and Stats

```bash
# Peek at a specific job (without reserving it)
echo -e "peek <id>\r\n" | nc localhost 11300
# Response: FOUND <id> <bytes>\r\n<body>

# Peek at next ready job
echo -e "peek-ready\r\n" | nc localhost 11300

# Peek at next delayed job
echo -e "peek-delayed\r\n" | nc localhost 11300

# Peek at oldest buried job
echo -e "peek-buried\r\n" | nc localhost 11300

# Server statistics
echo -e "stats\r\n" | nc localhost 11300

# Tube statistics
echo -e "stats-tube default\r\n" | nc localhost 11300

# Job statistics
echo -e "stats-job <id>\r\n" | nc localhost 11300
```

## Tuber Extensions (Not in Standard Beanstalkd)

```bash
# Idempotent put - same key won't create duplicate jobs
echo -e "put 0 0 60 5 idp:unique-key-123\r\nhello\r\n" | nc localhost 11300
# Response: INSERTED <id> (first time)
# Response: INSERTED <id> <state> (duplicate - returns existing job)

# Idempotent put with TTL (key expires after N seconds)
echo -e "put 0 0 60 5 idp:key123:300\r\nhello\r\n" | nc localhost 11300

# Job groups - tag related jobs
echo -e "put 0 0 60 5 grp:batch-1\r\nhello\r\n" | nc localhost 11300

# After-group dependency - job waits until group completes
echo -e "put 0 0 60 7 aft:batch-1\r\ncleanup\r\n" | nc localhost 11300

# Concurrency key - limit parallel execution
echo -e "put 0 0 60 5 con:user-42\r\nhello\r\n" | nc localhost 11300

# Concurrency key with explicit limit
echo -e "put 0 0 60 5 con:user-42:3\r\nhello\r\n" | nc localhost 11300

# Combine extensions
echo -e "put 0 0 60 5 idp:abc grp:g1 con:user-42\r\nhello\r\n" | nc localhost 11300

# Reserve batch (up to 1000)
echo -e "reserve-batch 5\r\n" | nc localhost 11300

# Delete batch
echo -e "delete-batch 1 2 3 4 5\r\n" | nc localhost 11300
# Response: DELETED_BATCH <deleted> <not_found>

# Peek at reserved jobs in current tube
echo -e "peek-reserved\r\n" | nc localhost 11300

# Weighted reserve mode
printf "reserve-mode weighted\r\nwatch high-priority 100\r\nwatch low-priority 1\r\nreserve-with-timeout 0\r\n" | nc localhost 11300

# Pause a tube (stop reserves for N seconds)
echo -e "pause-tube default 60\r\n" | nc localhost 11300
# Response: PAUSED

# Group statistics
echo -e "stats-group batch-1\r\n" | nc localhost 11300
```

## Quick Reference

| Command | Response |
|---|---|
| `put <pri> <delay> <ttr> <bytes>\r\n<body>\r\n` | `INSERTED <id>` |
| `reserve\r\n` | `RESERVED <id> <bytes>\r\n<body>` |
| `reserve-with-timeout <sec>\r\n` | `RESERVED ...` or `TIMED_OUT` |
| `delete <id>\r\n` | `DELETED` |
| `release <id> <pri> <delay>\r\n` | `RELEASED` |
| `bury <id> <pri>\r\n` | `BURIED` |
| `kick <count>\r\n` | `KICKED <n>` |
| `touch <id>\r\n` | `TOUCHED` |
| `use <tube>\r\n` | `USING <tube>` |
| `watch <tube>\r\n` | `WATCHING <count>` |
| `ignore <tube>\r\n` | `WATCHING <count>` |
| `peek <id>\r\n` | `FOUND <id> <bytes>\r\n<body>` or `NOT_FOUND` |
| `stats\r\n` | `OK <bytes>\r\n<yaml>` |
| `list-tubes\r\n` | `OK <bytes>\r\n<yaml>` |
| `flush-tube <tube>\r\n` | `FLUSHED <count>` |

## Error Responses

- `NOT_FOUND` - Job or tube doesn't exist
- `BAD_FORMAT` - Malformed command
- `JOB_TOO_BIG` - Body exceeds max size (default 65535 bytes)
- `EXPECTED_CRLF` - Missing `\r\n` after job body
- `DEADLINE_SOON` - Reserved job's TTR is about to expire
- `NOT_IGNORED` - Can't ignore last watched tube

## Tips

- **Byte count must be exact** - The `<bytes>` parameter in `put` must match the body length exactly, or you'll get `BAD_FORMAT` or `EXPECTED_CRLF`.
- **Use `printf` over `echo -e`** for multi-command sessions to avoid shell interpretation issues.
- **Default tube** is "default" - you don't need `use` or `watch` if working with it.
- **TTR matters** - if a worker doesn't `delete` or `touch` within TTR seconds, the job auto-releases back to ready.
- **Job IDs are sequential** integers starting from 1.
