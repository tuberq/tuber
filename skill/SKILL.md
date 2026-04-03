---
name: tuber
description: Interact with a Tuber or beanstalkd work queue server using tuber-cli (preferred) or echo and nc (netcat). Use when tasks involve job queues, background workers, or beanstalkd protocol commands.
---

# Tuber / Beanstalkd Work Queue Client

Use `tuber-cli` if available — it provides structured JSON output and proper error handling. Fall back to `echo`/`nc` if tuber-cli is not installed.

## Check for tuber-cli

```bash
command -v tuber-cli
```

---

## Using tuber-cli (preferred)

tuber-cli outputs JSON by default. Use `--format text` for human-readable output.

### Inspecting State

```bash
# Server-wide statistics
tuber-cli stats

# List all tubes
tuber-cli list-tubes

# Tube statistics
tuber-cli stats-tube default

# Peek at a specific job by ID
tuber-cli peek 42
```

### Producing Jobs

```bash
# Put a job into the default tube
tuber-cli put "hello world"

# Put into a specific tube with options
tuber-cli put --tube emails --priority 0 --delay 0 --ttr 120 "send user@example.com"

# Put from stdin (pipe or heredoc)
echo '{"user": "alice"}' | tuber-cli put --tube notifications
```

### Consuming & Managing Jobs

```bash
# Reserve the next available job (returns id + body as JSON)
tuber-cli reserve --timeout 5

# Delete a job by ID
tuber-cli delete 42

# Bury a reserved job
tuber-cli bury 42

# Kick buried/delayed jobs back to ready
tuber-cli kick 10 --tube emails

# Pause a tube for 60 seconds
tuber-cli pause emails --delay 60
```

### Global Options

```
-a, --addr <ADDR>      Server address (default: localhost:11300)
-f, --format <FORMAT>  Output format: json (default) or text
```

---

## Using echo + nc (fallback)

Tuber (and beanstalkd) use a text protocol over TCP (default port 11300). All commands are `\r\n` terminated.

```bash
echo -e "stats\r\n" | nc localhost 11300
```

### Inspecting State

```bash
# Server-wide statistics
echo -e "stats\r\n" | nc localhost 11300

# List all tubes
echo -e "list-tubes\r\n" | nc localhost 11300

# Tube statistics (job counts, paused state, etc.)
echo -e "stats-tube default\r\n" | nc localhost 11300

# Peek at next ready/delayed/buried job in the current tube
echo -e "peek-ready\r\n" | nc localhost 11300
echo -e "peek-delayed\r\n" | nc localhost 11300
echo -e "peek-buried\r\n" | nc localhost 11300

# Peek at a specific job by ID
echo -e "peek 42\r\n" | nc localhost 11300
# Response: FOUND <id> <bytes>\r\n<body>

# Job statistics (state, pri, age, TTR, reserves, timeouts, etc.)
echo -e "stats-job 42\r\n" | nc localhost 11300
```

### Debugging & Fixing

```bash
# Kick up to 10 buried jobs back to ready
echo -e "kick 10\r\n" | nc localhost 11300
# Response: KICKED <count>

# Kick a specific buried/delayed job
echo -e "kick-job 42\r\n" | nc localhost 11300

# Pause a tube (stop reserves for N seconds, 0 = unpause)
echo -e "pause-tube emails 60\r\n" | nc localhost 11300

# Delete all jobs in a tube (tuber extension)
echo -e "flush-tube mytube\r\n" | nc localhost 11300
# Response: FLUSHED <count>

# Delete a specific job
echo -e "delete 42\r\n" | nc localhost 11300
```

### Producing Jobs

```bash
# put <priority> <delay> <ttr> <bytes>\r\n<body>\r\n
# - priority: 0 = most urgent, higher = less urgent
# - delay: seconds before job becomes ready
# - ttr: time-to-run before auto-release
# - bytes: exact byte length of body

echo -e "put 0 0 60 5\r\nhello\r\n" | nc localhost 11300
# Response: INSERTED <id>

# Put into a specific tube
printf "use emails\r\nput 0 0 120 19\r\nsend user@example.com\r\n" | nc localhost 11300
```

### Consuming Jobs

```bash
# Reserve with timeout (0 = immediate return if empty)
echo -e "reserve-with-timeout 5\r\n" | nc localhost 11300
# Response: RESERVED <id> <bytes>\r\n<body>  or  TIMED_OUT

# Release a reserved job back to ready
echo -e "release 42 0 0\r\n" | nc localhost 11300

# Bury a problem job for later inspection
echo -e "bury 42 0\r\n" | nc localhost 11300

# Watch/ignore tubes
echo -e "watch emails\r\n" | nc localhost 11300
echo -e "ignore default\r\n" | nc localhost 11300
```

## Tuber Extensions

These work with both tuber-cli and echo/nc (the extensions are server-side).

```bash
# Idempotent put (deduplicates by key within the tube)
echo -e "put 0 0 60 5 idp:unique-key\r\nhello\r\n" | nc localhost 11300

# Idempotent put with TTL (key expires after 300 seconds)
echo -e "put 0 0 60 5 idp:unique-key:300\r\nhello\r\n" | nc localhost 11300
# Warning: Format is idp:<key> or idp:<key>:<ttl> — the LAST colon separates key from TTL.
# So idp:series:123 means key="series", TTL=123 — NOT key="series:123".
# Use dashes or dots instead of colons in keys: idp:series-123 or idp:series.123

# Job groups + after-group dependencies (fan-out/fan-in)
echo -e "put 0 0 60 5 grp:batch-1\r\nhello\r\n" | nc localhost 11300
echo -e "put 0 0 60 7 aft:batch-1\r\ncleanup\r\n" | nc localhost 11300

# Concurrency key (limit parallel execution per key)
echo -e "put 0 0 60 5 con:user-42\r\nhello\r\n" | nc localhost 11300

# Group statistics (debug why aft: jobs aren't running)
echo -e "stats-group batch-1\r\n" | nc localhost 11300

# Batch operations (up to 1000)
echo -e "reserve-batch 5\r\n" | nc localhost 11300
echo -e "delete-batch 1 2 3 4 5\r\n" | nc localhost 11300
```

## Tips

- **Prefer tuber-cli** — it handles byte counting, connection management, and outputs structured JSON.
- **Byte count must be exact** in `put` via nc/echo or you'll get `BAD_FORMAT` / `EXPECTED_CRLF`.
- **Use `printf`** over `echo -e` for multi-command nc sessions.
- **Default tube** is "default" — no `use`/`watch` needed for it.
- **TTR matters** — unreleased jobs auto-return to ready after TTR expires.
- **Job IDs** are sequential integers starting from 1.
