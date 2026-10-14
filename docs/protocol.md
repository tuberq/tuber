# Protocol Reference

beanstalkd uses a text-based protocol over TCP. Commands and responses are
terminated by `\r\n`. Job bodies are raw bytes of the declared length.

## Producer Commands

### put \<pri\> \<delay\> \<ttr\> \<bytes\> [tags...]\r\n\<body\>\r\n

Insert a job. Body must be exactly `<bytes>` bytes followed by `\r\n`.

Optional extension tags (space-separated, after `<bytes>`):

| Tag | Description |
|---|---|
| `idp:<key>` or `idp:<key>:<ttl>` | Idempotency key. Deduplicates within the tube — if a live job with the same key exists, returns the original ID and state. Optional TTL (seconds) keeps deduplicating after the job is deleted. |
| `grp:<name>` | Assign the job to a named group for fan-out/fan-in. |
| `aft:<name>` | Hold the job until all jobs in the named group are deleted. |
| `con:<key>` or `con:<key>:<N>` | Concurrency key. Limits parallel reservations sharing this key. Default limit is 1. Jobs with different limits for one key get the smallest declared by any of them that is ready or reserved. |

Responses:
- `INSERTED <id>\r\n` — new job created
- `INSERTED <id> <STATE>\r\n` — idempotency dedup hit (STATE is `READY`, `RESERVED`, `DELAYED`, `BURIED`, or `DELETED`)
- `BURIED <id>\r\n` (out of memory)
- `EXPECTED_CRLF\r\n` (body not terminated correctly — **closes the connection**, see below)
- `JOB_TOO_BIG\r\n`
- `DRAINING\r\n`
- `NOT_DRAINING\r\n`
- `INTERNAL_ERROR\r\n` — the job couldn't be persisted (`-b`). It won't run
  now, but its record may have reached disk, so a restart can bring it back:
  retry with an `idp:` key, which dedups against it if it does. Every put is
  refused this way until restart once the WAL has failed.

#### Getting `<bytes>` wrong

`<bytes>` is the byte length of the body, not its character count. The server
reads exactly that many bytes plus a trailing `\r\n`, and the two failure modes
differ:

- **Declared longer than sent** — the server blocks waiting for the rest of the
  body. Anything the client sends next is consumed *as body bytes*, not
  executed. No reply arrives until the declared count is satisfied or the
  connection drops.
- **Declared shorter than sent** — the trailer check fails, the server replies
  `EXPECTED_CRLF\r\n` and **closes the connection**. Nothing is stored.

The close is a deliberate divergence from beanstalkd, which replies and keeps
parsing. A short declared length desynchronises the stream: the excess is an
unknown length by definition, so there is no way to locate the next command
boundary, and resuming the parse loop interprets the remainder of the *body* as
commands. Given a client that miscounts bytes — JavaScript `.length` counts
UTF-16 code units, Ruby `String#length` is not `#bytesize`, Python `str` is not
`bytes` — that hands whoever controls the job payload a way to run arbitrary
queue commands (`delete`, `flush-tube`, `pause-tube`) by embedding them after a
`\r\n`. Multi-byte UTF-8 in an attacker-supplied payload is enough to trigger
it. Closing is the only safe recovery, since the excess cannot be drained.

In both cases the stored body is exactly `<bytes>` long; excess data never
enters a job body, and a put that fails the trailer check stores nothing.

Client authors: use the byte length of the encoded payload, and treat
`EXPECTED_CRLF` as a fatal, reconnect-required error rather than something to
retry on the same connection.

### use \<tube\>\r\n

Switch the connection's use tube.

Response: `USING <tube>\r\n`

## Worker Commands

### reserve\r\n / reserve-with-timeout \<seconds\>\r\n

Reserve a job from watched tubes. Blocks until a job is available or timeout.

Responses:
- `RESERVED <id> <bytes>\r\n<body>\r\n`
- `TIMED_OUT\r\n`
- `DEADLINE_SOON\r\n` — the connection has a reserved job whose TTR is about to expire (see below).

### reserve-job \<id\>\r\n

Reserve a specific job by ID (regardless of tube).

Responses:
- `RESERVED <id> <bytes>\r\n<body>\r\n`
- `NOT_FOUND\r\n`

### delete \<id\>\r\n

Delete a job.

Response: `DELETED\r\n` or `NOT_FOUND\r\n`

### release \<id\> \<pri\> \<delay\>\r\n

Release a reserved job back to ready (or delayed).

Response: `RELEASED\r\n`, `BURIED\r\n`, or `NOT_FOUND\r\n`

### bury \<id\> \<pri\>\r\n

Bury a reserved job.

Response: `BURIED\r\n` or `NOT_FOUND\r\n`

### touch \<id\>\r\n

Reset the TTR timer on a reserved job.

Response: `TOUCHED\r\n` or `NOT_FOUND\r\n`

### watch \<tube\> [weight]\r\n

Add a tube to the watch set. Optional weight for weighted reserve mode.

Response: `WATCHING <count>\r\n`

### ignore \<tube\>\r\n

Remove a tube from the watch set.

Response: `WATCHING <count>\r\n` or `NOT_IGNORED\r\n`

### reserve-mode \<mode\>\r\n

Set reserve mode to `fifo` (the default), `weighted` or `weighted-fair`.

Response: `USING <mode>\r\n` or `BAD_FORMAT\r\n`

#### DEADLINE_SOON

When a connection has a reserved job whose TTR (time-to-run) is within 1 second of expiring, the server returns `DEADLINE_SOON` instead of reserving a new job. This happens in two ways:

1. **On reserve**: If you send `reserve` or `reserve-with-timeout` while holding a nearly-expired reservation, the server responds immediately with `DEADLINE_SOON` (unless a ready job is available, in which case it's returned normally).

2. **Proactive wake**: If you're already blocked waiting on a `reserve-with-timeout` and one of your reserved jobs enters the 1-second safety margin, the server interrupts the wait and sends `DEADLINE_SOON` — you don't have to wait for your reserve timeout to expire.

When you receive `DEADLINE_SOON`, you should `touch`, `delete`, or `release` the expiring job before trying to reserve again. If you don't act, the job's TTR will expire and the server will return it to the ready queue for another worker to pick up.

The safety margin is 1 second. The minimum TTR is also 1 second, so a job with TTR=1 may trigger `DEADLINE_SOON` almost immediately after being reserved.

## Schedule Commands

Recurring schedules: entries tuber puts into a tube at the times a cron
spec names, so an app needs no scheduler process of its own. An app
registers its whole schedule at boot in one sync. How to adopt it from an
app, and what a client library should do, is in
[scheduling.md](scheduling.md); the design and its decisions are in
[scheduler.md](scheduler.md).

```
schedule-begin splat
SYNCING splat
schedule-put rollup splat.maintenance 1024 0 120 40 idp:rollup con:maintenance 5 * * * *
{"class":"HistogramRollupJob","args":[]}
STAGED
schedule-put retention splat.maintenance 1024 0 120 38 con:maintenance 0 2 * * * Australia/Melbourne
{"class":"DataRetentionJob","args":[]}
STAGED
schedule-commit
SYNCED 2 0 0 0
```

### schedule-begin \<owner\>\r\n

Start staging a replacement set of entries for `<owner>`, a name the app
chooses (tube-name rules). Owners are namespaces: one owner's sync never
touches another's entries. A second `schedule-begin` inside a sync is
`BAD_FORMAT` and makes the sync abort.

Response: `SYNCING <owner>\r\n`

### schedule-put \<name\> \<tube\> \<pri\> \<delay\> \<ttr\> \<bytes\> [tags] \<spec\>\r\n\<body\>\r\n

Stage one entry. Framed like `put`: `<body>` is what every fire puts into
`<tube>`. After `<bytes>` come tags — every one has a `prefix:` — then the
spec, which runs to the end of the line:

- Tags: `idp:`, `grp:`, `aft:`, `con:` as on `put`, carried by every fire,
  plus `catchup:<seconds>`: fire a missed tick only if it is at most that
  old (`catchup:0`: never). Without it, a run of missed ticks fires once.
- Spec: 5-field cron with an optional trailing IANA zone, or a `CRON_TZ=`
  prefix; `@hourly`, `@daily`/`@midnight`, `@weekly`, `@monthly`,
  `@yearly`/`@annually`, with an optional zone; or `every <n>(s|m|h|d)
  [offset <n>(s|m|h|d)]`, anchored to the Unix epoch. Zones default to
  UTC.

```
5 * * * *                                  hourly at :05, UTC
0 2 * * * Australia/Melbourne              02:00 Melbourne time
CRON_TZ=Australia/Melbourne 0 2 * * *      the same
@daily Australia/Melbourne                 00:00 Melbourne time
every 60s                                  :00 of every minute
every 1h offset 5m                         :05 of every hour
```

Response: `STAGED\r\n`, or `BAD_FORMAT\r\n` (malformed line, or a name
staged twice), `BAD_SCHEDULE\r\n` (a spec or zone tuber can't use, or a
cron that never matches), `JOB_TOO_BIG\r\n`, `OUT_OF_MEMORY\r\n` (the
`--max-jobs-size` budget, or more than 10,000 entries), `NOT_SYNCING\r\n`.
Any refusal makes the sync abort. The body is consumed either way.

### schedule-commit\r\n

Replace the owner's entries with the staged set, in one step, durable
before the reply. An entry with a new name is added; one whose definition
changed is updated in place, keeping its last fire; one missing from the
set is removed; one unchanged keeps everything, including its next fire.
A commit never fires anything: new and changed entries start at their
first tick after it. An empty set removes all the owner's entries.

Response: `SYNCED <added> <changed> <removed> <unchanged>\r\n`, or
`ABORTED\r\n` if any `schedule-put` in the sync was refused (nothing
changes), `OUT_OF_MEMORY\r\n`, `OUT_OF_STORAGE\r\n`, `INTERNAL_ERROR\r\n`
(the new set is live but couldn't be persisted), `NOT_SYNCING\r\n`.

### schedule-abort\r\n

Discard the staged set. Closing the connection does the same.

Response: `ABORTED\r\n` or `NOT_SYNCING\r\n`

### list-schedules [\<owner\>]\r\n

Response: `OK <bytes>\r\n<yaml>\r\n`, one `- <owner> <name>` line per entry.

### stats-schedule \<owner\> \<name\>\r\n

Response: `OK <bytes>\r\n<yaml>\r\n` or `NOT_FOUND\r\n`. Fields are in
[statistics.md](statistics.md#schedule-stats-stats-schedule-owner-name).

## Other Commands

### peek \<id\>\r\n / peek-ready\r\n / peek-delayed\r\n / peek-buried\r\n

Response: `FOUND <id> <bytes>\r\n<body>\r\n` or `NOT_FOUND\r\n`

### kick \<bound\>\r\n

Kick up to `<bound>` buried/delayed jobs in the use tube. Buried jobs are kicked if any can be, otherwise delayed ones. An `aft:` job whose group is incomplete is passed over.

Response: `KICKED <count>\r\n`

### kick-job \<id\>\r\n

Kick a specific job. `NOT_FOUND` also covers a job that isn't buried or delayed, and an `aft:` job whose group is incomplete.

Response: `KICKED\r\n` or `NOT_FOUND\r\n`

### stats\r\n / stats-job \<id\>\r\n / stats-tube \<tube\>\r\n

Response: `OK <bytes>\r\n<yaml>\r\n` or `NOT_FOUND\r\n`

### stats-group \<name\>\r\n

Statistics for a job group (used with `grp:`/`aft:` features).

Response: `OK <bytes>\r\n<yaml>\r\n` or `NOT_FOUND\r\n`

YAML fields: `name`, `ready`, `reserved`, `delayed`, `buried`, `waiting-jobs`.

### list-tubes\r\n / list-tube-used\r\n / list-tubes-watched\r\n

Response: `OK <bytes>\r\n<yaml>\r\n` or `USING <tube>\r\n`

### pause-tube \<tube> \<delay\>\r\n

Pause a tube for `<delay>` seconds. A `<delay>` of `0` unpauses the tube
immediately: `stats-tube` reports `pause: 0` / `pause-time-left: 0` and any
client blocked in `reserve` is served at once.

Response: `PAUSED\r\n` or `NOT_FOUND\r\n`

### flush-tube \<tube\>\r\n

Delete all jobs from a tube. An absent tube is treated as already-empty: the
response is `FLUSHED 0`, not `NOT_FOUND`.

Response: `FLUSHED <count>\r\n`

### flush-buried \<tube\>\r\n

Delete all buried jobs from a tube. Ready, delayed, and reserved jobs are left
untouched. Like `flush-tube`, an absent tube yields `FLUSHED 0`.

Response: `FLUSHED <count>\r\n`

### drain\r\n

Enter drain mode — rejects new `put` commands with `DRAINING`, allows in-flight work to complete. Also triggered by `SIGUSR1`.

Response: `DRAINING\r\n`

### undrain\r\n

Exit drain mode — resumes accepting `put` commands. Idempotent.

Response: `NOT_DRAINING\r\n`

### quit\r\n

Close the connection.

## Constants

| Constant | Value |
|---|---|
| Default port | 11300 |
| Max tube name length | 200 chars |
| Tube name chars | `A-Za-z0-9` `-+/;.$_()` |
| Default max job size | 1 MiB (1048576 bytes) — beanstalkd's default is 65535 |
| Max possible job size | 1GB |
| Urgent priority threshold | < 1024 |
| Default TTR | 1 second |
| Priority range | 0 (most urgent) to 4294967295 |
