# Running your app's recurring jobs in tuber

This guide is for developers moving an app's cron-style jobs into tuber, and
for anyone writing a client library that talks to tuber's scheduler. The wire
format is in [protocol.md](protocol.md#schedule-commands). The reasons behind
each behaviour are in [scheduler.md](scheduler.md).

## What changes

Most apps run a scheduler process beside their web and worker processes —
whenever, rufus-scheduler, sidekiq-cron, Solid Queue's recurring tasks, a
`bin/scheduler` container. It boots the whole app only to put jobs on a timer.
It has to run as exactly one replica, or every tick fires twice.

With tuber, the app sends its schedule to tuber, and tuber puts each entry's
job into its tube at the right times. You can delete the scheduler process.
Workers don't change: a scheduled job is an ordinary job, reserved, deleted
and buried like any other.

```
before:  web  worker  scheduler ──put──▶ tuber ◀──reserve── worker
after:   web  worker ──(sync at boot)──▶ tuber ◀──reserve── worker
                                          └── puts each entry's job on time
```

## Quick start

From Ruby, with [tuber-gem](https://github.com/tuberq/tuber-gem) 0.7.0 or
later. Its README lists every option `s.add` takes.

```ruby
tuber = Tuber.new   # reads TUBER_URL

result = tuber.schedules.sync("splat") do |s|
  s.add "histogram_rollup", tube: "splat.maintenance", cron: "5 * * * *",
        body: { class: "HistogramRollupJob", args: [] }.to_json,
        idp: "histogram_rollup", con: "maintenance", pri: 1024, ttr: 120
  s.add "data_retention", tube: "splat.maintenance",
        cron: "0 2 * * *", zone: "Australia/Melbourne",
        body: { class: "DataRetentionJob", args: [] }.to_json, con: "maintenance", pri: 1024, ttr: 120
  s.add "monitor_evaluation", tube: "splat.checkins", every: 60,
        body: { class: "MonitorEvaluationJob", args: [] }.to_json,
        idp: "monitor_evaluation", pri: 1024, ttr: 120
end
Rails.logger.info "tuber schedule: #{result.to_h}"   # {added: 3, changed: 0, removed: 0, unchanged: 0}
```

The same sync on the wire, from any language: one connection, one
`schedule-put` per entry, framed like `put`. `<bytes>` is the body's exact
byte length.

```
schedule-begin splat
SYNCING splat
schedule-put histogram_rollup splat.maintenance 1024 0 120 40 idp:histogram_rollup con:maintenance 5 * * * *
{"class":"HistogramRollupJob","args":[]}
STAGED
schedule-put data_retention splat.maintenance 1024 0 120 38 con:maintenance 0 2 * * * Australia/Melbourne
{"class":"DataRetentionJob","args":[]}
STAGED
schedule-put monitor_evaluation splat.checkins 1024 0 120 42 idp:monitor_evaluation every 60s
{"class":"MonitorEvaluationJob","args":[]}
STAGED
schedule-commit
SYNCED 3 0 0 0
```

## Five rules

### 1. One owner per app, and a stable name for each entry

The owner is your app's name for its set of entries (`splat`). It's a
namespace: another app syncing `booko` on the same tuber never touches
`splat`'s entries. Within an owner, the entry name is the entry's identity.

Keep names stable. An entry whose spec or body changes keeps its name and is
updated in place, along with its last fire and counters. Renaming an entry is
a remove plus an add: the new name starts with no history, and its first fire
is its next tick.

Owners and names follow tube-name rules: up to 200 bytes of letters, digits
and `- + / ; . $ _ ( )`, not starting with `-`.

### 2. Always send the complete set

On commit, the owner's entries become exactly what you sent. Anything you
didn't send is deleted — which is how removing an entry from your config
removes it from tuber. So build the sync from the whole config every time.
Never send "just the entry I changed".

### 3. Never sync an empty set by accident

An empty sync deletes every entry the owner has. If your config failed to
load, or an environment check filtered out every entry, don't sync. The gem
refuses an empty set unless you pass `allow_empty: true`; do the same in your
own code.

### 4. Sync when a new release starts, not on reconnect

Sync once per process boot, or once per deploy. Don't sync again when a
connection drops and reconnects.

The reason is rolling deploys. While old and new processes overlap, an old
process re-syncing would put back the entries the new release removed, until
it exits. Syncing at boot means the newest release wins. That's also what you
want for a rollback: the old release boots and its sync restores the old
schedule.

Where to sync:

- **A release step** (Kamal `pre-deploy` hook, Heroku `release`, a Kubernetes
  Job) runs once per deploy with the new code. That's the natural fit when
  tuber runs with `-b`, which persists the schedule across tuber restarts. It
  also has no rolling-deploy window at all, since app processes never sync.
- **Process boot** (an initializer, or your worker's start hook) also works.
  Every replica sending the same set is harmless: an identical sync changes
  nothing, not even when entries next fire (`SYNCED 0 0 0 n`). Prefer this if
  tuber runs without `-b`: then a tuber restart forgets every schedule until
  something syncs again. One gap remains: an *old* process that crashes and
  restarts during a rollout syncs the old set back until the next new
  process boots.

Avoid syncing from processes that don't run the app, such as a Rails console
or one-off rake tasks. Those are harmless while they run the same release,
but pointless.

### 5. Handle the reply

| Reply | What happened | What to do |
|---|---|---|
| `SYNCED a c r u` | The set is live and on disk | Log the counts |
| A refused `schedule-put`, then `ABORTED` | Nothing changed: the previous schedule keeps running | Log loudly, naming the entry and reason (below); don't crash the boot |
| `OUT_OF_MEMORY` / `OUT_OF_STORAGE` on commit | Nothing changed: the set would exceed `--max-jobs-size` / `--max-storage-bytes` | Alert |
| `INTERNAL_ERROR` on commit | The set **is** live, but couldn't be written to disk; a tuber restart would bring back the previous set | Alert, and sync again once the disk is fixed: only a commit rewrites the definitions |

From Ruby, `sync` raises `Tuber::ScheduleSyncError` for any reply but
`SYNCED`, naming the refused entry (`#entry`) and the reply (`#status`). Its
`#applied?` is true only for the `INTERNAL_ERROR` row.

Why a refused line aborts the sync: if tuber committed the rest, the refused
entry would be deleted — a typo in one zone would silently drop a job.
Instead, a bad deploy leaves the last good schedule running. The refusals:

| Reply | Usually means |
|---|---|
| `BAD_SCHEDULE` | A spec tuber can't read, an unknown zone (`Australia/Melborne`), or a cron that never matches (`0 0 30 2 *`) |
| `BAD_FORMAT` | A malformed line, an invalid name or tag, or the same name twice in one sync |
| `JOB_TOO_BIG` | The body is over `--max-job-size` |
| `OUT_OF_MEMORY` | The `--max-jobs-size` budget, more than 10,000 entries, or a set over 8 MiB |

If the connection drops mid-sync, tuber throws the staged set away. Resend
the whole sync on a new connection. That's safe: a sync is idempotent. Never
try to resume one partway.

## Writing entries

### Specs

| Spec | Fires |
|---|---|
| `5 * * * *` | hourly at :05, UTC |
| `0 2 * * * Australia/Melbourne` | 02:00 Melbourne time |
| `CRON_TZ=Australia/Melbourne 0 2 * * *` | the same |
| `0 9 * * MON-FRI America/New_York` | 09:00 New York time on weekdays |
| `@daily Europe/London` | midnight London time (`@hourly`, `@weekly`, `@monthly`, `@yearly` too) |
| `every 60s` | :00 of every minute |
| `every 10m` | :00, :10, :20 … |
| `every 1h offset 5m` | :05 of every hour |

- **Cron** is the standard 5 fields (minute, hour, day of month, month,
  day of week), with names like `JAN` and `MON`. When both day fields are
  restricted, a day matching either fires, as in crontab. There's no
  seconds field.
- **Zones** are IANA names from the server's tz database, defaulting to UTC.
  The existing rufus-scheduler / Solid Queue / sidekiq-cron strings
  (`0 2 * * * Australia/Melbourne`) work unchanged.
- **`every`** intervals are aligned to the Unix epoch, not to when you synced,
  so redeploying never shifts them. `d` is exactly 24 hours whatever the
  zone; for "every day at 02:00", use cron.

**Daylight saving.** A job at a fixed time fires once a day.

- On the spring-forward day, a time the clock skips runs late by the skip:
  `0 2` in Melbourne ran at 03:00 on 4 October 2026.
- On the fall-back day, a time the clock repeats runs on its first pass.
- An entry whose minute or hour field starts with `*` (`*/15 * * * *`,
  `@hourly`) follows the clock instead. It fires during both passes of a
  repeated hour, and at nothing in a skipped one.
- UTC entries never meet any of this.

### Bodies

The body is what every fire puts: exactly what your worker would receive from
a `put` today, such as `{"class":"DataRetentionJob","args":[]}`. Entries have
a priority, delay and TTR just like a put.

### Tags

| Tag | Use it to |
|---|---|
| `idp:<key>` | **Skip a fire while the previous fire's job is still in the tube.** Recommended for most recurring jobs: a slow run or a worker outage doesn't pile up copies. |
| `con:<key>` | Run them one at a time. The value is a key, not a number: every job with `con:maintenance` shares one slot, so entries that share it run one after another. `con:<key>:<n>` allows `n` at once. (`con:1` is a key named `1`, shared with every other `con:1` job.) |
| `catchup:<secs>` | Bound catch-up after downtime (below). `catchup:0` never catches up. |
| `grp:`, `aft:` | As on `put` |

**The `idp:` trap.** If a fire's job is *buried*, every later fire dedups
against it and queues nothing. The puts "succeed", and nothing runs until
someone kicks or deletes that job. Tuber makes this visible rather than
silent:

- `last-result: "INSERTED 47101 BURIED"` and a rising `buried-streak` in
  `stats-schedule`;
- a warning in tuber's log on every such fire;
- `tuber_schedule_fires_total{result="deduped_buried"}` in Prometheus.

Alert on it (see [Monitoring](#monitoring)).

## When tuber is down, restarts, or crashes

- **Downtime:** ticks missed while tuber was down fire **once** when it comes
  back, not as a burst. `catchup:300` fires that one only if it's under five
  minutes stale; `catchup:0` skips it. A tick at most 5 seconds late counts as
  on time, not missed, so a quick restart doesn't drop a `catchup:0` entry's
  tick.
- **A crash at the moment a job fires** loses that tick rather than running it
  twice. Tuber records the fire before it puts the job.
- **Persistence:**
  - With `-b`, schedules are saved in the WAL and survive restarts.
  - Without `-b`, they're held in memory and lost on restart, until your app
    syncs again.
- **Your jobs should still be safe to run twice.** Missed-tick catch-up, a
  forward clock step, or a worker that dies mid-job and has its job
  redelivered can all mean a job runs when you didn't expect it.

## Monitoring

```bash
tuber schedules                       # one line per entry: spec, tube, next fire, last result
tuber schedules -o splat
printf 'stats-schedule splat data_retention\r\n' | nc -w 1 localhost 11300
```

From Ruby, `tuber.schedules.list("splat")` and
`tuber.schedules.stats("splat", "data_retention")` read the same, with times
as `Time` and counters as Integers.

`stats-schedule` shows the next three fires in the entry's own zone, offset
included. That's the quickest check that tuber read a zone the way you meant.
It also shows the last fire, the last fire's reply, counts by result, and
`buried-streak`. Every field is described in
[statistics.md](statistics.md#schedule-stats-stats-schedule-owner-name).

With `--metrics-port`, each entry has `owner` and `name` labels on these
series:

```
# An entry that's firing but queueing nothing (the idp: trap)
increase(tuber_schedule_fires_total{result="deduped_buried"}[2h]) > 0

# An entry that has stopped firing at all (here: one that fires at least hourly)
time() - tuber_schedule_last_fire_timestamp_seconds > 2 * 3600

# Fires refused by the put path (drain mode, full budgets)
increase(tuber_schedule_fires_total{result="rejected"}[1h]) > 0
```

## Migrating from a scheduler process

| From | Spec | Notes |
|---|---|---|
| rufus-scheduler `cron '0 2 * * * Australia/Melbourne'` | `0 2 * * * Australia/Melbourne` | unchanged |
| rufus-scheduler `every '10m'` | `every 10m` | now aligned to :00/:10/… rather than to process start |
| whenever `every 1.day, at: '2:00 am'` | `0 2 * * *` (+ zone) | |
| sidekiq-cron `cron: "0 2 * * *"` | `0 2 * * *` | |
| Solid Queue `schedule: every day at 2am` | `0 2 * * *` (+ zone) | its Fugit natural-language forms need translating to cron |

A deploy that switches over:

1. Add the sync (an initializer or a release step), built from the same
   config the old scheduler reads.
2. Remove the scheduler process in the **same** deploy. Running both for a
   while means two fires per tick. If the entries carry `idp:` keys, the
   overlap dedups on its own — another reason to use them.
3. After deploying, check `tuber schedules`: every entry, with the next fire
   you expect.

## For client-library authors

- **One unit.** Hold your connection lock across the whole begin…commit.
  Treat the exchange as a single retryable unit. After a dropped connection,
  redo all of it.
- **Framing.** `schedule-put <name> <tube> <pri> <delay> <ttr> <bytes>
  [tags] <spec>`, then the body and `\r\n`, exactly as `put`. Tags come before
  the spec. Every tag has a `prefix:`, and no spec token contains a colon.
- **On a refused line:** send `schedule-abort`, then report the entry's name
  and the reply. The commit would only say `ABORTED`. Validate names and
  reject CR/LF in specs before sending anything.
- **Refuse an empty set by default** (rule 3).
- **Don't sync on reconnect** (rule 4). Make syncing an explicit call the app
  makes at boot.
- **`stats-schedule` values** contain `:`, `-` and quotes (times, `con:maintenance`,
  specs). Parse `key: value` lines by splitting on the first `": "` and
  stripping surrounding `"`. Don't run them through a YAML layer that rewrites
  values.
- **`list-schedules`** is a YAML list of `- <owner> <name>`.

## Limits

- Up to 10,000 entries across all owners.
- One owner's set is at most 8 MiB: its bodies, plus each entry's
  `schedule-put` line and up to 33 bytes more.
- A body is capped by `--max-job-size`.
- Entries count toward `--max-jobs-size`: their bodies and names, plus about
  500 bytes each, shown as `current-schedules-size` in `stats`. A sync in
  progress counts too.
- With `-b`, schedules are stored in the WAL and count toward
  `--max-storage-bytes`. A commit that grows a set needs room for all of it,
  since it writes the whole set again. One that only keeps or shrinks a set
  always goes through, even on a full disk.
