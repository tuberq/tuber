# Recurring schedules inside tuber

> **Status: implemented (2026-10-11).** Proposed and built the same day; the
> three decisions left open in the proposal — DST, missed ticks, crashes —
> are recorded under [Settled decisions](#settled-decisions). Prompted by
> Splat's notes of 2026-10-11. The guide for app developers is
> [scheduling.md](scheduling.md), the command reference
> [protocol.md](protocol.md#schedule-commands) and the fields
> [statistics.md](statistics.md#schedule-stats-stats-schedule-owner-name);
> this document is the reasoning behind them. The piece useful with or
> without a scheduler landed first: `idp:` dedups are counted per tube by
> the state of the job matched (`total-idp-dedups-*` in `stats-tube`,
> `tuber_tube_idp_dedups_total{tube,state}`), and `tuber put` warns on
> stderr when a dedup hits a buried job.

## Context

Almost every Rails app ends up running a scheduler process next to its web and
worker processes: whenever, rufus-scheduler, sidekiq-cron, Solid Queue's
recurring tasks. It boots the whole app to do one thing — `put` a job on a
timer. Splat's `bin/scheduler` is typical:

- **~110 MiB per instance**, nearly all of it Rails, for a process that only
  calls `put`.
- **A single-replica rule.** Two copies fire every tick twice, so the compose
  file carries a warning instead of a guarantee.
- **A silent failure.** On 2026-09-09 `HistogramRollupJob` buried. Its `idp:`
  key stayed live, so every hourly put after that replied
  `INSERTED <id> BURIED` and queued nothing — about 70 dropped over three
  days. The scheduler logged "firing" each time.

A fire is just a put, and tuber is one process. That second fact removes the
problem most app-side schedulers spend their design on — getting N scheduler
processes to fire each tick exactly once. Sidekiq Enterprise elects a leader;
Solid Queue and good_job put a unique index on (task, run_at); Kubernetes
names each Job after its tick (`<cronjob>-<unix/60>`). Inside tuber there is
nothing to elect. What is left is semantics, and that is what most of this
document is about.

beanstalkd never had recurring jobs. Tuber already goes well past it (`idp:`,
`grp:`, `aft:`, `con:`, `reserve-batch`, …), and this is the same kind of
extension: the app keeps reserving exactly as it does now.

## What it looks like

### From the app

The app owns its schedule: it lives in the app's repo and changes with its
releases. At boot, each app process sends tuber the complete set under a name
of its choosing (the *owner*), and tuber replaces that owner's set with it.
In Ruby that might be (illustrative — the client API is the gem's to design):

```ruby
tuber.schedule_sync("splat") do |sync|
  YAML.load_file("config/schedule.yml").each do |name, e|
    sync.put(name,
             tube: e["tube"], pri: 1024, delay: 0, ttr: 120,
             tags: e["tags"],                      # e.g. "idp:histogram_rollup con:maintenance"
             spec: e["cron"] || "every #{e['every']}",
             body: { class: e["class"], args: e["args"] || [] }.to_json)
  end
end
```

`bin/scheduler` and its container go away.

### Protocol

A sync is a short transaction on one connection: `schedule-begin`, one
`schedule-put` per entry, then `schedule-commit`. Each `schedule-put` has the
same framing as `put` (one line, then a data block), so clients reuse what they
already have, and the whole exchange pipelines.

```
schedule-begin splat
SYNCING splat
schedule-put monitor_evaluation splat.checkins 1024 0 120 42 idp:monitor_evaluation every 60s
{"class":"MonitorEvaluationJob","args":[]}
STAGED
schedule-put histogram_rollup splat.maintenance 1024 0 120 40 idp:histogram_rollup con:maintenance 5 * * * *
{"class":"HistogramRollupJob","args":[]}
STAGED
schedule-put data_retention splat.maintenance 1024 0 120 38 con:maintenance 0 2 * * * Australia/Melbourne
{"class":"DataRetentionJob","args":[]}
STAGED
  … six more …
schedule-commit
SYNCED 9 0 0 0
```

| Command | Reply | Notes |
|---|---|---|
| `schedule-begin <owner>` | `SYNCING <owner>` | Starts staging a replacement set for `<owner>`. |
| `schedule-put <name> <tube> <pri> <delay> <ttr> <bytes> [tags] <spec>` + data | `STAGED` | Only inside a sync. The data block is the body every fire will put. |
| `schedule-commit` | `SYNCED <added> <changed> <removed> <unchanged>` | Replaces the owner's set in one step; durable before it replies. `ABORTED` if any staged entry was rejected. |
| `schedule-abort` | `ABORTED` | Discards the staged set. So does closing the connection. |
| `list-schedules [<owner>]` | `OK <bytes>` + YAML list | One `- <owner> <name>` line per entry. |
| `stats-schedule <owner> <name>` | `OK <bytes>` + YAML map, or `NOT_FOUND` | See below. |

Errors from `schedule-put`: `BAD_FORMAT` (malformed line, or a name staged
twice in one sync), `BAD_SCHEDULE` (a spec or zone tuber can't use, or a cron
that never matches, e.g. `0 0 30 2 *`), `JOB_TOO_BIG`, `OUT_OF_MEMORY` (the
`--max-jobs-size` budget, or the 10,000-entry cap). Any of them makes the
sync's commit reply `ABORTED`. As with `put`, the data block is drained even
when the line is rejected, so a bad entry can't desync the connection; and
a rejected line still reaches the engine, so that the sync knows. A second
`schedule-begin` inside a sync is `BAD_FORMAT` and aborts it too.
`schedule-commit` can also reply `OUT_OF_MEMORY` or `OUT_OF_STORAGE` (nothing
changes), or `INTERNAL_ERROR` when the new set is live but couldn't be
written to disk. `schedule-put`, `-commit` and `-abort` outside a sync reply
`NOT_SYNCING`.

**Line layout.** Everything up to `<bytes>` is positional, like `put`. After
it come tags, then the spec, which runs to the end of the line. Every tag has a
`prefix:` (`idp:`, `grp:`, `aft:`, `con:`, and the schedule-only `catchup:`);
no spec token does, so the split is unambiguous. Owner and name follow the
tube-name rules (1–200 bytes, same character set, no leading `-`). Line length
is not a constraint: `MAX_LINE_LEN` is sized for `delete` batches.

**Spec grammar.**

```
spec     := cron [zone] | "CRON_TZ=" zone cron | macro [zone] | "every" dur ["offset" dur]
cron     := minute hour day-of-month month day-of-week     ; * , - / and names (JAN, MON); 0 and 7 are Sunday
macro    := @hourly | @daily | @midnight | @weekly | @monthly | @yearly | @annually
dur      := <n>s | <n>m | <n>h | <n>d                       ; d is exactly 86400 s
zone     := an IANA name (Australia/Melbourne) or UTC        ; default UTC
```

```
5 * * * *                                  hourly at :05, UTC
0 2 * * * Australia/Melbourne              02:00 Melbourne time
CRON_TZ=Australia/Melbourne 0 2 * * *      the same
@daily Australia/Melbourne                 00:00 Melbourne time
every 60s                                  :00 of every minute
every 1h offset 5m                         :05 of every hour (same as 5 * * * *)
```

**`stats-schedule`** shows times in the entry's own zone, offset included, so
a DST change is visible in the reply:

```
stats-schedule splat data_retention
OK <bytes>
---
owner: "splat"
name: "data_retention"
spec: "0 2 * * * Australia/Melbourne"
tube: "splat.maintenance"
pri: 1024
delay: 0
ttr: 120
tags: "con:maintenance"
catchup: "once"
synced-at: 2026-10-11T15:02:11+11:00
next-fire: 2026-10-12T02:00:00+11:00
next-fire-2: 2026-10-13T02:00:00+11:00
next-fire-3: 2026-10-14T02:00:00+11:00
last-fire: 2026-10-11T02:00:00+11:00
last-result: "INSERTED 48213"
total-fires: 38
total-inserted: 38
total-deduped-ready: 0
total-deduped-reserved: 0
total-deduped-delayed: 0
total-deduped-buried: 0
total-deduped-deleted: 0
total-rejected: 0
buried-streak: 0
```

`last-result` is the reply a client would have got. `buried-streak` counts
consecutive fires that deduped against a buried job, and resets on any other
result. Listing the next few fires, as Temporal's `describe` does, is the
cheapest way for someone to check tuber read their zone the way they meant.

**Metrics.**

| Metric | |
|---|---|
| `tuber_schedules` | Entries held, across all owners (`current-schedules` in `stats`) |
| `tuber_schedule_fires_total{owner,name,result}` | `result` is `inserted`, `deduped_ready`, `deduped_reserved`, `deduped_delayed`, `deduped_buried`, `deduped_deleted` or `rejected` |
| `tuber_schedule_last_fire_timestamp_seconds{owner,name}` | For staleness alerts: `time() - last_fire > 2 × interval` |
| `tuber_schedule_next_fire_timestamp_seconds{owner,name}` | |

The 09-09 failure becomes
`increase(tuber_schedule_fires_total{result="deduped_buried"}[2h]) > 0`.
Cardinality is bounded by the number of entries.

**CLI.** `tuber schedules [-o <owner>]` lists entries with spec, next fire and
last result, like `tuber tubes`:

```
splat data_retention: 0 2 * * * Australia/Melbourne -> splat.maintenance next=2026-10-12T02:00:00+11:00 last=INSERTED 48213
splat histogram_rollup: 5 * * * * -> splat.maintenance next=2026-10-11T05:05:00+00:00 last=INSERTED 47101 BURIED (deduped against a buried job 70 times running)
```

`stats` also reports `current-schedules`, and `current-schedules-size`: the
memory entries are charged against `--max-jobs-size`, beside
`current-jobs-size`.

## Decisions

### 1. Registration and identity

The schedule changes from release to release: entries are added, edited,
renamed and removed. Tuber's copy has to follow, and two questions decide
whether it does: *how does tuber know an entry is the same entry*, and *how
does it learn that one was removed?*

**Identity is (owner, name), chosen by the app — never the entry's content.**
BullMQ v5 shows why: it keyed repeatable jobs by a hash of their options
(name, pattern, tz, …), so editing a pattern produced a *second* schedule and
the old one kept firing. v5.16 replaced that with
`upsertJobScheduler(id, …)` and v6 removed the old API.

**Removal needs the complete set.** With only "register this entry", an entry
the app stops registering keeps firing forever — tuber can't tell "deleted"
from "this process hasn't registered it yet". A sync carries the whole set for
one owner, and anything of that owner's not in it is removed. This is what
Solid Queue does at boot (static tasks missing from `recurring.yml` are
deleted) and what sidekiq-cron's `load_from_hash!` does (removes entries of the
same source only).

How the cases play out for Splat:

| Release does | Sync reply | What happens |
|---|---|---|
| First boot with the feature, 9 entries | `SYNCED 9 0 0 0` | All 9 created; each first fires at its next tick. |
| Another replica boots, same file | `SYNCED 0 0 0 9` | Nothing changes — not the next fire, not the last fire, not the counters. This is what retires the single-replica rule: every process may sync, and tuber fires each tick once. |
| `histogram_rollup` moves from `5 * * * *` to `15 * * * *` | `SYNCED 0 1 0 8` | Updated in place. Keeps its last fire and counters; next fire is recomputed. With content-keyed identity this would have been a second schedule. |
| `wal_checkpoint` is deleted from `schedule.yml` | `SYNCED 0 0 1 8` | Removed. With upsert-only registration it would keep putting `WalCheckpointJob` into `splat.checkins` indefinitely. |
| `storage_stats` is renamed `storage_stats_hourly` | `SYNCED 1 0 1 8` | A rename is a remove plus an add: new identity, fresh counters, no last fire. |
| A typo: `0 2 * * * Australia/Melborne` | `BAD_SCHEDULE` on that put, then `ABORTED` | Nothing changes; the previous set keeps running. A bad deploy can't wipe the schedule. |
| Booko syncs owner `booko` on the same tuber | `SYNCED …` | Only `booko` entries are touched. Owners are namespaces. |

Details:

- **Changed** means any field differs after parsing — spec, zone, tube, pri,
  delay, ttr, tags, catch-up policy or body. Spellings of the same schedule
  (`CRON_TZ=X 0 2 * * *` and `0 2 * * * X`) are not a change.
- **A sync never fires anything.** New and changed entries start at the first
  tick strictly after the commit. Otherwise moving an entry from 03:00 to
  02:00 at 02:30 would fire it at the moment of the deploy.
- **An empty sync removes everything** the owner has. That is the declarative
  meaning, and the same property as Solid Queue; an app that might load an
  empty config by mistake should refuse to sync it.

**Sync at boot, not on reconnect.** In a rolling deploy, old and new releases
run side by side for a while:

```
10:00  v1 on A1 A2 A3, schedule has wal_checkpoint
10:05  B1 (v2) boots → sync without wal_checkpoint, with new vacuum → SYNCED 1 0 1 8
10:06  B2, B3 boot → SYNCED 0 0 0 9;  A1–A3 drain and exit
```

If A2 also re-synced whenever its connection dropped — say tuber restarted at
10:05:30 — it would put `wal_checkpoint` back and remove `vacuum` until it
exited. Syncing only at boot means the newest boot wins, which is right in
both directions: a rollback boots v1 processes, and their sync restores v1's
schedule. Because tuber persists schedules (decision 6), nothing needs
re-sending after a reconnect. A tuber running without `-b` loses them on
restart and gets them back from the next app process to boot.

Syncing at boot narrows the window but doesn't close it: an old process that
crashes and restarts mid-rollout boots the old release and syncs the old set
back, until the next new process boots. Syncing from a release step instead
(once per deploy, with the new code) closes it, since no app process syncs
at all. [Later](#later) has a guard that would close it for boot syncs too.

### 2. DST

The one decision with no consensus. Splat is directly exposed: Melbourne
skipped 02:00–02:59 on 2026-10-04 and repeats that hour on the first Sunday of
April, and `data_retention` (`0 2`) and `logs_fts_optimize` (`40 2`) both sit
inside it.

| | Local time doesn't exist (spring forward) | Local time happens twice (fall back) |
|---|---|---|
| Temporal | skipped — "no special handling of DST" | fires twice |
| AWS EventBridge Scheduler, OCPS 1.4 | skipped | once |
| robfig/cron | skipped ("will not be run") | — |
| Vixie cron(8) / cronie | fixed-time jobs run at the jump; wildcard jobs follow the clock | fixed-time once; wildcard follow the clock |
| croner-rust | fixed-time: first valid time after the gap; interval jobs skip it | fixed-time once; interval jobs both passes |
| RFC 5545, jiff `Compatible` | read with the offset before the gap: 02:40 → 03:40 | first occurrence |
| systemd timers, Quartz | undocumented; open bugs / self-contradicting docs | |

Cronie excludes `CRON_TZ` jobs from its DST handling ("jobs missed or run twice
during the DST change"), so even cron's careful rule doesn't cover per-job
zones.

**Decided: Vixie's split, with RFC 5545 for how late.** An entry is
*wildcard* if its minute or hour field starts with `*` (cron's own test, so
`*/15 …` and `@hourly` count); otherwise it is *fixed-time*.

- **Fixed-time** entries fire once for each local time they name. A time in a
  gap is read with the offset in force before the gap — later by the gap
  length — and a time in a fold fires on its first occurrence. This is jiff's
  default disambiguation, so it costs nothing to implement. If two named times
  land on the same instant (`30 2,3 * * *` on a gap day), it fires once.
- **Wildcard** entries fire whenever the local clock *reads* a matching time:
  both passes of a fold, and nothing lost in a gap, because no real time
  passes there. That avoids Quartz's documented failure, where a `*/15`
  trigger goes silent for the repeated hour.

| Entry | 2026-10-04, 02:00–02:59 skipped | 2027-04-04, 02:00–02:59 twice |
|---|---|---|
| `0 2 * * * Australia/Melbourne` | 03:00 AEDT | 02:00 AEDT only |
| `40 2 * * * Australia/Melbourne` | 03:40 AEDT | 02:40 AEDT only |
| `*/15 * * * * Australia/Melbourne` | …01:45 AEST, 03:00 AEDT, 03:15… | 02:00–02:45 AEDT, then 02:00–02:45 AEST |

Under the "skip" rule neither of Splat's 02:xx jobs would have run on 4
October. Vixie runs both at 03:00 together; RFC 5545 keeps them 40 minutes
apart. Entries in UTC, the default, never meet any of this.

### 3. Missed ticks

When tuber has been down across one or more ticks:

| Behaviour | Who |
|---|---|
| Skip | Solid Queue; Sidekiq Enterprise ("does not support backfill") |
| Fire once | systemd `Persistent=true`; Kubernetes (starts only the most recent missed time); Quartz's smart policy (`FIRE_NOW`); good_job's `cron_graceful_restart_period` |
| Fire every missed tick in a window | Temporal `catchup_window` (default one year) |

**Decided: fire once.** On startup, for each entry: if its most recent tick
is later than its last fire, put once now — however many ticks were missed —
then carry on from the next tick. Per entry, `catchup:<seconds>` fires that
one put only if the missed tick is at most that old, and `catchup:0` never
catches up. Splat's jobs recount on their own, so one fire is enough and a
burst is what it asked to avoid. This needs the last fire stored (decision 6).
The default is fire once whatever the age; skipping is opt-in per entry with
`catchup:0`.

A tick fired up to 5 seconds late counts as on time, not missed, so
`catchup:` governs downtime and clock steps rather than a slow engine step
— and a restart that takes a couple of seconds doesn't drop a `catchup:0`
entry's tick that fell inside it. The same rule covers a forward clock step
while running (decision 7).

### 4. Interval anchoring

rufus-scheduler's `every '10m'` and robfig's `@every` count from when the job
was added, so each Splat deploy shifts the phase. Temporal anchors intervals to
the epoch: `epoch + n·interval + offset`.

**Epoch-anchored**, with an optional `offset`. The next fire is then
a function of the spec and the clock alone, so restarts and re-syncs never
move it, and `every 10m` fires at :00, :10, :20. Intervals are real durations
and ignore zones (`every 1d` is exactly 24 h, as EventBridge's `rate(1 days)`
is); "every day at 02:00" belongs in cron syntax.

### 5. Overlap

What to do when a fire comes due while the previous fire's job still exists.
Kubernetes has `concurrencyPolicy` (Allow — the default — Forbid, Replace);
Temporal has six overlap policies, Skip by default; BullMQ only creates the
next job once the previous one starts.

**Tuber's tags already express this**, so there is no new knob:

| Entry has | Behaves like |
|---|---|
| `idp:<key>` | Skip — dedups against the still-live job |
| no key | Allow — every fire is a new job |
| `con:<key>` | queue them, run one at a time |

The gap is Skip against a *buried* job, which skips forever: 09-09. Temporal's
answer is `pause_on_failure`. Tuber's is visibility — `deduped_buried` fires,
`buried-streak`, and the per-tube counter that has already landed. Whether to
offer an opt-in to fire anyway when the match is buried is left for later.

### 6. Persistence and crashes

**Storage: two small files in the binlog directory**, not WAL records. WAL
segments are collected by tracking the live jobs each one holds, with
compaction copying them forward (`Wal::compaction_target`, `Wal::gc`);
schedule entries would have to join that, for state that changes a few times
per deploy.

- `schedules` holds every entry's definition, stored as its `schedule-put`
  line plus body and parsed back through the protocol parser on load, so a
  restored entry passes exactly the checks a synced one did. Rewritten on
  every commit, before `SYNCED` is sent.
- `schedules.fired` holds each entry's last fire. Rewritten whenever entries
  fire — one rewrite per tick that fires anything, however many entries
  fired in it — and kept apart so that rewrite is a few bytes per entry
  rather than every entry's body.

Both are versioned and checksummed, and replaced atomically (write
`<name>.tmp`, fsync, rename, fsync the directory). They count toward
`--max-storage-bytes`. This is the same shape as systemd's per-timer stamp
files. Nothing references across the two stores: a fired job is an ordinary
job in the WAL. On restart each entry resumes from the later of its last fire
and its sync time.

A damaged `schedules` file is moved to `schedules.corrupt`, as the WAL names
a corrupt segment (or `schedules.corrupt.N`, never over an earlier one), and
the server starts without schedules — the queue staying up matters more, and
each app's next sync restores its own. A checksummed file of another format version is not
damage but a newer tuber's schedules, after a downgrade say: that stops
startup with an explanation rather than being thrown away. Every format
version keeps the magic, version field and trailing CRC-32, so the two can be
told apart. A damaged `schedules.fired` only loses last
fires, so missed ticks are judged from each entry's sync time. A stored entry
that no longer parses (most likely a zone the system's tzdata dropped) is
logged and skipped.

Without `-b`, schedules live in memory like everything else, and the next app
process to boot syncs them back.

**A crash between deciding to fire and the put being durable** can go two
ways:

- **Stamp first, then put**: at-most-once per tick. A crash in the window
  loses that tick.
- **Put first, then stamp**: at-least-once. A crash in the window fires the
  tick again on restart; an entry with an `idp:` key absorbs the repeat if the
  first job is still live.

**Decided: stamp first.** Splat said missed ticks are fine and duplicates
are the complaint. The stamp is durable before the put is attempted, so a
crash in between loses that tick and nothing fires it later: on restart the
entry's last fire already covers it, so missed-tick catch-up (decision 3)
does not see it as missed either. If the stamps can't be written, nothing
fires that tick — recorded as `INTERNAL_ERROR` — for the same reason.

A commit that can't be written replies `INTERNAL_ERROR` with the new set live
in memory, the same contract as a put whose WAL write fails: the state stands,
the durability promise doesn't, and the client is told.

### 7. Firing

A fire goes through the same code path as a client `put`: validation,
idempotency, groups, concurrency keys, the memory and storage budgets, the WAL
record. The tube is created if absent; a paused tube accepts the job (pause
only holds reserves). Its result is recorded as `last-result`.

While the server is draining, or after its WAL has failed, nothing fires and
nothing is stamped: due ticks stay due, as if tuber were down. When the drain
ends (`undrain`) or the server restarts, they are missed ticks like any other,
and `catchup:` decides whether one fire catches them up. So the usual
maintenance routine of drain, wait, restart doesn't silently skip a daily job
due in between.

A fire refused by the put itself (`OUT_OF_MEMORY`, `OUT_OF_STORAGE`,
`JOB_TOO_BIG`, `INTERNAL_ERROR`) is counted and not retried — the next tick
tries again. One that queued nothing has the entry's last fire put back (in
memory and in `schedules.fired`): the staleness alert on
`tuber_schedule_last_fire_timestamp_seconds` still sees the gap, and after a
restart the tick counts as missed. That is all of them except the
`INTERNAL_ERROR` of the put that failed the WAL, which keeps its stamp: its
record may still have reached disk, and a restart can bring the job back.
An `INTERNAL_ERROR` from a failed TOAST write, or from a put refused because
an earlier fire in the tick failed the WAL, wrote nothing. A fire that
dedups against a buried job is logged as a warning each time, with the
streak.

The scheduler runs in the existing 100 ms tick, comparing each entry's next
fire with the wall clock (`jiff::Timestamp::now()`), not `Instant`, because
cron is defined on the wall clock. With at most 10,000 entries a scan per
tick is cheaper than keeping a heap in step with syncs. Fires are at most one
tick late. A forward clock step is treated like downtime (one catch-up
fire). After a backward step, nothing at or before an entry's last fire fires
again — for a step of up to an hour. A last fire or sync time more than an
hour in the future means the clock was wrong when it was recorded (a bad RTC
put right by NTP), not that it stepped back; honouring it would silence the
entry for as long as the clock was out, so the entry resumes from now, with a
warning.

The next-fire search walks the zone's stretches of constant offset, taking
each stretch's first match: within a stretch civil time and real time move
together. A fixed-time entry's stretch also takes the times a forward jump at
its end skips, read with its own offset, and skips the times a backward jump
at its start repeats. It costs about 100 ns a call, `* * * * *` included.

### 8. Spec syntax

- **5-field cron**, names, `@`-macros, `every`. No seconds field and none of
  Quartz's `L`, `W`, `#`, `?`.
- **Zone as a trailing IANA token** — Fugit's syntax, used by rufus, Solid
  Queue and sidekiq-cron, so Splat's existing strings paste in unchanged —
  and also a `CRON_TZ=` prefix (robfig, cronie, Temporal). Kubernetes rejects
  in-string zones in favour of a separate field, which a line protocol doesn't
  need.
- **Day-of-month and day-of-week both restricted: OR**, as crontab, cronie and
  OCPS do. Temporal ANDs them, which surprises people.
- **Validation at sync time:** an unknown zone, or a cron that never matches,
  is `BAD_SCHEDULE` then, not a silent no-op later.

### 9. Limits

Owner and name follow tube-name rules. The body is capped by
`--max-job-size`. A total entry cap of 10,000 keeps the files and the per-tick
scan bounded; entries count toward the `--max-jobs-size` memory budget as jobs
do (their body and strings plus 512 bytes each), reported as
`current-schedules-size`.

## Settled decisions

Open when the proposal was written; settled 2026-10-11.

1. **DST rule:** fixed-time entries fire once per named time (gap → later by
   the gap length, fold → first pass); wildcard entries follow the clock.
   Rejected: Temporal's "no special handling" (skip and double).
2. **Missed-tick default:** fire once, whatever the gap, with `catchup:` to
   bound or disable it per entry. Rejected: skip by default.
3. **Crash in the fire window:** stamp first, and lose the tick. Rejected: put
   first (may double a tick, absorbed by `idp:` only while the first job is
   live).

## Dependencies

- **jiff** (0.2) for zones, DST and civil-time arithmetic: two crates on
  Linux, `jiff` and `jiff-core`. Tuber had no date/time crate before it.
  jiff reads the system zone database;
  `debian:bookworm-slim` ships tzdata (checked: `tzdata 2026c`,
  `/usr/share/zoneinfo/Australia/Melbourne` present). Only a scratch or
  distroless image would need the `tzdb-bundle-always` feature, which adds
  `jiff-tzdb`.
- **The cron parser and next-fire search, written here.** jiff has none. croner
  4.0 (2026-08-31) can now run on jiff and its DST rules are close to the
  rule chosen here, but it is six weeks old and its README contradicts the OCPS spec it
  says it follows. The grammar is small, and the DST behaviour is the part
  worth owning and testing.

## Splat, entry by entry

| name | tube | spec | tags |
|---|---|---|---|
| `monitor_evaluation` | `splat.checkins` | `every 60s` | `idp:monitor_evaluation` |
| `wal_checkpoint` | `splat.checkins` | `every 10m` | `idp:wal_checkpoint` |
| `histogram_rollup` | `splat.maintenance` | `5 * * * *` | `idp:histogram_rollup con:1` |
| `data_retention` | `splat.maintenance` | `0 2 * * * Australia/Melbourne` | `con:1` |
| `logs_fts_optimize` | `splat.maintenance` | `40 2 * * * Australia/Melbourne` | `idp:logs_fts_optimize con:1` |
| `dictionary_drift` | `splat.maintenance` | `10 4 * * * Australia/Melbourne` | `idp:dictionary_drift con:1` |
| `cleanup_expired_oidc_sessions` | `splat.maintenance` | `17 * * * *` | `con:1` |
| `storage_stats` | `splat.maintenance` | `7 * * * *` | `idp:storage_stats con:1` |
| `storage_stats_deep` | `splat.maintenance` | `40 3 * * 0 Australia/Melbourne` | `idp:storage_stats_deep con:1` |

All at pri 1024, delay 0, ttr 120. `con:1` is Splat's config as it stands: a
concurrency *key* named `1`, not a limit, so the seven maintenance entries
share one slot and run one at a time — with each other and with any other
`con:1` job. That may well be the intent, but a named key
(`con:maintenance`) says so; the examples elsewhere use one.

One visible change: the two intervals become
epoch-anchored, so `monitor_evaluation` fires at :00 of each minute rather than
at an offset set by when the scheduler last booted.

Splat's seven asks, against this design:

| Ask | Met by |
|---|---|
| 1. Cron with an IANA zone, DST-aware; fixed intervals | Spec grammar; decisions 2 and 4 |
| 2. A fire is just a put | Decision 7 |
| 3. The app owns its schedule; removed entries go | `schedule-sync`, decision 1 |
| 4. Any number of processes register; each tick fires once | Identical re-sync is a no-op; one tuber fires |
| 5. No burst after downtime | Decision 3: one catch-up fire, or none |
| 6. Survives a tuber restart | Decision 6 |
| 7. Visibility, and dedups against BURIED counted | `stats-schedule`, metrics; per-tube counter already landed |

## Later

- **A generation on `schedule-begin`** (a release number or deploy time), with
  tuber refusing a sync older than the one it holds. That would make
  re-syncing on reconnect safe in a rolling deploy, and stop an old process
  that restarts mid-rollout from syncing the old set back. The cost: a
  rollback would then have to be done on purpose, by syncing with a newer
  generation, rather than happening by itself.
- Single-entry upsert and delete outside a sync, for hand-managed entries.
  Until then, hand-managed entries are just another owner (`ops`) synced from a
  file.
- `schedule-fire <owner> <name>` (Temporal's "trigger immediately"), pause and
  resume.
- Jitter: Temporal `jitter`, systemd `RandomizedDelaySec`/`FixedRandomDelay`,
  EventBridge's flexible window. If added, make it stable per name, as
  `FixedRandomDelay` is, so it doesn't move between restarts.
- An opt-in to fire anyway when the `idp:` match is buried.
- Recording on the job which schedule fired it (`stats-job`), which needs a
  job field and a WAL schema change.
- Per-owner authorisation once `docs/auth.md` lands: who may sync which owner.

## Prior art

- Vixie cron / cronie — [cron(8)](https://github.com/cronie-crond/cronie/blob/master/man/cron.8), [crontab(5)](https://github.com/cronie-crond/cronie/blob/master/man/crontab.5), [`src/cron.c`](https://github.com/cronie-crond/cronie/blob/master/src/cron.c)
- systemd — [systemd.timer](https://www.freedesktop.org/software/systemd/man/latest/systemd.timer.html), DST issues [#5595](https://github.com/systemd/systemd/issues/5595), [#8647](https://github.com/systemd/systemd/issues/8647)
- Kubernetes — [CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/), [controller `utils.go`](https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/cronjob/utils.go)
- Temporal — [schedule `message.proto`](https://github.com/temporalio/api/blob/master/temporal/api/schedule/v1/message.proto), [Schedules](https://docs.temporal.io/schedule)
- BullMQ — [Job Schedulers](https://docs.bullmq.io/guide/job-schedulers), [v5.16.0 `repeat.ts`](https://github.com/taskforcesh/bullmq/blob/v5.16.0/src/classes/repeat.ts)
- Solid Queue — [recurring tasks](https://github.com/rails/solid_queue#recurring-tasks), [`recurring_schedule.rb`](https://github.com/rails/solid_queue/blob/main/lib/solid_queue/scheduler/recurring_schedule.rb)
- Fugit / rufus-scheduler — [time zones](https://github.com/floraison/fugit#time-zones), [`jobs_repeat.rb`](https://github.com/jmettraux/rufus-scheduler/blob/master/lib/rufus/scheduler/jobs_repeat.rb)
- good_job — [README](https://github.com/bensheldon/good_job)
- Sidekiq — [Enterprise periodic jobs](https://github.com/sidekiq/sidekiq/wiki/Ent-Periodic-Jobs), [sidekiq-cron](https://github.com/sidekiq-cron/sidekiq-cron#readme)
- Quartz — [CronTrigger](https://www.quartz-scheduler.org/api/2.3.0/org/quartz/CronTrigger.html), [best practices](https://www.quartz-scheduler.org/documentation/2.3.1-SNAPSHOT/best-practices.html), [FAQ](https://www.quartz-scheduler.org/documentation/2.3.1-SNAPSHOT/faq.html)
- OCPS — [increment 1.4](https://github.com/open-source-cron/ocps/blob/main/increments/OCPS-increment-1.4.md), [issue #7](https://github.com/open-source-cron/ocps/issues/7); [croner-rust](https://github.com/Hexagon/croner-rust)
- robfig/cron — [`doc.go`](https://github.com/robfig/cron/blob/master/doc.go)
- jiff — [`Disambiguation`](https://docs.rs/jiff/latest/jiff/tz/enum.Disambiguation.html); [RFC 5545 §3.3.5](https://www.rfc-editor.org/rfc/rfc5545#section-3.3.5)
- AWS EventBridge Scheduler — [schedule types](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html)
