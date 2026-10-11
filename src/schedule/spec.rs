//! Schedule specs: the text a `schedule-put` line ends with, and when an entry
//! with that spec fires next.
//!
//! ```text
//! spec  := cron [zone] | "CRON_TZ=" zone cron | macro [zone] | "every" dur ["offset" dur]
//! cron  := minute hour day-of-month month day-of-week
//! macro := @hourly | @daily | @midnight | @weekly | @monthly | @yearly | @annually
//! dur   := <n>s | <n>m | <n>h | <n>d
//! ```
//!
//! Cron fields take `*`, `a`, `a-b`, `*/n`, `a-b/n` and comma lists of those,
//! plus `JAN`–`DEC` and `SUN`–`SAT`; 0 and 7 are both Sunday. When both
//! day-of-month and day-of-week are restricted a day matches either, as in
//! crontab(5). The zone defaults to UTC. Intervals are anchored to the Unix
//! epoch, so `every 10m` fires at :00, :10, :20 whenever it was registered.
//!
//! **DST.** An entry whose minute or hour field starts with `*` (Vixie cron's
//! test, so `*/15 …` and `@hourly` count) follows the clock: it fires whenever
//! the local clock reads a matching time — both passes of a repeated hour, and
//! nothing in a skipped one. Every other entry fires once for each local time
//! it names: a time the clock skips is read with the offset in force before
//! the skip (so `0 2` fires at 03:00 on spring-forward day in Melbourne), and
//! a time the clock repeats fires on its first pass. That is jiff's default
//! "compatible" disambiguation, as in RFC 5545.

use jiff::civil::{Date, DateTime};
use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp};

/// A parsed spec. Two specs are equal when they fire at the same times, so
/// `CRON_TZ=X 0 2 * * *` equals `0 2 * * * X`; the text is kept by the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Spec {
    Cron { cron: Cron, zone: Zone },
    Every { every: i64, offset: i64 },
}

/// An IANA zone, compared by name.
#[derive(Clone, Debug)]
pub struct Zone {
    name: String,
    tz: TimeZone,
}

impl PartialEq for Zone {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Zone {}

impl Zone {
    fn utc() -> Zone {
        Zone {
            name: "UTC".to_string(),
            tz: TimeZone::UTC,
        }
    }

    fn get(name: &str) -> Result<Zone, String> {
        let tz = TimeZone::get(name).map_err(|e| format!("unknown time zone {name:?}: {e}"))?;
        let name = tz.iana_name().unwrap_or(name).to_string();
        Ok(Zone { name, tz })
    }
}

/// The five cron fields as bitsets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cron {
    minutes: u64,
    hours: u32,
    /// Bit `d` for day `d`, 1–31.
    doms: u32,
    /// Bit `m` for month `m`, 1–12.
    months: u16,
    /// Bit `d` for day `d`, 0 (Sunday) – 6.
    dows: u8,
    /// Day-of-month / day-of-week field started with `*`. If either did, a
    /// day must match both fields; otherwise it may match either.
    dom_star: bool,
    dow_star: bool,
    /// Minute or hour field started with `*`: follow the clock through DST
    /// changes rather than fire once per named time.
    wildcard: bool,
}

/// Longest stretch searched for the next match. A valid cron matches within
/// 8 years (Feb 29 skips 2100); 30 leaves room for day-of-week combinations.
const HORIZON_DAYS: u32 = 366 * 30;

/// How far before `t` the search starts. A time the clock skips fires late by
/// the length of the skip, so a fire due just after `t` can belong to a civil
/// time named before it. Skips are an hour, rarely 30 minutes or two hours;
/// Samoa skipped a whole day in 2011.
const LOOKBACK: SignedDuration = SignedDuration::from_hours(48);

/// Bound on offset changes walked in one search; DST makes two a year.
const MAX_SEGMENTS: usize = 128;

/// Longest `every` interval or offset: 100 years.
const MAX_DURATION_SECS: i64 = 100 * 366 * 86_400;

impl Spec {
    pub fn parse(text: &str) -> Result<Spec, String> {
        let tokens: Vec<&str> = text.split_whitespace().collect();
        let Some(&first) = tokens.first() else {
            return Err("empty schedule".to_string());
        };
        if first == "every" {
            return parse_every(&tokens[1..]);
        }
        let (zone, rest) = match first.strip_prefix("CRON_TZ=") {
            Some(name) => (Some(Zone::get(name)?), &tokens[1..]),
            None => (None, &tokens[..]),
        };
        let (cron, rest) = match rest.first() {
            Some(m) if m.starts_with('@') => (Cron::from_macro(m)?, &rest[1..]),
            _ if rest.len() >= 5 => (Cron::parse(&rest[..5])?, &rest[5..]),
            _ => return Err(format!("expected 5 cron fields in {text:?}")),
        };
        let zone = match (zone, rest) {
            (Some(zone), []) => zone,
            (None, []) => Zone::utc(),
            (None, [name]) => Zone::get(name)?,
            _ => return Err(format!("unexpected text after the schedule in {text:?}")),
        };
        if cron
            .next_civil(Date::constant(2000, 1, 1).at(0, 0, 0, 0), None)
            .is_none()
        {
            return Err(format!("{text:?} never matches a date"));
        }
        Ok(Spec::Cron { cron, zone })
    }

    /// The first fire strictly after `t`.
    pub fn next_after(&self, t: Timestamp) -> Option<Timestamp> {
        match self {
            Spec::Every { every, offset } => {
                let k = (t.as_second() - offset).div_euclid(*every) + 1;
                Timestamp::from_second(k * every + offset).ok()
            }
            Spec::Cron { cron, zone } => cron.next_after(&zone.tz, t),
        }
    }

    /// The latest fire at or before `t`, if there is one later than `since`.
    /// Used to measure how stale a missed tick is.
    pub fn latest_at_or_before(&self, t: Timestamp, since: Timestamp) -> Option<Timestamp> {
        let mut found = self.next_after(since).filter(|&n| n <= t)?;
        // Gallop back from `t` until a window holds a fire, then walk that
        // window forward. Ticks are walked one by one only inside the final
        // window, which is at most twice the gap before the latest tick.
        let mut window = SignedDuration::from_mins(1);
        loop {
            let from = t
                .checked_sub(window)
                .ok()
                .filter(|&f| f > since)
                .unwrap_or(since);
            if let Some(n) = self.next_after(from).filter(|&n| n <= t) {
                found = n;
                break;
            }
            if from == since {
                break;
            }
            window = window.checked_mul(2).unwrap_or(SignedDuration::MAX);
        }
        while let Some(n) = self.next_after(found).filter(|&n| n <= t) {
            found = n;
        }
        Some(found)
    }

    /// The zone fire times are shown in.
    pub fn time_zone(&self) -> TimeZone {
        match self {
            Spec::Cron { zone, .. } => zone.tz.clone(),
            Spec::Every { .. } => TimeZone::UTC,
        }
    }
}

fn parse_every(tokens: &[&str]) -> Result<Spec, String> {
    let (every, offset) = match tokens {
        [every] => (parse_duration(every)?, 0),
        [every, "offset", offset] => (parse_duration(every)?, parse_duration(offset)?),
        _ => return Err("expected `every <dur> [offset <dur>]`".to_string()),
    };
    if every == 0 {
        return Err("`every` needs an interval of at least 1s".to_string());
    }
    if offset >= every {
        return Err("an `every` offset must be shorter than the interval".to_string());
    }
    Ok(Spec::Every { every, offset })
}

fn parse_duration(s: &str) -> Result<i64, String> {
    let unit = match s.as_bytes().last() {
        Some(b's') => 1,
        Some(b'm') => 60,
        Some(b'h') => 3_600,
        Some(b'd') => 86_400,
        _ => return Err(format!("duration {s:?} needs a unit: s, m, h or d")),
    };
    let digits = &s[..s.len() - 1];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("bad duration {s:?}"));
    }
    digits
        .parse::<i64>()
        .ok()
        .and_then(|n| n.checked_mul(unit))
        .filter(|&secs| secs <= MAX_DURATION_SECS)
        .ok_or_else(|| format!("duration {s:?} is too long"))
}

const MONTH_NAMES: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const DOW_NAMES: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

impl Cron {
    fn from_macro(m: &str) -> Result<Cron, String> {
        let fields = match m {
            "@hourly" => "0 * * * *",
            "@daily" | "@midnight" => "0 0 * * *",
            "@weekly" => "0 0 * * 0",
            "@monthly" => "0 0 1 * *",
            "@yearly" | "@annually" => "0 0 1 1 *",
            _ => return Err(format!("unknown macro {m:?}")),
        };
        Cron::parse(&fields.split(' ').collect::<Vec<_>>())
    }

    fn parse(fields: &[&str]) -> Result<Cron, String> {
        let [minute, hour, dom, month, dow] = fields else {
            return Err("expected 5 cron fields".to_string());
        };
        // Day-of-week is parsed over 0–7 and 7 folded onto Sunday.
        let dow_bits = parse_field(dow, 0, 7, &DOW_NAMES, 0)?;
        Ok(Cron {
            minutes: parse_field(minute, 0, 59, &[], 0)?,
            hours: parse_field(hour, 0, 23, &[], 0)? as u32,
            doms: parse_field(dom, 1, 31, &[], 0)? as u32,
            months: parse_field(month, 1, 12, &MONTH_NAMES, 1)? as u16,
            dows: ((dow_bits | dow_bits >> 7) & 0x7f) as u8,
            dom_star: dom.starts_with('*'),
            dow_star: dow.starts_with('*'),
            wildcard: minute.starts_with('*') || hour.starts_with('*'),
        })
    }

    fn day_matches(&self, date: Date) -> bool {
        let month = self.months & (1 << date.month()) != 0;
        let dom = self.doms & (1 << date.day()) != 0;
        let dow = self.dows & (1 << date.weekday().to_sunday_zero_offset()) != 0;
        month
            && if self.dom_star || self.dow_star {
                dom && dow
            } else {
                dom || dow
            }
    }

    /// The first matching civil time at or after `from` (minute-aligned) and
    /// before `before`, ignoring zones.
    fn next_civil(&self, from: DateTime, before: Option<DateTime>) -> Option<DateTime> {
        let mut date = from.date();
        let mut start = (from.hour() as u32, from.minute() as u32);
        for _ in 0..HORIZON_DAYS {
            if before.is_some_and(|b| date > b.date()) {
                return None;
            }
            if self.day_matches(date)
                && let Some((h, m)) = self.first_time_from(start)
            {
                let c = date.at(h as i8, m as i8, 0, 0);
                return before.is_none_or(|b| c < b).then_some(c);
            }
            date = date.tomorrow().ok()?;
            start = (0, 0);
        }
        None
    }

    fn first_time_from(&self, (hour, minute): (u32, u32)) -> Option<(u32, u32)> {
        let mut h = next_bit(self.hours as u64, hour)?;
        loop {
            let from = if h == hour { minute } else { 0 };
            if let Some(m) = next_bit(self.minutes, from) {
                return Some((h, m));
            }
            h = next_bit(self.hours as u64, h + 1)?;
        }
    }

    /// Walk the zone's stretches of constant offset from shortly before `t`,
    /// taking each stretch's first fire after `t`, until a stretch starts past
    /// the earliest found. Within a stretch civil time and real time move
    /// together, so its first civil match is its earliest fire.
    fn next_after(&self, tz: &TimeZone, t: Timestamp) -> Option<Timestamp> {
        let mut start = t.checked_sub(LOOKBACK).ok()?;
        let mut prev_civil_end: Option<DateTime> = None;
        let mut best: Option<Timestamp> = None;
        for _ in 0..MAX_SEGMENTS {
            if best.is_some_and(|b| start > b) {
                break;
            }
            let offset = tz.to_offset(start);
            let transition = tz.following(start).next();
            let end = transition.as_ref().map(|tr| tr.timestamp());

            // Civil times this stretch fires for. A fixed-time entry also
            // takes the times a forward jump at its end skips (read with
            // this stretch's offset, so they land just after the jump), and
            // skips times a backward jump repeats at its start.
            let mut lower = ceil_minute(offset.to_datetime(start))?;
            if !self.wildcard
                && let Some(p) = prev_civil_end
            {
                lower = lower.max(p);
            }
            lower = lower.max(
                floor_minute(offset.to_datetime(t))
                    .checked_add(SignedDuration::from_mins(1))
                    .ok()?,
            );
            let civil_end = transition.as_ref().map(|tr| {
                let reading = if self.wildcard {
                    offset
                } else {
                    offset.max(tr.offset())
                };
                reading.to_datetime(tr.timestamp())
            });

            if let Some(c) = self.next_civil(lower, civil_end)
                && let Ok(fire) = offset.to_timestamp(c)
            {
                best = Some(best.map_or(fire, |b| b.min(fire)));
            }
            prev_civil_end = civil_end;
            match end {
                Some(e) => start = e,
                None => break,
            }
        }
        best
    }
}

fn ceil_minute(dt: DateTime) -> Option<DateTime> {
    let floor = floor_minute(dt);
    if floor == dt {
        Some(dt)
    } else {
        floor.checked_add(SignedDuration::from_mins(1)).ok()
    }
}

fn floor_minute(dt: DateTime) -> DateTime {
    dt.date().at(dt.hour(), dt.minute(), 0, 0)
}

fn next_bit(mask: u64, from: u32) -> Option<u32> {
    if from >= 64 {
        return None;
    }
    let rest = mask >> from;
    (rest != 0).then(|| from + rest.trailing_zeros())
}

/// Parse one cron field into a bitset over `lo..=hi`. `names[i]` stands for
/// `name_base + i`.
fn parse_field(
    field: &str,
    lo: u32,
    hi: u32,
    names: &[&str],
    name_base: u32,
) -> Result<u64, String> {
    let bad = || format!("bad cron field {field:?}");
    let value = |s: &str| -> Result<u32, String> {
        if let Some(i) = names.iter().position(|n| n.eq_ignore_ascii_case(s)) {
            return Ok(name_base + i as u32);
        }
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        s.parse::<u32>()
            .ok()
            .filter(|v| (lo..=hi).contains(v))
            .ok_or_else(bad)
    };
    let mut bits = 0u64;
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => {
                let step: u32 = step.parse().ok().filter(|&s| s > 0).ok_or_else(bad)?;
                (range, step)
            }
            None => (part, 1),
        };
        let (a, b) = match range {
            "*" => (lo, hi),
            _ => match range.split_once('-') {
                Some((a, b)) => (value(a)?, value(b)?),
                // A step needs a range to step through: `5/15` is refused,
                // as Vixie cron refuses it, rather than guessed at.
                None if step == 1 => {
                    let v = value(range)?;
                    (v, v)
                }
                None => return Err(bad()),
            },
        };
        if a > b {
            return Err(bad());
        }
        for v in (a..=b).step_by(step as usize) {
            bits |= 1 << v;
        }
    }
    Ok(bits)
}

/// RFC 3339 with the zone's offset, as `stats-schedule` shows fire times.
pub fn format_time(t: Timestamp, tz: &TimeZone) -> String {
    t.to_zoned(tz.clone())
        .strftime("%Y-%m-%dT%H:%M:%S%:z")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn fires(spec: &str, from: &str, n: usize) -> Vec<String> {
        let spec = Spec::parse(spec).unwrap();
        let mut t = ts(from);
        let mut out = Vec::new();
        for _ in 0..n {
            t = spec.next_after(t).unwrap();
            out.push(format_time(t, &spec.time_zone()));
        }
        out
    }

    #[test]
    fn hourly_utc() {
        assert_eq!(
            fires("5 * * * *", "2026-10-11T03:04:59Z", 3),
            [
                "2026-10-11T03:05:00+00:00",
                "2026-10-11T04:05:00+00:00",
                "2026-10-11T05:05:00+00:00"
            ],
        );
    }

    #[test]
    fn next_is_strictly_after() {
        assert_eq!(
            fires("5 * * * *", "2026-10-11T03:05:00Z", 1),
            ["2026-10-11T04:05:00+00:00"],
        );
    }

    #[test]
    fn fixed_time_in_a_skipped_hour_fires_late_by_the_skip() {
        // Melbourne skipped 02:00–02:59 on 2026-10-04.
        assert_eq!(
            fires(
                "0 2 * * * Australia/Melbourne",
                "2026-10-03T12:00:00+10:00",
                2
            ),
            ["2026-10-04T03:00:00+11:00", "2026-10-05T02:00:00+11:00"],
        );
        assert_eq!(
            fires(
                "40 2 * * * Australia/Melbourne",
                "2026-10-03T12:00:00+10:00",
                2
            ),
            ["2026-10-04T03:40:00+11:00", "2026-10-05T02:40:00+11:00"],
        );
    }

    #[test]
    fn fixed_time_in_a_skipped_hour_is_found_from_just_after_the_jump() {
        // At 03:10 AEDT the 02:40 named time, read as 03:40, is still ahead.
        assert_eq!(
            fires(
                "40 2 * * * Australia/Melbourne",
                "2026-10-04T03:10:00+11:00",
                1
            ),
            ["2026-10-04T03:40:00+11:00"],
        );
    }

    #[test]
    fn fixed_times_landing_on_one_instant_fire_once() {
        // 02:30 is skipped and read as 03:30, which is also named.
        assert_eq!(
            fires(
                "30 2,3 * * * Australia/Melbourne",
                "2026-10-04T01:00:00+10:00",
                3
            ),
            [
                "2026-10-04T03:30:00+11:00",
                "2026-10-05T02:30:00+11:00",
                "2026-10-05T03:30:00+11:00",
            ],
        );
    }

    #[test]
    fn fixed_time_in_a_repeated_hour_fires_on_the_first_pass() {
        // Melbourne repeats 02:00–02:59 on 2027-04-04.
        assert_eq!(
            fires(
                "0 2 * * * Australia/Melbourne",
                "2027-04-03T12:00:00+11:00",
                2
            ),
            ["2027-04-04T02:00:00+11:00", "2027-04-05T02:00:00+10:00"],
        );
        assert_eq!(
            fires(
                "40 2 * * * Australia/Melbourne",
                "2027-04-04T02:41:00+11:00",
                1
            ),
            ["2027-04-05T02:40:00+10:00"],
        );
    }

    #[test]
    fn half_hour_skip() {
        // Lord Howe Island moves from +10:30 to +11 at 02:00 on 2026-10-04,
        // skipping 02:00–02:29. 02:15 is read with +10:30 and lands at 02:45.
        assert_eq!(
            fires(
                "15 2 * * * Australia/Lord_Howe",
                "2026-10-03T12:00:00+10:30",
                2
            ),
            ["2026-10-04T02:45:00+11:00", "2026-10-05T02:15:00+11:00"],
        );
    }

    #[test]
    fn wildcard_follows_the_clock_through_a_skipped_hour() {
        assert_eq!(
            fires(
                "*/15 * * * * Australia/Melbourne",
                "2026-10-04T01:40:00+10:00",
                3
            ),
            [
                "2026-10-04T01:45:00+10:00",
                "2026-10-04T03:00:00+11:00",
                "2026-10-04T03:15:00+11:00"
            ],
        );
    }

    #[test]
    fn wildcard_fires_both_passes_of_a_repeated_hour() {
        assert_eq!(
            fires(
                "*/15 * * * * Australia/Melbourne",
                "2027-04-04T01:50:00+11:00",
                9
            ),
            [
                "2027-04-04T02:00:00+11:00",
                "2027-04-04T02:15:00+11:00",
                "2027-04-04T02:30:00+11:00",
                "2027-04-04T02:45:00+11:00",
                "2027-04-04T02:00:00+10:00",
                "2027-04-04T02:15:00+10:00",
                "2027-04-04T02:30:00+10:00",
                "2027-04-04T02:45:00+10:00",
                "2027-04-04T03:00:00+10:00",
            ],
        );
    }

    #[test]
    fn hourly_macro_is_wildcard() {
        // @hourly is `0 * * * *`: the hour field is `*`, so it follows the
        // clock and stays an hour apart in real time across the repeat.
        assert_eq!(
            fires(
                "@hourly Australia/Melbourne",
                "2027-04-04T01:30:00+11:00",
                3
            ),
            [
                "2027-04-04T02:00:00+11:00",
                "2027-04-04T02:00:00+10:00",
                "2027-04-04T03:00:00+10:00"
            ],
        );
    }

    #[test]
    fn macros() {
        assert_eq!(
            fires("@daily Australia/Melbourne", "2026-10-11T12:00:00+11:00", 1),
            ["2026-10-12T00:00:00+11:00"]
        );
        assert_eq!(
            fires("@midnight", "2026-10-11T12:00:00Z", 1),
            ["2026-10-12T00:00:00+00:00"]
        );
        assert_eq!(
            fires("@weekly", "2026-10-11T12:00:00Z", 1),
            ["2026-10-18T00:00:00+00:00"]
        );
        assert_eq!(
            fires("@monthly", "2026-10-11T12:00:00Z", 1),
            ["2026-11-01T00:00:00+00:00"]
        );
        assert_eq!(
            fires("@yearly", "2026-10-11T12:00:00Z", 1),
            ["2027-01-01T00:00:00+00:00"]
        );
        assert_eq!(
            fires("@annually", "2026-10-11T12:00:00Z", 1),
            ["2027-01-01T00:00:00+00:00"]
        );
    }

    #[test]
    fn cron_tz_prefix_is_the_same_schedule() {
        assert_eq!(
            Spec::parse("CRON_TZ=Australia/Melbourne 0 2 * * *").unwrap(),
            Spec::parse("0 2 * * * Australia/Melbourne").unwrap(),
        );
        assert_eq!(
            Spec::parse("0 2 * * * UTC").unwrap(),
            Spec::parse("0 2 * * *").unwrap()
        );
        assert_ne!(
            Spec::parse("0 2 * * * Australia/Melbourne").unwrap(),
            Spec::parse("0 2 * * *").unwrap()
        );
    }

    #[test]
    fn day_of_month_or_day_of_week() {
        // Friday the 13th style: the 13th, and every Friday.
        assert_eq!(
            fires("0 0 13 * FRI", "2026-11-01T00:00:00Z", 4),
            [
                "2026-11-06T00:00:00+00:00",
                "2026-11-13T00:00:00+00:00",
                "2026-11-20T00:00:00+00:00",
                "2026-11-27T00:00:00+00:00",
            ],
        );
        // With either field starting `*`, both must match: odd days that
        // are Mondays.
        assert_eq!(
            fires("0 0 */2 * MON", "2026-11-01T00:00:00Z", 2),
            ["2026-11-09T00:00:00+00:00", "2026-11-23T00:00:00+00:00"]
        );
    }

    #[test]
    fn names_ranges_steps_and_sunday_as_7() {
        assert_eq!(
            fires("0 9 * jan-mar Mon-Fri", "2026-12-31T00:00:00Z", 2),
            ["2027-01-01T09:00:00+00:00", "2027-01-04T09:00:00+00:00"],
        );
        assert_eq!(
            fires("0 0 * * 7", "2026-10-11T01:00:00Z", 1),
            ["2026-10-18T00:00:00+00:00"]
        );
        assert_eq!(
            fires("0 0 * * 5-7", "2026-10-11T01:00:00Z", 1),
            ["2026-10-16T00:00:00+00:00"]
        );
        assert_eq!(
            fires("10-30/10 * * * *", "2026-10-11T01:00:00Z", 4),
            [
                "2026-10-11T01:10:00+00:00",
                "2026-10-11T01:20:00+00:00",
                "2026-10-11T01:30:00+00:00",
                "2026-10-11T02:10:00+00:00",
            ],
        );
    }

    #[test]
    fn leap_day() {
        assert_eq!(
            fires("0 0 29 2 *", "2026-10-11T00:00:00Z", 1),
            ["2028-02-29T00:00:00+00:00"]
        );
    }

    #[test]
    fn every_is_anchored_to_the_epoch() {
        assert_eq!(
            fires("every 60s", "2026-10-11T03:04:30Z", 2),
            ["2026-10-11T03:05:00+00:00", "2026-10-11T03:06:00+00:00"],
        );
        assert_eq!(
            fires("every 10m", "2026-10-11T03:04:30Z", 2),
            ["2026-10-11T03:10:00+00:00", "2026-10-11T03:20:00+00:00"],
        );
        assert_eq!(
            fires("every 1h offset 5m", "2026-10-11T03:05:00Z", 2),
            ["2026-10-11T04:05:00+00:00", "2026-10-11T05:05:00+00:00"],
        );
        // Days are exactly 86400 s; zones play no part.
        assert_eq!(
            fires("every 1d", "2026-10-11T03:05:00Z", 1),
            ["2026-10-12T00:00:00+00:00"]
        );
    }

    #[test]
    fn rejects() {
        for bad in [
            "",
            "* * * *",
            "* * * * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * * 13 *",
            "* * * * 8",
            "5/15 * * * *",
            "*/0 * * * *",
            "30-10 * * * *",
            "a * * * *",
            "-1 * * * *",
            "0 0 30 2 *",
            "0 0 31 4,6 *",
            "0 2 * * * Australia/Melborne",
            "CRON_TZ=Nowhere/Special 0 2 * * *",
            "CRON_TZ=UTC 0 2 * * * UTC",
            "@reboot",
            "@daily extra tokens",
            "every",
            "every 0s",
            "every 10",
            "every 10x",
            "every 1h offset 1h",
            "every 1h offset",
            "every 1h 5m",
            "every 999999999999d",
        ] {
            assert!(Spec::parse(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    #[ignore = "timing, run by hand"]
    fn every_minute_in_a_dst_zone_is_cheap() {
        let spec = Spec::parse("* * * * * Australia/Melbourne").unwrap();
        let mut t = ts("2026-10-01T00:00:00Z");
        let start = std::time::Instant::now();
        for _ in 0..100_000 {
            t = spec.next_after(t).unwrap();
        }
        eprintln!("100k next_after: {:?}, reached {t}", start.elapsed());
    }

    #[test]
    fn latest_at_or_before() {
        let spec = Spec::parse("0 2 * * *").unwrap();
        let since = ts("2026-10-01T00:00:00Z");
        assert_eq!(
            spec.latest_at_or_before(ts("2026-10-11T12:00:00Z"), since),
            Some(ts("2026-10-11T02:00:00Z")),
        );
        assert_eq!(
            spec.latest_at_or_before(ts("2026-10-11T02:00:00Z"), since),
            Some(ts("2026-10-11T02:00:00Z")),
        );
        // Nothing after `since`.
        assert_eq!(
            spec.latest_at_or_before(ts("2026-10-11T12:00:00Z"), ts("2026-10-11T03:00:00Z")),
            None
        );

        let every = Spec::parse("every 60s").unwrap();
        assert_eq!(
            every.latest_at_or_before(ts("2026-10-11T12:00:30Z"), ts("2025-01-01T00:00:00Z")),
            Some(ts("2026-10-11T12:00:00Z")),
        );
        let busy = Spec::parse("* 2 * * *").unwrap();
        assert_eq!(
            busy.latest_at_or_before(ts("2026-10-11T12:00:00Z"), since),
            Some(ts("2026-10-11T02:59:00Z")),
        );
    }
}
