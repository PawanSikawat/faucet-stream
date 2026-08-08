//! Pure window/unit planning for `faucet backfill` — boundary parsing, range
//! chunking (timezone/DST-correct), `${backfill.*}` token substitution, and
//! the stable range hash the progress marker is keyed by. No I/O.

use crate::error::{CliError, CliResult};
use chrono::{DateTime, Duration, FixedOffset, TimeZone, Utc};
use serde_json::Value;

/// Hard ceiling on planned units — a tiny `--window` over a huge range is a
/// config error, not a workload.
pub const MAX_UNITS: usize = 10_000;
/// Above this many units a loud warning is emitted (but planning proceeds).
pub const WARN_UNITS: usize = 1_000;

/// One independent, resumable slice of the backfill range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillUnit {
    /// Stable unit id — the UTC start instant, compact (`20260601T000000Z`).
    /// Doubles as the state-key suffix (`{name}::backfill::{id}`).
    pub id: String,
    /// Half-open window start (inclusive). Carried in the range's timezone
    /// offset so `${now.*}` renders local dates.
    pub start: DateTime<FixedOffset>,
    /// Half-open window end (exclusive).
    pub end: DateTime<FixedOffset>,
}

/// How a window advances the cursor.
///
/// The distinction matters only in a timezone that observes DST, and only for
/// day/week windows: stepping a "day" by a fixed 24 hours drifts off local
/// midnight after a transition, so the unit labelled `2026-03-08` would cover
/// 00:00 → *next day* 01:00 (25 local hours) and every later unit would start an
/// hour late. Sub-day windows have no such expectation — an hour is an hour — so
/// they stay absolute (#461).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowStep {
    /// A fixed elapsed duration (`s` / `m` / `h`, or a bare integer = seconds).
    Absolute(Duration),
    /// N calendar days in the backfill timezone.
    Days(i64),
    /// N calendar weeks in the backfill timezone.
    Weeks(i64),
}

impl std::fmt::Display for WindowStep {
    /// Stable form used in the range-hash descriptor that keys the progress
    /// marker.
    ///
    /// An absolute window renders as its **seconds**, exactly as before this type
    /// existed, so a backfill already in flight keeps its marker and resumes. A
    /// calendar window renders as `Nd` / `Nw`, which deliberately hashes
    /// differently: its unit boundaries are not the ones the old plan produced, so
    /// resuming against a marker from that plan would mix two different unit sets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absolute(d) => write!(f, "{}", d.num_seconds()),
            Self::Days(n) => write!(f, "{n}d"),
            Self::Weeks(n) => write!(f, "{n}w"),
        }
    }
}

impl WindowStep {
    /// The nominal duration, for logging and for the absolute fallback.
    fn nominal(self) -> Duration {
        match self {
            Self::Absolute(d) => d,
            Self::Days(n) => Duration::days(n),
            Self::Weeks(n) => Duration::weeks(n),
        }
    }
}

/// Parse a `--window` duration: `45s`, `30m`, `6h`, `1d`, `1w` (or a bare
/// integer = seconds). Must be positive.
///
/// `d` / `w` yield **calendar** steps; everything else is absolute. So `1d` and
/// `24h` differ across a DST transition, deliberately.
pub fn parse_window(s: &str) -> CliResult<WindowStep> {
    let s = s.trim();
    let err = || {
        CliError::Config(format!(
            "'{s}' is not a valid window — use e.g. 45s, 30m, 6h, 1d, 1w"
        ))
    };
    let (num, unit) = match s.chars().last() {
        Some(c) if c.is_ascii_digit() => (s, "s"),
        Some(c) => (&s[..s.len() - c.len_utf8()], &s[s.len() - c.len_utf8()..]),
        None => return Err(err()),
    };
    let n: i64 = num.parse().map_err(|_| err())?;
    if n <= 0 {
        return Err(CliError::Config(format!(
            "window '{s}' must be a positive duration"
        )));
    }
    let step = match unit {
        "s" => WindowStep::Absolute(Duration::seconds(n)),
        "m" => WindowStep::Absolute(Duration::minutes(n)),
        "h" => WindowStep::Absolute(Duration::hours(n)),
        "d" => WindowStep::Days(n),
        "w" => WindowStep::Weeks(n),
        _ => return Err(err()),
    };
    Ok(step)
}

/// Advance `cursor` by a calendar amount in `tz`, keeping the local wall-clock
/// time (so a day stays a day and midnight stays midnight across a DST change).
///
/// The naive local time is advanced first — naive arithmetic has no DST, so this
/// *is* calendar arithmetic — then re-resolved in `tz`. A spring-forward gap
/// (the wall-clock time does not exist that day) has no valid instant, so the
/// time is nudged forward an hour at a time until it does; a fall-back
/// (ambiguous, repeated) hour resolves to the **earliest** instant, matching
/// [`parse_boundary`].
fn advance_calendar(cursor: DateTime<Utc>, tz: chrono_tz::Tz, days: i64) -> Option<DateTime<Utc>> {
    let naive = cursor
        .with_timezone(&tz)
        .naive_local()
        .checked_add_signed(Duration::days(days))?;
    for extra_hours in 0..=3 {
        let candidate = naive.checked_add_signed(Duration::hours(extra_hours))?;
        if let Some(local) = tz.from_local_datetime(&candidate).earliest() {
            return Some(local.with_timezone(&Utc));
        }
    }
    None
}

/// Parse a `--from` / `--to` boundary: RFC3339 (`2026-06-01T00:00:00Z`) or a
/// bare date (`2026-06-01`, interpreted as midnight in `tz`). A date that
/// falls in a DST gap resolves to the earliest valid instant.
pub fn parse_boundary(s: &str, tz: chrono_tz::Tz) -> CliResult<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&tz).fixed_offset());
    }
    if let Ok(date) = s.parse::<chrono::NaiveDate>() {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::Config(format!("'{s}' has no valid midnight in {tz}")))?;
        let local = tz
            .from_local_datetime(&midnight)
            .earliest()
            .ok_or_else(|| {
                CliError::Config(format!("'{s}' midnight does not exist in {tz} (DST gap)"))
            })?;
        return Ok(local.fixed_offset());
    }
    Err(CliError::Config(format!(
        "'{s}' is not RFC3339 (2026-06-01T00:00:00Z) or a date (2026-06-01)"
    )))
}

/// Chunk `[from, to)` into contiguous half-open windows of `window` (the last
/// window truncated at `to`). `window: None` = the whole range as one unit.
/// Window arithmetic is absolute (instants), so units never gap or overlap —
/// including across DST transitions; boundaries are re-rendered in `tz` so
/// `${now.*}` tokens see local wall-clock time.
pub fn plan_windows(
    from: DateTime<FixedOffset>,
    to: DateTime<FixedOffset>,
    window: Option<WindowStep>,
    tz: chrono_tz::Tz,
) -> CliResult<Vec<BackfillUnit>> {
    if from >= to {
        return Err(CliError::Config(format!(
            "--from ({from}) must be before --to ({to})"
        )));
    }
    let mut units = Vec::new();
    let mut cursor = from.with_timezone(&Utc);
    let end = to.with_timezone(&Utc);
    let step = window.unwrap_or(WindowStep::Absolute(end - cursor));
    while cursor < end {
        if units.len() >= MAX_UNITS {
            return Err(CliError::Config(format!(
                "the range would produce more than {MAX_UNITS} units with this --window — \
                 use a larger window"
            )));
        }
        // Calendar steps keep the local wall clock; absolute steps add elapsed
        // time. Either way the next boundary must be strictly ahead of the
        // cursor, or the loop could not terminate — fall back to the nominal
        // duration if a zone quirk ever produced a non-advancing instant.
        let next = match step {
            WindowStep::Absolute(d) => cursor + d,
            WindowStep::Days(n) => {
                advance_calendar(cursor, tz, n).unwrap_or(cursor + step.nominal())
            }
            WindowStep::Weeks(n) => {
                advance_calendar(cursor, tz, n * 7).unwrap_or(cursor + step.nominal())
            }
        };
        let next = if next > cursor {
            next
        } else {
            cursor + step.nominal()
        };
        let unit_end = next.min(end);
        units.push(BackfillUnit {
            id: cursor.format("%Y%m%dT%H%M%SZ").to_string(),
            start: cursor.with_timezone(&tz).fixed_offset(),
            end: unit_end.with_timezone(&tz).fixed_offset(),
        });
        cursor = unit_end;
    }
    Ok(units)
}

/// Substitute `${backfill.*}` tokens in every string leaf of `value`:
/// `start` / `end` (RFC3339), `start_date` / `end_date` (`YYYY-MM-DD`, local),
/// `start_unix` / `end_unix` (epoch seconds), `unit` (the unit id). An
/// unrecognized `${backfill.*}` token is a typo — typed error.
pub fn substitute_unit_tokens(value: &mut Value, unit: &BackfillUnit) -> CliResult<()> {
    match value {
        Value::String(s) => {
            *s = substitute_in_str(s, unit)?;
            Ok(())
        }
        Value::Array(a) => a
            .iter_mut()
            .try_for_each(|v| substitute_unit_tokens(v, unit)),
        Value::Object(m) => m
            .values_mut()
            .try_for_each(|v| substitute_unit_tokens(v, unit)),
        _ => Ok(()),
    }
}

fn substitute_in_str(input: &str, unit: &BackfillUnit) -> CliResult<String> {
    const PREFIX: &str = "${backfill.";
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find(PREFIX) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + PREFIX.len()..];
        let close = after.find('}').ok_or_else(|| {
            CliError::Config(format!("unterminated ${{backfill.…}} token in '{input}'"))
        })?;
        let token = &after[..close];
        let rendered = match token {
            "start" => unit.start.to_rfc3339(),
            "end" => unit.end.to_rfc3339(),
            "start_date" => unit.start.format("%Y-%m-%d").to_string(),
            "end_date" => unit.end.format("%Y-%m-%d").to_string(),
            "start_unix" => unit.start.timestamp().to_string(),
            "end_unix" => unit.end.timestamp().to_string(),
            "unit" => unit.id.clone(),
            other => {
                return Err(CliError::Config(format!(
                    "unknown token ${{backfill.{other}}} — supported: start, end, start_date, \
                     end_date, start_unix, end_unix, unit"
                )));
            }
        };
        out.push_str(&rendered);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Deterministic 64-bit FNV-1a hash of the range descriptor, hex-encoded.
/// Keys the progress marker so `--resume` finds the same backfill across
/// process restarts (std's `DefaultHasher` is randomly seeded — unusable).
pub fn range_hash(descriptor: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for b in descriptor.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;
    use serde_json::json;

    fn tz(name: &str) -> chrono_tz::Tz {
        name.parse().unwrap()
    }

    #[test]
    fn window_durations_parse() {
        // Sub-day units are absolute elapsed time…
        assert_eq!(
            parse_window("45s").unwrap(),
            WindowStep::Absolute(Duration::seconds(45))
        );
        assert_eq!(
            parse_window("30m").unwrap(),
            WindowStep::Absolute(Duration::minutes(30))
        );
        assert_eq!(
            parse_window("6h").unwrap(),
            WindowStep::Absolute(Duration::hours(6))
        );
        assert_eq!(
            parse_window("3600").unwrap(),
            WindowStep::Absolute(Duration::seconds(3600))
        );
        // …while day/week units are calendar steps (#461).
        assert_eq!(parse_window("1d").unwrap(), WindowStep::Days(1));
        assert_eq!(parse_window("2w").unwrap(), WindowStep::Weeks(2));
        assert!(parse_window("0d").is_err());
        assert!(parse_window("-1h").is_err());
        assert!(parse_window("soon").is_err());
        assert!(parse_window("1y").is_err());
    }

    #[test]
    fn boundaries_parse_rfc3339_and_dates() {
        let utc = tz("UTC");
        let dt = parse_boundary("2026-06-01T12:30:00Z", utc).unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-06-01T12:30:00+00:00");
        // A bare date is midnight in the given timezone.
        let ny = tz("America/New_York");
        let dt = parse_boundary("2026-06-01", ny).unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-06-01T00:00:00-04:00");
        assert!(parse_boundary("yesterday", utc).is_err());
    }

    #[test]
    fn thirty_one_days_one_day_window_is_31_units() {
        // The acceptance-criteria example: a 31-day June-July range with a 1d
        // window plans exactly 31 units.
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-01", utc).unwrap();
        let to = parse_boundary("2026-07-02", utc).unwrap();
        let units = plan_windows(from, to, Some(WindowStep::Days(1)), utc).unwrap();
        assert_eq!(units.len(), 31);
        assert_eq!(units[0].id, "20260601T000000Z");
        assert_eq!(units[0].start.to_rfc3339(), "2026-06-01T00:00:00+00:00");
        assert_eq!(units[0].end.to_rfc3339(), "2026-06-02T00:00:00+00:00");
        // Contiguous half-open windows: each start equals the previous end.
        for w in units.windows(2) {
            assert_eq!(w[0].end, w[1].start);
        }
        assert_eq!(units[30].end.to_rfc3339(), "2026-07-02T00:00:00+00:00");
    }

    #[test]
    fn last_window_truncates_at_to() {
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-01T00:00:00Z", utc).unwrap();
        let to = parse_boundary("2026-06-01T05:30:00Z", utc).unwrap();
        let units = plan_windows(
            from,
            to,
            Some(WindowStep::Absolute(Duration::hours(2))),
            utc,
        )
        .unwrap();
        assert_eq!(units.len(), 3);
        assert_eq!(units[2].start.to_rfc3339(), "2026-06-01T04:00:00+00:00");
        assert_eq!(units[2].end.to_rfc3339(), "2026-06-01T05:30:00+00:00");
    }

    #[test]
    fn no_window_is_a_single_unit() {
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-01", utc).unwrap();
        let to = parse_boundary("2026-07-01", utc).unwrap();
        let units = plan_windows(from, to, None, utc).unwrap();
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].start, from);
        assert_eq!(units[0].end, to);
    }

    /// #461: a calendar day must stay a calendar day. Absolute 24h stepping used
    /// to drift off local midnight after a DST change — the unit labelled
    /// 2026-03-08 covered 00:00 → *next day* 01:00 (25 local hours) and every
    /// later unit started an hour late, so `${backfill.start_date}` no longer
    /// described the window it named.
    #[test]
    fn calendar_day_windows_stay_on_local_midnight_across_dst() {
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-03-07", ny).unwrap();
        let to = parse_boundary("2026-03-11", ny).unwrap();
        let units = plan_windows(from, to, Some(WindowStep::Days(1)), ny).unwrap();

        assert_eq!(units.len(), 4, "four calendar days");
        for u in &units {
            assert_eq!(
                (u.start.hour(), u.start.minute()),
                (0, 0),
                "unit {} must start at local midnight, got {}",
                u.id,
                u.start
            );
        }
        // Contiguous, and each unit's label matches the day it covers.
        for w in units.windows(2) {
            assert_eq!(w[0].end, w[1].start, "no gap/overlap");
        }
        let dates: Vec<String> = units
            .iter()
            .map(|u| u.start.format("%Y-%m-%d").to_string())
            .collect();
        assert_eq!(
            dates,
            ["2026-03-07", "2026-03-08", "2026-03-09", "2026-03-10"]
        );
        // The spring-forward day is genuinely 23 hours of elapsed time.
        let spring_forward = &units[1];
        assert_eq!(
            (spring_forward.end - spring_forward.start).num_hours(),
            23,
            "2026-03-08 loses an hour"
        );
    }

    /// Fall-back (an hour repeats) must also stay on midnight, at 25 elapsed hours.
    #[test]
    fn calendar_day_windows_handle_fall_back() {
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-10-31", ny).unwrap();
        let to = parse_boundary("2026-11-03", ny).unwrap();
        let units = plan_windows(from, to, Some(WindowStep::Days(1)), ny).unwrap();
        for u in &units {
            assert_eq!((u.start.hour(), u.start.minute()), (0, 0), "{}", u.id);
        }
        // 2026-11-01 is the fall-back day: 25 hours.
        let long_day = units
            .iter()
            .find(|u| u.start.format("%Y-%m-%d").to_string() == "2026-11-01")
            .expect("the fall-back day is planned");
        assert_eq!((long_day.end - long_day.start).num_hours(), 25);
    }

    /// `1d` and `24h` are deliberately different across a transition: one is a
    /// calendar day, the other is elapsed time.
    #[test]
    fn calendar_and_absolute_windows_differ_across_dst() {
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-03-07", ny).unwrap();
        let to = parse_boundary("2026-03-10", ny).unwrap();
        let cal = plan_windows(from, to, Some(parse_window("1d").unwrap()), ny).unwrap();
        let abs = plan_windows(from, to, Some(parse_window("24h").unwrap()), ny).unwrap();
        assert_eq!(cal[2].start.hour(), 0, "calendar stays on midnight");
        assert_eq!(abs[2].start.hour(), 1, "absolute drifts by the DST delta");
        assert_ne!(cal[2].start, abs[2].start);
    }

    /// The descriptor an absolute window contributes to the range hash is
    /// unchanged, so a backfill already in flight keeps resuming.
    #[test]
    fn window_descriptor_is_stable_for_absolute_and_distinct_for_calendar() {
        assert_eq!(
            WindowStep::Absolute(Duration::hours(6)).to_string(),
            "21600"
        );
        assert_eq!(WindowStep::Absolute(Duration::days(1)).to_string(), "86400");
        assert_eq!(WindowStep::Days(1).to_string(), "1d");
        assert_eq!(WindowStep::Weeks(2).to_string(), "2w");
    }

    #[test]
    fn dst_transition_produces_no_gap_or_overlap() {
        // US spring-forward 2026: March 8, 02:00 EST → 03:00 EDT. Absolute
        // 1-day windows stay contiguous; local render shows the offset flip.
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-03-07", ny).unwrap();
        let to = parse_boundary("2026-03-10T00:00:00-04:00", ny).unwrap();
        let units =
            plan_windows(from, to, Some(WindowStep::Absolute(Duration::days(1))), ny).unwrap();
        for w in units.windows(2) {
            assert_eq!(w[0].end, w[1].start, "no gap/overlap across DST");
        }
        // First window starts EST (-05:00); a later one renders EDT (-04:00).
        assert!(units[0].start.to_rfc3339().ends_with("-05:00"));
        assert!(units.last().unwrap().end.to_rfc3339().ends_with("-04:00"));
    }

    #[test]
    fn rejects_inverted_range_and_unit_explosion() {
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-02", utc).unwrap();
        let to = parse_boundary("2026-06-01", utc).unwrap();
        assert!(plan_windows(from, to, None, utc).is_err());

        let from = parse_boundary("2020-01-01", utc).unwrap();
        let to = parse_boundary("2026-01-01", utc).unwrap();
        let err = plan_windows(
            from,
            to,
            Some(WindowStep::Absolute(Duration::minutes(1))),
            utc,
        )
        .unwrap_err();
        assert!(err.to_string().contains("larger window"), "{err}");
    }

    #[test]
    fn tokens_substitute_in_nested_config() {
        let utc = tz("UTC");
        let unit = BackfillUnit {
            id: "20260601T000000Z".into(),
            start: parse_boundary("2026-06-01T00:00:00Z", utc).unwrap(),
            end: parse_boundary("2026-06-02T00:00:00Z", utc).unwrap(),
        };
        let mut cfg = json!({
            "query": "SELECT * FROM t WHERE ts >= '${backfill.start}' AND ts < '${backfill.end}'",
            "nested": { "path": "dt=${backfill.start_date}/part-${backfill.unit}.jsonl" },
            "unix": ["${backfill.start_unix}", "${backfill.end_unix}"],
            "count": 3,
        });
        substitute_unit_tokens(&mut cfg, &unit).unwrap();
        assert_eq!(
            cfg["query"],
            "SELECT * FROM t WHERE ts >= '2026-06-01T00:00:00+00:00' AND ts < '2026-06-02T00:00:00+00:00'"
        );
        assert_eq!(
            cfg["nested"]["path"],
            "dt=2026-06-01/part-20260601T000000Z.jsonl"
        );
        assert_eq!(cfg["unix"][0], "1780272000");
        assert_eq!(cfg["count"], 3);
    }

    #[test]
    fn unknown_or_unterminated_token_is_a_typed_error() {
        let utc = tz("UTC");
        let unit = BackfillUnit {
            id: "u".into(),
            start: parse_boundary("2026-06-01", utc).unwrap(),
            end: parse_boundary("2026-06-02", utc).unwrap(),
        };
        let mut bad = json!({"q": "${backfill.begin}"});
        let err = substitute_unit_tokens(&mut bad, &unit).unwrap_err();
        assert!(err.to_string().contains("backfill.begin"), "{err}");
        let mut unterminated = json!({"q": "${backfill.start"});
        assert!(substitute_unit_tokens(&mut unterminated, &unit).is_err());
    }

    #[test]
    fn range_hash_is_stable_and_distinct() {
        let a = range_hash("2026-06-01|2026-07-01|1d");
        assert_eq!(a, range_hash("2026-06-01|2026-07-01|1d"), "deterministic");
        assert_ne!(a, range_hash("2026-06-01|2026-07-01|6h"));
        assert_eq!(a.len(), 16);
    }
}
