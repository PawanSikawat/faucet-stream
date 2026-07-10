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

/// Parse a `--window` duration: `45s`, `30m`, `6h`, `1d`, `1w` (or a bare
/// integer = seconds). Must be positive.
pub fn parse_window(s: &str) -> CliResult<Duration> {
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
    let dur = match unit {
        "s" => Duration::seconds(n),
        "m" => Duration::minutes(n),
        "h" => Duration::hours(n),
        "d" => Duration::days(n),
        "w" => Duration::weeks(n),
        _ => return Err(err()),
    };
    Ok(dur)
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
    window: Option<Duration>,
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
    let step = window.unwrap_or_else(|| end - cursor);
    while cursor < end {
        if units.len() >= MAX_UNITS {
            return Err(CliError::Config(format!(
                "the range would produce more than {MAX_UNITS} units with this --window — \
                 use a larger window"
            )));
        }
        let unit_end = (cursor + step).min(end);
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
    use serde_json::json;

    fn tz(name: &str) -> chrono_tz::Tz {
        name.parse().unwrap()
    }

    #[test]
    fn window_durations_parse() {
        assert_eq!(parse_window("45s").unwrap(), Duration::seconds(45));
        assert_eq!(parse_window("30m").unwrap(), Duration::minutes(30));
        assert_eq!(parse_window("6h").unwrap(), Duration::hours(6));
        assert_eq!(parse_window("1d").unwrap(), Duration::days(1));
        assert_eq!(parse_window("2w").unwrap(), Duration::weeks(2));
        assert_eq!(parse_window("3600").unwrap(), Duration::seconds(3600));
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
        let units = plan_windows(from, to, Some(Duration::days(1)), utc).unwrap();
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
        let units = plan_windows(from, to, Some(Duration::hours(2)), utc).unwrap();
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

    #[test]
    fn dst_transition_produces_no_gap_or_overlap() {
        // US spring-forward 2026: March 8, 02:00 EST → 03:00 EDT. Absolute
        // 1-day windows stay contiguous; local render shows the offset flip.
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-03-07", ny).unwrap();
        let to = parse_boundary("2026-03-10T00:00:00-04:00", ny).unwrap();
        let units = plan_windows(from, to, Some(Duration::days(1)), ny).unwrap();
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
        let err = plan_windows(from, to, Some(Duration::minutes(1)), utc).unwrap_err();
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
