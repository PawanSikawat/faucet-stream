//! Exactly-once / idempotent delivery primitives.
//!
//! The pipeline issues a monotonic **commit token** for every page that carries
//! a bookmark. The token is persisted in the [`StateStore`](crate::state::StateStore)
//! value next to the bookmark and committed inside the sink's own transaction,
//! so a crash between "sink durably wrote" and "state persisted" is resolved on
//! resume by skipping pages the sink already committed. See
//! `docs/superpowers/specs/2026-06-09-exactly-once-delivery-design.md`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Delivery guarantee for a pipeline run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    /// Today's behaviour: a page may be re-delivered after a crash between the
    /// sink write and the bookmark persist. Downstream must tolerate duplicates.
    #[default]
    AtLeastOnce,
    /// The sink durably records a per-page commit token atomically with the
    /// data; on resume the pipeline skips already-committed pages. Requires a
    /// state store, an idempotent sink, and a deterministic-replay source.
    ExactlyOnce,
}

/// Reserved key marking the exactly-once state wrapper object.
const EO_MARKER: &str = "__faucet_eo";
const EO_BOOKMARK: &str = "bookmark";
const EO_SEQ: &str = "seq";

/// Width of the zero-padded decimal token. `u64::MAX` is 20 digits, so 20 makes
/// lexicographic order match numeric order for the full `u64` range.
const TOKEN_WIDTH: usize = 20;

/// Render a page sequence as a fixed-width, lexicographically-ordered token.
pub fn format_token(seq: u64) -> String {
    format!("{seq:0TOKEN_WIDTH$}")
}

/// Parse a token produced by [`format_token`]. Returns `None` on garbage.
pub fn parse_token(s: &str) -> Option<u64> {
    s.trim().parse::<u64>().ok()
}

/// Wrap a bookmark + sequence into the exactly-once state value.
pub fn wrap_state(bookmark: Option<&Value>, seq: u64) -> Value {
    serde_json::json!({
        EO_MARKER: 1,
        EO_BOOKMARK: bookmark.cloned().unwrap_or(Value::Null),
        EO_SEQ: seq,
    })
}

/// Unwrap a stored state value into `(bookmark, seq)`.
///
/// A value that is the exactly-once wrapper object unwraps to its inner
/// bookmark + seq. Anything else is treated as a legacy/at-least-once **bare
/// bookmark** with `seq = 0` — so switching an existing pipeline to
/// `exactly_once` resumes cleanly (the sink's own watermark is authoritative).
pub fn unwrap_state(value: &Value) -> (Option<Value>, u64) {
    if let Value::Object(map) = value
        && map.get(EO_MARKER).and_then(Value::as_u64) == Some(1)
    {
        let bookmark = match map.get(EO_BOOKMARK) {
            None | Some(Value::Null) => None,
            Some(v) => Some(v.clone()),
        };
        let seq = map.get(EO_SEQ).and_then(Value::as_u64).unwrap_or(0);
        return (bookmark, seq);
    }
    // Legacy bare bookmark.
    let bookmark = if value.is_null() { None } else { Some(value.clone()) };
    (bookmark, 0)
}

/// Canonical watermark table the SQL sinks UPSERT the commit token into.
pub const COMMIT_TOKEN_TABLE: &str = "_faucet_commit_token";
/// Watermark column holding the pipeline state-key (`{name}::{row_id}`).
pub const COMMIT_TOKEN_SCOPE_COL: &str = "scope";
/// Watermark column holding the latest committed token.
pub const COMMIT_TOKEN_TOKEN_COL: &str = "token";

/// Iceberg snapshot summary property names.
pub const ICEBERG_SCOPE_PROP: &str = "faucet.commit-scope";
pub const ICEBERG_TOKEN_PROP: &str = "faucet.commit-token";

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn token_round_trips_and_orders_lexicographically() {
        assert_eq!(format_token(42).len(), TOKEN_WIDTH);
        assert_eq!(parse_token(&format_token(42)), Some(42));
        assert_eq!(parse_token(&format_token(0)), Some(0));
        assert_eq!(parse_token(&format_token(u64::MAX)), Some(u64::MAX));
        assert!(format_token(9) < format_token(10));
        assert!(format_token(2) < format_token(1000));
    }

    #[test]
    fn parse_token_rejects_garbage() {
        assert_eq!(parse_token("abc"), None);
        assert_eq!(parse_token(""), None);
    }

    #[test]
    fn wrap_then_unwrap_preserves_bookmark_and_seq() {
        let bm = json!({"lsn": "0/16B2D58"});
        let wrapped = wrap_state(Some(&bm), 7);
        let (got_bm, got_seq) = unwrap_state(&wrapped);
        assert_eq!(got_bm, Some(bm));
        assert_eq!(got_seq, 7);
    }

    #[test]
    fn wrap_none_bookmark_unwraps_to_none() {
        let wrapped = wrap_state(None, 3);
        let (got_bm, got_seq) = unwrap_state(&wrapped);
        assert_eq!(got_bm, None);
        assert_eq!(got_seq, 3);
    }

    #[test]
    fn legacy_bare_bookmark_unwraps_with_seq_zero() {
        let (bm, seq) = unwrap_state(&json!("2024-12-01"));
        assert_eq!(bm, Some(json!("2024-12-01")));
        assert_eq!(seq, 0);
        let (bm2, seq2) = unwrap_state(&json!({"updated_at": "2024-12-01"}));
        assert_eq!(bm2, Some(json!({"updated_at": "2024-12-01"})));
        assert_eq!(seq2, 0);
    }

    #[test]
    fn object_with_non_sentinel_marker_is_treated_as_bare_bookmark() {
        // A legacy/user object that merely contains the key must NOT be misread
        // as an EO wrapper — only the typed sentinel `1` counts.
        let v = json!({"__faucet_eo": null, "offset": 500});
        let (bm, seq) = unwrap_state(&v);
        assert_eq!(bm, Some(v));
        assert_eq!(seq, 0);
    }

    #[test]
    fn null_value_unwraps_to_none_seq_zero() {
        let (bm, seq) = unwrap_state(&json!(null));
        assert_eq!(bm, None);
        assert_eq!(seq, 0);
    }

    #[test]
    fn delivery_mode_serde_is_snake_case_and_defaults_at_least_once() {
        assert_eq!(DeliveryMode::default(), DeliveryMode::AtLeastOnce);
        assert_eq!(serde_json::to_string(&DeliveryMode::ExactlyOnce).unwrap(), "\"exactly_once\"");
        let m: DeliveryMode = serde_json::from_str("\"at_least_once\"").unwrap();
        assert_eq!(m, DeliveryMode::AtLeastOnce);
    }
}
