//! Exactly-once delivery support for the Kafka sink.
//!
//! Implements the watermark mechanics behind the `Sink` idempotency hooks: a
//! transactional producer commits each page's records plus a commit-token
//! record into a compacted side-topic in one Kafka transaction, and the token
//! is read back on resume. See
//! `docs/superpowers/specs/2026-06-18-kafka-sink-exactly-once-design.md`.

/// Derive the producer `transactional.id` from a stable pipeline scope.
///
/// The result is `"{prefix}.{sanitized}"`, where `sanitized` replaces any
/// character outside `[A-Za-z0-9._-]` with `_`. This keeps the id stable across
/// restarts of the same pipeline-row (so a restart fences its own zombie) and
/// unique across rows/pipelines whose scopes differ after sanitization (so
/// distinct pipelines never fence each other). Sanitization is many-to-one, so
/// callers must keep their scopes distinct under it; faucet derives scopes from
/// the pipeline/row identity (`{name}::{row_id}`), which stay distinct. `prefix`
/// is interpolated verbatim — validating it as a legal `transactional.id`
/// fragment is the caller's responsibility.
pub(crate) fn derive_transactional_id(prefix: &str, scope: &str) -> String {
    let sanitized: String = scope
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{prefix}.{sanitized}")
}

/// The maximum commit-token value recorded for `scope` among consumed
/// side-topic records.
///
/// Records are `(key_bytes, value_bytes)`. Only records whose key equals
/// `scope` are considered; their values are parsed as commit tokens and the
/// maximum is returned (robust to pre-compaction duplicates / out-of-order
/// delivery). Returns `None` when no valid token exists for the scope.
pub(crate) fn max_token_for_scope(
    records: &[(Vec<u8>, Option<Vec<u8>>)],
    scope: &str,
) -> Option<u64> {
    records
        .iter()
        .filter(|(k, _)| k.as_slice() == scope.as_bytes())
        .filter_map(|(_, v)| v.as_ref())
        .filter_map(|v| std::str::from_utf8(v).ok())
        .filter_map(faucet_core::idempotency::parse_token)
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_sanitizes_scope_separators() {
        assert_eq!(
            derive_transactional_id("faucet", "pipe::row0"),
            "faucet.pipe__row0"
        );
    }

    #[test]
    fn derive_keeps_allowed_chars_and_prefix() {
        assert_eq!(derive_transactional_id("acme", "a.b-c_1"), "acme.a.b-c_1");
        assert_eq!(derive_transactional_id("faucet", "x/y z"), "faucet.x_y_z");
    }

    #[test]
    fn max_token_picks_highest_for_scope_only() {
        let recs = vec![
            (
                b"s1".to_vec(),
                Some(faucet_core::idempotency::format_token(3).into_bytes()),
            ),
            (
                b"s1".to_vec(),
                Some(faucet_core::idempotency::format_token(7).into_bytes()),
            ),
            (
                b"s2".to_vec(),
                Some(faucet_core::idempotency::format_token(99).into_bytes()),
            ),
        ];
        assert_eq!(max_token_for_scope(&recs, "s1"), Some(7));
        assert_eq!(max_token_for_scope(&recs, "s2"), Some(99));
        assert_eq!(max_token_for_scope(&recs, "absent"), None);
    }

    #[test]
    fn max_token_ignores_garbage_and_tombstones() {
        let recs = vec![
            (b"s1".to_vec(), None),
            (b"s1".to_vec(), Some(b"not-a-token".to_vec())),
            (
                b"s1".to_vec(),
                Some(faucet_core::idempotency::format_token(4).into_bytes()),
            ),
        ];
        assert_eq!(max_token_for_scope(&recs, "s1"), Some(4));
    }
}
