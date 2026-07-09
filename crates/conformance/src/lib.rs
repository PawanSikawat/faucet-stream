#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-conformance
//!
//! A reusable test battery that any faucet connector can call from its own
//! `tests/` to prove it upholds the connector contract. Passing this battery is
//! the **Tier-1** criterion for a connector — there is no separate tiering
//! scheme; a connector is "supported" exactly when it invokes and passes these
//! checks in CI.
//!
//! ```no_run
//! # async fn ex() {
//! use faucet_conformance as conf;
//! let source = /* your Source */
//! #     conf::doubles::CountingSource::new(1000, 100);
//! conf::assert_config_schema_valid(&source);
//! conf::assert_bounded_memory(&source, 100, 1000).await;
//! # }
//! ```
//!
//! Checks 1 (`assert_config_schema_valid`) and 2 (`assert_bounded_memory`) are
//! fully implemented; checks 3–6 are compiling skeletons with `// TODO` bodies
//! whose signatures are stable so wiring them into a connector now keeps
//! compiling as they are filled in.

pub mod doubles;

use std::collections::HashMap;

use faucet_core::{Sink, Source, Value};
use futures::StreamExt;

// ── Check 1: config schema validity ─────────────────────────────────────────

/// Anything that can expose a config JSON Schema + a label — blanket-implemented
/// for every [`Source`] so [`assert_config_schema_valid`] accepts a source
/// directly. (Sinks can be checked via [`assert_config_schema_valid_value`].)
pub trait HasConfigSchema {
    /// The connector's advertised config schema.
    fn conformance_schema(&self) -> Value;
    /// A human label for assertion messages.
    fn conformance_label(&self) -> String;
}

impl<T: Source + ?Sized> HasConfigSchema for T {
    fn conformance_schema(&self) -> Value {
        self.config_schema()
    }
    fn conformance_label(&self) -> String {
        self.connector_name().to_string()
    }
}

/// **Check 1.** Assert the connector's `config_schema()` is a structurally valid
/// JSON Schema that round-trips through `serde_json`.
///
/// Panics (fails the test) on: a non-object schema, a schema with no recognized
/// schema shape, a non-object `properties`, or a serialize→parse→serialize that
/// is not stable.
pub fn assert_config_schema_valid<C: HasConfigSchema + ?Sized>(connector: &C) {
    assert_config_schema_valid_value(
        &connector.conformance_schema(),
        &connector.conformance_label(),
    );
}

/// The value-level core of [`assert_config_schema_valid`] — usable for sinks:
/// `assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name())`.
pub fn assert_config_schema_valid_value(schema: &Value, label: &str) {
    let obj = schema.as_object().unwrap_or_else(|| {
        panic!("[{label}] config_schema() must be a JSON object, got: {schema}")
    });

    // Recognized as *some* JSON Schema shape.
    let recognized = [
        "type",
        "properties",
        "$ref",
        "oneOf",
        "allOf",
        "anyOf",
        "$schema",
        "enum",
    ]
    .iter()
    .any(|k| obj.contains_key(*k));
    assert!(
        recognized,
        "[{label}] config_schema() has no recognizable JSON Schema keyword: {schema}"
    );

    if let Some(props) = obj.get("properties") {
        assert!(
            props.is_object(),
            "[{label}] config_schema().properties must be an object, got: {props}"
        );
    }
    if let Some(ty) = obj.get("type") {
        assert!(
            ty.is_string() || ty.is_array(),
            "[{label}] config_schema().type must be a string or array, got: {ty}"
        );
    }

    // Round-trip: serialize → parse → serialize must be stable.
    let text = serde_json::to_string(schema).expect("schema serializes");
    let reparsed: Value = serde_json::from_str(&text).expect("schema re-parses");
    assert_eq!(
        &reparsed, schema,
        "[{label}] config_schema() does not round-trip through serde_json"
    );
}

// ── Check 2: bounded memory ──────────────────────────────────────────────────

/// **Check 2.** Drive `stream_pages` over a source that yields `total` records
/// and assert the consumer never holds more than ~`batch_size` records live at
/// once (i.e. the source pages instead of buffering everything).
///
/// Requires `batch_size > 0` and `total > batch_size` for a meaningful result.
/// Asserts: every record is streamed (`sum == total`), the largest single page
/// is `<= batch_size`, and strictly `< total` (proving the source did not emit
/// the whole set as one page).
pub async fn assert_bounded_memory<S: Source + ?Sized>(
    source: &S,
    batch_size: usize,
    total: usize,
) {
    assert!(
        batch_size > 0,
        "batch_size must be > 0 for a bounded-memory check"
    );
    assert!(
        total > batch_size,
        "total ({total}) must exceed batch_size ({batch_size}) for a meaningful check"
    );
    let label = source.connector_name();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut stream = source.stream_pages(&ctx, batch_size);
    let mut seen = 0usize;
    let mut peak = 0usize;
    while let Some(page) = stream.next().await {
        let page = page.unwrap_or_else(|e| panic!("[{label}] stream_pages errored: {e}"));
        peak = peak.max(page.records.len());
        seen += page.records.len();
        // `page` is dropped here — the consumer only ever holds one page.
    }

    assert_eq!(
        seen, total,
        "[{label}] streamed {seen} records, expected {total}"
    );
    assert!(
        peak <= batch_size,
        "[{label}] peak page {peak} exceeds batch_size {batch_size} (not bounded)"
    );
    assert!(
        peak < total,
        "[{label}] peak page {peak} == total: source buffered the whole set into one page"
    );
}

// ── Checks 3–6: compiling skeletons (signatures stable; bodies TODO) ─────────

/// **Check 3 (TODO).** Assert a source persists a bookmark and, given it back
/// via `apply_start_bookmark`, resumes from that point rather than restarting.
pub async fn assert_bookmark_roundtrip<S: Source + ?Sized>(source: &S) {
    let _ = source;
    // TODO: drive stream_pages to completion capturing the final bookmark;
    // call apply_start_bookmark(bookmark); re-drive and assert the second run
    // starts after the bookmark (fewer/zero records for a fully-consumed source).
}

/// **Check 4 (TODO).** Assert that re-delivering already-written rows to a
/// keyed/idempotent sink leaves no duplicates.
pub async fn assert_idempotent_replay<S: Sink + ?Sized>(sink: &S) {
    let _ = sink;
    // TODO: write a page, write an overlapping page, assert the destination
    // holds each key once (requires a keyed sink or the idempotent-commit path).
}

/// **Check 5 (TODO).** Assert a connector's advertised capabilities match its
/// real behavior (e.g. `supported_write_modes()` are actually accepted;
/// `supports_idempotent_writes()` implies a working `write_batch_idempotent`).
pub fn assert_capabilities_truthful<S: Sink + ?Sized>(sink: &S) {
    let _ = sink;
    // TODO: cross-check the boolean/enum capability advertisements against a
    // probe of the corresponding method.
}

/// **Check 6 (TODO).** Assert malformed input / transient failures surface as a
/// typed [`faucet_core::FaucetError`] rather than a panic.
pub async fn assert_errors_not_panics<S: Source + ?Sized>(source: &S) {
    let _ = source;
    // TODO: feed the source into failure conditions (bad config, unreachable
    // endpoint) and assert it returns Err(FaucetError::...) without unwinding.
}

#[cfg(test)]
mod tests {
    use super::*;
    use doubles::{CountingSource, TestSink};

    #[test]
    fn check1_accepts_a_valid_source_schema() {
        let s = CountingSource::new(10, 2);
        assert_config_schema_valid(&s);
    }

    #[test]
    fn check1_value_form_works_for_a_sink() {
        let sink = TestSink::new();
        assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name());
    }

    #[test]
    #[should_panic(expected = "no recognizable JSON Schema keyword")]
    fn check1_rejects_a_non_schema() {
        assert_config_schema_valid_value(&serde_json::json!({"nope": 1}), "bogus");
    }

    #[tokio::test]
    async fn check2_passes_for_a_paging_source() {
        let s = CountingSource::new(1000, 100);
        assert_bounded_memory(&s, 100, 1000).await;
    }

    #[tokio::test]
    #[should_panic(expected = "not bounded")]
    async fn check2_fails_when_source_emits_one_big_page() {
        // batch 0 => single page of `total`, which must trip the bounded check.
        let s = CountingSource::new(500, 0);
        assert_bounded_memory(&s, 100, 500).await;
    }
}
