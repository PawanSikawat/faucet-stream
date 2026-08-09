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
//! All six checks are fully implemented:
//! 1. [`assert_config_schema_valid`] — the config schema is a valid JSON Schema.
//! 2. [`assert_bounded_memory`] — the source pages instead of buffering.
//! 3. [`assert_bookmark_roundtrip`] — an incremental source resumes from its
//!    bookmark rather than restarting.
//! 4. [`assert_idempotent_replay`] — re-delivering committed rows leaves no
//!    duplicates (atomic-watermark or keyed-upsert mechanism).
//! 5. [`assert_capabilities_truthful`] — advertised capabilities match real
//!    behaviour.
//! 6. [`assert_errors_not_panics`] — failures surface as a typed
//!    [`faucet_core::FaucetError`], never a panic.
//!
//! Each check has both a passing and a `#[should_panic]` failing test in this
//! crate — a check that cannot fail is worthless.

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

// ── Check 3: bookmark round-trip (resumable sources) ─────────────────────────

/// **Check 3.** Drive an incremental source to completion, capture the bookmark
/// it emits, feed it back via
/// [`apply_start_bookmark`](Source::apply_start_bookmark), and assert the second
/// run resumes *after* that point — strictly fewer records reappear (zero for a
/// fully-consumed static source).
///
/// Only meaningful for a source that actually emits a bookmark and honours it.
/// Panics if the source produces no bookmark (nothing to round-trip), or if the
/// resumed run replays the same volume (the bookmark was ignored).
pub async fn assert_bookmark_roundtrip<S: Source + ?Sized>(source: &S) {
    let label = source.connector_name();
    let ctx: HashMap<String, Value> = HashMap::new();

    // First run: consume every page, remembering how many records we saw and the
    // last non-null bookmark.
    let (first_records, bookmark) = drain(source, &ctx, label).await;
    assert!(
        first_records > 0,
        "[{label}] produced no records — cannot exercise bookmark round-trip"
    );
    let bookmark = bookmark.unwrap_or_else(|| {
        panic!("[{label}] produced no bookmark to round-trip (stream_pages never set one)")
    });

    // Resume from the captured bookmark.
    source
        .apply_start_bookmark(bookmark.clone())
        .await
        .unwrap_or_else(|e| panic!("[{label}] apply_start_bookmark errored: {e}"));

    let (second_records, _) = drain(source, &ctx, label).await;
    assert!(
        second_records < first_records,
        "[{label}] resumed run replayed {second_records} records (first run: {first_records}); \
         the bookmark {bookmark} was ignored — no incremental resume"
    );
}

/// Drive `stream_pages` to completion, returning `(record_count, last_bookmark)`.
async fn drain<S: Source + ?Sized>(
    source: &S,
    ctx: &HashMap<String, Value>,
    label: &str,
) -> (usize, Option<Value>) {
    let mut stream = source.stream_pages(ctx, 100);
    let mut count = 0usize;
    let mut last_bookmark = None;
    while let Some(page) = stream.next().await {
        let page = page.unwrap_or_else(|e| panic!("[{label}] stream_pages errored: {e}"));
        count += page.records.len();
        if page.bookmark.is_some() {
            last_bookmark = page.bookmark;
        }
    }
    (count, last_bookmark)
}

// ── Check 4: idempotent replay (no duplicates on re-delivery) ─────────────────

/// **Check 4.** Assert re-delivering already-committed rows leaves no
/// duplicates in the destination — the trust-critical effectively-once check.
///
/// `distinct_count` returns the number of distinct rows the destination
/// currently holds (for a double, `|| async { sink.len() }`; for a real sink, a
/// `SELECT count(*)`). Records are keyed on the field `"id"`, so a real sink
/// under test must be configured `write_mode: upsert` with `key: ["id"]`.
///
/// Dispatches on the mechanism the sink advertises:
/// - `supports_idempotent_writes()` → the **atomic-watermark** path: writing a
///   page durably records a commit token; a crash-replay (guarded by
///   `last_committed_token`, exactly as the pipeline guards it) does not
///   re-write, and forward progress still advances.
/// - else `dedups_by_key()` → the **keyed-upsert** path: overlapping keys across
///   pages converge to one row each.
/// - neither → panics (the sink advertises no idempotency mechanism to test).
pub async fn assert_idempotent_replay<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let label = sink.connector_name();
    if sink.supports_idempotent_writes() {
        assert_watermark_idempotent(sink, &distinct_count, label).await;
    } else if sink.dedups_by_key() {
        assert_keyed_convergence(sink, &distinct_count, label).await;
    } else {
        panic!(
            "[{label}] advertises no idempotency mechanism \
             (supports_idempotent_writes=false, dedups_by_key=false) — nothing to verify"
        );
    }
}

/// Build test records keyed on `"id"` with a non-key `"v"` column, so a SQL
/// upsert (`ON CONFLICT(id) DO UPDATE SET v = …`) has something to set — a
/// single key-only column would produce an empty SET clause.
fn rows(ids: &[i64]) -> Vec<Value> {
    ids.iter()
        .map(|i| serde_json::json!({ "id": i, "v": format!("v{i}") }))
        .collect()
}

async fn assert_watermark_idempotent<S, F, Fut>(sink: &S, count: &F, label: &str)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let scope = "conformance::idem";
    let before = count().await;

    // Page 1 with the first commit token.
    let t1 = faucet_core::format_token(1);
    let p1 = rows(&[1, 2, 3]);
    sink.write_batch_idempotent(&p1, scope, &t1)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch_idempotent(page 1) errored: {e}"));
    let after_first = count().await;
    assert_eq!(
        after_first - before,
        3,
        "[{label}] first idempotent write did not add all 3 rows"
    );

    // The token must be durably recorded — this is what lets the pipeline skip a
    // replay. A sink that claims idempotency but never persists a token fails here.
    let committed = sink
        .last_committed_token(scope)
        .await
        .unwrap_or_else(|e| panic!("[{label}] last_committed_token errored: {e}"));
    assert_eq!(
        committed.as_deref(),
        Some(t1.as_str()),
        "[{label}] did not durably record its commit token — cannot skip a replay"
    );

    // Crash-replay of page 1: the pipeline compares the page token against the
    // committed token and *skips* the page when already committed. Assert that
    // decision resolves to "skip" — i.e. the sink's recorded token parses and is
    // ≥ the page token. (We deliberately do NOT re-invoke the sink and assert the
    // row count is unchanged: the no-duplication guarantee lives in the pipeline's
    // skip, not in the sink, so an append-mode idempotent sink re-delivered the
    // same committed page legitimately *would* grow. Testing that here would fail
    // correct sinks. This is the vacuous assertion #466 L4 removed.)
    let committed_seq = faucet_core::parse_token(committed.as_deref().unwrap_or_default())
        .unwrap_or_else(|| panic!("[{label}] committed token {committed:?} does not parse"));
    assert!(
        committed_seq >= faucet_core::parse_token(&t1).unwrap_or(0),
        "[{label}] committed token did not reach the written page's token — \
         run_stream could not skip the replay and would re-write the page"
    );

    // Forward progress with a new token still writes.
    let t2 = faucet_core::format_token(2);
    let p2 = rows(&[4, 5]);
    sink.write_batch_idempotent(&p2, scope, &t2)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch_idempotent(page 2) errored: {e}"));
    let after_second = count().await;
    assert_eq!(
        after_second - after_first,
        2,
        "[{label}] forward progress after a new token did not add the new rows"
    );
}

async fn assert_keyed_convergence<S, F, Fut>(sink: &S, count: &F, label: &str)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let before = count().await;
    sink.write_batch(&rows(&[1, 2, 3]))
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch(page 1) errored: {e}"));
    // Overlapping page: ids 2 and 3 are re-delivered.
    sink.write_batch(&rows(&[2, 3, 4]))
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch(overlapping page) errored: {e}"));
    let after = count().await;
    assert_eq!(
        after - before,
        4,
        "[{label}] overlapping keys did not converge: expected 4 distinct rows (ids 1-4), \
         got {}",
        after - before
    );
}

// ── Check 5: capabilities are truthful ───────────────────────────────────────

/// **Check 5.** Assert a sink's advertised capabilities match real behaviour:
/// - `Append` (always supported) actually adds rows;
/// - a declared idempotent/keyed mechanism actually dedups (reuses check 4);
/// - `supports_schema_evolution()` implies `evolve_schema` is callable (not the
///   default "unsupported" error);
/// - the honest-false branch: a non-idempotent sink's `write_batch_idempotent`
///   delegates to `write_batch` and records no token.
///
/// `distinct_count` reports the destination's current distinct-row count.
pub async fn assert_capabilities_truthful<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let label = sink.connector_name();

    // Every sink must accept Append.
    assert!(
        sink.supported_write_modes()
            .contains(&faucet_core::write_mode::WriteMode::Append),
        "[{label}] does not advertise Append — every sink must support append"
    );

    if sink.supports_idempotent_writes() || sink.dedups_by_key() {
        // The advertised idempotency mechanism must actually work.
        assert_idempotent_replay(sink, &distinct_count).await;
    } else {
        // Honest-false: the default idempotent path must delegate, not pretend.
        let before = distinct_count().await;
        sink.write_batch(&rows(&[100]))
            .await
            .unwrap_or_else(|e| panic!("[{label}] write_batch (append probe) errored: {e}"));
        assert_eq!(
            distinct_count().await - before,
            1,
            "[{label}] Append is advertised but write_batch did not add a row"
        );
        assert_eq!(
            sink.last_committed_token("conformance::honest")
                .await
                .unwrap_or_else(|e| panic!("[{label}] last_committed_token errored: {e}")),
            None,
            "[{label}] is not idempotent yet reports a committed token"
        );
    }

    if sink.supports_schema_evolution() {
        // A no-op evolution must be accepted (idempotent, `ADD … IF NOT EXISTS`
        // semantics) — not the default "does not support" error.
        let empty = faucet_core::drift::SchemaEvolution::default();
        sink.evolve_schema(&empty).await.unwrap_or_else(|e| {
            panic!("[{label}] advertises schema evolution but evolve_schema(no-op) errored: {e}")
        });
    }
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// **Check 6.** Drive a source configured to fail (unreachable endpoint / bad
/// config) and assert it surfaces a typed [`faucet_core::FaucetError`] **without
/// unwinding**. Catches any panic and re-raises it as a check failure, so a
/// connector that `unwrap()`s on bad input is caught rather than crashing the
/// test process silently.
///
/// Pass a source that is *expected to fail*. Panics if the source succeeds (the
/// failure path was not exercised) or if it panics instead of returning `Err`.
pub async fn assert_errors_not_panics<S: Source + ?Sized>(source: &S) {
    use futures::FutureExt;
    let label = source.connector_name();

    // `fetch_all` path.
    let outcome = std::panic::AssertUnwindSafe(source.fetch_all())
        .catch_unwind()
        .await;
    match outcome {
        Err(_) => panic!("[{label}] panicked instead of returning Err from fetch_all"),
        Ok(Ok(_)) => panic!("[{label}] expected a failure but fetch_all succeeded"),
        Ok(Err(_e)) => { /* typed FaucetError, no unwind — good */ }
    }

    // `stream_pages` path — the first poll must also error (typed), not panic.
    let ctx: HashMap<String, Value> = HashMap::new();
    let stream_outcome = std::panic::AssertUnwindSafe(async {
        let mut s = source.stream_pages(&ctx, 100);
        s.next().await
    })
    .catch_unwind()
    .await;
    match stream_outcome {
        Err(_) => panic!("[{label}] panicked instead of returning Err from stream_pages"),
        Ok(Some(Err(_e))) => { /* typed FaucetError on first page — good */ }
        Ok(None) => panic!("[{label}] stream_pages yielded no pages (expected an error)"),
        Ok(Some(Ok(_))) => {
            panic!("[{label}] expected a failure but stream_pages produced a page")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use doubles::{
        CountingSource, FailingSource, LyingIdempotentSink, LyingKeyedSink, PanickingSource,
        TestSink,
    };

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

    // ── Check 3: bookmark round-trip ─────────────────────────────────────────

    #[tokio::test]
    async fn check3_passes_for_a_resumable_source() {
        let s = CountingSource::new(500, 100);
        assert_bookmark_roundtrip(&s).await;
    }

    #[tokio::test]
    #[should_panic(expected = "was ignored")]
    async fn check3_fails_when_source_ignores_the_bookmark() {
        let s = CountingSource::non_resumable(500, 100);
        assert_bookmark_roundtrip(&s).await;
    }

    // ── Check 4: idempotent replay ───────────────────────────────────────────

    #[tokio::test]
    async fn check4_passes_for_a_watermark_sink() {
        let sink = TestSink::idempotent("id");
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    async fn check4_passes_for_a_keyed_upsert_sink() {
        let sink = TestSink::keyed("id");
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not durably record its commit token")]
    async fn check4_fails_for_a_lying_idempotent_sink() {
        let sink = LyingIdempotentSink::new();
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not converge")]
    async fn check4_fails_for_a_lying_keyed_sink() {
        let sink = LyingKeyedSink::new();
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "no idempotency mechanism")]
    async fn check4_fails_for_an_append_only_sink() {
        let sink = TestSink::new();
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    // ── Check 5: capabilities truthful ───────────────────────────────────────

    #[tokio::test]
    async fn check5_passes_for_an_honest_append_sink() {
        let sink = TestSink::new();
        let s = sink.clone();
        assert_capabilities_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    async fn check5_passes_for_an_honest_idempotent_sink() {
        let sink = TestSink::idempotent("id");
        let s = sink.clone();
        assert_capabilities_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not durably record its commit token")]
    async fn check5_fails_for_a_lying_idempotent_sink() {
        let sink = LyingIdempotentSink::new();
        let s = sink.clone();
        assert_capabilities_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    // ── Check 6: errors, not panics ──────────────────────────────────────────

    #[tokio::test]
    async fn check6_passes_for_a_source_that_returns_err() {
        assert_errors_not_panics(&FailingSource).await;
    }

    #[tokio::test]
    #[should_panic(expected = "panicked instead of returning Err")]
    async fn check6_fails_for_a_source_that_panics() {
        assert_errors_not_panics(&PanickingSource).await;
    }

    #[tokio::test]
    #[should_panic(expected = "expected a failure but fetch_all succeeded")]
    async fn check6_fails_for_a_source_that_succeeds() {
        // A healthy source fed to the failure check must be flagged — the check
        // is only meaningful against a source expected to fail.
        assert_errors_not_panics(&CountingSource::new(3, 1)).await;
    }
}
