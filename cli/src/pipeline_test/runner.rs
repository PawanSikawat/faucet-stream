//! Offline pipeline execution for `faucet test`.
//!
//! Runs the deterministic slice of a pipeline — transforms → quality →
//! contract — through the *real* `faucet_core::Pipeline` streaming loop, with
//! the configured source and sink replaced by in-memory fixtures/captures.
//! Because the genuine per-page code path runs (including DLQ routing and
//! abort semantics), what a test observes is exactly what production would do
//! for the same records.

use crate::config::TransformSpec;
use crate::error::{CliError, CliResult};
use crate::transforms::compile_transforms;
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use faucet_core::observability::Labels;
use faucet_core::{DlqConfig, FaucetError, Pipeline, Sink, Source, StreamPage};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// A test case with its pipeline logic and fixtures fully resolved — the
/// runner's only input, shared by the config-file and inline paths.
pub struct ResolvedCase {
    /// Case name (used as the observability row label).
    pub name: String,
    /// Transform chain to apply (already layered for config-file cases).
    pub transforms: Vec<TransformSpec>,
    /// Quality checks to enforce per page.
    #[cfg(feature = "quality")]
    pub quality: Option<faucet_core::QualitySpec>,
    /// Data contract to enforce per page.
    #[cfg(feature = "contract")]
    pub contract: Option<faucet_core::ContractSpec>,
    /// Fixture records fed to the pipeline.
    pub input: Vec<Value>,
    /// Page size for the fixture source (`0` = single page).
    pub page_size: usize,
    /// `${now.*}` clock applied to transform configs.
    pub clock: DateTime<FixedOffset>,
}

/// Everything a run produced, for the expectation pass.
#[derive(Debug)]
pub struct CaseRun {
    /// Records the (capturing) sink received, in write order.
    pub written: Vec<Value>,
    /// Original payloads of DLQ envelopes, in routing order.
    pub dlq_payloads: Vec<Value>,
    /// Count reported by the pipeline (equals `written.len()`).
    pub records_written: usize,
    /// The run's error, when it failed (e.g. quality `abort`, contract
    /// `on_breach: fail`, a transform error).
    pub error: Option<String>,
}

/// Execute one resolved case fully offline. Only *setup* problems (an invalid
/// transform config) surface as `Err`; a failing pipeline run is a legitimate,
/// assertable outcome and lands in `CaseRun::error`.
pub async fn run_case(case: &ResolvedCase) -> CliResult<CaseRun> {
    // Resolve `${now.*}` in transform configs against the case clock — the
    // same pre-pass `faucet run` applies (crate::executor) — so a `set`
    // transform stamping `${now.date}` is deterministic under `clock:`.
    let stages = if case.transforms.is_empty() {
        Vec::new()
    } else {
        let mut transforms = case.transforms.clone();
        for t in &mut transforms {
            crate::executor::resolve_now_inplace(&mut t.config, case.clock)?;
        }
        compile_transforms(&transforms)?
    };

    let labels = Labels::new(
        "faucet-test",
        case.name.clone(),
        uuid::Uuid::now_v7().to_string(),
    );
    let source: Box<dyn Source> = Box::new(FixtureSource {
        records: case.input.clone(),
        page_size: case.page_size,
    });
    let source: Box<dyn Source> = if stages.is_empty() {
        source
    } else {
        Box::new(faucet_core::TransformingSource::new(
            source,
            stages,
            labels.clone(),
        )?)
    };

    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = CollectingSink {
        buffer: Arc::clone(&written),
        payload_key: None,
    };
    // The DLQ capture unwraps each envelope down to its original payload so
    // expectations compare records, not timestamps/messages.
    let dlq_payloads = Arc::new(Mutex::new(Vec::new()));
    let dlq_sink = CollectingSink {
        buffer: Arc::clone(&dlq_payloads),
        payload_key: Some("payload"),
    };

    let pipeline = Pipeline::new(source.as_ref(), &sink)
        .with_name("faucet-test")
        .with_row(case.name.clone())
        // Always attach a capturing DLQ so quality/contract `quarantine`
        // policies are testable without a `dlq:` block in the config.
        .with_dlq(DlqConfig::new(Arc::new(dlq_sink)));
    #[cfg(feature = "quality")]
    let pipeline = match &case.quality {
        Some(spec) => {
            let compiled = faucet_core::CompiledQuality::compile(spec)
                .map_err(|e| CliError::Config(format!("test '{}': quality: {e}", case.name)))?;
            pipeline.with_quality(Arc::new(compiled))
        }
        None => pipeline,
    };
    #[cfg(feature = "contract")]
    let pipeline = match &case.contract {
        Some(spec) => {
            let compiled = faucet_core::CompiledContract::compile(spec)
                .map_err(|e| CliError::Config(format!("test '{}': contract: {e}", case.name)))?;
            pipeline.with_contract(Arc::new(compiled))
        }
        None => pipeline,
    };

    let (records_written, error) = match pipeline.run().await {
        Ok(result) => (result.records_written, None),
        Err(e) => (0, Some(e.to_string())),
    };

    // Take the buffers (the pipeline is done; the Arcs are only held here).
    let written = std::mem::take(&mut *written.lock().expect("buffer lock"));
    let dlq_payloads = std::mem::take(&mut *dlq_payloads.lock().expect("dlq lock"));
    let records_written = if error.is_none() {
        records_written
    } else {
        // On failure the pipeline reports no count; what the sink actually
        // received before the abort is still the honest number.
        written.len()
    };
    Ok(CaseRun {
        written,
        dlq_payloads,
        records_written,
        error,
    })
}

/// In-memory source that streams fixture records, chunked by the case's own
/// `page_size` (the pipeline's `batch_size` hint is ignored so a test's page
/// boundaries are exactly what the spec declares).
struct FixtureSource {
    records: Vec<Value>,
    page_size: usize,
}

#[async_trait]
impl Source for FixtureSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.records.clone())
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn faucet_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let chunk = if self.page_size == 0 {
            usize::MAX
        } else {
            self.page_size
        };
        Box::pin(faucet_core::async_stream::try_stream! {
            let mut iter = self.records.iter().cloned();
            loop {
                let page: Vec<Value> = iter.by_ref().take(chunk).collect();
                if page.is_empty() {
                    break;
                }
                yield StreamPage { records: page, bookmark: None };
            }
        })
    }

    fn connector_name(&self) -> &'static str {
        "fixture"
    }
}

/// In-memory sink that appends every record to a shared buffer. With
/// `payload_key` set, it stores only that field of each record — used to
/// unwrap DLQ envelopes down to the quarantined payload.
struct CollectingSink {
    buffer: Arc<Mutex<Vec<Value>>>,
    payload_key: Option<&'static str>,
}

#[async_trait]
impl Sink for CollectingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut buf = self.buffer.lock().expect("buffer lock");
        for r in records {
            match self.payload_key {
                Some(key) => buf.push(r.get(key).cloned().unwrap_or_else(|| r.clone())),
                None => buf.push(r.clone()),
            }
        }
        Ok(records.len())
    }

    fn connector_name(&self) -> &'static str {
        "capture"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn case(input: Vec<Value>) -> ResolvedCase {
        ResolvedCase {
            name: "t".into(),
            transforms: Vec::new(),
            #[cfg(feature = "quality")]
            quality: None,
            #[cfg(feature = "contract")]
            contract: None,
            input,
            page_size: 0,
            clock: chrono::Utc::now().fixed_offset(),
        }
    }

    #[tokio::test]
    async fn passthrough_captures_all_records() {
        let run = run_case(&case(vec![json!({"a": 1}), json!({"a": 2})]))
            .await
            .unwrap();
        assert_eq!(run.records_written, 2);
        assert_eq!(run.written, vec![json!({"a": 1}), json!({"a": 2})]);
        assert!(run.dlq_payloads.is_empty());
        assert!(run.error.is_none());
    }

    #[tokio::test]
    async fn empty_input_yields_empty_run() {
        let run = run_case(&case(vec![])).await.unwrap();
        assert_eq!(run.records_written, 0);
        assert!(run.written.is_empty());
    }

    #[tokio::test]
    async fn transforms_apply_in_order() {
        let mut c = case(vec![json!({"user": {"name": "Ada"}})]);
        c.transforms = vec![
            TransformSpec {
                kind: "flatten".into(),
                config: json!({"separator": "_"}),
            },
            TransformSpec {
                kind: "set".into(),
                config: json!({"values": {"src": "fixture"}}),
            },
        ];
        let run = run_case(&c).await.unwrap();
        assert_eq!(
            run.written,
            vec![json!({"user_name": "Ada", "src": "fixture"})]
        );
    }

    #[tokio::test]
    async fn now_tokens_resolve_against_case_clock() {
        let mut c = case(vec![json!({"a": 1})]);
        c.clock = chrono::DateTime::parse_from_rfc3339("2026-01-31T00:00:00Z").unwrap();
        c.transforms = vec![TransformSpec {
            kind: "set".into(),
            config: json!({"values": {"day": "${now.date}"}}),
        }];
        let run = run_case(&c).await.unwrap();
        assert_eq!(run.written, vec![json!({"a": 1, "day": "2026-01-31"})]);
    }

    #[tokio::test]
    async fn invalid_transform_is_a_setup_error() {
        let mut c = case(vec![json!({"a": 1})]);
        c.transforms = vec![TransformSpec {
            kind: "rename_keys".into(),
            config: json!({"pattern": "(", "replacement": ""}),
        }];
        assert!(run_case(&c).await.is_err());
    }

    #[tokio::test]
    async fn page_size_chunks_fixture_pages() {
        // A page-granular batch quality check observes the page boundary:
        // with page_size 2 and a 3-record fixture, a `row_count` min of 2
        // aborts on the second (1-record) page.
        let mut c = case(vec![json!({"a": 1}), json!({"a": 2}), json!({"a": 3})]);
        c.page_size = 2;
        #[cfg(feature = "quality")]
        {
            c.quality = Some(
                serde_json::from_value(json!({
                    "batch": [ { "type": "row_count", "min": 2, "on_failure": "abort" } ]
                }))
                .unwrap(),
            );
            let run = run_case(&c).await.unwrap();
            assert!(
                run.error.is_some(),
                "expected quality abort on the short page"
            );
            // The first (full) page was written before the abort.
            assert_eq!(run.written.len(), 2);
            assert_eq!(run.records_written, 2);
        }
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn quality_quarantine_routes_to_dlq_capture() {
        let mut c = case(vec![json!({"id": 1}), json!({"id": null})]);
        c.quality = Some(
            serde_json::from_value(json!({
                "record": [ { "type": "not_null", "field": "id", "on_failure": "quarantine" } ]
            }))
            .unwrap(),
        );
        let run = run_case(&c).await.unwrap();
        assert!(run.error.is_none());
        assert_eq!(run.written, vec![json!({"id": 1})]);
        assert_eq!(run.dlq_payloads, vec![json!({"id": null})]);
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn invalid_quality_spec_is_a_setup_error() {
        let mut c = case(vec![json!({"a": 1})]);
        c.quality = Some(
            serde_json::from_value(json!({
                "record": [ { "type": "regex_match", "field": "a", "pattern": "(", "on_failure": "abort" } ]
            }))
            .unwrap(),
        );
        assert!(run_case(&c).await.is_err());
    }

    #[cfg(feature = "contract")]
    #[tokio::test]
    async fn contract_fail_aborts_and_quarantine_routes() {
        let contract = |on_breach: &str| -> faucet_core::ContractSpec {
            serde_json::from_value(json!({
                "version": "1.0.0",
                "on_breach": on_breach,
                "fields": [ { "name": "id", "type": "integer", "required": true } ]
            }))
            .unwrap()
        };
        // fail → run error, nothing written from the breaching page.
        let mut c = case(vec![json!({"id": "not-an-int"})]);
        c.contract = Some(contract("fail"));
        let run = run_case(&c).await.unwrap();
        assert!(
            run.error
                .as_deref()
                .unwrap_or_default()
                .contains("Contract v1.0.0 violated"),
            "unexpected error: {:?}",
            run.error
        );
        assert!(run.written.is_empty());
        assert_eq!(run.records_written, 0);

        // quarantine → breaching record lands in the DLQ capture.
        let mut c = case(vec![json!({"id": 7}), json!({"id": "bad"})]);
        c.contract = Some(contract("quarantine"));
        let run = run_case(&c).await.unwrap();
        assert!(run.error.is_none());
        assert_eq!(run.written, vec![json!({"id": 7})]);
        assert_eq!(run.dlq_payloads, vec![json!({"id": "bad"})]);
    }
}
