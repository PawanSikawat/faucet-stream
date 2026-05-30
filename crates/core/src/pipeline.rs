//! Source-to-sink pipeline orchestration.
//!
//! The [`Pipeline`] struct connects any [`Source`] to any
//! [`Sink`] and handles moving data between them.
//!
//! # Batch mode
//!
//! Fetches all records from the source, then writes them to the sink in one
//! shot.  Supports incremental replication (returns a bookmark for the next
//! run).
//!
//! ```rust,no_run
//! use faucet_core::{Pipeline, Source, Sink};
//! # async fn example(source: impl Source, sink: impl Sink) -> Result<(), faucet_core::FaucetError> {
//! let result = Pipeline::new(&source, &sink).run().await?;
//! println!("wrote {} records", result.records_written);
//! // Persist result.bookmark for the next incremental run
//! # Ok(())
//! # }
//! ```
//!
//! # Streaming mode
//!
//! Writes records page-by-page as they arrive from a source's
//! [`stream_pages`](crate::Source::stream_pages) implementation, keeping
//! memory usage bounded.  [`Pipeline::run`] uses this internally; callers
//! that have already assembled a [`StreamPage`] stream can drive it directly
//! via [`run_stream`].
//!
//! ```rust,no_run
//! use faucet_core::{run_stream, RunStreamOptions, Sink, StreamPage, FaucetError};
//! use futures_core::Stream;
//! # async fn example(
//! #     pages: impl Stream<Item = Result<StreamPage, FaucetError>> + Unpin,
//! #     sink: impl Sink,
//! # ) -> Result<(), FaucetError> {
//! let result = run_stream(pages, &sink, RunStreamOptions::new()).await?;
//! # Ok(())
//! # }
//! ```

use crate::dlq::{DlqConfig, DlqStats};
use crate::error::FaucetError;
use crate::observability::RunStreamOptions;
use crate::state::{StateStore, validate_state_key};
use crate::traits::{Sink, Source};
use futures_core::Stream;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;

/// Default page size used when a caller does not specify one.
///
/// Sources are free to override this from their own config when implementing
/// [`Source::stream_pages`]; the value passed
/// from the pipeline acts as a hint when no source-side preference exists.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Hard upper bound on `batch_size`. Values above this (other than the
/// special `0` "no batching" sentinel) are rejected at config validation
/// time to prevent accidental O(total) buffering in the default
/// implementation of [`Source::stream_pages`].
pub const MAX_BATCH_SIZE: usize = 1_000_000;

/// Validate a `batch_size` value against the global constraints.
///
/// `batch_size = 0` is the **opt-out-of-batching sentinel**: sources and
/// sinks should treat it as "emit / accept the entire result set in one
/// page." This is useful for small lookup tables or for sinks (e.g. SQL
/// `COPY`, BigQuery load jobs) that prefer one large request to many small
/// ones. Any non-zero value above [`MAX_BATCH_SIZE`] is rejected to prevent
/// accidental unbounded buffering through a typo.
///
/// Returns the unchanged value on success. Returns `FaucetError::Config`
/// only for values strictly greater than [`MAX_BATCH_SIZE`].
pub fn validate_batch_size(batch_size: usize) -> Result<usize, FaucetError> {
    if batch_size > MAX_BATCH_SIZE {
        return Err(FaucetError::Config(format!(
            "batch_size {batch_size} exceeds maximum {MAX_BATCH_SIZE} \
             (use 0 to opt out of batching entirely)"
        )));
    }
    Ok(batch_size)
}

/// One page emitted by [`Source::stream_pages`].
///
/// `records` is the chunk of records for this page. `bookmark` is `Some` only
/// when the source has a durable checkpoint to advance — most sources emit
/// `Some` only on the final page (max-replication-value semantics); CDC-style
/// sources emit `Some` per committed transaction. The pipeline flushes the
/// sink and persists the bookmark every time a page carries one, so a
/// mid-stream crash never advances past records the sink has not durably
/// written.
#[derive(Debug, Clone, Default)]
pub struct StreamPage {
    /// Records to write to the sink for this page.
    pub records: Vec<Value>,
    /// Optional bookmark to checkpoint after this page is durably written.
    pub bookmark: Option<Value>,
}

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    /// Total number of records written to the sink.
    pub records_written: usize,
    /// Bookmark value for incremental replication.
    ///
    /// `Some(value)` when the source returned a bookmark on its final
    /// (or, for streaming CDC sources, most recent) page. Persist this and
    /// pass it back as `start_replication_value` on the next run; this is
    /// handled automatically when a [`StateStore`] is attached via
    /// [`Pipeline::with_state_store`].
    pub bookmark: Option<Value>,
    /// DLQ counters. `None` when no DLQ is configured.
    pub dlq: Option<DlqStats>,
}

/// A pipeline that moves data from a [`Source`] to a [`Sink`].
///
/// The pipeline is generic over the source and sink types — any combination
/// of connectors works as long as they implement the respective traits.
pub struct Pipeline<'a, So: Source + ?Sized, Si: Sink + ?Sized> {
    source: &'a So,
    sink: &'a Si,
    state_store: Option<Arc<dyn StateStore>>,
    name: Option<String>,
    row: Option<String>,
    run_id: Option<String>,
    dlq: Option<DlqConfig>,
    #[cfg(feature = "quality")]
    quality: Option<Arc<crate::quality::CompiledQuality>>,
    adaptive: Option<crate::adaptive::AdaptiveBatchConfig>,
}

impl<'a, So: Source + ?Sized, Si: Sink + ?Sized> Pipeline<'a, So, Si> {
    /// Create a new pipeline from a source and a sink.
    pub fn new(source: &'a So, sink: &'a Si) -> Self {
        Self {
            source,
            sink,
            state_store: None,
            name: None,
            row: None,
            run_id: None,
            dlq: None,
            #[cfg(feature = "quality")]
            quality: None,
            adaptive: None,
        }
    }

    /// Attach a [`StateStore`] for persistent incremental-replication bookmarks.
    ///
    /// When configured, `run()` will:
    /// 1. Read any previously stored bookmark at the source's
    ///    [`state_key`](Source::state_key) and call
    ///    [`apply_start_bookmark`](Source::apply_start_bookmark) on the source
    ///    so it can resume from that point.
    /// 2. Run the fetch + write as usual.
    /// 3. Persist the new bookmark **only after** the sink confirms the
    ///    batch was written and flushed.
    ///
    /// Sources that do not return a [`state_key`](Source::state_key) are
    /// unaffected — the store is consulted only when the source opts in.
    pub fn with_state_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Set the pipeline name used in spans and metric labels.
    /// Defaults to `"unnamed"` when unset.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the matrix row id used in spans and metric labels.
    /// Defaults to `""` (Prometheus treats empty labels as absent).
    pub fn with_row(mut self, row: impl Into<String>) -> Self {
        self.row = Some(row.into());
        self
    }

    /// Set an explicit run id (UUIDv7-shaped). When unset, `Pipeline::run`
    /// generates one. Used only as a tracing span attribute — never a metric
    /// label.
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Attach a DLQ for per-row failure routing.
    pub fn with_dlq(mut self, dlq: DlqConfig) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Attach a compiled quality spec. Checks run after transforms, before the
    /// sink, per page.
    #[cfg(feature = "quality")]
    pub fn with_quality(mut self, quality: Arc<crate::quality::CompiledQuality>) -> Self {
        self.quality = Some(quality);
        self
    }

    /// Attach an adaptive batch-size controller (opt-in). When `enabled`, the
    /// pipeline reslices each source page into sub-batches whose size the
    /// controller tunes from observed sink latency + error rate.
    pub fn with_adaptive(mut self, cfg: crate::adaptive::AdaptiveBatchConfig) -> Self {
        self.adaptive = Some(cfg);
        self
    }

    /// Run the pipeline in streaming mode.
    ///
    /// 1. Loads the stored bookmark and pushes it to the source (if a state
    ///    store is configured and the source returns a `state_key`).
    /// 2. Drives [`Source::stream_pages`] with [`DEFAULT_BATCH_SIZE`],
    ///    writing each page to the sink as it arrives via
    ///    [`Sink::write_batch`].
    /// 3. Whenever a page carries `Some(bookmark)`, flushes the sink and
    ///    persists the bookmark to the state store before polling the next
    ///    page. This makes per-page CDC checkpointing automatic.
    /// 4. Flushes the sink one final time after the stream completes.
    /// 5. Returns a [`PipelineResult`] with the total count and the last
    ///    bookmark observed.
    pub async fn run(&self) -> Result<PipelineResult, FaucetError> {
        use crate::observability::{
            DurationGuard, InstrumentedSink, InstrumentedSource, InstrumentedStateStore, Labels,
        };
        use metrics::{Label, SharedString, counter, gauge};
        use tracing::Instrument;

        // Resolve identity for this run.
        let name = self.name.clone().unwrap_or_else(|| "unnamed".to_string());
        let row = self.row.clone().unwrap_or_default();
        let run_id = self
            .run_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let obs_labels = Labels::new(name.clone(), row.clone(), run_id.clone());

        // Wrap source, sink, state-store.
        let wrapped_source = InstrumentedSource::new(self.source, obs_labels.clone());
        let wrapped_sink = InstrumentedSink::new(self.sink, obs_labels.clone());
        let wrapped_state_store: Option<Arc<dyn StateStore>> = self.state_store.as_ref().map(|s| {
            Arc::new(InstrumentedStateStore::new(
                Arc::clone(s),
                obs_labels.clone(),
            )) as Arc<dyn StateStore>
        });

        // Pipeline-level span. Use .instrument(span) on the inner future so
        // the span correctly enters/exits across awaits.
        let span = tracing::info_span!(
            "faucet.pipeline.run",
            pipeline = %name,
            row = %row,
            run_id = %run_id,
            source = %wrapped_source.connector_name(),
            sink = %wrapped_sink.connector_name(),
        );

        // Per-pipeline metric labels (pipeline + row).
        let base_labels: Vec<Label> = vec![
            Label::new("pipeline", SharedString::from(name.clone())),
            Label::new("row", SharedString::from(row.clone())),
        ];
        let run_labels: Vec<Label> = {
            let mut v = base_labels.clone();
            v.push(Label::new(
                "source",
                SharedString::from(wrapped_source.connector_name().to_string()),
            ));
            v.push(Label::new(
                "sink",
                SharedString::from(wrapped_sink.connector_name().to_string()),
            ));
            v
        };

        // RAII guard so the in-flight gauge stays consistent even on cancellation.
        struct InFlightGuard(Vec<Label>);
        impl Drop for InFlightGuard {
            fn drop(&mut self) {
                gauge!("faucet_pipeline_in_flight", self.0.clone()).decrement(1.0);
            }
        }
        gauge!("faucet_pipeline_in_flight", base_labels.clone()).increment(1.0);
        let _in_flight = InFlightGuard(base_labels.clone());

        // Stamp the start time so dashboards can compute uptime for long-running
        // (streaming / CDC) pipelines where `*_run_duration_seconds` never fires.
        let start_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        gauge!(
            "faucet_pipeline_start_time_unix_seconds",
            base_labels.clone()
        )
        .set(start_unix);

        // Histogram timer for the whole run.
        let _run_timer =
            DurationGuard::new("faucet_pipeline_run_duration_seconds", run_labels.clone());

        // Run inside the span.
        let result = async {
            // Bookmark resume — goes through the wrapped state store so the
            // get is instrumented too.
            let state_key = self.source.state_key();
            if let (Some(store), Some(key)) = (wrapped_state_store.as_ref(), state_key.as_ref()) {
                validate_state_key(key)?;
                if let Some(prior) = store.get(key).await? {
                    wrapped_source.apply_start_bookmark(prior).await?;
                }
            }

            let ctx = std::collections::HashMap::new();
            let pages = wrapped_source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

            let mut opts = RunStreamOptions::new()
                .with_name(name.clone())
                .with_row(row.clone())
                .with_run_id(run_id.clone());
            if let (Some(store), Some(key)) = (wrapped_state_store.clone(), state_key) {
                opts = opts.with_state(store, key);
            }
            if let Some(dlq) = self.dlq.clone() {
                opts = opts.with_dlq(dlq);
            }
            #[cfg(feature = "quality")]
            if let Some(q) = self.quality.clone() {
                opts = opts.with_quality(q);
            }
            if let Some(ad) = self.adaptive.clone() {
                opts = opts.with_adaptive(ad);
            }

            run_stream(pages, &wrapped_sink, opts).await
        }
        .instrument(span)
        .await;

        // Final run-counter increment. On error, also attach a `kind` label
        // (matching the FaucetError variant) so dashboards can break out failed
        // runs by error type without spelunking the *_errors_total surfaces.
        let status = if result.is_ok() { "ok" } else { "err" };
        let mut final_labels = run_labels;
        final_labels.push(Label::new("status", SharedString::const_str(status)));
        if let Err(ref e) = result {
            final_labels.push(Label::new(
                "kind",
                SharedString::const_str(crate::observability::decorator::error_kind(e)),
            ));
        }
        counter!("faucet_pipeline_runs_total", final_labels).increment(1);

        result
    }
}

/// Run a streaming pipeline, writing each [`StreamPage`] to the sink as it
/// arrives and persisting bookmarks per page.
///
/// This keeps memory usage bounded — only one page of records is held at a
/// time. The stream comes from [`Source::stream_pages`] (or any
/// `Stream<Item = Result<StreamPage, FaucetError>>` a caller assembles
/// directly).
///
/// Bookmark semantics: whenever a page carries `Some(bookmark)`, the sink is
/// flushed and the bookmark is persisted (when `state_store` and `state_key`
/// are both `Some`) before the next page is polled. Sources that only know
/// their bookmark after seeing every record emit `Some` on the final page;
/// CDC-style sources emit `Some` per committed transaction and get
/// per-transaction durability automatically.
///
/// Returns the cumulative [`PipelineResult`] — `records_written` is the sum
/// across all pages and `bookmark` is the last per-page bookmark observed.
pub async fn run_stream<S, Si>(
    mut pages: S,
    sink: &Si,
    options: RunStreamOptions,
) -> Result<PipelineResult, FaucetError>
where
    S: Stream<Item = Result<StreamPage, FaucetError>> + Unpin,
    Si: Sink + ?Sized,
{
    use crate::dlq::{DlqStats, OnBatchError, build_envelope};

    let state_store = options.state_store.clone();
    let state_key = options.state_key.clone();
    let pipeline_name = options.pipeline_name.unwrap_or_else(|| "unnamed".into());
    let row = options.row.unwrap_or_default();
    let run_id = options.run_id.unwrap_or_default();
    let dlq = options.dlq.clone();

    #[cfg(feature = "quality")]
    let quality = options.quality.clone();

    // Fail fast: quarantine requires a DLQ sink.
    #[cfg(feature = "quality")]
    if let Some(q) = quality.as_ref()
        && q.requires_dlq()
        && dlq.is_none()
    {
        return Err(FaucetError::Config(
            "quality: on_failure 'quarantine'/'quarantine_batch' requires a DLQ sink".into(),
        ));
    }

    if let Some(key) = state_key.as_ref() {
        validate_state_key(key)?;
    }

    let mut records_written = 0usize;
    let mut last_bookmark: Option<Value> = None;
    let mut dlq_stats = DlqStats::default();

    let adaptive_cfg = options.adaptive.clone().filter(|c| c.enabled);
    let mut controller: Option<crate::adaptive::AimdController> = None;
    let mut warned_noop_sink = false;

    let sink_name = sink.connector_name();
    let dlq_sink_name = dlq.as_ref().map(|d| d.sink.connector_name()).unwrap_or("");

    // Drive the streaming loop inside an inner future so that EVERY early exit
    // (a source error, a `?`-propagated write/flush/state failure, or a DLQ
    // budget overflow) funnels through one place. On any error we best-effort
    // flush the sinks before propagating, so a buffered sink that only commits
    // on flush — Parquet writes its footer there; without it the whole file is
    // unreadable — does not lose everything written so far (#78/#3).
    let loop_result: Result<(), FaucetError> = async {
        loop {
            let page = std::future::poll_fn(|cx| Pin::new(&mut pages).poll_next(cx)).await;
            match page {
                Some(Ok(page)) => {
                    if page.records.is_empty() && page.bookmark.is_none() {
                        continue;
                    }

                    // ── Quality pass (after transforms, before sink) ─────────
                    #[cfg(feature = "quality")]
                    let (records, quality_envelopes): (Vec<Value>, Vec<Value>) = if let Some(q) =
                        quality.as_ref()
                    {
                        let labels =
                            crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                        let outcome = crate::observability::instrumented_apply_quality(
                            page.records,
                            q,
                            &labels,
                        )?;
                        let envelopes: Vec<Value> = outcome
                            .quarantined
                            .iter()
                            .enumerate()
                            .map(|(i, qr)| {
                                let err = FaucetError::QualityFailure {
                                    check: qr.check.to_string(),
                                    message: qr.message.clone(),
                                };
                                build_envelope(&qr.record, &err, sink_name, &pipeline_name, &row, i)
                            })
                            .collect();
                        (outcome.survivors, envelopes)
                    } else {
                        (page.records, Vec::new())
                    };
                    #[cfg(not(feature = "quality"))]
                    let (records, quality_envelopes): (Vec<Value>, Vec<Value>) =
                        (page.records, Vec::new());

                    let page = StreamPage {
                        records,
                        bookmark: page.bookmark,
                    };

                    if let Some(ref dlq_cfg) = dlq {
                        // ── DLQ-enabled path ───────────────────────────────────
                        use crate::dlq::DlqReason;
                        use metrics::{Label, SharedString, counter};
                        let metric_labels: Vec<Label> = vec![
                            Label::new("pipeline", SharedString::from(pipeline_name.clone())),
                            Label::new("row", SharedString::from(row.clone())),
                            Label::new("connector", SharedString::from(sink_name.to_string())),
                            Label::new(
                                "dlq_connector",
                                SharedString::from(dlq_sink_name.to_string()),
                            ),
                        ];
                        let span = tracing::info_span!(
                            "faucet.dlq.route",
                            pipeline = %pipeline_name,
                            row = %row,
                            run_id = %run_id,
                            connector = %sink_name,
                            dlq_connector = %dlq_sink_name,
                        );
                        let _enter = span.enter();

                        let outcomes_result = if page.records.is_empty() {
                            Ok(Vec::new())
                        } else {
                            sink.write_batch_partial(&page.records).await
                        };
                        let mut outer_err_recovered = false;
                        let outcomes = match outcomes_result {
                            Ok(o) => o,
                            Err(e) => match dlq_cfg.on_batch_error {
                                OnBatchError::Propagate => return Err(e),
                                OnBatchError::DlqAll => {
                                    // Synthesize per-row failures echoing the
                                    // underlying message so envelopes carry it.
                                    // The flag distinguishes this from a sink
                                    // override that legitimately returned all
                                    // per-row Errs — that case keeps the
                                    // `partial` reason label.
                                    outer_err_recovered = true;
                                    let msg = e.to_string();
                                    (0..page.records.len())
                                        .map(|_| Err(FaucetError::Sink(msg.clone())))
                                        .collect()
                                }
                            },
                        };

                        // Partition into success count + failure envelopes.
                        let mut envelopes: Vec<Value> = Vec::new();
                        let mut page_success = 0usize;
                        for (i, outcome) in outcomes.iter().enumerate() {
                            match outcome {
                                Ok(()) => page_success += 1,
                                Err(err) => envelopes.push(build_envelope(
                                    &page.records[i],
                                    err,
                                    sink_name,
                                    &pipeline_name,
                                    &row,
                                    i,
                                )),
                            }
                        }
                        // Quality-quarantined records share the DLQ budget/write.
                        // Capture the quality count BEFORE the splice — the splice
                        // moves `quality_envelopes`, so its length is unavailable
                        // afterward. Used below to pick the page `reason` label.
                        #[cfg(feature = "quality")]
                        let quality_count = quality_envelopes.len();
                        #[cfg(not(feature = "quality"))]
                        let quality_count = 0usize;
                        envelopes.splice(0..0, quality_envelopes);
                        let page_failures = envelopes.len();

                        // Budget checks — increment counter before returning.
                        if let Some(limit) = dlq_cfg.max_failures_per_page
                            && page_failures > limit
                        {
                            let mut lbl = metric_labels.clone();
                            lbl.retain(|l| l.key() != "dlq_connector");
                            lbl.push(Label::new("scope", SharedString::const_str("per_page")));
                            counter!("faucet_sink_dlq_budget_exceeded_total", lbl).increment(1);
                            return Err(FaucetError::Sink(format!(
                                "DLQ per-page budget exceeded: {page_failures} > {limit}"
                            )));
                        }
                        let new_total = dlq_stats.records_dlq + page_failures;
                        if let Some(limit) = dlq_cfg.max_failures_total
                            && new_total > limit
                        {
                            let mut lbl = metric_labels.clone();
                            lbl.retain(|l| l.key() != "dlq_connector");
                            lbl.push(Label::new("scope", SharedString::const_str("total")));
                            counter!("faucet_sink_dlq_budget_exceeded_total", lbl).increment(1);
                            return Err(FaucetError::Sink(format!(
                                "DLQ total budget exceeded: {new_total} > {limit}"
                            )));
                        }

                        // Write to DLQ sink. Errors here are fatal, no recursion.
                        if !envelopes.is_empty() {
                            let _dlq_write_timer = crate::observability::DurationGuard::new(
                                "faucet_sink_dlq_write_duration_seconds",
                                metric_labels.clone(),
                            );
                            dlq_cfg.sink.write_batch(&envelopes).await.map_err(|e| {
                                let mut lbl = metric_labels.clone();
                                lbl.push(Label::new(
                                    "kind",
                                    SharedString::const_str(
                                        crate::observability::decorator::error_kind(&e),
                                    ),
                                ));
                                counter!("faucet_sink_dlq_errors_total", lbl).increment(1);
                                FaucetError::Sink(format!("DLQ sink write failed: {e}"))
                            })?;
                            dlq_stats.records_dlq += page_failures;
                            dlq_stats.pages_with_failures += 1;

                            // Page `reason` label, 3-way:
                            //  - `dlq_all`  — the whole page was synthesized from a
                            //    single outer sink error (OnBatchError::DlqAll).
                            //  - `partial`  — at least one row failed on the SINK
                            //    side (`page_failures > quality_count`). A mixed
                            //    page carrying both sink failures and quality
                            //    quarantines is labeled `partial`: the sink failure
                            //    dominates.
                            //  - `quality`  — every envelope is quality-sourced
                            //    (no sink-side failures on this page).
                            // The per-row quality volume is separately exposed via
                            // `faucet_quality_records_quarantined_total`.
                            let reason_label = if outer_err_recovered {
                                DlqReason::DlqAll.as_str()
                            } else if page_failures > quality_count {
                                DlqReason::Partial.as_str()
                            } else {
                                DlqReason::Quality.as_str()
                            };
                            counter!("faucet_sink_dlq_records_total", metric_labels.clone())
                                .increment(page_failures as u64);
                            let mut page_labels = metric_labels.clone();
                            page_labels
                                .push(Label::new("reason", SharedString::const_str(reason_label)));
                            counter!("faucet_sink_dlq_pages_total", page_labels).increment(1);
                        }

                        records_written += page_success;

                        if let Some(bookmark) = page.bookmark {
                            sink.flush().await?;
                            let _dlq_flush_timer = crate::observability::DurationGuard::new(
                                "faucet_sink_dlq_flush_duration_seconds",
                                metric_labels.clone(),
                            );
                            dlq_cfg.sink.flush().await.map_err(|e| {
                                let mut lbl = metric_labels.clone();
                                lbl.push(Label::new(
                                    "kind",
                                    SharedString::const_str(
                                        crate::observability::decorator::error_kind(&e),
                                    ),
                                ));
                                counter!("faucet_sink_dlq_errors_total", lbl).increment(1);
                                FaucetError::Sink(format!("DLQ sink flush failed: {e}"))
                            })?;
                            let bm_labels =
                                crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                            crate::observability::update_bookmark_lag(&bookmark, &bm_labels);
                            if let (Some(store), Some(key)) =
                                (state_store.as_ref(), state_key.as_ref())
                            {
                                store.put(key, &bookmark).await?;
                            }
                            last_bookmark = Some(bookmark);
                        }
                    } else {
                        // ── DLQ-disabled path (today's behaviour) ──────────────
                        debug_assert!(
                            quality_envelopes.is_empty(),
                            "quality quarantine without DLQ should have been rejected at run start"
                        );
                        if !page.records.is_empty() {
                            if let Some(cfg) = adaptive_cfg.as_ref() {
                                let ctrl = controller.get_or_insert_with(|| {
                                    crate::adaptive::AimdController::new(cfg, page.records.len())
                                });
                                maybe_warn_noop_sink(sink_name, &mut warned_noop_sink);
                                let mut offset = 0;
                                while offset < page.records.len() {
                                    let size = ctrl.current().max(1).min(page.records.len() - offset);
                                    let chunk = &page.records[offset..offset + size];
                                    let t0 = std::time::Instant::now();
                                    let n = sink.write_batch(chunk).await?;
                                    let latency = t0.elapsed();
                                    records_written += n;
                                    offset += size;
                                    let _adj = ctrl.observe(crate::adaptive::Observation {
                                        batch_len: chunk.len(),
                                        errors: 0,
                                        latency,
                                    });
                                    // (metric emission added in a later task)
                                }
                            } else {
                                records_written += sink.write_batch(&page.records).await?;
                            }
                        }
                        if let Some(bookmark) = page.bookmark {
                            sink.flush().await?;
                            let bm_labels =
                                crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                            crate::observability::update_bookmark_lag(&bookmark, &bm_labels);
                            if let (Some(store), Some(key)) =
                                (state_store.as_ref(), state_key.as_ref())
                            {
                                store.put(key, &bookmark).await?;
                            }
                            last_bookmark = Some(bookmark);
                        }
                    }
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(())
    }
    .await;

    // Error/early-return unwind: best-effort flush so any buffered output is
    // made durable, then propagate the ORIGINAL error. Flush errors here are
    // logged and swallowed — the source/sink error that triggered the unwind
    // is the meaningful one to surface. DLQ is flushed first (mirroring the
    // success path below): its records are only ever written here, whereas the
    // next run re-reads post-bookmark records from the source.
    if let Err(e) = loop_result {
        if let Some(ref dlq_cfg) = dlq
            && let Err(flush_err) = dlq_cfg.sink.flush().await
        {
            tracing::warn!(
                error = %flush_err,
                "DLQ sink flush failed during error unwind; original error preserved"
            );
        }
        if let Err(flush_err) = sink.flush().await {
            tracing::warn!(
                error = %flush_err,
                "sink flush failed during error unwind; original error preserved"
            );
        }
        return Err(e);
    }

    // Flush the DLQ sink BEFORE the main sink so quarantined records are made
    // durable even if the main sink's final flush fails. The next run will
    // re-read post-bookmark records from the source and re-route any that
    // would have fallen out of the main sink's unflushed buffer; DLQ records,
    // by contrast, are only ever written here and would otherwise be lost.
    if let Some(ref dlq_cfg) = dlq {
        let final_metric_labels: Vec<metrics::Label> = vec![
            metrics::Label::new(
                "pipeline",
                metrics::SharedString::from(pipeline_name.clone()),
            ),
            metrics::Label::new("row", metrics::SharedString::from(row.clone())),
            metrics::Label::new(
                "connector",
                metrics::SharedString::from(sink_name.to_string()),
            ),
            metrics::Label::new(
                "dlq_connector",
                metrics::SharedString::from(dlq_sink_name.to_string()),
            ),
        ];
        let _final_dlq_flush_timer = crate::observability::DurationGuard::new(
            "faucet_sink_dlq_flush_duration_seconds",
            final_metric_labels.clone(),
        );
        dlq_cfg.sink.flush().await.map_err(|e| {
            let mut lbl = final_metric_labels.clone();
            lbl.push(metrics::Label::new(
                "kind",
                metrics::SharedString::const_str(crate::observability::decorator::error_kind(&e)),
            ));
            metrics::counter!("faucet_sink_dlq_errors_total", lbl).increment(1);
            FaucetError::Sink(format!("DLQ sink flush failed: {e}"))
        })?;
    }
    sink.flush().await?;

    tracing::info!(
        records_written,
        has_bookmark = last_bookmark.is_some(),
        persisted = state_store.is_some() && state_key.is_some() && last_bookmark.is_some(),
        dlq_records = dlq_stats.records_dlq,
        "pipeline streaming run complete"
    );

    Ok(PipelineResult {
        records_written,
        bookmark: last_bookmark,
        dlq: dlq.is_some().then_some(dlq_stats),
    })
}

/// One-shot info when adaptive sizing targets a per-record sink that ignores
/// `batch_size` (its adjustments are harmless no-ops).
fn maybe_warn_noop_sink(sink_name: &str, warned: &mut bool) {
    if !*warned && matches!(sink_name, "jsonl" | "csv" | "stdout") {
        tracing::info!(
            sink = sink_name,
            "adaptive batch sizing is a no-op for this per-record sink"
        );
        *warned = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    // ── Mock Source ──────────────────────────────────────────────────────────

    struct MockSource(Vec<Value>);

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    struct IncrementalSource {
        records: Vec<Value>,
        bookmark: Value,
    }

    #[async_trait]
    impl Source for IncrementalSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.bookmark.clone())))
        }
    }

    struct FailingSource;

    #[async_trait]
    impl Source for FailingSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Auth("no credentials".into()))
        }
    }

    // ── Mock Sink ───────────────────────────────────────────────────────────

    struct MockSink(std::sync::Mutex<Vec<Value>>);

    impl MockSink {
        fn new() -> Self {
            Self(std::sync::Mutex::new(Vec::new()))
        }
        fn written(&self) -> Vec<Value> {
            self.0.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
    }

    struct FailingSink;

    #[async_trait]
    impl Sink for FailingSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("write failed".into()))
        }
    }

    /// Records writes and how many times `flush` was called. Used to assert the
    /// pipeline flushes the sink on the error/early-return path so partial
    /// output (e.g. a Parquet footer) is made durable before the error
    /// propagates.
    struct FlushTrackingSink {
        written: std::sync::Mutex<Vec<Value>>,
        flush_count: std::sync::atomic::AtomicUsize,
    }

    impl FlushTrackingSink {
        fn new() -> Self {
            Self {
                written: std::sync::Mutex::new(Vec::new()),
                flush_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn written(&self) -> Vec<Value> {
            self.written.lock().unwrap().clone()
        }
        fn flush_count(&self) -> usize {
            self.flush_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Sink for FlushTrackingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.written.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            self.flush_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    // ── StreamPage / batch_size tests ───────────────────────────────────────

    #[test]
    fn stream_page_constructs() {
        let page = StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: Some(json!("2026-05-18")),
        };
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.bookmark, Some(json!("2026-05-18")));
    }

    #[test]
    fn validate_batch_size_accepts_zero_as_no_batching_sentinel() {
        // 0 means "do not batch — emit/accept the whole result set in one page".
        assert_eq!(validate_batch_size(0).unwrap(), 0);
    }

    #[test]
    fn validate_batch_size_rejects_too_large() {
        let err = validate_batch_size(MAX_BATCH_SIZE + 1).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn validate_batch_size_accepts_one() {
        assert_eq!(validate_batch_size(1).unwrap(), 1);
    }

    #[test]
    fn validate_batch_size_accepts_max() {
        assert_eq!(validate_batch_size(MAX_BATCH_SIZE).unwrap(), MAX_BATCH_SIZE);
    }

    // Compile-time invariant: DEFAULT_BATCH_SIZE must be within [1, MAX_BATCH_SIZE].
    const _: () = {
        assert!(DEFAULT_BATCH_SIZE >= 1);
        assert!(DEFAULT_BATCH_SIZE <= MAX_BATCH_SIZE);
    };

    // ── Batch mode tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn batch_pipeline_writes_all_records() {
        let source = MockSource(vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]);
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 3);
        assert!(result.bookmark.is_none());
        assert_eq!(sink.written().len(), 3);
    }

    #[tokio::test]
    async fn batch_pipeline_returns_bookmark() {
        let source = IncrementalSource {
            records: vec![json!({"id": 1, "ts": "2024-12-01"})],
            bookmark: json!("2024-12-01"),
        };
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 1);
        assert_eq!(result.bookmark, Some(json!("2024-12-01")));
    }

    #[tokio::test]
    async fn batch_pipeline_empty_source() {
        let source = MockSource(vec![]);
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 0);
        assert!(sink.written().is_empty());
    }

    #[tokio::test]
    async fn batch_pipeline_source_error_propagates() {
        let source = FailingSource;
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_err());
        assert!(sink.written().is_empty());
    }

    #[tokio::test]
    async fn batch_pipeline_sink_error_propagates() {
        let source = MockSource(vec![json!({"id": 1})]);
        let sink = FailingSink;

        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn batch_pipeline_with_trait_objects() {
        let source: Box<dyn Source> = Box::new(MockSource(vec![json!({"id": 1})]));
        let sink: Box<dyn Sink> = Box::new(MockSink::new());

        let result = Pipeline::new(source.as_ref(), sink.as_ref())
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 1);
    }

    // ── Streaming mode tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn stream_pipeline_writes_pages() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1}), json!({"id": 2})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 3})],
                bookmark: None,
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new())
            .await
            .unwrap();

        assert_eq!(result.records_written, 3);
        assert!(result.bookmark.is_none());
        assert_eq!(sink.written().len(), 3);
    }

    #[tokio::test]
    async fn stream_pipeline_flushes_sink_on_source_error() {
        // Regression for #78/#3: a mid-stream source error must not skip the
        // sink flush. Without flushing, a buffered sink (e.g. Parquet, whose
        // footer is only written on flush) loses everything written so far.
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1}), json!({"id": 2})],
                bookmark: None,
            }),
            Err(FaucetError::Source("transient blip mid-stream".into())),
        ];
        let stream = futures::stream::iter(pages);
        let sink = FlushTrackingSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new()).await;

        // The original source error must still propagate.
        assert!(matches!(result, Err(FaucetError::Source(_))));
        // The good page must have been written before the error.
        assert_eq!(sink.written().len(), 2);
        // Crucially, the sink must have been flushed on the error path.
        assert!(
            sink.flush_count() >= 1,
            "sink must be flushed on the error path so partial output is durable"
        );
    }

    #[tokio::test]
    async fn stream_pipeline_empty() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new())
            .await
            .unwrap();

        assert_eq!(result.records_written, 0);
    }

    #[tokio::test]
    async fn stream_pipeline_skips_empty_pages() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: None,
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new())
            .await
            .unwrap();

        assert_eq!(result.records_written, 2);
    }

    #[tokio::test]
    async fn stream_pipeline_error_in_page_propagates() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Err(FaucetError::HttpStatus {
                status: 500,
                url: "https://example.com".into(),
                body: "Internal Server Error".into(),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new()).await;
        assert!(result.is_err());
        // First page was written before the error
        assert_eq!(sink.written().len(), 1);
    }

    #[tokio::test]
    async fn stream_pipeline_sink_error_propagates() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let sink = FailingSink;

        let result = run_stream(stream, &sink, RunStreamOptions::new()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stream_pipeline_with_trait_object_sink() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let sink: Box<dyn Sink> = Box::new(MockSink::new());

        let result = run_stream(stream, sink.as_ref(), RunStreamOptions::new())
            .await
            .unwrap();
        assert_eq!(result.records_written, 1);
    }

    #[tokio::test]
    async fn stream_pipeline_persists_bookmark_when_page_carries_one() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("checkpoint-final")),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(
            stream,
            &sink,
            RunStreamOptions::new().with_state(Arc::clone(&store), "k"),
        )
        .await
        .unwrap();

        assert_eq!(result.records_written, 2);
        assert_eq!(result.bookmark, Some(json!("checkpoint-final")));
        assert_eq!(
            store.get("k").await.unwrap(),
            Some(json!("checkpoint-final"))
        );
    }

    #[tokio::test]
    async fn stream_pipeline_persists_per_page_bookmarks() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: Some(json!("tx-1")),
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("tx-2")),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        run_stream(
            stream,
            &sink,
            RunStreamOptions::new().with_state(Arc::clone(&store), "k"),
        )
        .await
        .unwrap();

        // Latest per-page bookmark wins.
        assert_eq!(store.get("k").await.unwrap(), Some(json!("tx-2")));
    }

    // ── State-store integration tests ───────────────────────────────────────

    use crate::state::{FileStateStore, MemoryStateStore, StateStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Source that opts into state persistence. It records the bookmark it
    /// received via `apply_start_bookmark` so tests can verify the pipeline
    /// pushed the stored value back into it on resume.
    struct StatefulSource {
        key: String,
        records: Vec<Value>,
        new_bookmark: Value,
        seen_bookmark: std::sync::Mutex<Option<Value>>,
    }

    impl StatefulSource {
        fn new(key: &str, records: Vec<Value>, new_bookmark: Value) -> Self {
            Self {
                key: key.into(),
                records,
                new_bookmark,
                seen_bookmark: std::sync::Mutex::new(None),
            }
        }
        fn observed_start(&self) -> Option<Value> {
            self.seen_bookmark.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Source for StatefulSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.new_bookmark.clone())))
        }
        fn state_key(&self) -> Option<String> {
            Some(self.key.clone())
        }
        async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
            *self.seen_bookmark.lock().unwrap() = Some(bookmark);
            Ok(())
        }
    }

    #[tokio::test]
    async fn pipeline_with_state_store_persists_bookmark_after_sink() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = StatefulSource::new(
            "github_issues",
            vec![json!({"id": 1, "ts": "2026-05-01"})],
            json!("2026-05-01"),
        );
        let sink = MockSink::new();
        let result = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 1);
        assert_eq!(result.bookmark, Some(json!("2026-05-01")));
        // Stored value matches what the source returned.
        let stored = store.get("github_issues").await.unwrap();
        assert_eq!(stored, Some(json!("2026-05-01")));
    }

    #[tokio::test]
    async fn pipeline_with_state_store_resumes_from_stored_bookmark() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        store
            .put("github_issues", &json!("2026-04-30"))
            .await
            .unwrap();

        let source =
            StatefulSource::new("github_issues", vec![json!({"id": 2})], json!("2026-05-01"));
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();

        // The pipeline pushed the previously-stored bookmark back into the source.
        assert_eq!(source.observed_start(), Some(json!("2026-04-30")));
        // And then overwrote it with the new value from this run.
        assert_eq!(
            store.get("github_issues").await.unwrap(),
            Some(json!("2026-05-01"))
        );
    }

    #[tokio::test]
    async fn pipeline_with_state_store_does_not_persist_when_sink_fails() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = StatefulSource::new("k", vec![json!({"id": 1})], json!("2026-05-01"));
        let sink = FailingSink;

        let result = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await;
        assert!(result.is_err());
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pipeline_with_state_store_no_state_key_means_no_persist() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = IncrementalSource {
            records: vec![json!({"id": 1})],
            bookmark: json!("ignored"),
        };
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        // IncrementalSource doesn't override state_key, so nothing was persisted.
        // Cross-check that no keys exist by trying a likely one.
        assert!(store.get("anything").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pipeline_with_state_store_skips_persist_when_bookmark_is_none() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        struct NoBookmarkSource;
        #[async_trait]
        impl Source for NoBookmarkSource {
            async fn fetch_with_context(
                &self,
                _ctx: &std::collections::HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(vec![json!({"id": 1})])
            }
            fn state_key(&self) -> Option<String> {
                Some("k".into())
            }
        }
        let source = NoBookmarkSource;
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert!(store.get("k").await.unwrap().is_none());
    }

    // ── Pipeline::run drives stream_pages ──────────────────────────────────

    /// A source with a custom `stream_pages` impl that yields three pages.
    /// Used to prove `Pipeline::run` drives the streaming path.
    struct PagedSource;

    #[async_trait]
    impl Source for PagedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            // Should never be called when stream_pages is overridden.
            unreachable!("Pipeline::run must drive stream_pages, not fetch_with_context");
        }
        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a std::collections::HashMap<String, Value>,
            _batch_size: usize,
        ) -> std::pin::Pin<
            Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>,
        > {
            Box::pin(async_stream::try_stream! {
                yield StreamPage { records: vec![json!({"i": 1})], bookmark: None };
                yield StreamPage { records: vec![json!({"i": 2})], bookmark: None };
                yield StreamPage { records: vec![json!({"i": 3})], bookmark: Some(json!("final")) };
            })
        }
    }

    /// Sink that counts how many distinct write_batch calls happen.
    struct CountingSink {
        calls: std::sync::Mutex<Vec<usize>>,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl Sink for CountingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.calls.lock().unwrap().push(records.len());
            Ok(records.len())
        }
    }

    #[tokio::test]
    async fn pipeline_run_drives_stream_pages() {
        let source = PagedSource;
        let sink = CountingSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        // Three pages of one record each → three sink calls, three records.
        assert_eq!(sink.call_count(), 3);
        assert_eq!(result.records_written, 3);
        assert_eq!(result.bookmark, Some(json!("final")));
    }

    #[tokio::test]
    async fn pipeline_with_file_state_store_round_trips_across_runs() {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path()));

        // Run 1: nothing stored yet, persist new bookmark.
        let s1 = StatefulSource::new("k", vec![json!({"i": 1})], json!("v1"));
        let sink1 = MockSink::new();
        Pipeline::new(&s1, &sink1)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert_eq!(s1.observed_start(), None);
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));

        // Run 2: resume from v1, persist v2.
        let s2 = StatefulSource::new("k", vec![json!({"i": 2})], json!("v2"));
        let sink2 = MockSink::new();
        Pipeline::new(&s2, &sink2)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert_eq!(s2.observed_start(), Some(json!("v1")));
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v2")));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pipeline_run_increments_runs_total() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource(vec![json!({"i": 1})]);
        let sink = MockSink::new();
        let _ = Pipeline::new(&source, &sink)
            .with_name("test-pipeline")
            .with_row("rowA")
            .run()
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                key.key().name() == "faucet_pipeline_runs_total"
                    && key.key().labels().any(|l: &metrics::Label| {
                        l.key() == "pipeline" && l.value() == "test-pipeline"
                    })
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "row" && l.value() == "rowA")
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "status" && l.value() == "ok")
                    && matches!(v, DebugValue::Counter(c) if c >= 1)
            },
        );
        assert!(
            found,
            "expected faucet_pipeline_runs_total{{pipeline=test-pipeline, row=rowA, status=ok}}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pipeline_failure_attaches_kind_label_to_runs_total() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = FailingSource;
        let sink = MockSink::new();
        let _ = Pipeline::new(&source, &sink)
            .with_name("err-pipeline")
            .with_row("rowE")
            .run()
            .await;

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                key.key().name() == "faucet_pipeline_runs_total"
                    && key.key().labels().any(|l: &metrics::Label| {
                        l.key() == "pipeline" && l.value() == "err-pipeline"
                    })
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "status" && l.value() == "err")
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "kind" && l.value() == "Auth")
                    && matches!(v, DebugValue::Counter(c) if c >= 1)
            },
        );
        assert!(
            found,
            "expected faucet_pipeline_runs_total{{status=err, kind=Auth}} for failing source"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pipeline_run_emits_start_time_gauge() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource(vec![json!({"i": 1})]);
        let sink = MockSink::new();
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let _ = Pipeline::new(&source, &sink)
            .with_name("start-time-pipeline")
            .with_row("rowS")
            .run()
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                if key.key().name() != "faucet_pipeline_start_time_unix_seconds" {
                    return false;
                }
                let labels_match = key.key().labels().any(|l: &metrics::Label| {
                    l.key() == "pipeline" && l.value() == "start-time-pipeline"
                }) && key
                    .key()
                    .labels()
                    .any(|l: &metrics::Label| l.key() == "row" && l.value() == "rowS");
                if !labels_match {
                    return false;
                }
                matches!(v, DebugValue::Gauge(g) if g.into_inner() >= before)
            },
        );
        assert!(
            found,
            "expected faucet_pipeline_start_time_unix_seconds gauge >= test-start timestamp"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn register_build_info_sets_version_gauge() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        crate::observability::register_build_info();

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                key.key().name() == "faucet_build_info"
                    && key.key().labels().any(|l: &metrics::Label| {
                        l.key() == "version" && l.value() == env!("CARGO_PKG_VERSION")
                    })
                    && matches!(v, DebugValue::Gauge(g) if (g.into_inner() - 1.0).abs() < f64::EPSILON)
            },
        );
        assert!(
            found,
            "expected faucet_build_info{{version=CARGO_PKG_VERSION}} = 1.0 after register_build_info()"
        );
    }

    // ── DLQ routing tests ──────────────────────────────────────────────────

    use crate::dlq::{DlqConfig, OnBatchError};

    /// Sink that returns mixed per-row outcomes: failure indices come from
    /// the constructor; everything else succeeds. Captures the rows that
    /// *would* have committed to the main sink.
    struct PartialSink {
        fail_indices: std::sync::Mutex<Vec<usize>>,
        committed: std::sync::Mutex<Vec<Value>>,
    }

    impl PartialSink {
        fn new(fail_indices: Vec<usize>) -> Self {
            Self {
                fail_indices: std::sync::Mutex::new(fail_indices),
                committed: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for PartialSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            unreachable!("PartialSink only overrides write_batch_partial");
        }
        async fn write_batch_partial(
            &self,
            records: &[Value],
        ) -> Result<Vec<crate::traits::RowOutcome>, FaucetError> {
            let fails: std::collections::HashSet<usize> =
                self.fail_indices.lock().unwrap().iter().copied().collect();
            let mut outcomes = Vec::with_capacity(records.len());
            for (i, rec) in records.iter().enumerate() {
                if fails.contains(&i) {
                    outcomes.push(Err(FaucetError::Sink(format!("row {i} rejected"))));
                } else {
                    self.committed.lock().unwrap().push(rec.clone());
                    outcomes.push(Ok(()));
                }
            }
            Ok(outcomes)
        }
    }

    #[tokio::test]
    async fn dlq_routes_only_failed_rows_for_partial_success_sink() {
        let main = PartialSink::new(vec![1, 3]); // 4 rows, indices 1 and 3 fail
        let dlq = std::sync::Arc::new(MockSink::new());
        let dlq_cfg = DlqConfig::new(dlq.clone());

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: (0..4).map(|i| json!({"i": i})).collect(),
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg))
            .await
            .unwrap();

        assert_eq!(result.records_written, 2); // 0 and 2 committed
        assert_eq!(main.committed.lock().unwrap().len(), 2);
        let envelopes = dlq.0.lock().unwrap();
        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0]["payload"]["i"], 1);
        assert_eq!(envelopes[0]["record_index"], 1);
        assert_eq!(envelopes[1]["payload"]["i"], 3);
        assert_eq!(envelopes[1]["record_index"], 3);
        let stats = result.dlq.unwrap();
        assert_eq!(stats.records_dlq, 2);
        assert_eq!(stats.pages_with_failures, 1);
    }

    #[tokio::test]
    async fn dlq_propagate_policy_bubbles_outer_err() {
        let main = FailingSink;
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.on_batch_error = OnBatchError::Propagate;

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;
        assert!(matches!(result, Err(FaucetError::Sink(_))));
        assert!(dlq.0.lock().unwrap().is_empty());
        // Bookmark must NOT be persisted on a propagated failure.
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dlq_dlq_all_policy_routes_every_row_on_outer_err() {
        let main = FailingSink;
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.on_batch_error = OnBatchError::DlqAll;

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 2})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await
        .unwrap();
        assert_eq!(result.records_written, 0);
        {
            let envelopes = dlq.0.lock().unwrap();
            assert_eq!(envelopes.len(), 3);
            // Every envelope's error.message includes the underlying message.
            for env in envelopes.iter() {
                let msg = env["error"]["message"].as_str().unwrap();
                assert!(msg.contains("write failed"), "got: {msg}");
            }
        }
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));
        assert_eq!(result.dlq.unwrap().records_dlq, 3);
    }

    #[tokio::test]
    async fn dlq_per_page_budget_exceeded_aborts() {
        let main = PartialSink::new(vec![0, 1, 2]);
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.max_failures_per_page = Some(2);

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: (0..3).map(|i| json!({"i": i})).collect(),
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg)).await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("per-page budget exceeded")),
            "got: {result:?}"
        );
    }

    #[tokio::test]
    async fn dlq_total_budget_exceeded_aborts_on_later_page() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: (0..3).map(|i| json!({"i": i})).collect(),
                bookmark: None,
            }),
            Ok(StreamPage {
                records: (3..6).map(|i| json!({"i": i})).collect(),
                bookmark: None,
            }),
        ];
        // Fail every row across both pages.
        let main = PartialSink::new(vec![0, 1, 2]); // applied per page
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.max_failures_total = Some(4);

        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg)).await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("total budget exceeded")),
            "got: {result:?}"
        );
    }

    /// DLQ sink that always fails. Used to assert the router does not
    /// recurse into itself.
    struct FailingDlqSink;
    #[async_trait]
    impl Sink for FailingDlqSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("dlq disk full".into()))
        }
    }

    /// DLQ sink that succeeds on write but fails on flush. Used to assert
    /// the router wraps DLQ flush errors and bails without persisting the
    /// bookmark.
    struct FailingFlushDlqSink {
        written: std::sync::Mutex<Vec<Value>>,
    }
    impl FailingFlushDlqSink {
        fn new() -> Self {
            Self {
                written: std::sync::Mutex::new(Vec::new()),
            }
        }
    }
    #[async_trait]
    impl Sink for FailingFlushDlqSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.written.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            Err(FaucetError::Sink("dlq flush failed".into()))
        }
    }

    #[tokio::test]
    async fn dlq_sink_failure_is_fatal_no_recursion() {
        let main = PartialSink::new(vec![0]);
        let dlq: std::sync::Arc<dyn Sink> = std::sync::Arc::new(FailingDlqSink);
        let dlq_cfg = DlqConfig::new(dlq);

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("DLQ sink write failed")),
            "got: {result:?}"
        );
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dlq_bookmark_advances_only_after_both_flushes() {
        let main = PartialSink::new(vec![1]); // row 1 fails, row 0 commits
        let dlq = std::sync::Arc::new(MockSink::new());
        let dlq_cfg = DlqConfig::new(dlq.clone());

        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await
        .unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));
        assert_eq!(dlq.0.lock().unwrap().len(), 1);
        assert_eq!(main.committed.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn dlq_disabled_pipeline_behaves_identically_to_today() {
        // Regression guard: omitting DLQ keeps existing behavior bit-identical.
        let main = MockSink::new();
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new())
            .await
            .unwrap();
        assert_eq!(result.records_written, 2);
        assert!(result.dlq.is_none());
    }

    #[tokio::test]
    async fn dlq_per_page_flush_failure_is_fatal_and_blocks_bookmark() {
        // Per-page flush path: page carries a bookmark, row 1 fails, the
        // DLQ write succeeds but the DLQ flush at the bookmark gate errors.
        // The pipeline must bail with "DLQ sink flush failed" and the
        // bookmark must NOT be persisted.
        let main = PartialSink::new(vec![1]);
        let dlq: std::sync::Arc<dyn Sink> = std::sync::Arc::new(FailingFlushDlqSink::new());
        let dlq_cfg = DlqConfig::new(dlq);

        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("DLQ sink flush failed")),
            "got: {result:?}"
        );
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dlq_end_of_stream_flush_failure_is_fatal() {
        // End-of-stream flush path: no page carries a bookmark, but DLQ
        // received envelopes during the run. The final post-loop flush of
        // the DLQ sink errors. The pipeline must bail with "DLQ sink flush
        // failed".
        let main = PartialSink::new(vec![1]);
        let dlq: std::sync::Arc<dyn Sink> = std::sync::Arc::new(FailingFlushDlqSink::new());
        let dlq_cfg = DlqConfig::new(dlq);

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg)).await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("DLQ sink flush failed")),
            "got: {result:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn dlq_emits_records_total_and_pages_total() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource(vec![json!({"i": 0}), json!({"i": 1})]);
        let main = PartialSink::new(vec![1]);
        let dlq = std::sync::Arc::new(MockSink::new());
        let _ = Pipeline::new(&source, &main)
            .with_name("p_dlq_metrics")
            .with_row("r1")
            .with_dlq(DlqConfig::new(dlq.clone()))
            .run()
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let mut saw_records = false;
        let mut saw_pages = false;
        for (k, _u, _d, v) in snapshot.into_vec() {
            let key = k.key();
            let labels = key.labels().collect::<Vec<_>>();
            let has = |k: &str, v: &str| labels.iter().any(|l| l.key() == k && l.value() == v);
            if key.name() == "faucet_sink_dlq_records_total"
                && has("pipeline", "p_dlq_metrics")
                && has("row", "r1")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
            {
                saw_records = true;
            }
            if key.name() == "faucet_sink_dlq_pages_total"
                && has("pipeline", "p_dlq_metrics")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
            {
                saw_pages = true;
            }
        }
        assert!(saw_records, "faucet_sink_dlq_records_total not emitted");
        assert!(saw_pages, "faucet_sink_dlq_pages_total not emitted");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn dlq_budget_exceeded_emits_counter() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource((0..3).map(|i| json!({"i": i})).collect());
        let main = PartialSink::new(vec![0, 1, 2]);
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut cfg = DlqConfig::new(dlq);
        cfg.max_failures_per_page = Some(1);
        let _ = Pipeline::new(&source, &main)
            .with_name("p_budget")
            .with_dlq(cfg)
            .run()
            .await;

        let snapshot = snap.snapshot();
        let saw = snapshot.into_vec().into_iter().any(|(k, _, _, v)| {
            k.key().name() == "faucet_sink_dlq_budget_exceeded_total"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "scope" && l.value() == "per_page")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(saw, "faucet_sink_dlq_budget_exceeded_total not emitted");
    }

    #[tokio::test]
    async fn pipeline_run_with_dlq_routes_partial_failures_end_to_end() {
        // Source: 3 records. Main sink: fails index 1. DLQ: in-memory.
        let source = MockSource(vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 2})]);
        let main = PartialSink::new(vec![1]);
        let dlq = std::sync::Arc::new(MockSink::new());

        let result = Pipeline::new(&source, &main)
            .with_dlq(DlqConfig::new(dlq.clone()))
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 2);
        let stats = result.dlq.unwrap();
        assert_eq!(stats.records_dlq, 1);
        {
            let dlq_records = dlq.0.lock().unwrap();
            assert_eq!(dlq_records.len(), 1);
        }
    }

    // ── Quality routing tests ──────────────────────────────────────────────

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn quality_quarantines_to_dlq_and_writes_survivors() {
        use crate::dlq::DlqConfig;
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};

        let main = Arc::new(MockSink::new());
        let dlq_sink = Arc::new(MockSink::new());
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": null}), json!({"id": 3})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new()
            .with_dlq(DlqConfig::new(dlq_sink.clone()))
            .with_quality(quality);
        let result = run_stream(futures::stream::iter(pages), main.as_ref(), opts)
            .await
            .unwrap();

        assert_eq!(result.records_written, 2); // survivors
        assert_eq!(main.written(), vec![json!({"id": 1}), json!({"id": 3})]);
        // one quarantined record reached the DLQ with a QualityFailure envelope
        let dlq = dlq_sink.written();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0]["error"]["kind"], "QualityFailure");
        assert_eq!(result.dlq.unwrap().records_dlq, 1);
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn quality_only_page_emits_quality_reason() {
        // A page whose DLQ traffic is entirely quality-sourced (the main sink
        // accepts every survivor) must label `faucet_sink_dlq_pages_total`
        // with `reason="quality"`, not `partial`.
        use crate::dlq::DlqConfig;
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        // MockSink accepts everything → no sink-side failures, so the only DLQ
        // traffic comes from the quality quarantine.
        let main = Arc::new(MockSink::new());
        let dlq_sink = Arc::new(MockSink::new());
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": null}), json!({"id": 3})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new()
            .with_name("p_quality_reason")
            .with_dlq(DlqConfig::new(dlq_sink.clone()))
            .with_quality(quality);
        let _ = run_stream(futures::stream::iter(pages), main.as_ref(), opts)
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let saw_quality_reason = snapshot.into_vec().into_iter().any(|(k, _, _, v)| {
            k.key().name() == "faucet_sink_dlq_pages_total"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "pipeline" && l.value() == "p_quality_reason")
                && k.key()
                    .labels()
                    .any(|l| l.key() == "reason" && l.value() == "quality")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(
            saw_quality_reason,
            "expected faucet_sink_dlq_pages_total with reason=\"quality\""
        );
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn quality_abort_fails_run() {
        use crate::quality::{BatchCheck, CompiledQuality, OnFailure, QualitySpec};
        let main = MockSink::new();
        let spec = QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::RowCount {
                min: Some(5),
                max: None,
                on_failure: OnFailure::Abort,
            }],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new().with_quality(quality);
        let result = run_stream(futures::stream::iter(pages), &main, opts).await;
        assert!(matches!(result, Err(FaucetError::QualityFailure { .. })));
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn quality_quarantine_without_dlq_is_rejected() {
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};
        let main = MockSink::new();
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": null})],
            bookmark: None,
        })];
        // No .with_dlq(...) — must be rejected up front.
        let opts = RunStreamOptions::new().with_quality(quality);
        let result = run_stream(futures::stream::iter(pages), &main, opts).await;
        assert!(matches!(result, Err(FaucetError::Config(_))));
    }

    // ── Adaptive batch-size tests ──────────────────────────────────────────

    /// A sink that records each write_batch call's size and reports a fixed
    /// per-call latency, so we can assert the adaptive controller resliced.
    struct RecordingSink {
        calls: std::sync::Mutex<Vec<usize>>,
        latency: std::time::Duration,
    }
    impl RecordingSink {
        fn new(latency_ms: u64) -> Self {
            Self { calls: std::sync::Mutex::new(Vec::new()), latency: std::time::Duration::from_millis(latency_ms) }
        }
        fn call_sizes(&self) -> Vec<usize> { self.calls.lock().unwrap().clone() }
    }
    #[async_trait]
    impl Sink for RecordingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            tokio::time::sleep(self.latency).await;
            self.calls.lock().unwrap().push(records.len());
            Ok(records.len())
        }
    }

    #[tokio::test]
    async fn adaptive_reslices_non_dlq_page_into_subbatches() {
        use crate::adaptive::AdaptiveBatchConfig;
        let page = StreamPage {
            records: (0..1000).map(|i| json!({ "i": i })).collect(),
            bookmark: None,
        };
        let stream = futures::stream::iter(vec![Ok(page)]);
        let sink = RecordingSink::new(0);
        let cfg: AdaptiveBatchConfig =
            serde_json::from_value(json!({"enabled": true, "min": 100, "max": 1000})).unwrap();
        let result = run_stream(stream, &sink, RunStreamOptions::new().with_adaptive(cfg))
            .await
            .unwrap();
        assert_eq!(result.records_written, 1000);
        // current starts at min(max, page_len)=1000 → one chunk (no regression).
        assert_eq!(sink.call_sizes(), vec![1000]);
    }

    #[tokio::test]
    async fn adaptive_shrinks_under_latency_target_then_smaller_chunks() {
        use crate::adaptive::AdaptiveBatchConfig;
        let mk = || StreamPage { records: (0..400).map(|i| json!({"i": i})).collect(), bookmark: None };
        let stream = futures::stream::iter(vec![Ok(mk()), Ok(mk()), Ok(mk())]);
        let sink = RecordingSink::new(50);
        let cfg: AdaptiveBatchConfig = serde_json::from_value(json!({
            "enabled": true, "min": 50, "max": 400,
            "decrease_factor": 0.5, "cooldown_batches": 0,
            "target_latency_ms": 10, "latency_window": 1
        })).unwrap();
        let result = run_stream(stream, &sink, RunStreamOptions::new().with_adaptive(cfg))
            .await
            .unwrap();
        assert_eq!(result.records_written, 1200);
        let sizes = sink.call_sizes();
        assert_eq!(sizes[0], 400);
        assert!(sizes.last().unwrap() < &400, "controller should have shrunk: {sizes:?}");
    }
}
