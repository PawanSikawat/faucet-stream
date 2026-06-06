//! The run lifecycle: validate + queue a submission (`submit`), then run it under
//! a permit. A cancel / timeout / shutdown trigger cooperatively cancels the
//! pipeline (so a buffered sink flushes at its next page boundary, #146 H16) and
//! grants a bounded flush grace before hard-dropping it; the task then finalizes
//! an authoritative terminal status. See spec §7 + §20.

use crate::auth_catalog::build_auth_catalog;
use crate::executor::{ExecuteOptions, RunSummary, run_expanded};
use crate::serve::error::ServeError;
use crate::serve::history::{Claim, InvocationRecord, RunRecord, RunStatus};
use crate::serve::load::{ConfigFormat, LoadedSubmission, load_submission};
use crate::serve::state::ServerState;
use crate::serve::{idempotency, metrics};
use chrono::{DateTime, FixedOffset, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// `Retry-After` advertised when the queue is full.
const QUEUE_FULL_RETRY_AFTER_SECS: u64 = 5;

/// Grace granted to a cancelled / timed-out / shutting-down run to flush
/// buffered sink output cooperatively before its future is hard-dropped (which
/// aborts the pipeline's task set, the backstop for a run stuck mid-write so a
/// hung run can't wedge shutdown). Generous enough for an S3 multipart
/// completion (#146 H16).
const RUN_FLUSH_GRACE: Duration = Duration::from_secs(30);

/// `POST /v1/runs` request body.
#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    pub name: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub doctor_first: bool,
    pub idempotency_key: Option<String>,
    pub clock: Option<String>,
}

/// Wire enum mirroring `load::ConfigFormat` with serde rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormatWire {
    #[default]
    Yaml,
    Json,
}

impl From<ConfigFormatWire> for ConfigFormat {
    fn from(w: ConfigFormatWire) -> Self {
        match w {
            ConfigFormatWire::Yaml => ConfigFormat::Yaml,
            ConfigFormatWire::Json => ConfigFormat::Json,
        }
    }
}

/// `POST /v1/runs` success body (202).
#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    pub run_id: String,
    pub status: RunStatus,
    pub submitted_at: DateTime<Utc>,
}

/// Validate, idempotency-claim, queue, and spawn a submission.
pub async fn submit(state: ServerState, req: SubmitRequest) -> Result<SubmitResponse, ServeError> {
    let format: ConfigFormat = req.config_format.into();
    let loaded = load_submission(&req.config, format, state.default_base()).await?;

    // Reserve a queue slot first, so a Fresh idempotency claim is always followed
    // by a spawned run (no orphaned claims — spec §20.2).
    if !state.registry().try_reserve() {
        return Err(ServeError::QueueFull {
            retry_after_secs: QUEUE_FULL_RETRY_AFTER_SECS,
        });
    }
    // Releases the reservation on ANY early return below (doctor_first 422 /
    // replay / conflict / claim or upsert error). Defused just before spawn.
    let reservation = ReservationGuard::new(state.clone());

    // doctor_first preflight — run BEHIND the reservation so concurrent preflight
    // probing is bounded by `max_queued_runs` rather than running unthrottled
    // before any limit applies (#146 R). On failure the guard releases the slot
    // via the early `?`. The (redacted) report is stored on the run record below
    // so `GET /v1/runs/{id}` exposes it (#146 R: doctor_report was never set).
    let doctor_report = if req.doctor_first {
        Some(run_doctor_first(&state, &loaded).await?)
    } else {
        None
    };

    let run_id = uuid::Uuid::now_v7().to_string();

    // Idempotency claim (if a key was supplied).
    if let Some(key) = &req.idempotency_key {
        let merged = serde_json::to_value(&loaded.cfg).unwrap_or(serde_json::Value::Null);
        // Fold the run-affecting request fields (clock / timeout_secs / labels)
        // into the fingerprint, not just the config — so a key replayed with a
        // different backfill `clock` is a 409, not a replay of the original
        // run's window (#146 M7).
        let fp_config = idempotency::fingerprint(&merged, loaded.cfg.name.as_deref());
        let fp = idempotency::request_fingerprint(
            &fp_config,
            req.clock.as_deref(),
            req.timeout_secs,
            &req.labels,
        );
        match state
            .history()
            .claim_idempotency(key, &fp, &run_id, state.idempotency_retention())
            .await
            .map_err(|e| match e {
                // Degraded backend can't safely honor idempotency → 503, retry.
                crate::serve::history::HistoryError::Degraded(m) => ServeError::Unavailable(m),
                other => ServeError::Internal(other.to_string()),
            })? {
            Claim::Fresh => {}
            Claim::Replay(existing) => {
                metrics::record_idempotency_hit();
                return replay_response(&state, &existing).await;
            }
            Claim::Conflict => {
                return Err(ServeError::Conflict(
                    "idempotency key reused with a different payload".into(),
                ));
            }
        }
        // NOTE (Phase 5 / SQL backends): a `Fresh` claim is recorded BEFORE the
        // record upsert below. The memory backend's `upsert` is infallible, so the
        // claim and record are always consistent today. A future fallible backend
        // whose `upsert` errors here would leave an orphaned claim (a replay of the
        // key returns 404 until the claim self-expires within the retention
        // window). When SQL backends land, claim after a successful upsert, or add
        // a claim-release to RunHistory.
    }

    let submitted_at = Utc::now();
    let mut rec = RunRecord::queued(
        run_id.clone(),
        req.name.clone(),
        req.labels.clone(),
        req.idempotency_key.clone(),
        submitted_at,
    );
    rec.doctor_report = doctor_report;
    state
        .history()
        .upsert(&rec)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;

    let run_token = CancellationToken::new();
    state.registry().register(run_id.clone(), run_token.clone());
    metrics::set_run_gauges(&state);

    // The spawned task now owns the queued→running→finished lifecycle.
    reservation.defuse();
    spawn_run(
        state.clone(),
        loaded,
        req,
        run_id.clone(),
        run_token,
        submitted_at,
    );

    Ok(SubmitResponse {
        run_id,
        status: RunStatus::Queued,
        submitted_at,
    })
}

/// Run the `doctor_first` probes; on any failure return 422 with the report.
/// Run the `doctor_first` probes. On success returns the (redacted) report so
/// the caller can store it on the run record (`doctor_report`); on any probe
/// failure returns 422 with the same redacted report as `details`.
async fn run_doctor_first(
    state: &ServerState,
    loaded: &LoadedSubmission,
) -> Result<serde_json::Value, ServeError> {
    use faucet_core::check::CheckContext;
    let auth =
        build_auth_catalog(loaded.cfg.auth.as_ref()).map_err(|e| ServeError::Unprocessable {
            message: e.to_string(),
            details: None,
        })?;
    let ctx = CheckContext {
        timeout: state.probe_timeout(),
    };
    let mut invs = crate::commands::doctor::probe_roots(&loaded.nodes, &auth, &ctx).await;
    let failed = crate::commands::doctor::count_failures(&invs);
    // Redact regardless of outcome — the report is surfaced either way (as the
    // 422 `details` on failure, or stored on the run record on success).
    crate::commands::doctor::redact_invocations(&mut invs);
    let report = serde_json::json!({ "invocations": invs });
    if failed > 0 {
        return Err(ServeError::Unprocessable {
            message: format!("doctor_first preflight failed: {failed} probe(s) failed"),
            details: Some(report),
        });
    }
    Ok(report)
}

/// Build the replay response for an idempotency hit (the existing run's status).
async fn replay_response(state: &ServerState, run_id: &str) -> Result<SubmitResponse, ServeError> {
    let rec = state
        .history()
        .get(run_id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    Ok(SubmitResponse {
        run_id: rec.run_id,
        status: rec.status,
        submitted_at: rec.submitted_at,
    })
}

/// Releases a queue reservation on drop unless [`Self::defuse`]d. Guarantees the
/// `queued` counter is balanced on every early-return path (replay / conflict /
/// claim-or-upsert error) without a manual `release_reservation` at each site.
/// Defused once the run is handed to the spawned task, which then owns the
/// queued→running transition via `mark_running`.
struct ReservationGuard {
    state: Option<ServerState>,
}

impl ReservationGuard {
    fn new(state: ServerState) -> Self {
        Self { state: Some(state) }
    }

    /// Hand the reservation off to the spawned task (no release on drop).
    fn defuse(mut self) {
        self.state = None;
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.registry().release_reservation();
            metrics::set_run_gauges(&state);
        }
    }
}

/// Releases the in-flight slot (decrement `in_flight`, drop the cancel token, wake
/// the shutdown drain) on drop — on EVERY path including panic. Without this, a
/// panic between `mark_running` and a manual `mark_finished` would leak the
/// counter and hang graceful shutdown forever.
struct InFlightGuard {
    state: ServerState,
    run_id: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.state.registry().mark_finished(&self.run_id);
        metrics::set_run_gauges(&self.state);
    }
}

/// Terminal classification of a run task.
enum Terminal {
    Completed {
        records: u64,
        invs: Vec<InvocationRecord>,
    },
    Failed {
        reason: String,
        records: u64,
        invs: Vec<InvocationRecord>,
    },
    Timeout {
        secs: u64,
    },
    Cancelled,
    ShutdownFailed,
}

impl Terminal {
    /// (status, metric reason label, records, invocations, error message)
    fn into_parts(
        self,
    ) -> (
        RunStatus,
        &'static str,
        u64,
        Vec<InvocationRecord>,
        Option<String>,
    ) {
        match self {
            Terminal::Completed { records, invs } => {
                (RunStatus::Completed, "ok", records, invs, None)
            }
            Terminal::Failed {
                reason,
                records,
                invs,
            } => (RunStatus::Failed, "error", records, invs, Some(reason)),
            Terminal::Timeout { secs } => (
                RunStatus::Failed,
                "timeout",
                0,
                Vec::new(),
                Some(format!("run exceeded timeout_secs ({secs}s)")),
            ),
            Terminal::Cancelled => (RunStatus::Cancelled, "cancelled", 0, Vec::new(), None),
            Terminal::ShutdownFailed => (
                RunStatus::Failed,
                "server_shutdown",
                0,
                Vec::new(),
                Some("server shutdown before the run finished".into()),
            ),
        }
    }
}

/// Classify a finished `run_expanded` result into a `Terminal`.
fn classify_run(result: crate::error::CliResult<RunSummary>) -> Terminal {
    match result {
        Ok(summary) => {
            let records: u64 = summary
                .invocations
                .iter()
                .map(|i| i.records_written as u64)
                .sum();
            let invs: Vec<InvocationRecord> = summary
                .invocations
                .iter()
                .map(InvocationRecord::from)
                .collect();
            if summary.had_failures() {
                Terminal::Failed {
                    reason: format!("{} invocation(s) failed", summary.failure_count()),
                    records,
                    invs,
                }
            } else {
                Terminal::Completed { records, invs }
            }
        }
        Err(e) => Terminal::Failed {
            reason: e.to_string(),
            records: 0,
            invs: Vec::new(),
        },
    }
}

/// Parse the optional request `clock` (RFC3339), defaulting to `submitted_at`.
fn resolve_clock(
    flag: Option<&str>,
    default: DateTime<Utc>,
) -> Result<DateTime<FixedOffset>, ServeError> {
    match flag {
        None => Ok(default.fixed_offset()),
        Some(s) => DateTime::parse_from_rfc3339(s)
            .map_err(|_| ServeError::BadConfig(format!("clock '{s}' is not RFC3339"))),
    }
}

/// Spawn the detached run task: acquire a permit, run under the 3-arm select,
/// finalize the terminal status.
fn spawn_run(
    state: ServerState,
    loaded: LoadedSubmission,
    req: SubmitRequest,
    run_id: String,
    run_token: CancellationToken,
    submitted_at: DateTime<Utc>,
) {
    let server_shutdown = state.shutdown_token();
    let LoadedSubmission { cfg, nodes } = loaded;

    tokio::spawn(async move {
        // Race the permit acquisition against cancel / shutdown so a run
        // cancelled while STILL QUEUED (before any permit frees) is finalized
        // immediately, instead of only after it eventually acquires a permit
        // (#146 R). `biased` prefers the cancel/shutdown signals over a
        // simultaneously-available permit.
        let _permit = tokio::select! {
            biased;
            _ = run_token.cancelled() => {
                finalize_queued_cancel(&state, &run_id, submitted_at, Terminal::Cancelled).await;
                return;
            }
            _ = server_shutdown.cancelled() => {
                finalize_queued_cancel(&state, &run_id, submitted_at, Terminal::ShutdownFailed).await;
                return;
            }
            permit = state.semaphore().acquire_owned() => permit.expect("semaphore not closed"),
        };

        // Queued → running. From here the guard guarantees `mark_finished` (and a
        // gauge refresh) on EVERY exit, including early returns and panics.
        state.registry().mark_running();
        let _guard = InFlightGuard {
            state: state.clone(),
            run_id: run_id.clone(),
        };
        let started = Utc::now();
        if let Ok(Some(mut rec)) = state.history().get(&run_id).await {
            rec.status = RunStatus::Running;
            rec.started_at = Some(started);
            let _ = state.history().upsert(&rec).await;
        }
        metrics::set_run_gauges(&state);

        // Build execution options (auth/clock failures finalize as Failed).
        let pipeline_name = cfg.name.clone().unwrap_or_else(|| "serve".to_string());
        let auth = match build_auth_catalog(cfg.auth.as_ref()) {
            Ok(a) => a,
            Err(e) => {
                finalize(
                    &state,
                    &run_id,
                    started,
                    Terminal::Failed {
                        reason: format!("auth catalog: {e}"),
                        records: 0,
                        invs: Vec::new(),
                    },
                )
                .await;
                return;
            }
        };
        let clock = match resolve_clock(req.clock.as_deref(), submitted_at) {
            Ok(c) => c,
            Err(e) => {
                finalize(
                    &state,
                    &run_id,
                    started,
                    Terminal::Failed {
                        reason: e.api_error().error.message,
                        records: 0,
                        invs: Vec::new(),
                    },
                )
                .await;
                return;
            }
        };

        // Cooperative-cancel token the pipeline observes so it flushes buffered
        // output (e.g. a Parquet footer, an S3 multipart upload) at its next
        // page boundary on cancel / timeout / shutdown — instead of having its
        // future hard-dropped, which flushes nothing (#146 H16).
        let coop = CancellationToken::new();
        let opts = ExecuteOptions {
            pipeline_name,
            execution: cfg.execution.clone(),
            dry_run: false,
            limit: None,
            state_path_override: None,
            auth,
            clock,
            cancel: Some(coop.clone()),
            // Lineage emitter is wired in Task 28 (`faucet serve`); `None` here so
            // the literal compiles under the `lineage` feature meanwhile.
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "lineage")]
            lineage_cfg: None,
        };
        let timeout_secs = req.timeout_secs;

        let span = tracing::info_span!("faucet.serve.run", serve_run_id = %run_id);
        let work = async move {
            // Emitted inside the run span so it is captured by the SSE log layer
            // (and gives every `/logs` reader at least one line to anchor on).
            tracing::info!("pipeline run starting");
            classify_run(run_expanded(nodes, opts).await)
        }
        .instrument(span);
        tokio::pin!(work);

        // The run timeout is modelled as a cancel trigger (not a hard
        // `tokio::time::timeout` drop) so a timed-out run still flushes.
        let timeout_fut = async {
            match timeout_secs {
                Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        enum Trigger {
            Done(Terminal),
            Cancel,
            Shutdown,
            Timeout(u64),
        }

        // Phase 1: run to natural completion, or until a cancel trigger fires.
        // `biased` prefers a just-completed run over a simultaneous trigger.
        let trigger = tokio::select! {
            biased;
            t = &mut work => Trigger::Done(t),
            _ = run_token.cancelled() => Trigger::Cancel,
            _ = server_shutdown.cancelled() => Trigger::Shutdown,
            _ = &mut timeout_fut => Trigger::Timeout(timeout_secs.unwrap_or(0)),
        };

        let terminal = match trigger {
            Trigger::Done(t) => t,
            triggered => {
                // Phase 2: a trigger fired. Cancel cooperatively and give the
                // pipeline a bounded grace to flush at its next page boundary,
                // then hard-drop it (drops the JoinSet, aborting any pipeline
                // genuinely stuck mid-write) so a hung run can't wedge shutdown.
                coop.cancel();
                let _ = tokio::time::timeout(RUN_FLUSH_GRACE, &mut work).await;
                match triggered {
                    Trigger::Cancel => Terminal::Cancelled,
                    Trigger::Shutdown => Terminal::ShutdownFailed,
                    Trigger::Timeout(secs) => Terminal::Timeout { secs },
                    Trigger::Done(_) => unreachable!("matched in the outer arm"),
                }
            }
        };

        finalize(&state, &run_id, started, terminal).await;
        // Signal `/logs` readers the run is done, then drop the buffer after a
        // drain window so a late fetcher can still replay it (spec §12).
        state.log_hub().finish(&run_id);
        schedule_log_drop(state.clone(), run_id.clone());
        // `_guard` drops here → mark_finished + gauge refresh.
    });
}

/// Write the authoritative terminal record + the run-finished metric.
async fn finalize(state: &ServerState, run_id: &str, started: DateTime<Utc>, term: Terminal) {
    let finished = Utc::now();
    let elapsed = (finished - started).to_std().ok().map(|d| d.as_secs_f64());
    let (status, reason, records, invs, error) = term.into_parts();
    // Read-modify-write the existing record to preserve its metadata
    // (name / labels / idempotency_key / submitted_at). If it can't be read —
    // the backend errored, or the record was purged / landed in another store
    // under degraded fallback — DON'T silently drop the terminal status (#146
    // M6): reconstruct a minimal terminal record and upsert it, so the run
    // never lingers non-terminal while `record_run_finished` has already fired.
    let mut rec = match state.history().get(run_id).await {
        Ok(Some(rec)) => rec,
        Ok(None) => {
            tracing::warn!(
                run_id,
                "finalize: run record not found; writing a fresh terminal record"
            );
            RunRecord::queued(run_id.to_string(), None, BTreeMap::new(), None, started)
        }
        Err(e) => {
            tracing::warn!(
                run_id,
                error = %e,
                "finalize: failed to read run record; writing a fresh terminal record"
            );
            RunRecord::queued(run_id.to_string(), None, BTreeMap::new(), None, started)
        }
    };
    rec.status = status;
    rec.started_at.get_or_insert(started);
    rec.finished_at = Some(finished);
    rec.elapsed_secs = elapsed;
    rec.records_written = records;
    rec.invocations = invs;
    rec.error = error;
    if let Err(e) = state.history().upsert(&rec).await {
        tracing::error!(
            run_id,
            error = %e,
            "finalize: failed to persist terminal run record"
        );
    }
    metrics::record_run_finished(status, reason);
}

/// Finalize a run that was cancelled / hit shutdown while still QUEUED (before
/// it acquired an execution permit, so it never became in-flight and has no
/// `InFlightGuard`). Releases the queue slot, writes the terminal record, closes
/// the log buffer, and refreshes the gauges — the queued-path analogue of the
/// normal `finalize` + `InFlightGuard`-drop cleanup.
async fn finalize_queued_cancel(
    state: &ServerState,
    run_id: &str,
    submitted_at: DateTime<Utc>,
    term: Terminal,
) {
    state.registry().mark_queued_cancelled(run_id);
    finalize(state, run_id, submitted_at, term).await;
    state.log_hub().finish(run_id);
    schedule_log_drop(state.clone(), run_id.to_string());
    metrics::set_run_gauges(state);
}

/// Spawn a detached timer that drops a finished run's log buffer after the drain
/// window, freeing its ring once late `/logs` fetchers have had a chance to read.
fn schedule_log_drop(state: ServerState, run_id: String) {
    tokio::spawn(async move {
        tokio::time::sleep(crate::serve::logs::LOG_DRAIN).await;
        state.log_hub().drop_run(&run_id);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_ok_no_failures_is_completed() {
        let summary = RunSummary {
            invocations: vec![crate::executor::InvocationOutcome {
                row_id: "r".into(),
                parent_record_key: None,
                records_written: 3,
                error: None,
            }],
        };
        let (status, reason, records, _, error) = classify_run(Ok(summary)).into_parts();
        assert_eq!(status, RunStatus::Completed);
        assert_eq!(reason, "ok");
        assert_eq!(records, 3);
        assert!(error.is_none());
    }

    #[test]
    fn classify_ok_with_failures_is_failed() {
        let summary = RunSummary {
            invocations: vec![crate::executor::InvocationOutcome {
                row_id: "r".into(),
                parent_record_key: None,
                records_written: 0,
                error: Some("boom".into()),
            }],
        };
        let (status, reason, _, _, error) = classify_run(Ok(summary)).into_parts();
        assert_eq!(status, RunStatus::Failed);
        assert_eq!(reason, "error");
        assert!(error.unwrap().contains("invocation(s) failed"));
    }

    #[test]
    fn timeout_maps_to_failed_with_timeout_reason() {
        let (status, reason, _, _, error) = Terminal::Timeout { secs: 30 }.into_parts();
        assert_eq!(status, RunStatus::Failed);
        assert_eq!(reason, "timeout");
        assert!(error.unwrap().contains("30s"));
    }

    #[test]
    fn resolve_clock_defaults_and_parses() {
        let default = Utc::now();
        assert_eq!(
            resolve_clock(None, default).unwrap(),
            default.fixed_offset()
        );
        assert!(resolve_clock(Some("2026-01-31T00:00:00Z"), default).is_ok());
        assert!(resolve_clock(Some("not-a-time"), default).is_err());
    }

    #[tokio::test]
    async fn conflict_releases_reservation() {
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        use crate::serve::history::RunHistory;
        use crate::serve::history::memory::MemoryHistory;
        use crate::serve::state::ServerState;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: Duration::from_secs(60),
            retain_terminal_runs: Duration::from_secs(60),
            idempotency_retention: Duration::from_secs(60),
            lease_ttl: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
        };
        let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
        let state = ServerState::new(
            &cfg,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
        );

        // Pre-claim the key with a DIFFERENT fingerprint so submit() hits Conflict.
        state
            .history()
            .claim_idempotency("k", "different-fp", "prior", Duration::from_secs(60))
            .await
            .unwrap();

        let req = SubmitRequest {
            config: "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n".into(),
            config_format: ConfigFormatWire::Yaml,
            name: None,
            labels: BTreeMap::new(),
            timeout_secs: None,
            doctor_first: false,
            idempotency_key: Some("k".into()),
            clock: None,
        };

        let err = submit(state.clone(), req).await.unwrap_err();
        assert!(
            matches!(err, ServeError::Conflict(_)),
            "expected Conflict, got {err:?}"
        );
        // The reservation taken before the claim must have been released by the guard.
        assert_eq!(state.registry().queued(), 0);
    }

    /// A `ServerState` backed by an in-memory history, for finalize tests.
    fn memory_state() -> crate::serve::state::ServerState {
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        use crate::serve::history::RunHistory;
        use crate::serve::history::memory::MemoryHistory;
        use crate::serve::state::ServerState;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: Duration::from_secs(60),
            retain_terminal_runs: Duration::from_secs(60),
            idempotency_retention: Duration::from_secs(60),
            lease_ttl: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
        };
        let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
        ServerState::new(
            &cfg,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
        )
    }

    #[tokio::test]
    async fn finalize_writes_terminal_record_when_record_is_missing() {
        // M6 (#146): if the run record can't be read at finalize time (purged,
        // or split to another store under degraded fallback), the terminal
        // status must NOT be silently dropped — a fresh terminal record is
        // written so the run never lingers non-terminal while the run-finished
        // metric has already fired.
        let state = memory_state();
        let started = Utc::now();
        finalize(
            &state,
            "ghost",
            started,
            Terminal::Failed {
                reason: "boom".into(),
                records: 0,
                invs: Vec::new(),
            },
        )
        .await;
        let rec = state
            .history()
            .get("ghost")
            .await
            .unwrap()
            .expect("finalize must create a terminal record even when none existed");
        assert_eq!(rec.status, RunStatus::Failed);
        assert!(rec.finished_at.is_some());
        assert!(rec.started_at.is_some());
        assert_eq!(rec.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn finalize_preserves_metadata_of_existing_record() {
        // The happy path still read-modify-writes, preserving name/labels/key.
        let state = memory_state();
        let started = Utc::now();
        let mut rec = RunRecord::queued(
            "r1".into(),
            Some("nightly".into()),
            BTreeMap::new(),
            Some("idem-k".into()),
            started,
        );
        rec.status = RunStatus::Running;
        rec.started_at = Some(started);
        state.history().upsert(&rec).await.unwrap();

        finalize(
            &state,
            "r1",
            started,
            Terminal::Completed {
                records: 5,
                invs: Vec::new(),
            },
        )
        .await;
        let got = state.history().get("r1").await.unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Completed);
        assert_eq!(got.records_written, 5);
        assert_eq!(got.name.as_deref(), Some("nightly"));
        assert_eq!(got.idempotency_key.as_deref(), Some("idem-k"));
    }
}
