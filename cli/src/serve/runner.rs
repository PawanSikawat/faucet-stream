//! The run lifecycle: validate + queue a submission (`submit`), then run it under
//! a permit inside a 3-arm `select!` (user-cancel token / server-shutdown token /
//! timeout) and finalize an authoritative terminal status. See spec §7 + §20.

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

    // doctor_first preflight (before reserving any slot or claiming the key).
    if req.doctor_first {
        run_doctor_first(&state, &loaded).await?;
    }

    // Reserve a queue slot first, so a Fresh idempotency claim is always followed
    // by a spawned run (no orphaned claims — spec §20.2).
    if !state.registry().try_reserve() {
        return Err(ServeError::QueueFull {
            retry_after_secs: QUEUE_FULL_RETRY_AFTER_SECS,
        });
    }
    // Releases the reservation on ANY early return below (replay / conflict /
    // claim or upsert error). Defused just before the run is spawned.
    let reservation = ReservationGuard::new(state.clone());

    let run_id = uuid::Uuid::now_v7().to_string();

    // Idempotency claim (if a key was supplied).
    if let Some(key) = &req.idempotency_key {
        let merged = serde_json::to_value(&loaded.cfg).unwrap_or(serde_json::Value::Null);
        let fp = idempotency::fingerprint(&merged, loaded.cfg.name.as_deref());
        match state
            .history()
            .claim_idempotency(key, &fp, &run_id, state.idempotency_retention())
            .await
            .map_err(|e| ServeError::Internal(e.to_string()))?
        {
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
    let rec = RunRecord::queued(
        run_id.clone(),
        req.name.clone(),
        req.labels.clone(),
        req.idempotency_key.clone(),
        submitted_at,
    );
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
async fn run_doctor_first(
    state: &ServerState,
    loaded: &LoadedSubmission,
) -> Result<(), ServeError> {
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
    if failed > 0 {
        crate::commands::doctor::redact_invocations(&mut invs);
        return Err(ServeError::Unprocessable {
            message: format!("doctor_first preflight failed: {failed} probe(s) failed"),
            details: Some(serde_json::json!({ "invocations": invs })),
        });
    }
    Ok(())
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
        let _permit = state
            .semaphore()
            .acquire_owned()
            .await
            .expect("semaphore not closed");

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

        let opts = ExecuteOptions {
            pipeline_name,
            execution: cfg.execution.clone(),
            dry_run: false,
            limit: None,
            state_path_override: None,
            auth,
            clock,
        };
        let timeout = req.timeout_secs.map(Duration::from_secs);

        let span = tracing::info_span!("faucet.serve.run", serve_run_id = %run_id);
        let work = async move {
            // Emitted inside the run span so it is captured by the SSE log layer
            // (and gives every `/logs` reader at least one line to anchor on).
            tracing::info!("pipeline run starting");
            match timeout {
                Some(d) => match tokio::time::timeout(d, run_expanded(nodes, opts)).await {
                    Ok(r) => classify_run(r),
                    Err(_) => Terminal::Timeout { secs: d.as_secs() },
                },
                None => classify_run(run_expanded(nodes, opts).await),
            }
        }
        .instrument(span);

        // Cooperative cancel: dropping `work` cancels in-flight pipeline work at
        // its next await; the task stays alive to write its own terminal status.
        let terminal = tokio::select! {
            _ = run_token.cancelled() => Terminal::Cancelled,
            _ = server_shutdown.cancelled() => Terminal::ShutdownFailed,
            t = work => t,
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
    if let Ok(Some(mut rec)) = state.history().get(run_id).await {
        rec.status = status;
        rec.finished_at = Some(finished);
        rec.elapsed_secs = elapsed;
        rec.records_written = records;
        rec.invocations = invs;
        rec.error = error;
        let _ = state.history().upsert(&rec).await;
    }
    metrics::record_run_finished(status, reason);
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
}
