//! axum router assembly and the bind / graceful-shutdown serve loop.

use crate::error::{CliError, CliResult};
use crate::serve::config::ServeConfig;
use crate::serve::handlers::{health, logs, runs};
use crate::serve::history::RunHistory;
use crate::serve::state::ServerState;
use crate::serve::{auth, metrics};
use axum::Router;
use axum::routing::{get, post};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

/// Build the full router: unauthenticated probes + the bearer-guarded `/v1` API.
pub fn build_router(state: ServerState, config: &ServeConfig) -> Router {
    let public = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/metrics", get(health::metrics));

    // `/v1` routes guarded by the bearer middleware via `route_layer` (only runs
    // for matched routes; OPTIONS preflight is allowed through inside the layer).
    let api = Router::new()
        .route("/v1/runs", post(runs::submit_run).get(runs::list_runs))
        .route("/v1/runs/{id}", get(runs::get_run).delete(runs::delete_run))
        .route("/v1/runs/{id}/cancel", post(runs::cancel_run))
        .route("/v1/runs/{id}/logs", get(logs::stream_logs))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth,
        ));

    let cors = if config.cors_origins.is_empty() {
        CorsLayer::new()
    } else {
        let origins: Vec<axum::http::HeaderValue> = config
            .cors_origins
            .iter()
            .filter_map(|o| match o.parse() {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(origin = %o, error = %e, "ignoring invalid --cors-origin");
                    None
                }
            })
            .collect();
        CorsLayer::new().allow_origin(AllowOrigin::list(origins))
    };

    public
        .merge(api)
        .layer(RequestBodyLimitLayer::new(config.body_limit_bytes))
        .layer(axum::middleware::from_fn(metrics::track_metrics))
        .layer(cors)
        .with_state(state)
}

/// Load the optional `--default-config` once at startup, fully resolved, as a
/// merge base `Value`.
async fn load_default_base(config: &ServeConfig) -> CliResult<Option<Value>> {
    match &config.default_config_path {
        None => Ok(None),
        Some(path) => {
            let cfg = crate::config::PipelineConfig::from_path_async(path).await?;
            Ok(Some(serde_json::to_value(&cfg).map_err(|e| {
                CliError::Serve(format!("serializing --default-config: {e}"))
            })?))
        }
    }
}

/// How often the background maintenance task purges expired history.
///
/// A quarter of the shorter of the two retention windows, clamped to
/// `[60s, 1h]`: frequent enough to bound store growth (and to honour a short
/// `idempotency_retention`) without churning on the multi-day default
/// `retain_terminal_runs`.
fn purge_interval(retain_terminal: Duration, idem_retention: Duration) -> Duration {
    (retain_terminal.min(idem_retention) / 4)
        .clamp(Duration::from_secs(60), Duration::from_secs(3600))
}

/// Background history-maintenance loop: every `period`, drop terminal run
/// records older than `retain` and expired idempotency claims, until `shutdown`
/// fires. Without this, the history store (in-memory `DashMap`s or the SQL
/// `faucet_serve_runs` / `faucet_serve_idem` tables) grows without bound for the
/// life of the process and the `--retain-terminal-runs-secs` /
/// `--idempotency-retention-secs` knobs are inert (audit #146 C4).
pub(crate) async fn maintenance_loop(
    history: Arc<dyn RunHistory>,
    retain: Duration,
    period: Duration,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate first tick so we don't purge at t=0
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => match history.purge_expired(retain).await {
                Ok(n) if n > 0 => {
                    tracing::info!(purged = n, "purged expired run records / idempotency claims")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "history purge_expired failed"),
            },
        }
    }
}

/// Boot the server: install observability, build state + router, bind, serve
/// until SIGTERM/SIGINT, then drain in-flight runs up to the grace window.
pub async fn serve(config: ServeConfig) -> CliResult<()> {
    let (prom, log_hub) = crate::serve::observability::install(&config.log_level);

    let history =
        crate::serve::history::connect(&config.history, config.idempotency_retention).await?;
    let recovered = history
        .recover_orphans()
        .await
        .map_err(|e| CliError::Serve(format!("history recovery: {e}")))?;
    if recovered > 0 {
        tracing::warn!(
            recovered,
            "marked orphaned non-terminal runs as failed (server restart)"
        );
    }
    let default_base = load_default_base(&config).await?;

    let shutdown = CancellationToken::new();
    let state = ServerState::new(
        &config,
        prom,
        shutdown.clone(),
        history,
        log_hub,
        default_base,
    );
    let app = build_router(state.clone(), &config);

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|e| CliError::Serve(format!("failed to bind {}: {e}", config.listen)))?;
    let local = listener
        .local_addr()
        .map_err(|e| CliError::Serve(e.to_string()))?;
    tracing::info!(listen = %local, "faucet serve listening");

    // Background history maintenance: bounds run-record / idempotency-claim
    // growth and makes the retention knobs effective (audit #146 C4).
    let purge_period = purge_interval(config.retain_terminal_runs, config.idempotency_retention);
    tracing::info!(
        interval_secs = purge_period.as_secs(),
        retain_secs = config.retain_terminal_runs.as_secs(),
        "history maintenance task started"
    );
    let maintenance = tokio::spawn(maintenance_loop(
        state.history(),
        config.retain_terminal_runs,
        purge_period,
        shutdown.clone(),
    ));

    // The HTTP graceful-shutdown future resolves on signal and stops accepting
    // new connections / drains in-flight HTTP — it does NOT cancel run tasks.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            wait_for_signal().await;
            tracing::info!("shutdown signal received; draining in-flight runs");
        })
        .await
        .map_err(|e| CliError::Serve(format!("server error: {e}")))?;

    // Now drain run tasks: wait up to the grace window, then cancel the rest.
    let drained =
        tokio::time::timeout(config.shutdown_grace, state.registry().wait_drained()).await;
    if drained.is_err() {
        let remaining = state.registry().in_flight();
        tracing::warn!(remaining, "grace window expired; cancelling in-flight runs");
        shutdown.cancel();
        // Give cancelled tasks a brief moment to write their terminal status.
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            state.registry().wait_drained(),
        )
        .await;
    }
    maintenance.abort();
    tracing::info!("faucet serve stopped");
    Ok(())
}

/// Resolve on SIGTERM (Unix) or Ctrl-C (any platform).
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use crate::serve::history::{RunRecord, RunStatus};
    use chrono::Utc;
    use std::collections::BTreeMap;

    #[test]
    fn purge_interval_is_quarter_of_shorter_window_clamped() {
        // Defaults (retain 7d, idem 1d) → min 1d, /4 = 6h → clamped to the 1h cap.
        assert_eq!(
            purge_interval(Duration::from_secs(604_800), Duration::from_secs(86_400)),
            Duration::from_secs(3600)
        );
        // A short idempotency window drives a faster cadence (but never below 60s).
        assert_eq!(
            purge_interval(Duration::from_secs(604_800), Duration::from_secs(120)),
            Duration::from_secs(60)
        );
        // Both tiny → the 60s floor.
        assert_eq!(
            purge_interval(Duration::from_secs(1), Duration::from_secs(1)),
            Duration::from_secs(60)
        );
        // A 40-minute window lands inside the range: 2400/4 = 600s.
        assert_eq!(
            purge_interval(Duration::from_secs(2400), Duration::from_secs(2400)),
            Duration::from_secs(600)
        );
    }

    #[tokio::test]
    async fn maintenance_loop_purges_expired_terminal_runs() {
        let history: Arc<dyn RunHistory> = Arc::new(MemoryHistory::new(Duration::from_secs(60)));

        // An old terminal record (eligible for purge with retain=0) and a
        // non-terminal one (must be kept).
        let mut old = RunRecord::queued(
            "old".into(),
            None,
            BTreeMap::new(),
            None,
            Utc::now() - chrono::Duration::seconds(10),
        );
        old.status = RunStatus::Completed;
        old.finished_at = Some(Utc::now() - chrono::Duration::seconds(10));
        history.upsert(&old).await.unwrap();
        let live = RunRecord::queued("live".into(), None, BTreeMap::new(), None, Utc::now());
        history.upsert(&live).await.unwrap();

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(maintenance_loop(
            history.clone(),
            Duration::ZERO,            // retain=0 → every terminal record is expired
            Duration::from_millis(10), // fast tick for the test
            shutdown.clone(),
        ));

        // Allow several ticks (the first is consumed at t=0).
        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        let _ = handle.await;

        assert!(
            history.get("old").await.unwrap().is_none(),
            "expired terminal run should have been purged by the maintenance loop"
        );
        assert!(
            history.get("live").await.unwrap().is_some(),
            "non-terminal run must be kept"
        );
    }
}
