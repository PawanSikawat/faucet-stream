//! Shared, cheaply-cloneable server state handed to every handler via
//! `axum::extract::State`. Holds auth, the Prometheus render handle, the
//! server-wide shutdown token, the run registry, the execution semaphore, the
//! run-history backend, and the `--default-config` merge base.

use crate::serve::config::{AuthMode, ServeConfig};
use crate::serve::history::RunHistory;
use crate::serve::logs::LogHub;
use crate::serve::registry::Registry;
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ServerState {
    inner: Arc<Inner>,
}

struct Inner {
    auth: AuthMode,
    prometheus: Option<PrometheusHandle>,
    shutdown: CancellationToken,
    registry: Registry,
    semaphore: Arc<Semaphore>,
    history: Arc<dyn RunHistory>,
    log_hub: LogHub,
    default_base: Option<Value>,
    idempotency_retention: Duration,
    probe_timeout: Duration,
}

impl ServerState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &ServeConfig,
        prometheus: Option<PrometheusHandle>,
        shutdown: CancellationToken,
        history: Arc<dyn RunHistory>,
        log_hub: LogHub,
        default_base: Option<Value>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                auth: config.auth.clone(),
                prometheus,
                shutdown,
                registry: Registry::new(config.max_queued_runs),
                semaphore: Arc::new(Semaphore::new(config.max_concurrent_runs)),
                history,
                log_hub,
                default_base,
                idempotency_retention: config.idempotency_retention,
                probe_timeout: config.probe_timeout,
            }),
        }
    }

    pub fn auth_token(&self) -> Option<&str> {
        match &self.inner.auth {
            AuthMode::Token(t) => Some(t),
            AuthMode::None => None,
        }
    }

    pub fn render_metrics(&self) -> Option<String> {
        self.inner.prometheus.as_ref().map(|h| h.render())
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    pub fn registry(&self) -> &Registry {
        &self.inner.registry
    }

    pub fn semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.inner.semaphore)
    }

    pub fn history(&self) -> Arc<dyn RunHistory> {
        Arc::clone(&self.inner.history)
    }

    /// The per-run log buffer registry shared with the tracing [`LogHub`] layer.
    pub fn log_hub(&self) -> &LogHub {
        &self.inner.log_hub
    }

    pub fn default_base(&self) -> Option<&Value> {
        self.inner.default_base.as_ref()
    }

    pub fn idempotency_retention(&self) -> Duration {
        self.inner.idempotency_retention
    }

    pub fn probe_timeout(&self) -> Duration {
        self.inner.probe_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::config::HistoryBackendSpec;
    use crate::serve::history::memory::MemoryHistory;

    fn cfg(auth: AuthMode) -> ServeConfig {
        ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            auth,
            max_concurrent_runs: 4,
            max_queued_runs: 32,
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
            ui_enabled: true,
        }
    }

    fn state(auth: AuthMode) -> ServerState {
        use crate::serve::logs::LogHub;
        let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
        ServerState::new(
            &cfg(auth),
            None,
            CancellationToken::new(),
            history,
            LogHub::new(),
            None,
        )
    }

    #[test]
    fn auth_token_reflects_mode() {
        assert_eq!(state(AuthMode::Token("x".into())).auth_token(), Some("x"));
        assert_eq!(state(AuthMode::None).auth_token(), None);
    }

    #[test]
    fn render_metrics_none_without_handle() {
        assert!(state(AuthMode::None).render_metrics().is_none());
    }

    #[test]
    fn registry_starts_empty() {
        let s = state(AuthMode::None);
        assert_eq!(s.registry().queued(), 0);
        assert_eq!(s.registry().in_flight(), 0);
    }
}
