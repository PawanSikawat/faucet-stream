//! Shared, cheaply-cloneable server state handed to every handler via
//! `axum::extract::State`. Phase 1 holds auth + the Prometheus render handle +
//! the server-wide shutdown token. Later phases add the run registry + history.

use crate::serve::config::{AuthMode, ServeConfig};
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ServerState {
    inner: Arc<Inner>,
}

struct Inner {
    auth: AuthMode,
    prometheus: Option<PrometheusHandle>,
    shutdown: CancellationToken,
}

impl ServerState {
    pub fn new(
        config: &ServeConfig,
        prometheus: Option<PrometheusHandle>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                auth: config.auth.clone(),
                prometheus,
                shutdown,
            }),
        }
    }

    /// The expected bearer token, or `None` when started with `--no-auth`.
    pub fn auth_token(&self) -> Option<&str> {
        match &self.inner.auth {
            AuthMode::Token(t) => Some(t),
            AuthMode::None => None,
        }
    }

    /// Render the current Prometheus exposition, or `None` if no recorder handle
    /// was installed (e.g. a second server in the same test process).
    pub fn render_metrics(&self) -> Option<String> {
        self.inner.prometheus.as_ref().map(|h| h.render())
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::config::HistoryBackendSpec;
    use std::time::Duration;

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
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
        }
    }

    #[test]
    fn auth_token_reflects_mode() {
        let s = ServerState::new(&cfg(AuthMode::Token("x".into())), None, CancellationToken::new());
        assert_eq!(s.auth_token(), Some("x"));
        let s = ServerState::new(&cfg(AuthMode::None), None, CancellationToken::new());
        assert_eq!(s.auth_token(), None);
    }

    #[test]
    fn render_metrics_none_without_handle() {
        let s = ServerState::new(&cfg(AuthMode::None), None, CancellationToken::new());
        assert!(s.render_metrics().is_none());
    }
}
