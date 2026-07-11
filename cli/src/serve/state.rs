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
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
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
    /// The `--default-config` merge base, hot-reloadable via `POST /v1/reload`
    /// (#198). An `RwLock` so a reload can swap it while runs read it.
    default_base: RwLock<Option<Value>>,
    /// Path the default-config was loaded from, so a reload can re-read it.
    default_config_path: Option<PathBuf>,
    idempotency_retention: Duration,
    probe_timeout: Duration,
    cluster: crate::serve::cluster::ClusterHandle,
    #[cfg(feature = "triggers")]
    triggers: crate::serve::triggers::health::TriggersHandle,
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
        #[cfg(feature = "triggers")] triggers: crate::serve::triggers::health::TriggersHandle,
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
                default_base: RwLock::new(default_base),
                default_config_path: config.default_config_path.clone(),
                idempotency_retention: config.idempotency_retention,
                probe_timeout: config.probe_timeout,
                cluster: crate::serve::cluster::ClusterHandle::from_config(config),
                #[cfg(feature = "triggers")]
                triggers,
            }),
        }
    }

    pub fn auth_token(&self) -> Option<&str> {
        match &self.inner.auth {
            AuthMode::Token(t) => Some(t),
            AuthMode::Rbac(_) | AuthMode::None => None,
        }
    }

    /// The configured authentication mode (bearer resolution + RBAC).
    pub fn auth_mode(&self) -> &AuthMode {
        &self.inner.auth
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

    /// A snapshot of the `--default-config` merge base (cloned under the read
    /// lock, so a concurrent hot reload can't tear it).
    pub fn default_base(&self) -> Option<Value> {
        self.inner.default_base.read().unwrap().clone()
    }

    /// Path the default-config was loaded from (`None` when `--default-config`
    /// was not passed).
    pub fn default_config_path(&self) -> Option<&PathBuf> {
        self.inner.default_config_path.as_ref()
    }

    /// Atomically swap the `--default-config` merge base (hot reload, #198).
    pub fn set_default_base(&self, base: Option<Value>) {
        *self.inner.default_base.write().unwrap() = base;
    }

    pub fn idempotency_retention(&self) -> Duration {
        self.inner.idempotency_retention
    }

    pub fn probe_timeout(&self) -> Duration {
        self.inner.probe_timeout
    }

    pub fn cluster(&self) -> &crate::serve::cluster::ClusterHandle {
        &self.inner.cluster
    }

    #[cfg(feature = "triggers")]
    pub fn triggers(&self) -> &crate::serve::triggers::health::TriggersHandle {
        &self.inner.triggers
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
            cluster: crate::serve::cluster::ClusterConfig::disabled(),
            triggers_path: None,
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
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
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
    fn default_base_swaps_atomically() {
        let s = state(AuthMode::None);
        assert!(s.default_base().is_none());
        assert!(s.default_config_path().is_none());
        s.set_default_base(Some(serde_json::json!({"version": 1})));
        assert_eq!(s.default_base(), Some(serde_json::json!({"version": 1})));
        s.set_default_base(None);
        assert!(s.default_base().is_none());
    }

    // The reload handler is a no-op (200 `reloaded:false`) when the server was
    // started without `--default-config`.
    #[tokio::test]
    async fn reload_handler_noop_without_default_config() {
        let s = state(AuthMode::None);
        let axum::Json(body) = crate::serve::handlers::reload::reload(axum::extract::State(s))
            .await
            .expect("reload ok");
        assert_eq!(body["reloaded"], serde_json::json!(false));
    }

    #[test]
    fn registry_starts_empty() {
        let s = state(AuthMode::None);
        assert_eq!(s.registry().queued(), 0);
        assert_eq!(s.registry().in_flight(), 0);
    }
}
