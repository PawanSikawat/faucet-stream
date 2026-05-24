//! Idempotent global installer for the Prometheus recorder and a
//! `tracing-subscriber`. Safe to call more than once; subsequent calls warn
//! and continue rather than panicking. Port-in-use becomes a typed error.

use thiserror::Error;

/// Configuration for `install_observability`. Either or both sections may be
/// `None`; unset sections install nothing.
#[derive(Debug, Clone, Default)]
pub struct ObservabilityConfig {
    pub prometheus: Option<PrometheusConfig>,
    pub tracing: Option<TracingConfig>,
}

#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    /// `host:port` to bind a `/metrics` HTTP endpoint. Recommended:
    /// `127.0.0.1:9464`.
    pub listen: String,
    /// Histogram bucket overrides (in seconds). When `None`, sensible defaults
    /// apply (0.001..300s spanning sub-ms through five-minute durations).
    pub buckets: Option<Vec<f64>>,
}

#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// `EnvFilter`-style directive, e.g. `"info"` or `"faucet_core=debug,info"`.
    pub level: String,
}

/// Report from `install_observability` so callers can log what actually
/// happened (recorder installed vs. already-installed vs. disabled).
#[derive(Debug, Clone, Default)]
pub struct InstallReport {
    pub prometheus_listen: Option<String>,
    pub prometheus_already_installed: bool,
    pub tracing_already_installed: bool,
}

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("failed to bind Prometheus listener at {listen}: {source}")]
    PrometheusBind {
        listen: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to install Prometheus recorder: {0}")]
    PrometheusInstall(String),
}

/// Install observability if requested. Always returns; never panics.
///
/// Behavior:
/// - If `prometheus` is set, builds a `PrometheusBuilder` and installs the
///   recorder + HTTP `/metrics` endpoint at the configured listen address.
///   Already-installed recorder is logged via `tracing::warn!` and continues.
///   Listen-parse / bind failures return `InstallError`.
/// - If `tracing` is set, installs a `tracing-subscriber` registry with the
///   given env-filter directive as the default subscriber. Already-set-default
///   is logged via `tracing::warn!` and continues.
#[cfg(feature = "observability-install")]
pub fn install_observability(cfg: &ObservabilityConfig) -> Result<InstallReport, InstallError> {
    let mut report = InstallReport::default();

    if let Some(p) = cfg.prometheus.as_ref() {
        use metrics_exporter_prometheus::PrometheusBuilder;

        let listen: std::net::SocketAddr =
            p.listen.parse().map_err(|e: std::net::AddrParseError| {
                InstallError::PrometheusBind {
                    listen: p.listen.clone(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()),
                }
            })?;

        const DEFAULT_BUCKETS: &[f64] = &[
            0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0,
        ];
        let buckets = p.buckets.as_deref().unwrap_or(DEFAULT_BUCKETS);

        let builder = PrometheusBuilder::new()
            .with_http_listener(listen)
            .set_buckets(buckets)
            .map_err(|e| InstallError::PrometheusInstall(e.to_string()))?;

        match builder.install() {
            Ok(()) => report.prometheus_listen = Some(p.listen.clone()),
            Err(e) => {
                let msg = e.to_string();
                // metrics-exporter-prometheus surfaces "already initialized"
                // via BuildError::FailedToSetGlobalRecorder(SetRecorderError)
                // whose Display is:
                //   "attempted to set a recorder after the metrics system was
                //    already initialized"
                if msg.to_lowercase().contains("already")
                    && msg.to_lowercase().contains("initialized")
                {
                    tracing::warn!("Prometheus recorder already installed; continuing");
                    report.prometheus_already_installed = true;
                } else {
                    return Err(InstallError::PrometheusInstall(msg));
                }
            }
        }
    }

    if let Some(t) = cfg.tracing.as_ref() {
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let filter = EnvFilter::try_new(&t.level).unwrap_or_else(|_| EnvFilter::new("info"));
        let registry = tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer());
        if registry.try_init().is_err() {
            // Some other code path has already set a global default. Log and
            // continue — observability still works through the previously-
            // installed subscriber.
            tracing::warn!("tracing subscriber already installed; continuing");
            report.tracing_already_installed = true;
        }
    }

    // Register build_info after any Prometheus install attempt — set!() into
    // a not-yet-installed recorder is a no-op, so we order it last.
    register_build_info();

    Ok(report)
}

/// Non-`observability-install` stub. Returns an empty report, never panics.
#[cfg(not(feature = "observability-install"))]
pub fn install_observability(_cfg: &ObservabilityConfig) -> Result<InstallReport, InstallError> {
    register_build_info();
    Ok(InstallReport::default())
}

/// Register the `faucet_build_info{version}` gauge (set to 1) under the
/// currently-installed `metrics` recorder. Safe to call from any code path
/// that wants to ensure the gauge is set; `install_observability` invokes
/// this automatically. Gauges are naturally idempotent under the `metrics`
/// model — repeat calls just re-set the same value.
///
/// The version label is `CARGO_PKG_VERSION` of `faucet-core` — matches the
/// crate that owns the observability layer. Dashboards `group_left` the gauge
/// onto every other metric to annotate panels with the running version.
pub fn register_build_info() {
    metrics::gauge!(
        "faucet_build_info",
        "version" => env!("CARGO_PKG_VERSION"),
    )
    .set(1.0);
}

#[cfg(all(test, feature = "observability-install"))]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn no_config_returns_empty_report() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let r = install_observability(&ObservabilityConfig::default()).unwrap();
        assert!(r.prometheus_listen.is_none());
        assert!(!r.prometheus_already_installed);
        assert!(!r.tracing_already_installed);
    }

    #[test]
    fn malformed_listen_returns_bind_error() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = ObservabilityConfig {
            prometheus: Some(PrometheusConfig {
                listen: "not-a-socket".into(),
                buckets: None,
            }),
            tracing: None,
        };
        match install_observability(&cfg) {
            Err(InstallError::PrometheusBind { .. }) => {}
            other => panic!("expected PrometheusBind error, got {other:?}"),
        }
    }
}
