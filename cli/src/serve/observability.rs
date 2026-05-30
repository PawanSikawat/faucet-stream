//! serve-owned observability: install the Prometheus recorder (returning a
//! render handle for the `/metrics` route) and a tracing subscriber whose fmt
//! layer routes through the secret-redacting writer. Both are process-global and
//! set-once; a second install in the same process is tolerated (returns no
//! handle / leaves the existing subscriber).

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Install the recorder + tracing subscriber. Returns the render handle when
/// this call installed the recorder; `None` if a recorder was already present.
pub fn install(level: &str) -> Option<PrometheusHandle> {
    let handle = match PrometheusBuilder::new().install_recorder() {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!("Prometheus recorder already installed or failed: {e}");
            None
        }
    };
    // Register faucet_build_info into whatever recorder is now global.
    faucet_core::register_build_info();

    install_subscriber(level);
    handle
}

#[cfg(feature = "observability")]
fn install_subscriber(level: &str) {
    use crate::secrets::registry::RedactingMakeWriter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(RedactingMakeWriter));
    // A later phase adds the per-run SSE log layer here.
    if registry.try_init().is_err() {
        tracing::warn!("tracing subscriber already installed; continuing");
    }
}

#[cfg(not(feature = "observability"))]
fn install_subscriber(_level: &str) {}
