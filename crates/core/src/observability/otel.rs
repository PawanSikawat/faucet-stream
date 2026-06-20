//! OpenTelemetry (OTLP) export of traces + metrics (#201).
//!
//! `OtelConfig` and its enums are pure data (no opentelemetry types) so they
//! compile unconditionally. Everything that touches the OTel SDK is gated on
//! `#[cfg(feature = "otel")]` further down this file.

use serde::{Deserialize, Serialize};

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OtelProtocol {
    /// gRPC over tonic (default OTLP port 4317). Must be initialised inside a
    /// tokio runtime.
    #[default]
    Grpc,
    /// HTTP/protobuf (default OTLP port 4318).
    Http,
}

/// A telemetry signal that can be exported over OTLP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtelSignal {
    Traces,
    Metrics,
}

fn default_protocol() -> OtelProtocol {
    OtelProtocol::Grpc
}
fn default_sample_ratio() -> f64 {
    1.0
}
fn default_export() -> Vec<OtelSignal> {
    vec![OtelSignal::Traces, OtelSignal::Metrics]
}
fn default_service_name() -> String {
    "faucet".to_string()
}
fn default_timeout_secs() -> u64 {
    10
}
fn default_metric_interval_secs() -> u64 {
    60
}

/// Pure-data OTLP export configuration. Contains no opentelemetry types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtelConfig {
    /// Collector endpoint. When empty, resolved per protocol
    /// (`http://localhost:4317` grpc / `:4318` http) by [`OtelConfig::resolve_endpoint`].
    #[serde(default)]
    pub endpoint: String,
    #[serde(default = "default_protocol")]
    pub protocol: OtelProtocol,
    /// Extra headers sent on every export (e.g. backend auth tokens).
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// Head-based trace sampling ratio, 0.0..=1.0.
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
    /// Which signals to export.
    #[serde(default = "default_export")]
    pub export: Vec<OtelSignal>,
    /// OTel resource `service.name`.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Per-export timeout (seconds).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Metric push interval (seconds).
    #[serde(default = "default_metric_interval_secs")]
    pub metric_interval_secs: u64,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            protocol: default_protocol(),
            headers: std::collections::HashMap::new(),
            sample_ratio: default_sample_ratio(),
            export: default_export(),
            service_name: default_service_name(),
            timeout_secs: default_timeout_secs(),
            metric_interval_secs: default_metric_interval_secs(),
        }
    }
}

impl OtelConfig {
    /// Whether the given signal is in the export list.
    pub fn exports(&self, signal: OtelSignal) -> bool {
        self.export.contains(&signal)
    }

    /// The collector endpoint, filling in the protocol-specific default when
    /// `endpoint` is empty.
    pub fn resolve_endpoint(&self) -> String {
        if !self.endpoint.is_empty() {
            return self.endpoint.clone();
        }
        match self.protocol {
            OtelProtocol::Grpc => "http://localhost:4317".to_string(),
            OtelProtocol::Http => "http://localhost:4318".to_string(),
        }
    }

    /// Validate ranges + endpoint URL at config-load time. Returns a message on
    /// the first problem.
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=1.0).contains(&self.sample_ratio) {
            return Err(format!(
                "otel.sample_ratio must be in 0.0..=1.0, got {}",
                self.sample_ratio
            ));
        }
        if self.timeout_secs == 0 {
            return Err("otel.timeout_secs must be > 0".to_string());
        }
        if self.metric_interval_secs == 0 {
            return Err("otel.metric_interval_secs must be > 0".to_string());
        }
        // resolve_endpoint() always yields a value; validate the effective URL.
        let ep = self.resolve_endpoint();
        url::Url::parse(&ep).map_err(|e| format!("otel.endpoint is not a valid URL ({ep}): {e}"))?;
        Ok(())
    }
}

/// Map an `opentelemetry*` tracing event target to a `signal` label for the
/// `faucet_otel_export_failures_total` counter. Pure — unit-testable without a
/// tracing `Context`.
pub(crate) fn otel_signal_label(target: &str) -> &'static str {
    if target.contains("metric") {
        "metrics"
    } else if target.contains("trace") || target.contains("span") {
        "traces"
    } else {
        "export"
    }
}

/// Register HELP text for the OTLP export metric. Called from
/// `install_observability` (a `describe!` into a not-yet-installed recorder is a
/// no-op, so ordering is forgiving).
pub fn describe() {
    metrics::describe_counter!(
        "faucet_otel_export_failures_total",
        "OTLP export attempts that failed (collector unreachable, serialization error, etc.)."
    );
}

#[cfg(feature = "otel")]
mod layer {
    use super::otel_signal_label;
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, Layer};

    /// A `tracing` layer that counts ERROR/WARN events emitted by the
    /// opentelemetry SDK (its only error channel in the 0.31 line, since
    /// `global::set_error_handler` was removed) into
    /// `faucet_otel_export_failures_total{signal}`.
    pub struct OtelErrorCountLayer;

    impl<S: Subscriber> Layer<S> for OtelErrorCountLayer {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let meta = event.metadata();
            let target = meta.target();
            if target.starts_with("opentelemetry")
                && matches!(*meta.level(), Level::ERROR | Level::WARN)
            {
                metrics::counter!(
                    "faucet_otel_export_failures_total",
                    "signal" => otel_signal_label(target),
                )
                .increment(1);
            }
        }
    }
}

#[cfg(feature = "otel")]
pub use layer::OtelErrorCountLayer;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_otel_error_classifies_signal_by_target() {
        assert_eq!(otel_signal_label("opentelemetry_sdk::metrics::periodic_reader"), "metrics");
        assert_eq!(otel_signal_label("opentelemetry_sdk::trace::span_processor"), "traces");
        assert_eq!(otel_signal_label("opentelemetry_otlp::exporter"), "export");
        assert_eq!(otel_signal_label("opentelemetry"), "export");
    }

    #[test]
    fn config_defaults_are_applied_from_empty_yaml() {
        let cfg: OtelConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.protocol, OtelProtocol::Grpc);
        assert_eq!(cfg.sample_ratio, 1.0);
        assert_eq!(cfg.export, vec![OtelSignal::Traces, OtelSignal::Metrics]);
        assert_eq!(cfg.service_name, "faucet");
        assert_eq!(cfg.timeout_secs, 10);
        assert_eq!(cfg.metric_interval_secs, 60);
        assert!(cfg.exports(OtelSignal::Traces));
        assert!(cfg.exports(OtelSignal::Metrics));
    }

    #[test]
    fn resolve_endpoint_defaults_per_protocol() {
        let mut cfg = OtelConfig::default();
        assert_eq!(cfg.resolve_endpoint(), "http://localhost:4317");
        cfg.protocol = OtelProtocol::Http;
        assert_eq!(cfg.resolve_endpoint(), "http://localhost:4318");
        cfg.endpoint = "http://collector:4317".into();
        assert_eq!(cfg.resolve_endpoint(), "http://collector:4317");
    }

    #[test]
    fn validate_rejects_bad_values() {
        let mut cfg = OtelConfig::default();
        assert!(cfg.validate().is_ok());

        cfg.sample_ratio = 1.5;
        assert!(cfg.validate().is_err());
        cfg.sample_ratio = -0.1;
        assert!(cfg.validate().is_err());
        cfg.sample_ratio = 0.5;
        assert!(cfg.validate().is_ok());

        cfg.timeout_secs = 0;
        assert!(cfg.validate().is_err());
        cfg.timeout_secs = 10;

        cfg.metric_interval_secs = 0;
        assert!(cfg.validate().is_err());
        cfg.metric_interval_secs = 60;

        cfg.endpoint = "not a url".into();
        assert!(cfg.validate().is_err());
    }
}
