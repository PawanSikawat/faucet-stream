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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
