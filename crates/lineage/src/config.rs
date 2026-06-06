//! User-facing `lineage:` configuration types.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Top-level `lineage:` block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LineageConfig {
    /// Lineage format. Only `openlineage` in v1.
    #[serde(rename = "type", default)]
    pub kind: LineageKind,
    /// OpenLineage namespace for the emitted job/datasets.
    pub namespace: String,
    /// Where events are sent.
    pub transport: Transport,
    /// Job-name template. `${name}` / `${row_id}` / `${now.*}` are resolved
    /// per matrix row at run time.
    #[serde(default = "default_job_name")]
    pub job_name: String,
    /// Optional parent job (orchestrator linkage).
    #[serde(default)]
    pub parent_job: Option<ParentJob>,
    /// Emit column-level lineage facets where the transform chain is mappable.
    #[serde(default)]
    pub include_column_lineage: bool,
    /// Emit dataset schema facets (inferred from a record sample).
    #[serde(default)]
    pub include_schema_facet: bool,
    /// Emit the resolved config body as a SourceCode facet. Off by default —
    /// the resolved config may contain secrets; enabling warns.
    #[serde(default)]
    pub include_source_code_facet: bool,
    /// Which lifecycle events to emit.
    #[serde(default)]
    pub emit_on: EmitOn,
    /// Max records sampled for schema/column facets. Default 100.
    #[serde(default = "default_sample")]
    pub sample_records: usize,
    /// RUNNING heartbeat interval. Default 30s; only used when `emit_on.running`.
    #[serde(with = "faucet_core::config::duration_secs", default = "default_heartbeat")]
    #[schemars(with = "u64")]
    pub heartbeat_interval: Duration,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineageKind {
    #[default]
    Openlineage,
}

/// Lineage event transport.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Transport {
    /// POST each event to an OpenLineage HTTP endpoint (e.g. Marquez).
    Http {
        url: String,
        #[serde(with = "faucet_core::config::duration_secs", default = "default_http_timeout")]
        #[schemars(with = "u64")]
        timeout_secs: Duration,
        #[serde(default)]
        auth: Option<HttpAuth>,
    },
    /// Append each event as one JSON line to a local file.
    File { path: PathBuf },
    /// Produce each event as a JSON message to a Kafka topic.
    #[cfg(feature = "transport-kafka")]
    Kafka { brokers: String, topic: String },
}

/// HTTP transport auth.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HttpAuth {
    Bearer { token: String },
}

/// Parent-job linkage (Airflow, Dagster, …).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParentJob {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub run_id: Option<String>,
}

/// Per-event emit toggles.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmitOn {
    #[serde(default = "default_true")]
    pub start: bool,
    #[serde(default)]
    pub running: bool,
    #[serde(default = "default_true")]
    pub complete: bool,
    #[serde(default = "default_true")]
    pub fail: bool,
    #[serde(default = "default_true")]
    pub abort: bool,
}

impl Default for EmitOn {
    fn default() -> Self {
        Self { start: true, running: false, complete: true, fail: true, abort: true }
    }
}

fn default_true() -> bool { true }
fn default_job_name() -> String { "${name}::${row_id}".to_string() }
fn default_sample() -> usize { 100 }
fn default_heartbeat() -> Duration { Duration::from_secs(30) }
fn default_http_timeout() -> Duration { Duration::from_secs(10) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_http_transport_with_defaults() {
        let json = serde_json::json!({
            "type": "openlineage",
            "namespace": "prod.warehouse",
            "transport": { "type": "http", "url": "https://marquez/api/v1/lineage" }
        });
        let cfg: LineageConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.namespace, "prod.warehouse");
        assert_eq!(cfg.job_name, "${name}::${row_id}");
        assert!(cfg.emit_on.start && cfg.emit_on.complete && !cfg.emit_on.running);
        assert_eq!(cfg.sample_records, 100);
        matches!(cfg.transport, Transport::Http { .. });
    }

    #[test]
    fn deserializes_file_transport() {
        let json = serde_json::json!({
            "namespace": "n",
            "transport": { "type": "file", "path": "/tmp/ol.jsonl" }
        });
        let cfg: LineageConfig = serde_json::from_value(json).unwrap();
        matches!(cfg.transport, Transport::File { .. });
    }

    #[test]
    fn rejects_unknown_field() {
        let json = serde_json::json!({
            "namespace": "n",
            "transport": { "type": "file", "path": "/tmp/ol.jsonl" },
            "bogus": true
        });
        assert!(serde_json::from_value::<LineageConfig>(json).is_err());
    }

    #[test]
    fn schema_generates() {
        let _ = schemars::schema_for!(LineageConfig);
    }
}
