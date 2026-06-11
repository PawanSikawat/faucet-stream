//! Serde config types for the `--triggers` file. Pure data + `JsonSchema`; no IO.
//! Validation lives in `compiled.rs`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level `--triggers` document.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TriggersFile {
    /// Schema version; must be `1`.
    pub version: u32,
    pub triggers: Vec<TriggerSpec>,
}

/// One configured trigger.
// Note: `deny_unknown_fields` is intentionally absent here — serde does not
// support it on structs that contain a `#[serde(flatten)]` field.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TriggerSpec {
    /// Unique name; used in metrics, idempotency keys, and the webhook path.
    pub name: String,
    /// Spawn this trigger? Default `true`.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// The pipeline to enqueue when the trigger fires (path string or inline doc).
    pub config: PipelineRef,
    /// Optional run-shaping.
    #[serde(default)]
    pub run: RunTemplate,
    /// Coalesce events within this many seconds into one fire. Default 0.
    #[serde(default)]
    pub debounce_secs: u64,
    /// Type-specific settings.
    #[serde(flatten)]
    pub kind: TriggerKind,
}

/// A pipeline reference: a path to a config file, OR an inline config document.
/// Untagged so YAML `config: ./x.yaml` and `config: { pipeline: … }` both parse.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PipelineRef {
    Path(String),
    Inline(serde_json::Value),
}

/// Trigger type + its settings. Internally-tagged on `type` — variant fields
/// sit flat alongside `type` in the serialized form.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerKind {
    ObjectArrival {
        store: StoreSpec,
        #[serde(default = "default_poll_secs")]
        poll_interval_secs: u64,
        #[serde(default)]
        mode: ArrivalMode,
        #[serde(default)]
        start_at: StartAt,
    },
    Webhook {
        #[serde(default = "default_webhook_methods")]
        methods: Vec<String>,
        /// Header whose value is used as the idempotency key (else per-request UUID).
        #[serde(default)]
        dedupe_header: Option<String>,
    },
    QueueDepth {
        queue: QueueSpec,
        #[serde(default = "default_threshold")]
        threshold: u64,
        #[serde(default = "default_poll_secs")]
        poll_interval_secs: u64,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArrivalMode {
    #[default]
    PerObject,
    Batch,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StartAt {
    #[default]
    Now,
    Beginning,
}

/// Object-store connection for `object_arrival`. Internally-tagged on `type`;
/// variant fields sit flat alongside `type`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoreSpec {
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
    },
    Gcs {
        bucket: String,
        #[serde(default)]
        prefix: Option<String>,
    },
}

/// Queue connection for `queue_depth`. Internally-tagged on `type`;
/// variant fields sit flat alongside `type`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueueSpec {
    Redis {
        url: String,
        key: String,
        #[serde(default)]
        kind: RedisQueueKind,
    },
    Kafka {
        brokers: String,
        topic: String,
        group: String,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedisQueueKind {
    #[default]
    List,
    Stream,
}

/// Optional run-shaping applied to the enqueued run.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunTemplate {
    /// Run name template; `{field}` tokens resolve from trigger fields
    /// (`name`, `type`, `object_key`, `bucket`, `queue`, `depth`).
    #[serde(default)]
    pub name: Option<String>,
    /// Static labels merged with the auto-derived trigger labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Per-run timeout in seconds.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

fn default_true() -> bool {
    true
}
fn default_poll_secs() -> u64 {
    30
}
fn default_threshold() -> u64 {
    1
}
fn default_webhook_methods() -> Vec<String> {
    vec!["POST".to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_object_arrival_with_defaults() {
        let yaml = r#"
version: 1
triggers:
  - name: drop
    type: object_arrival
    config: ./pipelines/load.yaml
    store: { type: s3, bucket: b, prefix: incoming/ }
"#;
        let f: TriggersFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(f.version, 1);
        assert_eq!(f.triggers.len(), 1);
        let t = &f.triggers[0];
        assert_eq!(t.name, "drop");
        assert!(t.enabled);
        assert!(matches!(t.config, PipelineRef::Path(ref p) if p == "./pipelines/load.yaml"));
        match &t.kind {
            TriggerKind::ObjectArrival { poll_interval_secs, mode, start_at, .. } => {
                assert_eq!(*poll_interval_secs, 30);
                assert!(matches!(mode, ArrivalMode::PerObject));
                assert!(matches!(start_at, StartAt::Now));
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn parses_inline_pipeline_and_webhook_and_queue() {
        let yaml = r#"
version: 1
triggers:
  - name: hook
    type: webhook
    config: { pipeline: { sources: {}, sinks: {} } }
    dedupe_header: Idempotency-Key
  - name: drain
    type: queue_depth
    config: ./drain.yaml
    queue: { type: redis, url: "redis://x", key: jobs, kind: stream }
    threshold: 5
"#;
        let f: TriggersFile = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(f.triggers[0].config, PipelineRef::Inline(_)));
        match &f.triggers[0].kind {
            TriggerKind::Webhook { methods, dedupe_header } => {
                assert_eq!(methods, &vec!["POST".to_string()]);
                assert_eq!(dedupe_header.as_deref(), Some("Idempotency-Key"));
            }
            _ => panic!("wrong kind"),
        }
        match &f.triggers[1].kind {
            TriggerKind::QueueDepth { threshold, queue, .. } => {
                assert_eq!(*threshold, 5);
                assert!(matches!(queue, QueueSpec::Redis { kind: RedisQueueKind::Stream, .. }));
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let yaml = "version: 1\ntriggers: []\nbogus: 1\n";
        assert!(serde_yaml::from_str::<TriggersFile>(yaml).is_err());
    }
}
