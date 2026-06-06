//! Glue between the CLI config and `faucet-lineage`: builds the shared emitter
//! and maps resolved transform specs onto column-lineage ops.

use crate::config::TransformSpec;
use faucet_lineage::{ColumnOp, LineageConfig, LineageEmitter};
use faucet_core::FaucetError;
use std::sync::Arc;

/// Build the shared emitter from the parsed config. Returns `Ok(None)` when no
/// `lineage:` block is present.
pub fn build_emitter(cfg: Option<&LineageConfig>) -> Result<Option<Arc<LineageEmitter>>, FaucetError> {
    match cfg {
        Some(c) => Ok(Some(LineageEmitter::new(c.clone())?)),
        None => Ok(None),
    }
}

/// Best-effort transport reachability check (used by validate + doctor).
/// Never fails a run — returns a human-readable Ok/Err string.
pub async fn check_transport(cfg: &LineageConfig) -> Result<String, String> {
    match &cfg.transport {
        faucet_lineage::Transport::File { path } => {
            let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            if parent.exists() || tokio::fs::create_dir_all(parent).await.is_ok() {
                Ok(format!("file path writable: {}", path.display()))
            } else {
                Err(format!("cannot create parent dir for {}", path.display()))
            }
        }
        faucet_lineage::Transport::Http { url, .. } => {
            match reqwest::Client::new().head(url).send().await {
                Ok(_) => Ok(format!("http endpoint reachable: {url}")),
                Err(e) => Err(format!("http endpoint unreachable: {e}")),
            }
        }
        #[cfg(feature = "lineage-kafka")]
        faucet_lineage::Transport::Kafka { brokers, .. } => {
            Ok(format!("kafka brokers configured: {brokers} (not probed)"))
        }
    }
}

/// Map the resolved transform chain onto column-lineage ops. Transforms that
/// change structure or rewrite keys (`flatten`, `explode`, `keys_case`,
/// `rename_keys`) and any unknown transform become `Opaque`, which makes
/// `faucet_lineage::derive` omit the column-lineage facet (never fabricated).
pub fn column_ops(specs: &[TransformSpec]) -> Vec<ColumnOp> {
    specs.iter().map(map_one).collect()
}

fn map_one(s: &TransformSpec) -> ColumnOp {
    match s.kind.as_str() {
        "cast" | "redact" | "value_case" | "spell_symbols" => ColumnOp::Identity,
        "select" => ColumnOp::Select(string_array(&s.config, "fields")),
        "drop" => ColumnOp::Drop(string_array(&s.config, "fields")),
        "set" => ColumnOp::Set(object_keys(&s.config, "values")),
        "rename_field" => ColumnOp::Rename(string_pairs(&s.config, "fields")),
        // structure-changing / key-rewriting / unknown
        _ => ColumnOp::Opaque,
    }
}

fn string_array(config: &serde_json::Value, key: &str) -> Vec<String> {
    config.get(key).and_then(|v| v.as_array()).map(|a| {
        a.iter().filter_map(|x| x.as_str().map(String::from)).collect()
    }).unwrap_or_default()
}

fn object_keys(config: &serde_json::Value, key: &str) -> Vec<String> {
    config.get(key).and_then(|v| v.as_object()).map(|m| {
        m.keys().cloned().collect()
    }).unwrap_or_default()
}

fn string_pairs(config: &serde_json::Value, key: &str) -> Vec<(String, String)> {
    config.get(key).and_then(|v| v.as_object()).map(|m| {
        m.iter().filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string()))).collect()
    }).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TransformSpec;
    use serde_json::json;

    fn spec(kind: &str, config: serde_json::Value) -> TransformSpec {
        TransformSpec { kind: kind.into(), config }
    }

    #[test]
    fn maps_explicit_mapping_transforms() {
        let specs = vec![
            spec("rename_field", json!({"fields": {"a": "b"}})),
            spec("select", json!({"fields": ["b", "c"]})),
            spec("cast", json!({"fields": {"b": "integer"}})),
        ];
        let ops = column_ops(&specs);
        assert!(matches!(ops[0], faucet_lineage::ColumnOp::Rename(_)));
        assert!(matches!(ops[1], faucet_lineage::ColumnOp::Select(_)));
        assert!(matches!(ops[2], faucet_lineage::ColumnOp::Identity));
    }

    #[test]
    fn maps_structure_changing_to_opaque() {
        for k in ["flatten", "explode", "keys_case", "rename_keys", "weird_custom"] {
            let ops = column_ops(&[spec(k, json!({}))]);
            assert!(matches!(ops[0], faucet_lineage::ColumnOp::Opaque), "{k}");
        }
    }
}
