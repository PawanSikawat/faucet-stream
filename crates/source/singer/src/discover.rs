//! Singer catalog discovery: run `<tap> --config <tmp> --discover` and return
//! the catalog it prints. Used by `faucet init --source singer --discover`.

use std::process::Stdio;

use faucet_core::{FaucetError, Value};
use tokio::process::Command;

use crate::config::SingerSourceConfig;
use crate::process::write_temp;

/// Run the tap in discovery mode and parse its stdout as a Singer catalog.
///
/// Spawns `<executable> --config <tmp> --discover <args…>`, materializing
/// `tap_config` to a private temp file. A non-zero exit or non-JSON output is a
/// [`FaucetError::Source`] (with the tap's stderr on failure).
pub async fn discover(config: &SingerSourceConfig) -> Result<Value, FaucetError> {
    let config_file = write_temp("config", &config.tap_config)?;

    let output = Command::new(&config.executable)
        .arg("--config")
        .arg(config_file.path())
        .arg("--discover")
        .args(&config.args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| {
            FaucetError::Source(format!(
                "failed to spawn tap '{}' for discovery: {e}",
                config.executable
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(FaucetError::Source(format!(
            "tap discovery exited with status {}; stderr:\n{}",
            output.status,
            stderr.trim()
        )));
    }

    serde_json::from_slice(&output.stdout)
        .map_err(|e| FaucetError::Source(format!("tap discovery output is not valid JSON: {e}")))
}

/// Extract the stream ids from a Singer catalog (`tap_stream_id`, falling back
/// to `stream`), in catalog order.
pub fn catalog_stream_ids(catalog: &Value) -> Vec<String> {
    catalog
        .get("streams")
        .and_then(Value::as_array)
        .map(|streams| {
            streams
                .iter()
                .filter_map(|s| {
                    s.get("tap_stream_id")
                        .or_else(|| s.get("stream"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_stream_ids_in_order() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "users", "stream": "users" },
                { "stream": "orders" },
                { "tap_stream_id": "audit_log" }
            ]
        });
        assert_eq!(
            catalog_stream_ids(&catalog),
            vec!["users", "orders", "audit_log"]
        );
        assert!(catalog_stream_ids(&json!({})).is_empty());
    }
}
