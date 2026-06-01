//! Redis sink executor.

use crate::config::{RedisSinkConfig, RedisSinkType};
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;

/// A configured Redis sink that writes records to Redis data structures.
///
/// The connection is established once during construction and reused across
/// all `write_batch()` calls.
pub struct RedisSink {
    config: RedisSinkConfig,
    conn: redis::aio::MultiplexedConnection,
}

impl RedisSink {
    /// Create a new Redis sink from the given configuration.
    ///
    /// This opens a multiplexed async connection to Redis immediately.
    pub async fn new(config: RedisSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;

        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| FaucetError::Sink(format!("Redis connection failed: {e}")))?;

        Ok(Self { config, conn })
    }
}

#[async_trait]
impl faucet_core::Sink for RedisSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(RedisSinkConfig))
            .expect("schema serialization")
    }

    /// Non-mutating preflight probe: issue a Redis `PING` over the existing
    /// multiplexed connection (probe name `"ping"`).
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // MultiplexedConnection is cheaply cloneable; clone to satisfy &self.
        let mut conn = self.conn.clone();
        let started = std::time::Instant::now();
        let hint = "check the Redis url / that the server is reachable and accepting connections";

        let probe = match tokio::time::timeout(
            ctx.timeout,
            redis::cmd("PING").query_async::<String>(&mut conn),
        )
        .await
        {
            Ok(Ok(_)) => Probe::pass("ping", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint("ping", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("ping", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // MultiplexedConnection is cheaply cloneable (it shares the
        // underlying connection), so we clone to satisfy the &self receiver.
        let mut conn = self.conn.clone();
        let mut written = 0usize;

        // `batch_size = 0` is the "no batching" sentinel: pack the entire
        // upstream slice into a single Redis pipeline, preserving
        // `StreamPage` framing. Otherwise re-chunk into `batch_size`-sized
        // slices so each Redis pipeline stays near the recommended
        // ~1000-command working set.
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        // Process in chunks of batch_size using redis pipelines.
        for chunk in records.chunks(effective_chunk) {
            let mut pipe = redis::pipe();

            for record in chunk {
                match &self.config.sink_type {
                    RedisSinkType::List { key } => {
                        let serialized = serde_json::to_string(record).map_err(|e| {
                            FaucetError::Sink(format!("JSON serialization failed: {e}"))
                        })?;
                        pipe.cmd("RPUSH").arg(key.as_str()).arg(serialized);
                    }
                    RedisSinkType::Stream { key } => {
                        let fields = flatten_record_to_fields(record);
                        if fields.is_empty() {
                            // XADD requires at least one field.
                            let serialized = serde_json::to_string(record).map_err(|e| {
                                FaucetError::Sink(format!("JSON serialization failed: {e}"))
                            })?;
                            pipe.cmd("XADD")
                                .arg(key.as_str())
                                .arg("*")
                                .arg("_data")
                                .arg(serialized);
                        } else {
                            let mut cmd = redis::cmd("XADD");
                            cmd.arg(key.as_str()).arg("*");
                            for (field_name, field_value) in &fields {
                                cmd.arg(field_name.as_str()).arg(field_value.as_str());
                            }
                            pipe.add_command(cmd);
                        }
                    }
                    RedisSinkType::KeyValue { key_field } => {
                        let key = record
                            .get(key_field)
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .ok_or_else(|| {
                                FaucetError::Sink(format!("record missing key field '{key_field}'"))
                            })?;
                        let serialized = serde_json::to_string(record).map_err(|e| {
                            FaucetError::Sink(format!("JSON serialization failed: {e}"))
                        })?;
                        pipe.cmd("SET").arg(key).arg(serialized);
                    }
                }
            }

            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("Redis pipeline execution failed: {e}")))?;

            written += chunk.len();
        }

        tracing::debug!(records = written, "Redis batch written");
        Ok(written)
    }
}

/// Flatten a JSON record's top-level fields into string key-value pairs
/// suitable for Redis stream entries.
fn flatten_record_to_fields(record: &Value) -> Vec<(String, String)> {
    match record.as_object() {
        Some(map) => map
            .iter()
            .map(|(k, v)| {
                let val = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                (k.clone(), val)
            })
            .collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedisSinkConfig;
    use serde_json::json;

    #[test]
    fn config_fields_accessible() {
        let config = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::List { key: "test".into() },
        );
        // RedisSink::new() is async and requires a live Redis connection,
        // so we only verify the config here.
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn flatten_object_record() {
        let record = json!({"name": "Alice", "age": 30});
        let fields = flatten_record_to_fields(&record);
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().any(|(k, v)| k == "name" && v == "Alice"));
        assert!(fields.iter().any(|(k, v)| k == "age" && v == "30"));
    }

    #[test]
    fn flatten_non_object_returns_empty() {
        let record = json!("just a string");
        let fields = flatten_record_to_fields(&record);
        assert!(fields.is_empty());
    }

    #[test]
    fn flatten_nested_value_serializes_as_json() {
        let record = json!({"data": {"nested": true}});
        let fields = flatten_record_to_fields(&record);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "data");
        assert_eq!(fields[0].1, r#"{"nested":true}"#);
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::List { key: "k".into() },
        );
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match RedisSink::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }
}
