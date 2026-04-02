//! Redis source stream executor.

use crate::config::{RedisSourceConfig, RedisSourceType};
use async_trait::async_trait;
use faucet_core::FaucetError;
use redis::AsyncCommands;
use serde_json::{Value, json};

/// A configured Redis source that reads records from Redis data structures.
pub struct RedisSource {
    config: RedisSourceConfig,
}

impl RedisSource {
    /// Create a new Redis source from the given configuration.
    pub fn new(config: RedisSourceConfig) -> Self {
        Self { config }
    }

    /// Fetch all records from the configured Redis source.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let client = redis::Client::open(self.config.url.as_str())
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| FaucetError::Config(format!("Redis connection failed: {e}")))?;

        let mut records = match &self.config.source_type {
            RedisSourceType::List { key } => self.fetch_list(&mut conn, key).await?,
            RedisSourceType::Stream {
                key,
                group,
                consumer,
                count,
            } => {
                self.fetch_stream(&mut conn, key, group, consumer, count)
                    .await?
            }
            RedisSourceType::Keys { pattern } => self.fetch_keys(&mut conn, pattern).await?,
        };

        if let Some(max) = self.config.max_records {
            records.truncate(max);
        }

        tracing::info!(records = records.len(), "Redis fetch complete");
        Ok(records)
    }

    /// Read all elements from a Redis list.
    async fn fetch_list(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
    ) -> Result<Vec<Value>, FaucetError> {
        let values: Vec<String> = conn
            .lrange(key, 0, -1)
            .await
            .map_err(|e| FaucetError::Config(format!("LRANGE failed on '{key}': {e}")))?;

        let records = values
            .into_iter()
            .map(|v| serde_json::from_str::<Value>(&v).unwrap_or_else(|_| Value::String(v.clone())))
            .collect();

        Ok(records)
    }

    /// Read entries from a Redis stream.
    async fn fetch_stream(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
        group: &Option<String>,
        consumer: &Option<String>,
        count: &Option<usize>,
    ) -> Result<Vec<Value>, FaucetError> {
        let entries: redis::streams::StreamReadReply = match (group, consumer) {
            (Some(group_name), Some(consumer_name)) => {
                let opts = redis::streams::StreamReadOptions::default().count(count.unwrap_or(100));
                conn.xread_options(&[key], &[">"], &opts.group(group_name, consumer_name))
                    .await
                    .map_err(|e| {
                        FaucetError::Config(format!("XREADGROUP failed on '{key}': {e}"))
                    })?
            }
            _ => {
                let mut opts = redis::streams::StreamReadOptions::default();
                if let Some(c) = count {
                    opts = opts.count(*c);
                }
                conn.xread_options(&[key], &["0"], &opts)
                    .await
                    .map_err(|e| FaucetError::Config(format!("XREAD failed on '{key}': {e}")))?
            }
        };

        let mut records = Vec::new();
        for stream_key in &entries.keys {
            for entry in &stream_key.ids {
                let mut fields = serde_json::Map::new();
                for (field_name, field_value) in &entry.map {
                    let val = match field_value {
                        redis::Value::BulkString(bytes) => {
                            let s = String::from_utf8_lossy(bytes);
                            serde_json::from_str::<Value>(&s)
                                .unwrap_or_else(|_| Value::String(s.into_owned()))
                        }
                        redis::Value::SimpleString(s) => serde_json::from_str::<Value>(s)
                            .unwrap_or_else(|_| Value::String(s.clone())),
                        redis::Value::Int(n) => json!(n),
                        redis::Value::Double(n) => json!(n),
                        redis::Value::Boolean(b) => json!(b),
                        redis::Value::Nil => Value::Null,
                        other => Value::String(format!("{other:?}")),
                    };
                    fields.insert(field_name.clone(), val);
                }
                records.push(json!({
                    "id": entry.id,
                    "fields": Value::Object(fields),
                }));
            }
        }

        Ok(records)
    }

    /// Scan for keys matching a pattern, then GET each key.
    async fn fetch_keys(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        pattern: &str,
    ) -> Result<Vec<Value>, FaucetError> {
        let keys: Vec<String> = {
            let mut collected = Vec::new();
            let mut iter: redis::AsyncIter<String> =
                conn.scan_match(pattern).await.map_err(|e| {
                    FaucetError::Config(format!("SCAN failed with pattern '{pattern}': {e}"))
                })?;

            while let Some(key) = iter.next_item().await {
                collected.push(key);
            }
            collected
        };

        let mut records = Vec::new();
        for key in &keys {
            let value: Option<String> = conn
                .get(key)
                .await
                .map_err(|e| FaucetError::Config(format!("GET failed for key '{key}': {e}")))?;

            if let Some(v) = value {
                let parsed =
                    serde_json::from_str::<Value>(&v).unwrap_or_else(|_| Value::String(v.clone()));
                records.push(json!({
                    "key": key,
                    "value": parsed,
                }));
            }
        }

        Ok(records)
    }
}

#[async_trait]
impl faucet_core::Source for RedisSource {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        RedisSource::fetch_all(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedisSourceConfig;

    #[test]
    fn creates_source() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "test".into() },
        );
        let _source = RedisSource::new(config);
    }
}
