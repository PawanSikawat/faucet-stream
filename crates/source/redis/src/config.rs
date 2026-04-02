//! Redis source configuration.

use serde::{Deserialize, Serialize};

/// The type of Redis data structure to read from.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RedisSourceType {
    /// Read all elements from a Redis list via `LRANGE 0 -1`.
    List {
        /// The list key.
        key: String,
    },
    /// Read entries from a Redis stream via `XREAD` or `XREADGROUP`.
    Stream {
        /// The stream key.
        key: String,
        /// Optional consumer group name. When set, uses `XREADGROUP`.
        group: Option<String>,
        /// Consumer name within the group (required when `group` is set).
        consumer: Option<String>,
        /// Maximum number of entries to read per call.
        count: Option<usize>,
    },
    /// Scan for keys matching a pattern, then `GET` each key.
    Keys {
        /// Glob pattern for `SCAN` (e.g. `"user:*"`).
        pattern: String,
    },
}

/// Configuration for the Redis source connector.
#[derive(Clone, Serialize, Deserialize)]
pub struct RedisSourceConfig {
    /// Redis connection URL (e.g. `"redis://127.0.0.1:6379"`).
    pub url: String,
    /// The type of Redis data structure to read from.
    pub source_type: RedisSourceType,
    /// Optional maximum number of records to return.
    pub max_records: Option<usize>,
}

impl std::fmt::Debug for RedisSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSourceConfig")
            .field("url", &"***")
            .field("source_type", &self.source_type)
            .field("max_records", &self.max_records)
            .finish()
    }
}

impl RedisSourceConfig {
    /// Create a new config with the given URL and source type.
    pub fn new(url: impl Into<String>, source_type: RedisSourceType) -> Self {
        Self {
            url: url.into(),
            source_type,
            max_records: None,
        }
    }

    /// Set the maximum number of records to return.
    pub fn max_records(mut self, max: usize) -> Self {
        self.max_records = Some(max);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_list() {
        let config = RedisSourceConfig::new(
            "redis://127.0.0.1:6379",
            RedisSourceType::List {
                key: "my_list".into(),
            },
        );
        assert!(config.max_records.is_none());
        // Debug should mask URL
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("127.0.0.1"));
    }

    #[test]
    fn builder_methods() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::Stream {
                key: "events".into(),
                group: Some("mygroup".into()),
                consumer: Some("worker1".into()),
                count: Some(100),
            },
        )
        .max_records(500);
        assert_eq!(config.max_records, Some(500));
    }

    #[test]
    fn keys_source_type() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::Keys {
                pattern: "user:*".into(),
            },
        );
        if let RedisSourceType::Keys { ref pattern } = config.source_type {
            assert_eq!(pattern, "user:*");
        } else {
            panic!("expected Keys variant");
        }
    }
}
