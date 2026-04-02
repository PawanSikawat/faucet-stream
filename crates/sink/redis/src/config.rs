//! Redis sink configuration.

/// The type of Redis data structure to write to.
#[derive(Debug, Clone)]
pub enum RedisSinkType {
    /// Append records to a Redis list via `RPUSH`.
    List {
        /// The list key.
        key: String,
    },
    /// Add entries to a Redis stream via `XADD`.
    Stream {
        /// The stream key.
        key: String,
    },
    /// Set individual keys via `SET`, using a field from each record as the key.
    KeyValue {
        /// The field name in each record whose value becomes the Redis key.
        key_field: String,
    },
}

/// Configuration for the Redis sink connector.
#[derive(Clone)]
pub struct RedisSinkConfig {
    /// Redis connection URL (e.g. `"redis://127.0.0.1:6379"`).
    pub url: String,
    /// The type of Redis data structure to write to.
    pub sink_type: RedisSinkType,
    /// Number of commands to batch in a single pipeline. Defaults to 500.
    pub batch_size: usize,
}

impl std::fmt::Debug for RedisSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSinkConfig")
            .field("url", &"***")
            .field("sink_type", &self.sink_type)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl RedisSinkConfig {
    /// Create a new config with the given URL and sink type.
    pub fn new(url: impl Into<String>, sink_type: RedisSinkType) -> Self {
        Self {
            url: url.into(),
            sink_type,
            batch_size: 500,
        }
    }

    /// Set the pipeline batch size.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = RedisSinkConfig::new(
            "redis://127.0.0.1:6379",
            RedisSinkType::List {
                key: "my_list".into(),
            },
        );
        assert_eq!(config.batch_size, 500);
        // Debug should mask URL
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("127.0.0.1"));
    }

    #[test]
    fn builder_methods() {
        let config = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::Stream {
                key: "events".into(),
            },
        )
        .batch_size(100);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn key_value_sink_type() {
        let config = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
        );
        if let RedisSinkType::KeyValue { ref key_field } = config.sink_type {
            assert_eq!(key_field, "id");
        } else {
            panic!("expected KeyValue variant");
        }
    }
}
