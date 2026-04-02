//! MongoDB sink configuration.

use std::fmt;

/// Configuration for the MongoDB sink connector.
///
/// # Example
///
/// ```
/// use faucet_sink_mongodb::MongoSinkConfig;
///
/// let config = MongoSinkConfig::new(
///     "mongodb://localhost:27017",
///     "my_database",
///     "my_collection",
/// )
/// .batch_size(1000);
/// ```
#[derive(Clone)]
pub struct MongoSinkConfig {
    /// MongoDB connection URI (e.g. `mongodb://localhost:27017`).
    pub connection_uri: String,
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Number of documents to insert per `insert_many` call (default: 500).
    pub batch_size: usize,
}

impl MongoSinkConfig {
    /// Create a new config with the required connection URI, database, and collection.
    pub fn new(
        connection_uri: impl Into<String>,
        database: impl Into<String>,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            connection_uri: connection_uri.into(),
            database: database.into(),
            collection: collection.into(),
            batch_size: 500,
        }
    }

    /// Set the number of documents per `insert_many` batch.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl fmt::Debug for MongoSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoSinkConfig")
            .field("connection_uri", &"***")
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = MongoSinkConfig::new("mongodb://localhost:27017", "testdb", "users");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.collection, "users");
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn builder_methods() {
        let config =
            MongoSinkConfig::new("mongodb://localhost:27017", "testdb", "users").batch_size(1000);
        assert_eq!(config.batch_size, 1000);
    }

    #[test]
    fn debug_masks_connection_uri() {
        let config = MongoSinkConfig::new("mongodb://user:secret@host:27017/db", "testdb", "users");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
    }
}
