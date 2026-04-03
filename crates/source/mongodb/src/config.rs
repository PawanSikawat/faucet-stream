//! MongoDB source configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Configuration for the MongoDB source connector.
///
/// # Example
///
/// ```
/// use faucet_source_mongodb::MongoSourceConfig;
/// use serde_json::json;
///
/// let config = MongoSourceConfig::new(
///     "mongodb://localhost:27017",
///     "my_database",
///     "my_collection",
/// )
/// .filter(json!({"status": "active"}))
/// .projection(json!({"_id": 0, "name": 1, "email": 1}))
/// .sort(json!({"created_at": -1}))
/// .limit(1000)
/// .batch_size(200);
/// ```
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct MongoSourceConfig {
    /// MongoDB connection URI (e.g. `mongodb://localhost:27017`).
    pub connection_uri: String,
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Optional query filter as JSON, converted to a BSON `Document` at query time.
    pub filter: Option<Value>,
    /// Optional field projection as JSON.
    pub projection: Option<Value>,
    /// Optional sort specification as JSON.
    pub sort: Option<Value>,
    /// Maximum number of documents to return.
    pub limit: Option<i64>,
    /// Cursor batch size (number of documents per server round-trip).
    pub batch_size: Option<u32>,
}

impl MongoSourceConfig {
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
            filter: None,
            projection: None,
            sort: None,
            limit: None,
            batch_size: None,
        }
    }

    /// Set the query filter (JSON object converted to BSON at query time).
    pub fn filter(mut self, filter: Value) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the field projection.
    pub fn projection(mut self, projection: Value) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set the sort specification.
    pub fn sort(mut self, sort: Value) -> Self {
        self.sort = Some(sort);
        self
    }

    /// Set the maximum number of documents to return.
    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the cursor batch size.
    pub fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }
}

impl fmt::Debug for MongoSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoSourceConfig")
            .field("connection_uri", &"***")
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("filter", &self.filter)
            .field("projection", &self.projection)
            .field("sort", &self.sort)
            .field("limit", &self.limit)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = MongoSourceConfig::new("mongodb://localhost:27017", "testdb", "users");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.collection, "users");
        assert!(config.filter.is_none());
        assert!(config.projection.is_none());
        assert!(config.sort.is_none());
        assert!(config.limit.is_none());
        assert!(config.batch_size.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = MongoSourceConfig::new("mongodb://localhost:27017", "testdb", "users")
            .filter(json!({"active": true}))
            .projection(json!({"_id": 0, "name": 1}))
            .sort(json!({"name": 1}))
            .limit(500)
            .batch_size(100);

        assert_eq!(config.filter.unwrap(), json!({"active": true}));
        assert_eq!(config.projection.unwrap(), json!({"_id": 0, "name": 1}));
        assert_eq!(config.sort.unwrap(), json!({"name": 1}));
        assert_eq!(config.limit, Some(500));
        assert_eq!(config.batch_size, Some(100));
    }

    #[test]
    fn debug_masks_connection_uri() {
        let config =
            MongoSourceConfig::new("mongodb://user:secret@host:27017/db", "testdb", "users");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
    }
}
