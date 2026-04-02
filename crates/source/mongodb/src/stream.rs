//! MongoDB stream executor.

use crate::config::MongoSourceConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use mongodb::Client;
use mongodb::bson::{self, Bson, Document};
use mongodb::options::FindOptions;
use serde_json::Value;

/// A configured MongoDB source that connects to a collection and fetches documents.
pub struct MongoSource {
    config: MongoSourceConfig,
}

impl MongoSource {
    /// Create a new MongoDB source from the given configuration.
    pub fn new(config: MongoSourceConfig) -> Self {
        Self { config }
    }

    /// Fetch all matching documents from the configured collection.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let client = Client::with_uri_str(&self.config.connection_uri)
            .await
            .map_err(|e| FaucetError::Config(format!("MongoDB connection failed: {e}")))?;

        let db = client.database(&self.config.database);
        let collection = db.collection::<Document>(&self.config.collection);

        let filter = self
            .config
            .filter
            .as_ref()
            .map(json_value_to_document)
            .transpose()?;

        let mut find_options = FindOptions::default();

        if let Some(ref proj) = self.config.projection {
            find_options.projection = Some(json_value_to_document(proj)?);
        }
        if let Some(ref sort) = self.config.sort {
            find_options.sort = Some(json_value_to_document(sort)?);
        }
        if let Some(limit) = self.config.limit {
            find_options.limit = Some(limit);
        }
        if let Some(batch_size) = self.config.batch_size {
            find_options.batch_size = Some(batch_size);
        }

        let mut cursor = collection
            .find(filter.unwrap_or_default())
            .with_options(find_options)
            .await
            .map_err(|e| FaucetError::Config(format!("MongoDB find failed: {e}")))?;

        let mut records = Vec::new();

        while cursor
            .advance()
            .await
            .map_err(|e| FaucetError::Config(format!("MongoDB cursor advance failed: {e}")))?
        {
            let doc = cursor
                .deserialize_current()
                .map_err(|e| FaucetError::Config(format!("MongoDB deserialization failed: {e}")))?;

            let value = bson_document_to_json_value(&doc)?;
            records.push(value);
        }

        tracing::info!(
            records = records.len(),
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB fetch complete"
        );

        Ok(records)
    }
}

#[async_trait]
impl faucet_core::Source for MongoSource {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        MongoSource::fetch_all(self).await
    }
}

/// Convert a `serde_json::Value` to a `bson::Document`.
///
/// The value must be a JSON object; other types produce a `Config` error.
fn json_value_to_document(val: &Value) -> Result<Document, FaucetError> {
    let bson = bson::to_bson(val)
        .map_err(|e| FaucetError::Config(format!("failed to convert JSON to BSON: {e}")))?;
    match bson {
        Bson::Document(doc) => Ok(doc),
        other => Err(FaucetError::Config(format!(
            "expected a JSON object, got BSON type: {other:?}"
        ))),
    }
}

/// Convert a `bson::Document` to a `serde_json::Value`.
fn bson_document_to_json_value(doc: &Document) -> Result<Value, FaucetError> {
    let bson = Bson::Document(doc.clone());
    let relaxed = bson.into_relaxed_extjson();
    Ok(relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_object_to_document() {
        let val = json!({"name": "Alice", "age": 30});
        let doc = json_value_to_document(&val).unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
        assert_eq!(doc.get_i64("age").unwrap(), 30);
    }

    #[test]
    fn json_non_object_to_document_fails() {
        let val = json!([1, 2, 3]);
        let result = json_value_to_document(&val);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Config(_))));
    }

    #[test]
    fn json_string_to_document_fails() {
        let val = json!("not an object");
        let result = json_value_to_document(&val);
        assert!(result.is_err());
    }

    #[test]
    fn bson_document_roundtrip() {
        let mut doc = Document::new();
        doc.insert("name", "Bob");
        doc.insert("score", 42);
        let value = bson_document_to_json_value(&doc).unwrap();
        assert_eq!(value["name"], "Bob");
        assert_eq!(value["score"], 42);
    }

    #[test]
    fn nested_document_conversion() {
        let val = json!({"user": {"name": "Alice", "tags": ["admin", "user"]}});
        let doc = json_value_to_document(&val).unwrap();
        let inner = doc.get_document("user").unwrap();
        assert_eq!(inner.get_str("name").unwrap(), "Alice");

        let back = bson_document_to_json_value(&doc).unwrap();
        assert_eq!(back["user"]["name"], "Alice");
        assert_eq!(back["user"]["tags"][0], "admin");
    }

    #[test]
    fn empty_filter_converts() {
        let val = json!({});
        let doc = json_value_to_document(&val).unwrap();
        assert!(doc.is_empty());
    }
}
