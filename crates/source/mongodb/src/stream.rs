//! MongoDB stream executor.

use crate::config::MongoSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use mongodb::Client;
use mongodb::bson::{self, Bson, Document};
use mongodb::options::FindOptions;
use serde_json::Value;
use std::pin::Pin;

/// A configured MongoDB source that connects to a collection and fetches documents.
///
/// The MongoDB `Client` is created once during construction and reused across
/// all `fetch_all()` calls. It maintains an internal connection pool.
pub struct MongoSource {
    config: MongoSourceConfig,
    client: Client,
}

impl MongoSource {
    /// Create a new MongoDB source from the given configuration.
    ///
    /// This establishes the MongoDB client (with its internal connection pool)
    /// immediately.
    pub async fn new(config: MongoSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let client = Client::with_uri_str(&config.connection_uri)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB connection failed: {e}")))?;

        Ok(Self { config, client })
    }

    /// Fetch all matching documents from the configured collection.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let db = self.client.database(&self.config.database);
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
        if let Some(cursor_batch_size) = self.config.cursor_batch_size {
            find_options.batch_size = Some(cursor_batch_size);
        }

        let mut cursor = collection
            .find(filter.unwrap_or_default())
            .with_options(find_options)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB find failed: {e}")))?;

        let mut records = Vec::new();

        while cursor
            .advance()
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB cursor advance failed: {e}")))?
        {
            let doc = cursor
                .deserialize_current()
                .map_err(|e| FaucetError::Source(format!("MongoDB deserialization failed: {e}")))?;

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
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            return MongoSource::fetch_all(self).await;
        }

        // Substitute context placeholders into filter, projection, and sort.
        let filter = substitute_optional_value(&self.config.filter, context, "filter")?;
        let projection = substitute_optional_value(&self.config.projection, context, "projection")?;
        let sort = substitute_optional_value(&self.config.sort, context, "sort")?;

        let db = self.client.database(&self.config.database);
        let collection = db.collection::<Document>(&self.config.collection);

        let filter_doc = filter.as_ref().map(json_value_to_document).transpose()?;

        let mut find_options = FindOptions::default();
        if let Some(ref proj) = projection {
            find_options.projection = Some(json_value_to_document(proj)?);
        }
        if let Some(ref s) = sort {
            find_options.sort = Some(json_value_to_document(s)?);
        }
        if let Some(limit) = self.config.limit {
            find_options.limit = Some(limit);
        }
        if let Some(cursor_batch_size) = self.config.cursor_batch_size {
            find_options.batch_size = Some(cursor_batch_size);
        }

        let mut cursor = collection
            .find(filter_doc.unwrap_or_default())
            .with_options(find_options)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB find failed: {e}")))?;

        let mut records = Vec::new();
        while cursor
            .advance()
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB cursor advance failed: {e}")))?
        {
            let doc = cursor
                .deserialize_current()
                .map_err(|e| FaucetError::Source(format!("MongoDB deserialization failed: {e}")))?;
            records.push(bson_document_to_json_value(&doc)?);
        }

        tracing::info!(
            records = records.len(),
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB fetch complete (with context)"
        );

        Ok(records)
    }

    /// Stream documents from the underlying MongoDB cursor without buffering
    /// the full result set. Each emitted [`StreamPage`] holds up to
    /// [`MongoSourceConfig::batch_size`] documents.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. The
    /// MongoDB source has no incremental-replication mode today, so every
    /// emitted page carries `bookmark: None`.
    ///
    /// Note: [`MongoSourceConfig::cursor_batch_size`] is independent — it
    /// controls the driver's per-round-trip batch size, while `batch_size`
    /// controls how many documents are buffered before a `StreamPage` is
    /// yielded to the pipeline.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            // Substitute context placeholders into filter, projection, sort
            // (matching fetch_with_context's behaviour).
            let (filter, projection, sort) = if context.is_empty() {
                (
                    self.config.filter.clone(),
                    self.config.projection.clone(),
                    self.config.sort.clone(),
                )
            } else {
                (
                    substitute_optional_value(&self.config.filter, context, "filter")?,
                    substitute_optional_value(&self.config.projection, context, "projection")?,
                    substitute_optional_value(&self.config.sort, context, "sort")?,
                )
            };

            let db = self.client.database(&self.config.database);
            let collection = db.collection::<Document>(&self.config.collection);

            let filter_doc = filter.as_ref().map(json_value_to_document).transpose()?;

            let mut find_options = FindOptions::default();
            if let Some(ref proj) = projection {
                find_options.projection = Some(json_value_to_document(proj)?);
            }
            if let Some(ref s) = sort {
                find_options.sort = Some(json_value_to_document(s)?);
            }
            if let Some(limit) = self.config.limit {
                find_options.limit = Some(limit);
            }
            if let Some(cursor_batch_size) = self.config.cursor_batch_size {
                find_options.batch_size = Some(cursor_batch_size);
            }

            let mut cursor = collection
                .find(filter_doc.unwrap_or_default())
                .with_options(find_options)
                .await
                .map_err(|e| FaucetError::Source(format!("MongoDB find failed: {e}")))?;

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while cursor
                .advance()
                .await
                .map_err(|e| FaucetError::Source(format!("MongoDB cursor advance failed: {e}")))?
            {
                let doc = cursor
                    .deserialize_current()
                    .map_err(|e| FaucetError::Source(format!("MongoDB deserialization failed: {e}")))?;
                buffer.push(bson_document_to_json_value(&doc)?);
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }
            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                records = total,
                batch_size,
                database = %self.config.database,
                collection = %self.config.collection,
                "MongoDB source stream complete",
            );
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MongoSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}/{}",
            faucet_core::redact_uri_credentials(&self.config.connection_uri),
            self.config.database,
            self.config.collection
        )
    }
}

/// Substitute context placeholders in an optional JSON value.
///
/// Serialises the value to a string, runs [`substitute_context_json`] (which
/// properly escapes string values for JSON safety), then deserialises back.
/// Returns `None` when the input is `None`.
fn substitute_optional_value(
    value: &Option<Value>,
    context: &std::collections::HashMap<String, Value>,
    field_name: &str,
) -> Result<Option<Value>, FaucetError> {
    match value {
        Some(v) => {
            let s = serde_json::to_string(v).map_err(|e| {
                FaucetError::Config(format!("failed to serialize {field_name}: {e}"))
            })?;
            let s = faucet_core::util::substitute_context_json(&s, context);
            let resolved = serde_json::from_str(&s).map_err(|e| {
                FaucetError::Config(format!("failed to parse substituted {field_name}: {e}"))
            })?;
            Ok(Some(resolved))
        }
        None => Ok(None),
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

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = MongoSourceConfig::new("mongodb://localhost:27017", "db", "c");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match MongoSource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // dataset_uri is a pure-config method; MongoSource requires an async
    // constructor with a live server, so we verify the logic directly.
    #[test]
    fn dataset_uri_strips_credentials() {
        let config = MongoSourceConfig::new("mongodb://u:p@h:27017", "mydb", "events");
        let uri = format!(
            "{}/{}/{}",
            faucet_core::redact_uri_credentials(&config.connection_uri),
            config.database,
            config.collection
        );
        assert_eq!(uri, "mongodb://h:27017/mydb/events");
    }
}
