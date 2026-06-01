//! MongoDB sink implementation.

use crate::config::MongoSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use mongodb::Client;
use mongodb::bson::{self, Bson, Document};
use serde_json::Value;

/// A sink that inserts JSON records into a MongoDB collection.
///
/// Each record must be a JSON object. Non-object values produce an error.
/// Records are inserted in batches using `insert_many`.
pub struct MongoSink {
    config: MongoSinkConfig,
    client: Client,
}

impl MongoSink {
    /// Create a new MongoDB sink, establishing the client connection.
    pub async fn new(config: MongoSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let client = Client::with_uri_str(&config.connection_uri)
            .await
            .map_err(|e| FaucetError::Config(format!("MongoDB connection failed: {e}")))?;

        Ok(Self { config, client })
    }

    /// Convert a `serde_json::Value` to a `bson::Document`.
    ///
    /// Returns a `Sink` error if the value is not a JSON object.
    fn value_to_document(val: &Value) -> Result<Document, FaucetError> {
        let bson = bson::to_bson(val)
            .map_err(|e| FaucetError::Sink(format!("failed to convert JSON to BSON: {e}")))?;
        match bson {
            Bson::Document(doc) => Ok(doc),
            other => Err(FaucetError::Sink(format!(
                "expected a JSON object, got BSON type: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl faucet_core::Sink for MongoSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MongoSinkConfig))
            .expect("schema serialization")
    }

    /// Non-mutating preflight probe: run the `ping` admin command against the
    /// configured database via the existing client (probe name `"ping"`).
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let hint = "check connection_uri / credentials / that the MongoDB server is reachable";

        let db = self.client.database(&self.config.database);
        let probe =
            match tokio::time::timeout(ctx.timeout, db.run_command(bson::doc! {"ping": 1})).await {
                Ok(Ok(_)) => Probe::pass("ping", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint("ping", started.elapsed(), e.to_string(), hint),
                Err(_) => Probe::fail_hint("ping", started.elapsed(), "timed out", hint),
            };
        Ok(CheckReport::single(probe))
    }

    /// Write records to MongoDB.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` documents
    /// and each chunk is sent as a separate `insert_many` call. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// `insert_many` request — useful when upstream `StreamPage`s are already
    /// sized for MongoDB's preferred per-request limits.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let collection = self
            .client
            .database(&self.config.database)
            .collection::<Document>(&self.config.collection);

        // `batch_size = 0` is the "no batching" sentinel: forward whatever
        // upstream handed us as a single `insert_many`, preserving
        // `StreamPage` framing. Otherwise re-chunk into `batch_size` slices.
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total_written = 0usize;

        for chunk in records.chunks(effective_chunk) {
            let docs: Vec<Document> = chunk
                .iter()
                .map(Self::value_to_document)
                .collect::<Result<Vec<_>, _>>()?;

            let opts = mongodb::options::InsertManyOptions::builder()
                .ordered(self.config.ordered)
                .build();
            collection
                .insert_many(&docs)
                .with_options(opts)
                .await
                .map_err(|e| FaucetError::Sink(format!("MongoDB insert_many failed: {e}")))?;

            total_written += docs.len();
            tracing::debug!(batch_size = docs.len(), "MongoDB batch inserted");
        }

        tracing::info!(
            records = total_written,
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB write complete"
        );

        Ok(total_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn value_to_document_object() {
        let val = json!({"name": "Alice", "age": 30});
        let doc = MongoSink::value_to_document(&val).unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
        assert_eq!(doc.get_i64("age").unwrap(), 30);
    }

    #[test]
    fn value_to_document_non_object_fails() {
        let val = json!([1, 2, 3]);
        let result = MongoSink::value_to_document(&val);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Sink(_))));
    }

    #[test]
    fn value_to_document_string_fails() {
        let val = json!("not an object");
        let result = MongoSink::value_to_document(&val);
        assert!(result.is_err());
    }

    #[test]
    fn value_to_document_nested() {
        let val = json!({"user": {"name": "Bob"}, "tags": ["a", "b"]});
        let doc = MongoSink::value_to_document(&val).unwrap();
        let inner = doc.get_document("user").unwrap();
        assert_eq!(inner.get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn value_to_document_empty_object() {
        let val = json!({});
        let doc = MongoSink::value_to_document(&val).unwrap();
        assert!(doc.is_empty());
    }

    #[test]
    fn value_to_document_null_fails() {
        let val = Value::Null;
        let result = MongoSink::value_to_document(&val);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = MongoSinkConfig::new("mongodb://localhost:27017", "db", "c");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match MongoSink::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }
}
