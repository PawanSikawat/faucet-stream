//! MongoDB sink implementation.

use crate::config::MongoSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use futures::StreamExt;
use mongodb::Client;
use mongodb::bson::{self, Bson, Document};
use serde_json::{Map, Value};

/// Max in-flight `replace_one` / `delete_one` operations issued concurrently
/// from a single planned page. The planner has already deduped by key, so
/// every concurrent op targets a distinct key — there is no intra-batch
/// ordering hazard. A modest bound keeps round-trips overlapping without
/// flooding the connection pool.
const APPLY_CONCURRENCY: usize = 50;

/// Convert a JSON object map (a key filter from
/// [`faucet_core::key_to_filter`]) into a BSON filter [`Document`].
///
/// Returns a `Sink` error if the map does not convert to a BSON document.
fn json_map_to_bson_filter(map: &Map<String, Value>) -> Result<Document, FaucetError> {
    MongoSink::value_to_document(&Value::Object(map.clone()))
}

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
        // Validate write-mode config up front (config-only, so before connecting
        // is fine): upsert/delete require a non-empty `key`. MongoDB is
        // schemaless, so there is no column-mapping guard to apply.
        config.write.validate()?;
        let client = Client::with_uri_str(&config.connection_uri)
            .await
            .map_err(|e| FaucetError::Config(format!("MongoDB connection failed: {e}")))?;

        Ok(Self { config, client })
    }

    /// Build the match-filter [`Document`] for an upsert row by pulling the
    /// configured `key` columns out of the row. The planner
    /// ([`faucet_core::plan_writes`]) has already validated that every key
    /// column is present and non-null on each upsert row, so a missing column
    /// here is an internal invariant violation rather than user data error.
    fn filter_from_row(row: &Value, key: &[String]) -> Result<Document, FaucetError> {
        let obj = row
            .as_object()
            .ok_or_else(|| FaucetError::Sink("upsert row is not a JSON object".to_string()))?;
        let mut filter = Map::with_capacity(key.len());
        for col in key {
            match obj.get(col) {
                Some(v) => {
                    filter.insert(col.clone(), v.clone());
                }
                None => {
                    return Err(FaucetError::Sink(format!(
                        "upsert row missing key column '{col}' after planning"
                    )));
                }
            }
        }
        json_map_to_bson_filter(&filter)
    }

    /// Apply a planned page of upserts and deletes to the collection.
    ///
    /// Each upsert row is committed with `replace_one(filter, replacement)
    /// .upsert(true)` and each delete with `delete_one(filter)`. We use the
    /// per-document `replace_one(upsert)` / `delete_one` primitives (not the
    /// namespaced `Client::bulk_write`) for compatibility with all supported
    /// MongoDB server versions; throughput is recovered by issuing the ops
    /// concurrently via `buffer_unordered`. The planner already deduped keys
    /// (last-write-wins), so concurrent ops target distinct keys and there is
    /// no intra-batch ordering hazard.
    ///
    /// Returns the number of upserts + deletes applied.
    async fn apply_plan(&self, plan: &faucet_core::WritePlan) -> Result<usize, FaucetError> {
        let collection = self
            .client
            .database(&self.config.database)
            .collection::<Document>(&self.config.collection);
        let key = &self.config.write.key;

        // Build a single homogeneous op stream of (filter, replacement?) so
        // upserts and deletes run through one bounded `buffer_unordered`.
        enum Op {
            Upsert(Document, Document),
            Delete(Document),
        }

        let mut ops: Vec<Op> = Vec::with_capacity(plan.upserts.len() + plan.deletes.len());
        for row in &plan.upserts {
            let filter = Self::filter_from_row(row, key)?;
            let replacement = Self::value_to_document(row)?;
            ops.push(Op::Upsert(filter, replacement));
        }
        for kt in &plan.deletes {
            let filter = json_map_to_bson_filter(&faucet_core::key_to_filter(kt))?;
            ops.push(Op::Delete(filter));
        }

        let applied = ops.len();

        futures::stream::iter(ops.into_iter().map(|op| {
            let collection = collection.clone();
            async move {
                match op {
                    Op::Upsert(filter, replacement) => collection
                        .replace_one(filter, replacement)
                        .upsert(true)
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            FaucetError::Sink(format!("MongoDB replace_one (upsert) failed: {e}"))
                        }),
                    Op::Delete(filter) => collection
                        .delete_one(filter)
                        .await
                        .map(|_| ())
                        .map_err(|e| FaucetError::Sink(format!("MongoDB delete_one failed: {e}"))),
                }
            }
        }))
        .buffer_unordered(APPLY_CONCURRENCY)
        .collect::<Vec<Result<(), FaucetError>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<()>, FaucetError>>()?;

        tracing::info!(
            applied,
            upserts = plan.upserts.len(),
            deletes = plan.deletes.len(),
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB upsert/delete write complete"
        );

        Ok(applied)
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

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
        ]
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}/{}",
            faucet_core::redact_uri_credentials(&self.config.connection_uri),
            self.config.database,
            self.config.collection
        )
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

        // Upsert / delete routing: plan the page (dedup last-write-wins, strip
        // the delete marker) and apply per-document `replace_one(upsert)` /
        // `delete_one` ops. Append falls through to the `insert_many` fast path.
        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "mongodb {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.apply_plan(&plan).await;
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

    /// Write a batch and report per-row outcomes.
    ///
    /// In append mode this delegates to [`write_batch`](Self::write_batch) and
    /// maps a single success onto an all-`Ok(())` vector (the trait default).
    /// In upsert/delete mode the good rows are applied (upserts + deletes), and
    /// only the rows whose key could not be extracted (missing / null key) are
    /// reported as `Err` so the pipeline routes them to the DLQ per-row instead
    /// of sending the whole page.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        let plan = faucet_core::plan_writes(records, &self.config.write);
        self.apply_plan(&plan).await?;

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "mongodb {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // dataset_uri test is skipped: MongoSink::new() requires a live MongoDB
    // connection (Client::with_uri_str connects in new()), and no offline
    // constructor exists.

    #[test]
    fn filter_doc_from_key_tuple() {
        let kt = faucet_core::KeyTuple(vec![
            ("tenant".to_string(), serde_json::json!("acme")),
            ("id".to_string(), serde_json::json!(7)),
        ]);
        let m = faucet_core::key_to_filter(&kt);
        assert_eq!(m.get("tenant"), Some(&serde_json::json!("acme")));
        assert_eq!(m.get("id"), Some(&serde_json::json!(7)));
        // and it converts to a bson filter Document via the sink's converter:
        let doc = super::json_map_to_bson_filter(&m).expect("filter converts to bson");
        assert_eq!(doc.get_str("tenant").unwrap(), "acme");
        assert_eq!(doc.get_i64("id").unwrap(), 7);
    }

    #[test]
    fn filter_from_row_pulls_only_key_columns() {
        let row = json!({"_id": 5, "name": "a", "extra": true});
        let doc = MongoSink::filter_from_row(&row, &["_id".to_string()]).expect("filter");
        assert_eq!(doc.get_i64("_id").unwrap(), 5);
        assert!(
            !doc.contains_key("name"),
            "filter must contain only key columns"
        );
        assert!(!doc.contains_key("extra"));
    }

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
