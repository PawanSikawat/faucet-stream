//! MongoDB change-stream executor.

use crate::config::{MongoCdcSourceConfig, StartFrom};
use crate::envelope::to_envelope;
use crate::state::{Bookmark, state_key};
use async_trait::async_trait;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use mongodb::Client;
use mongodb::bson::{self, Document, Timestamp};
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::{FullDocumentBeforeChangeType, FullDocumentType};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::Mutex;

/// Effective start position resolved for a given fetch cycle.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StartPosition {
    /// Server default (current cluster time).
    Now,
    /// `start_at_operation_time`.
    AtOperationTime(Timestamp),
    /// `resume_after(token)`.
    ResumeAfter(ResumeToken),
}

/// Resolve the effective start position from config + any persisted bookmark.
///
/// Precedence: explicit `resume_token`/`timestamp` win over a persisted
/// bookmark; `now`/`earliest` yield to a persisted bookmark.
pub(crate) fn resolve_start(
    start_from: &StartFrom,
    pending: Option<&Bookmark>,
) -> Result<StartPosition, FaucetError> {
    match start_from {
        StartFrom::ResumeToken { token } => {
            let b = Bookmark {
                resume_token: token.clone(),
            };
            Ok(StartPosition::ResumeAfter(b.to_token()?))
        }
        StartFrom::Timestamp { timestamp_secs } => Ok(StartPosition::AtOperationTime(Timestamp {
            time: *timestamp_secs,
            increment: 0,
        })),
        StartFrom::Now | StartFrom::Earliest => {
            if let Some(b) = pending {
                Ok(StartPosition::ResumeAfter(b.to_token()?))
            } else if matches!(start_from, StartFrom::Earliest) {
                // Earliest retained oplog entry (errors at open if rolled).
                Ok(StartPosition::AtOperationTime(Timestamp {
                    time: 1,
                    increment: 1,
                }))
            } else {
                Ok(StartPosition::Now)
            }
        }
    }
}

/// A configured MongoDB CDC source.
pub struct MongoCdcSource {
    config: MongoCdcSourceConfig,
    client: Client,
    state_key_value: String,
    pending_bookmark: Mutex<Option<Bookmark>>,
}

impl MongoCdcSource {
    /// Connect, validate config + topology, and build the source.
    pub async fn new(config: MongoCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = Client::with_uri_str(&config.connection_uri)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB connection failed: {e}")))?;
        ensure_changestream_capable(&client).await?;
        if config.full_document_before_change != crate::config::FullDocumentBeforeChange::Off {
            tracing::warn!(
                "mongodb-cdc: full_document_before_change requires MongoDB 6.0+ and the \
                 collection's changeStreamPreAndPostImages enabled; the server will error \
                 at stream open if unavailable"
            );
        }
        let state_key_value = state_key(&config.scope);
        Ok(Self {
            config,
            client,
            state_key_value,
            pending_bookmark: Mutex::new(None),
        })
    }
}

/// Run `hello` and assert the deployment supports change streams.
async fn ensure_changestream_capable(client: &Client) -> Result<(), FaucetError> {
    let hello = client
        .database("admin")
        .run_command(bson::doc! { "hello": 1 })
        .await
        .map_err(|e| FaucetError::Source(format!("mongodb-cdc hello failed: {e}")))?;
    if is_changestream_topology(&hello) {
        Ok(())
    } else {
        Err(FaucetError::Source(
            "mongodb-cdc: Change Streams require a replica set or sharded cluster; \
             the target appears to be a standalone mongod"
                .into(),
        ))
    }
}

/// Classify a `hello` response: replica-set (`setName`) or sharded (`msg=isdbgrid`).
pub(crate) fn is_changestream_topology(hello: &Document) -> bool {
    hello.get_str("setName").is_ok()
        || hello
            .get_str("msg")
            .map(|m| m == "isdbgrid")
            .unwrap_or(false)
}

/// Map the config post-image mode to the driver type (`None` = don't request).
pub(crate) fn full_document_type(fd: crate::config::FullDocument) -> Option<FullDocumentType> {
    use crate::config::FullDocument as F;
    match fd {
        F::Off => None,
        F::WhenAvailable => Some(FullDocumentType::WhenAvailable),
        F::Required => Some(FullDocumentType::Required),
        F::UpdateLookup => Some(FullDocumentType::UpdateLookup),
    }
}

/// Map the config pre-image mode to the driver type (`None` = don't request).
pub(crate) fn full_document_before_type(
    fd: crate::config::FullDocumentBeforeChange,
) -> Option<FullDocumentBeforeChangeType> {
    use crate::config::FullDocumentBeforeChange as F;
    match fd {
        F::Off => None,
        F::WhenAvailable => Some(FullDocumentBeforeChangeType::WhenAvailable),
        F::Required => Some(FullDocumentBeforeChangeType::Required),
    }
}

/// Build the server-side pipeline: operation-type `$match` + user stages.
pub(crate) fn build_pipeline(config: &MongoCdcSourceConfig) -> Result<Vec<Document>, FaucetError> {
    let mut pipeline: Vec<Document> = Vec::new();
    if !config.operation_types.is_empty() {
        pipeline.push(bson::doc! {
            "$match": { "operationType": { "$in": config.operation_types.clone() } }
        });
    }
    for (i, stage) in config.aggregation_pipeline.iter().enumerate() {
        let b = bson::to_bson(stage)
            .map_err(|e| FaucetError::Config(format!("mongodb-cdc: pipeline stage {i}: {e}")))?;
        match b {
            bson::Bson::Document(d) => pipeline.push(d),
            other => {
                return Err(FaucetError::Config(format!(
                    "mongodb-cdc: aggregation_pipeline[{i}] must be an object, got {other:?}"
                )));
            }
        }
    }
    Ok(pipeline)
}

#[async_trait]
impl Source for MongoCdcSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages(ctx, 0);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_impl(ctx, self.config.batch_size)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(MongoCdcSourceConfig)).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let b = Bookmark::from_value(bookmark)?;
        // Round-trip-validate the token now so a corrupt bookmark fails fast.
        b.to_token()?;
        *self.pending_bookmark.lock().await = Some(b);
        Ok(())
    }

    fn supports_exactly_once(&self) -> bool {
        // Durable resumeToken + deterministic replay from it + per-event
        // (per-page) bookmarks — the requirements for exactly-once delivery.
        true
    }

    fn connector_name(&self) -> &'static str {
        "mongodb-cdc"
    }

    fn dataset_uri(&self) -> String {
        use crate::config::Scope;
        let base = faucet_core::redact_uri_credentials(&self.config.connection_uri);
        match &self.config.scope {
            Scope::Collection {
                database,
                collection,
            } => {
                format!("{base}/{database}/{collection}")
            }
            Scope::Database { database } => format!("{base}/{database}"),
            Scope::Cluster => base,
        }
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();

        // Run `hello` once under the probe timeout, then derive two probes from
        // the outcome: `connection` (could we reach + auth to the server?) and
        // `topology` (does it support change streams?). On a connection
        // failure/timeout the topology probe cannot run, so only `connection`
        // is reported.
        let hello = tokio::time::timeout(
            ctx.timeout,
            self.client
                .database("admin")
                .run_command(bson::doc! { "hello": 1 }),
        )
        .await;

        let hello = match hello {
            Ok(Ok(doc)) => doc,
            Ok(Err(e)) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connection",
                    start.elapsed(),
                    format!("could not run hello: {e}"),
                    "verify the URI is reachable and credentials are valid",
                )));
            }
            Err(_elapsed) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connection",
                    start.elapsed(),
                    "connection timed out",
                    "the server did not respond within the check timeout",
                )));
            }
        };

        let connection = Probe::pass("connection", start.elapsed());
        let topology = if is_changestream_topology(&hello) {
            Probe::pass("topology", start.elapsed())
        } else {
            Probe::fail_hint(
                "topology",
                start.elapsed(),
                "target is a standalone mongod",
                "Change Streams require a replica set or sharded cluster",
            )
        };
        Ok(CheckReport {
            probes: vec![connection, topology],
        })
    }
}

impl MongoCdcSource {
    /// Open the change stream and drain it with an idle-timeout terminator.
    fn stream_pages_impl<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let idle_timeout = self.config.idle_timeout;
        let max_await = std::time::Duration::from_millis(self.config.max_await_time_ms);
        let per_batch = batch_size != 0;

        Box::pin(async_stream::try_stream! {
            use futures::StreamExt;

            let pending = self.pending_bookmark.lock().await.take();
            let start = resolve_start(&self.config.start_from, pending.as_ref())?;
            let pipeline = build_pipeline(&self.config)?;

            // Open the change stream scoped to collection, database, or cluster.
            // Each arm resolves to a ChangeStream<ChangeStreamEvent<Document>>.
            let change_stream: mongodb::change_stream::ChangeStream<
                mongodb::change_stream::event::ChangeStreamEvent<Document>,
            > = match &self.config.scope {
                crate::config::Scope::Collection { database, collection } => {
                    let coll: mongodb::Collection<Document> =
                        self.client.database(database).collection(collection);
                    let mut w = coll
                        .watch()
                        .pipeline(pipeline)
                        .max_await_time(max_await);
                    if let Some(fd) = full_document_type(self.config.full_document) {
                        w = w.full_document(fd);
                    }
                    if let Some(fb) =
                        full_document_before_type(self.config.full_document_before_change)
                    {
                        w = w.full_document_before_change(fb);
                    }
                    w = match &start {
                        StartPosition::Now => w,
                        StartPosition::AtOperationTime(ts) => w.start_at_operation_time(*ts),
                        StartPosition::ResumeAfter(tok) => w.resume_after(tok.clone()),
                    };
                    w.await
                        .map_err(|e| FaucetError::Source(format!("mongodb-cdc watch failed: {e}")))?
                }
                crate::config::Scope::Database { database } => {
                    let db = self.client.database(database);
                    let mut w = db
                        .watch()
                        .pipeline(pipeline)
                        .max_await_time(max_await);
                    if let Some(fd) = full_document_type(self.config.full_document) {
                        w = w.full_document(fd);
                    }
                    if let Some(fb) =
                        full_document_before_type(self.config.full_document_before_change)
                    {
                        w = w.full_document_before_change(fb);
                    }
                    w = match &start {
                        StartPosition::Now => w,
                        StartPosition::AtOperationTime(ts) => w.start_at_operation_time(*ts),
                        StartPosition::ResumeAfter(tok) => w.resume_after(tok.clone()),
                    };
                    w.await
                        .map_err(|e| FaucetError::Source(format!("mongodb-cdc watch failed: {e}")))?
                }
                crate::config::Scope::Cluster => {
                    let mut w = self.client
                        .watch()
                        .pipeline(pipeline)
                        .max_await_time(max_await);
                    if let Some(fd) = full_document_type(self.config.full_document) {
                        w = w.full_document(fd);
                    }
                    if let Some(fb) =
                        full_document_before_type(self.config.full_document_before_change)
                    {
                        w = w.full_document_before_change(fb);
                    }
                    w = match &start {
                        StartPosition::Now => w,
                        StartPosition::AtOperationTime(ts) => w.start_at_operation_time(*ts),
                        StartPosition::ResumeAfter(tok) => w.resume_after(tok.clone()),
                    };
                    w.await
                        .map_err(|e| FaucetError::Source(format!("mongodb-cdc watch failed: {e}")))?
                }
            };
            let mut change_stream = std::pin::pin!(change_stream);

            let chunk = if per_batch { batch_size } else { usize::MAX };
            let mut buffer: Vec<Value> = Vec::new();
            // Bookmark of the last record currently in `buffer`. `None` only when
            // the buffer is empty (no events seen this cycle).
            let mut last_bookmark: Option<Value> = None;

            // Graceful shutdown is handled one layer up: `run_stream`'s page loop
            // `biased`-`select!`s the pipeline cancel token (Pipeline::with_cancel,
            // #146) against polling this stream for its next page, so a cancel
            // mid-idle-wait stops at the page boundary and flushes the sinks — no
            // source-side signal handling needed. Nothing is persisted until a
            // page yields, so a dropped in-flight buffer is simply re-fetched
            // (the resume token has not advanced).
            loop {
                match tokio::time::timeout(idle_timeout, change_stream.next()).await {
                    Ok(Some(Ok(event))) => {
                        let bookmark = Bookmark::from_token(&event.id)?;
                        let is_invalidate = matches!(
                            event.operation_type,
                            mongodb::change_stream::event::OperationType::Invalidate
                        );
                        buffer.push(to_envelope(&event, &bookmark)?);
                        last_bookmark = Some(bookmark.to_value()?);

                        // An invalidate closes the stream server-side: flush the
                        // page (including the invalidate event) and end.
                        if is_invalidate {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                            break;
                        }
                        if per_batch && buffer.len() >= chunk {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                        }
                    }
                    Ok(Some(Err(e))) => {
                        Err(FaucetError::Source(format!("mongodb-cdc stream error: {e}")))?;
                    }
                    // Idle (timeout) or cursor closed: flush whatever we have and
                    // end this fetch cycle.
                    Ok(None) | Err(_) => {
                        if !buffer.is_empty() {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                        }
                        break;
                    }
                }
            }

            tracing::info!(connector = "mongodb-cdc", "change stream fetch cycle complete");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn explicit_timestamp_overrides_bookmark() {
        let pending = Bookmark {
            resume_token: json!({ "_data": "AA" }),
        };
        let pos = resolve_start(
            &StartFrom::Timestamp {
                timestamp_secs: 100,
            },
            Some(&pending),
        )
        .unwrap();
        assert_eq!(
            pos,
            StartPosition::AtOperationTime(Timestamp {
                time: 100,
                increment: 0
            })
        );
    }

    #[test]
    fn explicit_resume_token_overrides_bookmark() {
        // An explicit ResumeToken in config must win over any persisted
        // bookmark and resolve to ResumeAfter(token).
        let pending = Bookmark {
            resume_token: json!({ "_data": "PENDING" }),
        };
        let pos = resolve_start(
            &StartFrom::ResumeToken {
                token: json!({ "_data": "8264AB00" }),
            },
            Some(&pending),
        )
        .unwrap();
        // The resolved position must round-trip back to the configured token,
        // not the pending bookmark's token.
        match pos {
            StartPosition::ResumeAfter(tok) => {
                let b = Bookmark::from_token(&tok).unwrap();
                assert_eq!(b.resume_token["_data"], json!("8264AB00"));
            }
            other => panic!("expected ResumeAfter, got {other:?}"),
        }
    }

    #[test]
    fn now_yields_to_bookmark() {
        let pending = Bookmark {
            resume_token: json!({ "_data": "8264AB00" }),
        };
        let pos = resolve_start(&StartFrom::Now, Some(&pending)).unwrap();
        assert!(matches!(pos, StartPosition::ResumeAfter(_)));
    }

    #[test]
    fn now_without_bookmark_is_now() {
        assert_eq!(
            resolve_start(&StartFrom::Now, None).unwrap(),
            StartPosition::Now
        );
    }

    #[test]
    fn earliest_without_bookmark_uses_operation_time() {
        assert_eq!(
            resolve_start(&StartFrom::Earliest, None).unwrap(),
            StartPosition::AtOperationTime(Timestamp {
                time: 1,
                increment: 1
            })
        );
    }

    #[test]
    fn topology_classification() {
        assert!(is_changestream_topology(&bson::doc! { "setName": "rs0" }));
        assert!(is_changestream_topology(&bson::doc! { "msg": "isdbgrid" }));
        assert!(!is_changestream_topology(&bson::doc! { "ok": 1.0 }));
    }

    #[test]
    fn pipeline_prepends_operation_match() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "operation_types": ["insert", "delete"]
        }))
        .unwrap();
        let p = build_pipeline(&c).unwrap();
        assert_eq!(p.len(), 1);
        assert!(p[0].contains_key("$match"));
    }

    #[test]
    fn pipeline_rejects_non_object_stage() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "aggregation_pipeline": [[1, 2, 3]]
        }))
        .unwrap();
        assert!(matches!(build_pipeline(&c), Err(FaucetError::Config(_))));
    }

    #[test]
    fn pipeline_appends_object_stages_after_match() {
        // operation_types prepends a $match; each aggregation_pipeline object
        // stage is appended in order.
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "operation_types": ["insert"],
            "aggregation_pipeline": [
                { "$match": { "fullDocument.tier": "gold" } },
                { "$project": { "fullDocument._id": 1 } }
            ]
        }))
        .unwrap();
        let p = build_pipeline(&c).unwrap();
        assert_eq!(p.len(), 3, "operation $match + two user stages");
        // First is the operation-type $match.
        assert!(
            p[0].get_document("$match")
                .unwrap()
                .contains_key("operationType")
        );
        // User object stages preserved verbatim, in order.
        assert!(p[1].contains_key("$match"));
        assert!(p[2].contains_key("$project"));
    }

    #[test]
    fn pipeline_object_stages_without_operation_match() {
        // No operation_types → no leading $match; only the user object stage.
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "aggregation_pipeline": [ { "$project": { "_id": 1 } } ]
        }))
        .unwrap();
        let p = build_pipeline(&c).unwrap();
        assert_eq!(p.len(), 1);
        assert!(p[0].contains_key("$project"));
    }

    #[test]
    fn full_document_type_mapping() {
        use crate::config::FullDocument as F;
        assert!(full_document_type(F::Off).is_none());
        assert!(matches!(
            full_document_type(F::WhenAvailable),
            Some(FullDocumentType::WhenAvailable)
        ));
        assert!(matches!(
            full_document_type(F::Required),
            Some(FullDocumentType::Required)
        ));
        assert!(matches!(
            full_document_type(F::UpdateLookup),
            Some(FullDocumentType::UpdateLookup)
        ));
    }

    #[test]
    fn full_document_before_type_mapping() {
        use crate::config::FullDocumentBeforeChange as F;
        assert!(full_document_before_type(F::Off).is_none());
        assert!(matches!(
            full_document_before_type(F::WhenAvailable),
            Some(FullDocumentBeforeChangeType::WhenAvailable)
        ));
        assert!(matches!(
            full_document_before_type(F::Required),
            Some(FullDocumentBeforeChangeType::Required)
        ));
    }

    // dataset_uri is a pure-config method; MongoCdcSource requires a live
    // MongoDB async constructor, so we verify the logic directly using config
    // deserialization (which is sync).
    #[test]
    fn dataset_uri_cluster_scope() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://u:p@h:27017/?replicaSet=rs0",
            "scope": { "type": "cluster" }
        }))
        .unwrap();
        use crate::config::Scope;
        let base = faucet_core::redact_uri_credentials(&c.connection_uri);
        let uri = match &c.scope {
            Scope::Collection {
                database,
                collection,
            } => format!("{base}/{database}/{collection}"),
            Scope::Database { database } => format!("{base}/{database}"),
            Scope::Cluster => base,
        };
        assert_eq!(uri, "mongodb://h:27017/?replicaSet=rs0");
    }

    #[test]
    fn dataset_uri_collection_scope() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://u:p@h:27017/?replicaSet=rs0",
            "scope": { "type": "collection", "database": "mydb", "collection": "orders" }
        }))
        .unwrap();
        use crate::config::Scope;
        let base = faucet_core::redact_uri_credentials(&c.connection_uri);
        let uri = match &c.scope {
            Scope::Collection {
                database,
                collection,
            } => format!("{base}/{database}/{collection}"),
            Scope::Database { database } => format!("{base}/{database}"),
            Scope::Cluster => base,
        };
        assert_eq!(uri, "mongodb://h:27017/?replicaSet=rs0/mydb/orders");
    }
}
