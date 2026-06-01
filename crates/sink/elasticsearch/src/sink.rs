//! Elasticsearch bulk index sink.

use crate::config::{ElasticsearchAuth, ElasticsearchSinkConfig};
use async_trait::async_trait;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider};
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};

/// True when a page is split into more than one `_bulk` chunk *and* documents
/// get auto-generated IDs (no `id_field`). In that configuration an earlier
/// chunk can commit before a later chunk fails; because the bookmark only
/// advances after the whole page is written, a resumed run re-sends the earlier
/// chunk, and auto-generated IDs make those re-sends **duplicates** rather than
/// idempotent overwrites. Setting `id_field` (or configuring a DLQ, whose
/// per-row `write_batch_partial` path avoids the whole-page re-send) makes
/// resume idempotent.
fn resume_dup_risk(chunk_count: usize, has_id_field: bool) -> bool {
    chunk_count > 1 && !has_id_field
}

/// A sink that writes JSON records to an Elasticsearch index using the bulk API.
pub struct ElasticsearchSink {
    config: ElasticsearchSinkConfig,
    client: Client,
    /// Optional shared auth provider. When set it takes precedence over inline
    /// auth. Injected by the CLI (to resolve `auth: { ref }`) or directly by
    /// library callers who want to share one token across multiple sinks.
    auth_provider: Option<SharedAuthProvider>,
    /// One-shot guard so the resume-duplication warning (see [`resume_dup_risk`])
    /// is logged at most once per sink instance, not per page.
    resume_dup_warned: AtomicBool,
}

impl ElasticsearchSink {
    /// Create a new Elasticsearch sink from the given configuration.
    ///
    /// Returns [`FaucetError::Config`] if `batch_size` exceeds
    /// `MAX_BATCH_SIZE` (#78/#44).
    pub fn new(config: ElasticsearchSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            client: Client::new(),
            auth_provider: None,
            resume_dup_warned: AtomicBool::new(false),
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking precedence
    /// over inline auth). Used by the CLI to resolve `auth: { ref }`, and by
    /// library callers who inject one provider into many sinks.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Resolve the effective [`ElasticsearchAuth`] for the current batch.
    ///
    /// Resolution order:
    /// 1. If a shared provider is attached, call it and map the credential.
    /// 2. Otherwise use the inline auth from config.
    /// 3. If the config is a `Reference` with no provider, return an error.
    async fn resolve_auth(&self) -> Result<ElasticsearchAuth, FaucetError> {
        if let Some(p) = &self.auth_provider {
            return faucet_elasticsearch_common::credential_to_auth(p.credential().await?);
        }
        match &self.config.auth {
            AuthSpec::Inline(a) => Ok(a.clone()),
            AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                "auth references provider '{}' but no provider was supplied",
                r.name
            ))),
        }
    }

    /// Apply an [`ElasticsearchAuth`] to a request builder.
    fn apply_auth_value(
        req: reqwest::RequestBuilder,
        auth: &ElasticsearchAuth,
    ) -> reqwest::RequestBuilder {
        match auth {
            ElasticsearchAuth::None => req,
            ElasticsearchAuth::Basic { username, password } => {
                req.basic_auth(username, Some(password))
            }
            ElasticsearchAuth::Bearer { token } => req.bearer_auth(token),
            ElasticsearchAuth::ApiKey { key } => {
                req.header("Authorization", format!("ApiKey {key}"))
            }
        }
    }

    /// Send a `POST /_bulk` request for a slice of records and return the raw
    /// response body as a [`Value`].
    ///
    /// All HTTP-level errors (non-2xx status, network failures, JSON parse
    /// errors) surface as `Err(FaucetError::…)`. Item-level errors inside the
    /// response body are left to the caller to inspect.
    ///
    /// `auth` must be pre-resolved by the caller (once per `write_batch`) so
    /// the provider is not called on every chunk.
    async fn send_bulk_raw(
        &self,
        chunk: &[Value],
        auth: &ElasticsearchAuth,
    ) -> Result<Value, FaucetError> {
        let body = self.build_bulk_body(chunk)?;
        let url = format!("{}/_bulk", self.config.base_url);
        let req = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(body);
        let req = Self::apply_auth_value(req, auth);
        let resp = req.send().await?;
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let resp_body: Value = resp.json().await?;
        Ok(resp_body)
    }

    /// Build the NDJSON bulk request body for a slice of records.
    ///
    /// Each record is preceded by an `{"index": {...}}` action line.
    /// If `id_field` is configured, the corresponding value from each record
    /// is used as the document `_id`.
    fn build_bulk_body(&self, records: &[Value]) -> Result<String, FaucetError> {
        let mut body = String::new();

        for record in records {
            // Build the action metadata.
            let mut action_meta = serde_json::Map::new();
            action_meta.insert(
                "_index".to_string(),
                Value::String(self.config.index.clone()),
            );

            if let Some(ref id_field) = self.config.id_field
                && let Some(id_val) = record.get(id_field)
            {
                let id_str = match id_val {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                action_meta.insert("_id".to_string(), Value::String(id_str));
            }

            let action_line = serde_json::to_string(&serde_json::json!({"index": action_meta}))
                .map_err(|e| FaucetError::Sink(format!("failed to serialize bulk action: {e}")))?;
            body.push_str(&action_line);
            body.push('\n');

            let record_line = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
            body.push_str(&record_line);
            body.push('\n');
        }

        Ok(body)
    }
}

/// Extract per-item error messages from a `_bulk` response body.
///
/// Each `items` entry is `{ "<action>": { ..., "error": {...} } }` where
/// `<action>` is `index` / `create` / `update` / `delete`. We read the error
/// from whichever action key is present, so all bulk operation types are
/// handled (not just `index`).
fn extract_bulk_error_messages(resp_body: &Value) -> Vec<String> {
    resp_body
        .get("items")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.as_object()
                        .and_then(|m| m.values().next())
                        .and_then(|action| action.get("error"))
                        .map(|e| e.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

#[async_trait]
impl faucet_core::Sink for ElasticsearchSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(ElasticsearchSinkConfig))
            .expect("schema serialization")
    }

    /// Non-mutating preflight probe.
    ///
    /// Runs `GET /_cluster/health` over the existing reqwest client (probe
    /// name `"health"`). When an index is configured, a second probe
    /// (`"schema"`) issues `HEAD /<index>`: a `404` is reported as a
    /// [`Skip`](faucet_core::check::ProbeStatus::Skip) ("index not found"),
    /// any other HTTP response is a pass, and a transport error is a failure.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // Auth is shared by both probes; if it can't be resolved the whole
        // check fails on the `health` probe.
        let auth = match self.resolve_auth().await {
            Ok(a) => a,
            Err(e) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "health",
                    std::time::Duration::ZERO,
                    e.to_string(),
                    "check the configured auth / that a shared auth provider is wired up",
                )));
            }
        };

        let mut probes = Vec::new();
        let health_hint =
            "check base_url / auth / that the Elasticsearch cluster is reachable and healthy";

        // ── Probe 1: GET /_cluster/health ───────────────────────────────────
        // The per-request `.timeout(ctx.timeout)` bounds the call; this crate
        // has no direct `tokio` dependency so we rely on reqwest's own timeout.
        let started = std::time::Instant::now();
        let health_url = format!("{}/_cluster/health", self.config.base_url);
        let req = Self::apply_auth_value(self.client.get(&health_url), &auth).timeout(ctx.timeout);
        let health_probe = match req.send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    Probe::pass("health", started.elapsed())
                } else {
                    Probe::fail_hint(
                        "health",
                        started.elapsed(),
                        format!("cluster health returned HTTP {}", resp.status().as_u16()),
                        health_hint,
                    )
                }
            }
            Err(e) if e.is_timeout() => {
                Probe::fail_hint("health", started.elapsed(), "timed out", health_hint)
            }
            Err(e) => Probe::fail_hint("health", started.elapsed(), e.to_string(), health_hint),
        };
        let health_failed = matches!(
            health_probe.status,
            faucet_core::check::ProbeStatus::Fail { .. }
        );
        probes.push(health_probe);

        // ── Probe 2 (optional): HEAD /<index> ───────────────────────────────
        // Only run when the cluster itself is reachable — a transport failure
        // on the index HEAD would just duplicate the health failure.
        if !health_failed && !self.config.index.is_empty() {
            let started = std::time::Instant::now();
            let index_hint = "check that the index exists / base_url is correct";
            let index_url = format!("{}/{}", self.config.base_url, self.config.index);
            let req =
                Self::apply_auth_value(self.client.head(&index_url), &auth).timeout(ctx.timeout);
            let schema_probe = match req.send().await {
                // 404 → index absent: report as Skip, not a failure (it may be
                // auto-created on first write).
                Ok(resp) if resp.status().as_u16() == 404 => {
                    Probe::skip("schema", format!("index '{}' not found", self.config.index))
                }
                // Any other HTTP response means the host answered — the index
                // exists (2xx) or the request was rejected for some non-404
                // reason; either way the endpoint is reachable.
                Ok(_) => Probe::pass("schema", started.elapsed()),
                Err(e) if e.is_timeout() => {
                    Probe::fail_hint("schema", started.elapsed(), "timed out", index_hint)
                }
                Err(e) => Probe::fail_hint("schema", started.elapsed(), e.to_string(), index_hint),
            };
            probes.push(schema_probe);
        }

        Ok(CheckReport { probes })
    }

    /// Write records to Elasticsearch using the `_bulk` API.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size`
    /// documents and each chunk is sent as a separate `POST /_bulk` HTTP
    /// call. When `config.batch_size == 0`, the entire upstream
    /// [`StreamPage`](faucet_core::StreamPage) is sent in a single bulk
    /// request — useful when the source already sizes pages for
    /// Elasticsearch's `_bulk` sweet spot (5–15 MB NDJSON per call).
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Resolve auth once per write_batch call; reuse across chunks.
        let auth = self.resolve_auth().await?;
        let mut total_written = 0;

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: forward the entire upstream page as a single
            // `_bulk` POST. Caller is responsible for staying under
            // Elasticsearch's per-request limits.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        // At-least-once + auto-generated IDs + multi-chunk page = duplicates on a
        // resumed run (an earlier chunk commits, a later one fails, the bookmark
        // doesn't advance, and the re-sent earlier chunk is re-indexed under new
        // IDs). Warn once so operators set `id_field` (idempotent overwrite) or a
        // DLQ (per-row outcomes, no whole-page re-send).
        if resume_dup_risk(chunks.len(), self.config.id_field.is_some())
            && !self.resume_dup_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                index = %self.config.index,
                chunks = chunks.len(),
                "Elasticsearch sink: a page split across multiple _bulk chunks with \
                 auto-generated document IDs (no id_field) can produce DUPLICATES on a \
                 resumed run. Set id_field for idempotent overwrites, or configure a DLQ.",
            );
        }

        for chunk in chunks {
            let resp_body = self.send_bulk_raw(chunk, &auth).await?;

            // Check for item-level errors in the bulk response.
            if resp_body
                .get("errors")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                let error_items = extract_bulk_error_messages(&resp_body);
                if let Some(first) = error_items.first() {
                    return Err(FaucetError::Sink(format!(
                        "Elasticsearch bulk request had {} errors: {first}",
                        error_items.len(),
                    )));
                }
                // `errors: true` but no per-item error could be extracted (an
                // items shape the parser doesn't recognise). Treat as a hard
                // failure rather than counting the chunk as written — otherwise
                // failed rows are silently dropped (#78/#32).
                return Err(FaucetError::Sink(
                    "Elasticsearch bulk request reported errors:true but no per-item error \
                     could be extracted from the response — treating as a hard failure to \
                     avoid silently dropping records"
                        .into(),
                ));
            }

            total_written += chunk.len();
            tracing::debug!(records = chunk.len(), "Elasticsearch bulk batch written");
        }

        Ok(total_written)
    }

    /// Write records using the `_bulk` API, returning a per-row outcome.
    ///
    /// Unlike [`write_batch`](faucet_core::Sink::write_batch), this method never collapses
    /// item-level Elasticsearch errors into a single outer `Err`. Each
    /// document maps to exactly one [`faucet_core::RowOutcome`]:
    ///
    /// - `Ok(())` — the item was accepted (no `"error"` key in the response
    ///   action object).
    /// - `Err(FaucetError::Sink(_))` — Elasticsearch rejected the document
    ///   (the `"error"` object from the response is included in the message).
    ///
    /// HTTP-level failures (non-2xx status, network errors) still surface as
    /// an outer `Err`, because the entire chunk could not be sent.
    ///
    /// When the server returns fewer items than records sent (a malformed
    /// response), the missing tail positions are padded with
    /// `Err(FaucetError::Sink("… truncated …"))` so the caller always
    /// receives exactly `records.len()` outcomes.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Resolve auth once per write_batch_partial call; reuse across chunks.
        let auth = self.resolve_auth().await?;

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());

        for chunk in chunks {
            let resp_body = self.send_bulk_raw(chunk, &auth).await?;

            let items = resp_body
                .get("items")
                .and_then(|v| v.as_array())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            let mut chunk_outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(chunk.len());

            for item in items.iter().take(chunk.len()) {
                let action = item.get("index").or_else(|| item.get("create"));
                let error = action.and_then(|a| a.get("error"));
                if let Some(err) = error {
                    chunk_outcomes.push(Err(FaucetError::Sink(format!(
                        "Elasticsearch item rejected: {err}"
                    ))));
                } else {
                    chunk_outcomes.push(Ok(()));
                }
            }

            // Pad any missing tail positions defensively.
            while chunk_outcomes.len() < chunk.len() {
                chunk_outcomes.push(Err(FaucetError::Sink(
                    "Elasticsearch bulk response truncated — row outcome missing".into(),
                )));
            }

            outcomes.extend(chunk_outcomes);
        }

        Ok(outcomes)
    }

    fn connector_name(&self) -> &'static str {
        "elasticsearch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_dup_risk_only_when_multichunk_and_no_id_field() {
        // The duplication-on-resume risk exists only when a page splits into
        // >1 _bulk chunk AND documents get auto-generated IDs.
        assert!(
            resume_dup_risk(2, false),
            "multi-chunk + no id_field is risky"
        );
        assert!(
            !resume_dup_risk(1, false),
            "single chunk can't partially commit"
        );
        assert!(
            !resume_dup_risk(5, true),
            "id_field makes re-sends idempotent"
        );
        assert!(!resume_dup_risk(1, true));
    }
    use serde_json::json;

    #[test]
    fn extract_bulk_errors_reads_any_action_type() {
        // index + create actions, one with an error each.
        let body = json!({
            "errors": true,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
                {"create": {"status": 409, "error": {"type": "version_conflict"}}},
            ]
        });
        let errs = extract_bulk_error_messages(&body);
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(errs.iter().any(|e| e.contains("mapper_parsing_exception")));
        assert!(errs.iter().any(|e| e.contains("version_conflict")));
    }

    #[test]
    fn extract_bulk_errors_empty_when_no_item_errors() {
        // errors:true but the items shape carries no extractable error — the
        // caller treats this empty result as a hard failure (#78/#32).
        let body = json!({"errors": true, "items": [{"weird": {"status": 500}}]});
        assert!(extract_bulk_error_messages(&body).is_empty());
    }

    #[test]
    fn new_rejects_oversized_batch_size() {
        // Regression for #78/#44.
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(ElasticsearchSink::new(config).is_err());
    }

    #[test]
    fn bulk_body_without_id_field() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx");
        let sink = ElasticsearchSink::new(config).unwrap();

        let records = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
        ];

        let body = sink.build_bulk_body(&records).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();

        // 2 records = 4 lines (action + data for each).
        assert_eq!(lines.len(), 4);

        // Verify action lines contain the index.
        let action: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action["index"]["_index"], "test_idx");
        assert!(action["index"].get("_id").is_none());

        // Verify data lines.
        let data: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(data["name"], "Alice");
    }

    #[test]
    fn bulk_body_with_id_field() {
        let config =
            ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx").id_field("doc_id");
        let sink = ElasticsearchSink::new(config).unwrap();

        let records = vec![
            json!({"doc_id": "abc-123", "name": "Alice"}),
            json!({"doc_id": 42, "name": "Bob"}),
            json!({"name": "Charlie"}), // missing id field
        ];

        let body = sink.build_bulk_body(&records).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();
        assert_eq!(lines.len(), 6);

        // First record: string id.
        let action0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action0["index"]["_id"], "abc-123");

        // Second record: numeric id serialized as string.
        let action1: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(action1["index"]["_id"], "42");

        // Third record: no id field, so no _id in action.
        let action2: Value = serde_json::from_str(lines[4]).unwrap();
        assert!(action2["index"].get("_id").is_none());
    }
}
