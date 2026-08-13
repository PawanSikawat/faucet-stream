//! Elasticsearch bulk index sink.

use crate::config::{ElasticsearchAuth, ElasticsearchSinkConfig};
use async_trait::async_trait;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{
    AuthSpec, FaucetError, SchemaEvolution, SharedAuthProvider, SqlBaseType, json_schema_base_type,
};
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
    /// One-shot guard for the "cannot change an existing field's mapping" debug
    /// log emitted by [`evolve_schema`](faucet_core::Sink::evolve_schema) when an
    /// evolution carries widenings / nullability relaxations (no-ops on ES).
    evolve_noop_warned: AtomicBool,
}

impl ElasticsearchSink {
    /// Create a new Elasticsearch sink from the given configuration.
    ///
    /// Returns [`FaucetError::Config`] if `batch_size` exceeds
    /// `MAX_BATCH_SIZE` (#78/#44).
    pub fn new(config: ElasticsearchSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        // Schemaless target: upsert/delete only need a non-empty `key` (no
        // column-mapping guard like the SQL sinks).
        config.write.validate()?;
        Ok(Self {
            config,
            client: Client::new(),
            auth_provider: None,
            resume_dup_warned: AtomicBool::new(false),
            evolve_noop_warned: AtomicBool::new(false),
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
            return faucet_common_elasticsearch::credential_to_auth(p.credential().await?);
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
        self.send_bulk_body(body, auth).await
    }

    /// Send a pre-built NDJSON `_bulk` body and return the parsed response.
    ///
    /// Shared by the append path ([`send_bulk_raw`](Self::send_bulk_raw)) and
    /// the upsert/delete path ([`build_plan_body`](Self::build_plan_body)).
    async fn send_bulk_body(
        &self,
        body: String,
        auth: &ElasticsearchAuth,
    ) -> Result<Value, FaucetError> {
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

    /// Check a `_bulk` response body for item-level errors and return an outer
    /// `Err` matching the append path's behaviour (#78/#32). `Ok(())` when the
    /// bulk request reports no errors.
    fn check_bulk_errors(resp_body: &Value) -> Result<(), FaucetError> {
        if resp_body
            .get("errors")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let error_items = extract_bulk_error_messages(resp_body);
            if let Some(first) = error_items.first() {
                return Err(FaucetError::Sink(format!(
                    "Elasticsearch bulk request had {} errors: {first}",
                    error_items.len(),
                )));
            }
            return Err(FaucetError::Sink(
                "Elasticsearch bulk request reported errors:true but no per-item error \
                 could be extracted from the response — treating as a hard failure to \
                 avoid silently dropping records"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Build the action-metadata map for a `_bulk` action line, seeded with the
    /// configured `_index` and an optional explicit `_id`.
    fn action_meta(&self, id: Option<String>) -> serde_json::Map<String, Value> {
        let mut action_meta = serde_json::Map::new();
        action_meta.insert(
            "_index".to_string(),
            Value::String(self.config.index.clone()),
        );
        if let Some(id) = id {
            action_meta.insert("_id".to_string(), Value::String(id));
        }
        action_meta
    }

    /// Append an `{ "<action>": {...} }` line followed by an optional doc line
    /// to the NDJSON `body`.
    fn push_bulk_line(
        body: &mut String,
        action: &str,
        meta: serde_json::Map<String, Value>,
        doc: Option<&Value>,
    ) -> Result<(), FaucetError> {
        let action_line = serde_json::to_string(&serde_json::json!({ action: meta }))
            .map_err(|e| FaucetError::Sink(format!("failed to serialize bulk action: {e}")))?;
        body.push_str(&action_line);
        body.push('\n');
        if let Some(doc) = doc {
            let doc_line = serde_json::to_string(doc)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
            body.push_str(&doc_line);
            body.push('\n');
        }
        Ok(())
    }

    /// Build the NDJSON bulk request body for a slice of records (append mode).
    ///
    /// Each record is preceded by an `{"index": {...}}` action line.
    /// If `id_field` is configured, the corresponding value from each record
    /// is used as the document `_id`.
    fn build_bulk_body(&self, records: &[Value]) -> Result<String, FaucetError> {
        let mut body = String::new();

        for record in records {
            let id = self.config.id_field.as_ref().and_then(|id_field| {
                record.get(id_field).map(|id_val| match id_val {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            });
            let meta = self.action_meta(id);
            Self::push_bulk_line(&mut body, "index", meta, Some(record))?;
        }

        Ok(body)
    }

    /// Build the NDJSON bulk body for an upsert/delete [`WritePlan`](faucet_core::WritePlan).
    ///
    /// Each `plan.upserts` row becomes an `{"index":{"_id":…}}` action (an
    /// idempotent overwrite) whose `_id` is derived from the row's `key`
    /// columns — this **overrides** any `id_field`. The row itself (already
    /// marker-stripped by the planner) is the doc line.
    ///
    /// Each `plan.deletes` key tuple becomes a `{"delete":{"_id":…}}` action
    /// with **no** doc line.
    fn build_plan_body(&self, plan: &faucet_core::WritePlan) -> Result<String, FaucetError> {
        let key = &self.config.write.key;
        let mut body = String::new();

        for row in &plan.upserts {
            let id = doc_id_from_row(row, key);
            let meta = self.action_meta(Some(id));
            Self::push_bulk_line(&mut body, "index", meta, Some(row))?;
        }
        for kt in &plan.deletes {
            // Composite ids are canonical-JSON encoded by the injective core
            // helper; the separator is retained for API stability and unused
            // for multi-column keys.
            let id = faucet_core::key_to_doc_id(kt, ":");
            let meta = self.action_meta(Some(id));
            Self::push_bulk_line(&mut body, "delete", meta, None)?;
        }

        Ok(body)
    }
}

/// Build a document `_id` from an upsert row's `key` columns, in `key` order.
///
/// Delegates to the injective [`faucet_core::key_to_doc_id`] so a single-column
/// key renders as its plain string / JSON form and a **composite** key renders
/// as a canonical JSON array of its values (never a separator-join, which is not
/// injective — `["a_","b"]` and `["a","_b"]` would both collapse to `"a__b"`,
/// silently overwriting two distinct rows). The separator argument is retained
/// only for API stability and is unused for composite keys.
///
/// Key columns are guaranteed present — [`faucet_core::plan_writes`] validated
/// them before the row reached `plan.upserts`. A missing column would only
/// occur on a planner contract violation, so it renders as `null` (via the core
/// helper) rather than panicking.
fn doc_id_from_row(row: &Value, key: &[String]) -> String {
    let kt = faucet_core::KeyTuple(
        key.iter()
            .map(|col| {
                let v = row.get(col).cloned().unwrap_or(Value::Null);
                (col.clone(), v)
            })
            .collect(),
    );
    faucet_core::key_to_doc_id(&kt, ":")
}

/// A [`faucet_core::WritePlan`] paired with, for each emitted bulk action (in
/// body order: all upserts then all deletes), the **original page indices**
/// that deduped into it. Used by `write_batch_partial` to attribute per-item
/// `_bulk` results back to original records for per-row DLQ routing (#F14).
struct PlanWithOrigins {
    plan: faucet_core::WritePlan,
    /// One entry per emitted bulk action, in the same order as
    /// `plan.upserts` followed by `plan.deletes`. Each entry is the list of
    /// original page indices that the (deduped) action represents.
    origins: Vec<Vec<usize>>,
    /// `(page_index, message)` for rows whose key could not be extracted —
    /// mirrors [`faucet_core::WritePlan::failed`].
    failed: Vec<(usize, String)>,
}

/// Replay the [`faucet_core::plan_writes`] partition (same key extraction, same
/// last-write-wins dedup, same upsert/delete routing) while additionally
/// recording the original page indices behind each emitted action.
///
/// This is intentionally a faithful re-derivation of the core planner so the
/// resulting `plan` is byte-for-byte what `plan_writes` would produce; the only
/// extra output is the origin-index mapping, which the core planner discards.
/// `WriteMode::Append` must never reach here (callers route append separately).
fn plan_origins(page: &[Value], spec: &faucet_core::WriteSpec) -> PlanWithOrigins {
    use faucet_core::{KeyTuple, WriteMode};

    // A planned action plus the original indices that fed into it.
    enum Slot {
        Upsert(Value, Vec<usize>),
        Delete(KeyTuple, Vec<usize>),
    }

    let key = &spec.key;
    let marker = spec.delete_marker.as_ref();
    let mut failed: Vec<(usize, String)> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut order: Vec<Slot> = Vec::new();

    for (i, rec) in page.iter().enumerate() {
        // Extract key in `key` order; missing/null key → failed (matches core).
        let obj = match rec.as_object() {
            Some(o) => o,
            None => {
                failed.push((i, "record is not a JSON object".to_string()));
                continue;
            }
        };
        let mut kv: Vec<(String, Value)> = Vec::with_capacity(key.len());
        let mut key_err: Option<String> = None;
        for col in key {
            match obj.get(col) {
                None => {
                    key_err = Some(format!("missing key column '{col}'"));
                    break;
                }
                Some(Value::Null) => {
                    key_err = Some(format!("null value for key column '{col}'"));
                    break;
                }
                Some(v) => kv.push((col.clone(), v.clone())),
            }
        }
        if let Some(msg) = key_err {
            failed.push((i, msg));
            continue;
        }
        let key_tuple = KeyTuple(kv);

        // Stable canonical dedup string (matches core's `canonical`).
        let canon = {
            let arr: Vec<&Value> = key_tuple.0.iter().map(|(_, v)| v).collect();
            serde_json::to_string(&arr).expect("a Vec<&serde_json::Value> always serializes")
        };

        let is_delete = match spec.write_mode {
            WriteMode::Delete => true,
            WriteMode::Upsert => is_delete_marked(rec, marker),
            WriteMode::Append => false,
        };

        let new_slot = if is_delete {
            Slot::Delete(key_tuple, vec![i])
        } else {
            Slot::Upsert(strip_marker(rec.clone(), marker), vec![i])
        };

        match index.get(&canon) {
            Some(&pos) => {
                // Last-write-wins: replace the action but ACCUMULATE origins so
                // a per-item failure for this key fails every input row for it.
                let origins = match &order[pos] {
                    Slot::Upsert(_, o) | Slot::Delete(_, o) => {
                        let mut o = o.clone();
                        o.push(i);
                        o
                    }
                };
                order[pos] = match new_slot {
                    Slot::Upsert(v, _) => Slot::Upsert(v, origins),
                    Slot::Delete(k, _) => Slot::Delete(k, origins),
                };
            }
            None => {
                index.insert(canon, order.len());
                order.push(new_slot);
            }
        }
    }

    // Split into the WritePlan + origins in body order: upserts first, then
    // deletes (exactly what `build_plan_body` emits).
    let mut plan = faucet_core::WritePlan {
        upserts: Vec::new(),
        deletes: Vec::new(),
        failed: failed.clone(),
    };
    let mut upsert_origins: Vec<Vec<usize>> = Vec::new();
    let mut delete_origins: Vec<Vec<usize>> = Vec::new();
    for slot in order {
        match slot {
            Slot::Upsert(v, o) => {
                plan.upserts.push(v);
                upsert_origins.push(o);
            }
            Slot::Delete(k, o) => {
                plan.deletes.push(k);
                delete_origins.push(o);
            }
        }
    }
    let mut origins = upsert_origins;
    origins.extend(delete_origins);

    PlanWithOrigins {
        plan,
        origins,
        failed,
    }
}

/// True when `rec`'s `marker.field` equals one of `marker.values`. Mirrors the
/// private `is_delete_marked` in `faucet_core::write_mode`.
fn is_delete_marked(rec: &Value, marker: Option<&faucet_core::DeleteMarker>) -> bool {
    let Some(dm) = marker else { return false };
    let Some(v) = rec.get(&dm.field) else {
        return false;
    };
    let Some(s) = v.as_str() else { return false };
    dm.values.iter().any(|m| m == s)
}

/// Remove `marker.field` from an upsert row. Mirrors the private `strip_marker`
/// in `faucet_core::write_mode`.
fn strip_marker(mut rec: Value, marker: Option<&faucet_core::DeleteMarker>) -> Value {
    if let (Some(dm), Value::Object(map)) = (marker, &mut rec) {
        map.remove(&dm.field);
    }
    rec
}

/// Map an Elasticsearch field-mapping `type` to a JSON-Schema base type name.
///
/// Numeric families collapse to `integer`/`number`, `boolean` is its own base,
/// `object`/`nested` are `object`, and everything else (`keyword`, `text`,
/// `date`, `ip`, …) is treated as `string`.
fn es_type_to_json(es_type: &str) -> &'static str {
    match es_type {
        "long" | "integer" | "short" | "byte" => "integer",
        "double" | "float" | "half_float" | "scaled_float" => "number",
        "boolean" => "boolean",
        "object" | "nested" => "object",
        _ => "string",
    }
}

/// Map a backend-neutral [`SqlBaseType`] to the Elasticsearch field-mapping
/// `type` used when adding a column via `PUT /<index>/_mapping`.
fn base_to_es(base: SqlBaseType) -> &'static str {
    match base {
        SqlBaseType::Integer => "long",
        SqlBaseType::Double => "double",
        SqlBaseType::Boolean => "boolean",
        SqlBaseType::Text => "keyword",
        SqlBaseType::Json => "object",
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

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}",
            faucet_core::redact_uri_credentials(&self.config.base_url),
            self.config.index
        )
    }

    /// Elasticsearch is schemaless and `_id`-addressable, so all three write
    /// modes are supported: upsert and delete derive the document `_id` from
    /// the configured `key`, and the `_bulk` `index` / `delete` actions are
    /// idempotent overwrites / removals by `_id`.
    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
        ]
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    /// Elasticsearch can add new fields to an existing index in place via
    /// `PUT /<index>/_mapping`, so additive schema evolution is supported.
    /// (Changing an existing field's mapping type is *not* possible — see
    /// [`evolve_schema`](faucet_core::Sink::evolve_schema).)
    fn supports_schema_evolution(&self) -> bool {
        true
    }

    /// Read the index's live field mappings via `GET /<index>/_mapping`.
    ///
    /// Returns an `infer_schema`-shaped object schema with every field marked
    /// nullable (Elasticsearch has no NOT NULL concept). A `404` (index does not
    /// exist) yields `Ok(None)`; an index that exists with no explicit
    /// `properties` yields an empty `{"type":"object","properties":{}}`.
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        let auth = self.resolve_auth().await?;
        let url = format!("{}/{}/_mapping", self.config.base_url, self.config.index);
        let req = Self::apply_auth_value(self.client.get(&url), &auth);
        let resp = req.send().await?;

        // A missing index is reported as drift-inert (Ok(None)), not an error.
        // Detect the 404 *before* check_http_response, which treats it as an error.
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let body: Value = resp.json().await?;

        // Shape: { "<index>": { "mappings": { "properties": { "<f>": {"type": …} } } } }.
        // The top-level key is the concrete index name (exactly one entry).
        let index_obj = body
            .get(&self.config.index)
            .or_else(|| body.as_object().and_then(|m| m.values().next()));
        let mappings = index_obj.and_then(|v| v.get("mappings"));
        let properties = mappings
            .and_then(|m| m.get("properties"))
            .and_then(|p| p.as_object());

        let mut out_props = serde_json::Map::new();
        if let Some(properties) = properties {
            for (field, def) in properties {
                let es_type = def.get("type").and_then(|t| t.as_str()).unwrap_or("object");
                let base = es_type_to_json(es_type);
                // ES has no NOT NULL → every field is nullable.
                out_props.insert(field.clone(), serde_json::json!({ "type": [base, "null"] }));
            }
        }

        Ok(Some(serde_json::json!({
            "type": "object",
            "properties": out_props,
        })))
    }

    /// Apply an additive schema evolution to the index via
    /// `PUT /<index>/_mapping`.
    ///
    /// Only [`additions`](faucet_core::SchemaEvolution::additions) are applied —
    /// Elasticsearch cannot change an existing field's mapping type or
    /// nullability in place, so
    /// [`widenings`](faucet_core::SchemaEvolution::widenings) and
    /// [`relax_nullability`](faucet_core::SchemaEvolution::relax_nullability) are
    /// no-ops (a one-shot `debug` log notes the limitation). A `PUT` is only
    /// issued when there is at least one addition.
    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        if (!evolution.widenings.is_empty() || !evolution.relax_nullability.is_empty())
            && !self.evolve_noop_warned.swap(true, Ordering::Relaxed)
        {
            tracing::debug!(
                index = %self.config.index,
                "elasticsearch cannot change an existing field's mapping type / nullability; \
                 left as-is"
            );
        }

        if evolution.additions.is_empty() {
            return Ok(());
        }

        let mut properties = serde_json::Map::new();
        for change in &evolution.additions {
            let base = json_schema_base_type(&change.to).unwrap_or(SqlBaseType::Text);
            properties.insert(
                change.name.clone(),
                serde_json::json!({ "type": base_to_es(base) }),
            );
        }
        let body = serde_json::json!({ "properties": properties });

        let auth = self.resolve_auth().await?;
        let url = format!("{}/{}/_mapping", self.config.base_url, self.config.index);
        let req = self
            .client
            .put(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&body).map_err(|e| {
                FaucetError::Sink(format!("failed to serialize mapping update: {e}"))
            })?);
        let req = Self::apply_auth_value(req, &auth);
        let resp = req.send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        tracing::debug!(
            index = %self.config.index,
            added = evolution.additions.len(),
            "Elasticsearch mapping evolved (fields added)"
        );
        Ok(())
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

        // Upsert / delete routing: plan the page (dedup last-write-wins, strip
        // the delete marker) and emit `index` / `delete` bulk actions whose
        // `_id` derives from `key`. Append falls through to the existing
        // chunked `index` fast path below.
        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "elasticsearch {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            let body = self.build_plan_body(&plan)?;
            let written = plan.upserts.len() + plan.deletes.len();
            if written == 0 {
                return Ok(0);
            }
            let resp_body = self.send_bulk_body(body, &auth).await?;
            Self::check_bulk_errors(&resp_body)?;
            tracing::debug!(
                upserts = plan.upserts.len(),
                deletes = plan.deletes.len(),
                "Elasticsearch upsert/delete bulk written"
            );
            return Ok(written);
        }

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

            // Check for item-level errors in the bulk response. `errors: true`
            // with no extractable per-item error is treated as a hard failure
            // rather than silently dropping the chunk (#78/#32).
            Self::check_bulk_errors(&resp_body)?;

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

        // Upsert / delete routing with PER-ROW fidelity (#F14).
        //
        // The whole point of overriding `write_batch_partial` is to give the
        // DLQ router per-row outcomes so `OnBatchError::DlqAll` only enqueues
        // rows that genuinely failed. The old code parsed the `_bulk` response
        // but then called `check_bulk_errors(..)?`, collapsing any per-item
        // error into an OUTER `Err` — under `dlq_all` that re-routed EVERY row
        // in the page (including the upsert/delete rows Elasticsearch had
        // already applied) to the DLQ while the bookmark still advanced →
        // silent downstream duplication. We now attribute each `_bulk` item
        // result back to its original page index/indices and return per-row
        // outcomes, never an outer `Err` for an item-level rejection.
        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            // `plan_origins` mirrors `plan_writes` (same key extraction, same
            // last-write-wins dedup) but additionally records, for each emitted
            // bulk action, the original page indices that fed into it. Because
            // the planner dedups by key, several input rows can map to one
            // action; if that action is rejected we mark all of those indices
            // `Err` (the final write for that key failed); if it succeeds they
            // are all `Ok`.
            let planned = plan_origins(records, &self.config.write);

            let mut outcomes: Vec<faucet_core::RowOutcome> =
                (0..records.len()).map(|_| Ok(())).collect();
            // Missing/null-key rows: marked `Err` at their original index.
            for (idx, msg) in &planned.failed {
                outcomes[*idx] = Err(FaucetError::Sink(format!(
                    "elasticsearch {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }

            if !planned.plan.upserts.is_empty() || !planned.plan.deletes.is_empty() {
                let body = self.build_plan_body(&planned.plan)?;
                // A transport/HTTP failure means the whole chunk could not be
                // sent — that genuinely aborts the batch (outer `Err`), matching
                // the append path. Item-level rejections are handled per-row
                // below.
                let resp_body = self.send_bulk_body(body, &auth).await?;

                // `items` are in request order: all `index` (upsert) actions
                // first, then all `delete` actions — exactly the order
                // `build_plan_body` emits and `planned.origins` records.
                let items = resp_body
                    .get("items")
                    .and_then(|v| v.as_array())
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let action_count = planned.plan.upserts.len() + planned.plan.deletes.len();

                for (action_pos, origins) in planned.origins.iter().enumerate() {
                    // Read the per-item result. The `_bulk` item object is keyed
                    // by the action verb (`index` for upserts, `delete` for
                    // deletes); accept any of the verbs defensively.
                    let item_err = items.get(action_pos).and_then(|item| {
                        item.get("index")
                            .or_else(|| item.get("create"))
                            .or_else(|| item.get("delete"))
                            .or_else(|| item.get("update"))
                            .and_then(|a| a.get("error"))
                    });
                    let outcome: faucet_core::RowOutcome = if let Some(err) = item_err {
                        Err(FaucetError::Sink(format!(
                            "Elasticsearch item rejected: {err}"
                        )))
                    } else if action_pos >= items.len() {
                        // Server returned fewer items than actions sent — treat
                        // the missing tail as failed rather than silently
                        // dropping records.
                        Err(FaucetError::Sink(
                            "Elasticsearch bulk response truncated — item outcome missing".into(),
                        ))
                    } else {
                        Ok(())
                    };
                    // Propagate this action's result to every original index that
                    // deduped into it.
                    for &orig in origins {
                        // A genuine per-row failure overrides the default `Ok`;
                        // never overwrite an already-recorded missing-key `Err`.
                        if outcomes[orig].is_ok() {
                            outcomes[orig] = match &outcome {
                                Ok(()) => Ok(()),
                                Err(e) => Err(FaucetError::Sink(e.to_string())),
                            };
                        }
                    }
                }
                debug_assert_eq!(planned.origins.len(), action_count);
            }
            return Ok(outcomes);
        }

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
    use faucet_core::Sink as _;

    #[test]
    fn dataset_uri_combines_base_url_and_index() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "my-index");
        let sink = ElasticsearchSink::new(config).unwrap();
        assert_eq!(sink.dataset_uri(), "http://localhost:9200/my-index");
    }

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

    #[test]
    fn es_mapping_to_json_schema_types() {
        // Numeric families collapse to integer / number.
        assert_eq!(es_type_to_json("long"), "integer");
        assert_eq!(es_type_to_json("integer"), "integer");
        assert_eq!(es_type_to_json("short"), "integer");
        assert_eq!(es_type_to_json("byte"), "integer");
        assert_eq!(es_type_to_json("double"), "number");
        assert_eq!(es_type_to_json("float"), "number");
        assert_eq!(es_type_to_json("half_float"), "number");
        assert_eq!(es_type_to_json("scaled_float"), "number");
        // Boolean + object families.
        assert_eq!(es_type_to_json("boolean"), "boolean");
        assert_eq!(es_type_to_json("object"), "object");
        assert_eq!(es_type_to_json("nested"), "object");
        // Everything else → string.
        assert_eq!(es_type_to_json("keyword"), "string");
        assert_eq!(es_type_to_json("text"), "string");
        assert_eq!(es_type_to_json("date"), "string");
        assert_eq!(es_type_to_json("ip"), "string");
        assert_eq!(es_type_to_json("geo_point"), "string");
    }

    #[test]
    fn base_to_es_types() {
        assert_eq!(base_to_es(SqlBaseType::Integer), "long");
        assert_eq!(base_to_es(SqlBaseType::Double), "double");
        assert_eq!(base_to_es(SqlBaseType::Boolean), "boolean");
        assert_eq!(base_to_es(SqlBaseType::Text), "keyword");
        assert_eq!(base_to_es(SqlBaseType::Json), "object");
    }

    #[test]
    fn doc_id_composite_key_is_canonical_json_not_separator_join() {
        // F13: the core helper is now injective — a composite key is encoded as
        // a canonical JSON array, NOT a `:`-join, so the separator can't collide.
        let kt = faucet_core::KeyTuple(vec![
            ("tenant".to_string(), serde_json::json!("acme")),
            ("id".to_string(), serde_json::json!(7)),
        ]);
        assert_eq!(faucet_core::key_to_doc_id(&kt, ":"), "[\"acme\",7]");
    }

    #[test]
    fn doc_id_from_row_uses_injective_core_encoding() {
        // Composite key: now canonical-JSON encoded (F13) — NOT a separator
        // join — so it is injective. In `key` declaration order.
        let row = json!({"id": 7, "tenant": "acme", "v": "x"});
        let key = vec!["tenant".to_string(), "id".to_string()];
        // Matches faucet_core::key_to_doc_id for the same KeyTuple.
        let kt = faucet_core::KeyTuple(vec![
            ("tenant".to_string(), json!("acme")),
            ("id".to_string(), json!(7)),
        ]);
        assert_eq!(
            doc_id_from_row(&row, &key),
            faucet_core::key_to_doc_id(&kt, ":")
        );

        // Single string key column → rendered plain (no separator possible).
        let row = json!({"id": "abc-123"});
        assert_eq!(doc_id_from_row(&row, &["id".to_string()]), "abc-123");
    }

    #[test]
    fn doc_id_from_row_composite_is_injective_no_collision() {
        // F13 regression: two distinct composite keys that would collide under a
        // naive separator-join must now produce DISTINCT `_id`s.
        let key = vec!["a".to_string(), "b".to_string()];
        let id1 = doc_id_from_row(&json!({"a": "x_", "b": "y"}), &key);
        let id2 = doc_id_from_row(&json!({"a": "x", "b": "_y"}), &key);
        assert_ne!(
            id1, id2,
            "distinct composite keys must not collapse to the same _id"
        );
        // And both go through the injective core helper, not a `:`-join.
        assert!(!id1.contains("x_:y") && !id2.contains("x:_y"));
    }

    #[test]
    fn plan_body_upsert_uses_key_id_and_strips_marker() {
        use faucet_core::{DeleteMarker, WriteMode, WriteSpec};

        let config = ElasticsearchSinkConfig {
            id_field: Some("ignored".to_string()),
            write: WriteSpec {
                write_mode: WriteMode::Upsert,
                key: vec!["id".to_string()],
                delete_marker: Some(DeleteMarker {
                    field: "__op".to_string(),
                    values: vec!["d".to_string()],
                }),
                cleanup: None,
            },
            ..ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
        };
        let sink = ElasticsearchSink::new(config).unwrap();

        let records = vec![
            json!({"id": 1, "v": "a"}),
            json!({"id": 2, "v": "x", "__op": "d"}),
        ];
        let plan = faucet_core::plan_writes(&records, &sink.config.write);
        let body = sink.build_plan_body(&plan).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();

        // 1 upsert (action + doc) + 1 delete (action only) = 3 lines.
        assert_eq!(lines.len(), 3, "{lines:?}");

        // Upsert action: `_id` derived from the key (overrides id_field).
        let action0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action0["index"]["_id"], "1");
        assert_eq!(action0["index"]["_index"], "idx");
        // Doc line: the marker field is stripped.
        let doc0: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(doc0["v"], "a");
        assert!(doc0.get("__op").is_none());

        // Delete action: key-derived `_id`, NO doc line follows.
        let action1: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(action1["delete"]["_id"], "2");
        assert_eq!(action1["delete"]["_index"], "idx");
    }
}
