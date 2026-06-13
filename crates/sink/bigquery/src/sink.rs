//! BigQuery streaming insert sink.

use crate::config::BigQuerySinkConfig;
use crate::idempotent;
use crate::merge;
use async_trait::async_trait;
use faucet_common_bigquery::build_client;
use faucet_core::FaucetError;
use faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::query_parameter::QueryParameter;
use gcp_bigquery_client::model::query_parameter_type::QueryParameterType;
use gcp_bigquery_client::model::query_parameter_value::QueryParameterValue;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::{QueryResponse, ResultSet};
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_data_insert_all_response::TableDataInsertAllResponse;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::OnceCell;

/// Max wall-clock spent polling an idempotent-write / token-read job to
/// completion before giving up. Exactly-once pages are small, so this is a
/// generous safety cap, not a steady-state wait.
const IDEMPOTENT_JOB_TIMEOUT: Duration = Duration::from_secs(120);

/// Server-side long-poll window per `getQueryResults` completion check —
/// BigQuery holds the connection open up to this long, so we don't busy-wait.
const JOB_POLL_LONG_POLL_MS: i32 = 10_000;

/// Serialize planned delete key tuples into a JSON array of `{key_col: value}`
/// objects for the `@deletes` parameter consumed by the semi-join `DELETE`.
fn deletes_to_payload(deletes: &[faucet_core::KeyTuple]) -> String {
    let arr: Vec<Value> = deletes
        .iter()
        .map(|kt| {
            let mut obj = serde_json::Map::new();
            for (k, v) in &kt.0 {
                obj.insert(k.clone(), v.clone());
            }
            Value::Object(obj)
        })
        .collect();
    Value::Array(arr).to_string()
}

/// A sink that writes JSON records to a BigQuery table using the streaming
/// insert API (`tabledata.insertAll`).
pub struct BigQuerySink {
    config: BigQuerySinkConfig,
    client: Client,
    /// Target table schema, fetched once on the first exactly-once call and
    /// reused for every page in the run. Empty on the streaming path.
    schema_cache: OnceCell<Vec<idempotent::FieldSpec>>,
}

impl BigQuerySink {
    /// Create a new BigQuery sink from the given configuration.
    ///
    /// This initialises the BigQuery client and authenticates with GCP.
    /// Returns a [`FaucetError::Auth`] if authentication fails.
    pub async fn new(config: BigQuerySinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        config.write.validate()?;
        let client = build_client(&config.auth).await?;
        Ok(Self {
            config,
            client,
            schema_cache: OnceCell::new(),
        })
    }

    /// Construct a sink from a pre-built BigQuery client.
    ///
    /// This is a low-level escape hatch for callers that build their own
    /// [`gcp_bigquery_client::Client`] — for example to target the
    /// [`bigquery-emulator`](https://github.com/goccy/bigquery-emulator) via
    /// [`ClientBuilder::with_v2_base_url`](gcp_bigquery_client::client_builder::ClientBuilder::with_v2_base_url),
    /// or to drive a wiremock-backed test fixture. Production code should
    /// prefer [`BigQuerySink::new`], which handles credential loading.
    #[doc(hidden)]
    pub fn from_parts(config: BigQuerySinkConfig, client: Client) -> Self {
        Self {
            config,
            client,
            schema_cache: OnceCell::new(),
        }
    }

    /// Issue a single `tabledata.insertAll` call and return the raw response.
    ///
    /// Returns `Err` only on transport-level or HTTP-level failures. Per-row
    /// `insertErrors` in the response body are surfaced to the caller as-is;
    /// it is the caller's responsibility to inspect them.
    ///
    /// `skip_invalid_rows` maps to BigQuery's `skipInvalidRows` flag. When
    /// `false` (the all-or-nothing [`write_batch`](Self::write_batch) path) a
    /// single invalid row makes BigQuery commit *nothing* and return per-row
    /// errors. When `true` (the [`write_batch_partial`] DLQ path) BigQuery
    /// commits every valid row and reports `insertErrors` only for the rejected
    /// ones — which is what makes the per-row `Ok`/`Err` mapping in
    /// `write_batch_partial` truthful (without it, the "good" siblings are
    /// reported `Ok` but were never actually committed → silent data loss).
    ///
    /// [`write_batch_partial`]: faucet_core::Sink::write_batch_partial
    async fn insert_chunk_raw(
        &self,
        rows: &[Value],
        skip_invalid_rows: bool,
    ) -> Result<TableDataInsertAllResponse, FaucetError> {
        let mut insert_request = TableDataInsertAllRequest::new();
        if skip_invalid_rows {
            insert_request.skip_invalid_rows();
        }
        for row in rows {
            // When `insert_id_field` is configured, send that field's value as
            // the streaming `insertId` so BigQuery can de-duplicate retries
            // (#78/#31). A row lacking the field is inserted without one.
            let insert_id = self.config.insert_id_field.as_ref().and_then(|field| {
                row.get(field).map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            });
            insert_request.add_row(insert_id, row).map_err(|e| {
                FaucetError::Sink(format!("failed to serialize row for BigQuery: {e}"))
            })?;
        }
        self.client
            .tabledata()
            .insert_all(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                insert_request,
            )
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery insertAll failed: {e}")))
    }

    /// Insert a single chunk of rows in one `tabledata.insertAll` call,
    /// collapsing any per-row errors into a single [`FaucetError::Sink`].
    ///
    /// Used by [`write_batch`](Self::write_batch). Callers that need per-row
    /// error granularity should use
    /// [`write_batch_partial`](faucet_core::Sink::write_batch_partial) instead,
    /// which calls [`insert_chunk_raw`](Self::insert_chunk_raw) directly.
    async fn insert_batch(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        if rows.is_empty() {
            return Ok(0);
        }

        // All-or-nothing path: `skipInvalidRows=false` so BigQuery commits the
        // whole chunk or nothing. Any `insertErrors` below becomes an outer
        // `Err`, so the pipeline aborts before the bookmark advances — no
        // partial commit to resume past.
        let response = self.insert_chunk_raw(rows, false).await?;

        // Check for per-row errors.
        if let Some(errors) = response.insert_errors
            && !errors.is_empty()
        {
            let count = errors.len();
            let first = &errors[0];
            return Err(FaucetError::Sink(format!(
                "BigQuery insertAll: {count} row(s) failed; first error on row {:?}: {:?}",
                first.index,
                first
                    .errors
                    .as_ref()
                    .and_then(|errs| errs.first())
                    .map(|e| &e.message),
            )));
        }

        Ok(rows.len())
    }

    // -----------------------------------------------------------------------
    // Exactly-once helpers
    // -----------------------------------------------------------------------

    /// Build a NAMED STRING query parameter.
    fn string_param(name: &str, value: &str) -> QueryParameter {
        QueryParameter {
            name: Some(name.to_string()),
            parameter_type: Some(QueryParameterType {
                r#type: "STRING".to_string(),
                array_type: None,
                struct_types: None,
            }),
            parameter_value: Some(QueryParameterValue {
                value: Some(value.to_string()),
                array_values: None,
                struct_values: None,
            }),
        }
    }

    /// Fetch (once) and cache the target table's schema as [`idempotent::FieldSpec`]s.
    async fn target_schema(&self) -> Result<&Vec<idempotent::FieldSpec>, FaucetError> {
        self.schema_cache
            .get_or_try_init(|| async {
                let table = self
                    .client
                    .table()
                    .get(
                        &self.config.project_id,
                        &self.config.dataset_id,
                        &self.config.table_id,
                        Some(vec!["schema"]),
                    )
                    .await
                    .map_err(|e| {
                        FaucetError::Sink(format!("BigQuery tables.get (schema) failed: {e}"))
                    })?;
                // Table.schema is TableSchema (not Option); TableSchema.fields is Option<Vec<...>>.
                let fields: Vec<idempotent::FieldSpec> = table
                    .schema
                    .fields
                    .as_ref()
                    .map(|fs| {
                        fs.iter()
                            .map(idempotent::FieldSpec::from_table_field)
                            .collect()
                    })
                    .unwrap_or_default();
                if fields.is_empty() {
                    return Err(FaucetError::Sink(format!(
                        "BigQuery target table {}.{}.{} has no schema fields; exactly-once \
                         delivery requires a table with a defined schema",
                        self.config.project_id, self.config.dataset_id, self.config.table_id
                    )));
                }
                Ok(fields)
            })
            .await
    }

    /// Create the commit-token watermark table if it does not exist.
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql =
            idempotent::build_create_commit_table(&self.config.project_id, &self.config.dataset_id);
        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery commit-table create failed: {e}")))?;
        self.await_query_complete(resp).await
    }

    /// Wait for a query/script job to finish, then authoritatively verify it
    /// succeeded. Returns `Ok(())` only once the job reaches a terminal state
    /// with no `errorResult`.
    ///
    /// Why `get_job` rather than the response `errors` field: the client maps
    /// only non-2xx HTTP to `Err`, so a job that fails at *runtime* (a CAST
    /// failure, a NULL into a REQUIRED column, …) comes back as `Ok` with the
    /// failure recorded in the job body. `Job.status.error_result` is the
    /// authoritative terminal-failure signal; the `errors` array can also carry
    /// non-fatal warnings, so it must not be treated as failure on its own.
    async fn await_query_complete(&self, initial: QueryResponse) -> Result<(), FaucetError> {
        let (job_id, location) = Self::job_reference(&initial)?;

        // Phase 1 — wait for completion via server-side long-poll (not a busy wait).
        if !initial.job_complete.unwrap_or(false) {
            let started = std::time::Instant::now();
            loop {
                let params = GetQueryResultsParameters {
                    location: location.clone(),
                    timeout_ms: Some(JOB_POLL_LONG_POLL_MS),
                    max_results: Some(0),
                    ..Default::default()
                };
                let resp = self
                    .client
                    .job()
                    .get_query_results(&self.config.project_id, &job_id, params)
                    .await
                    .map_err(|e| {
                        FaucetError::Sink(format!("BigQuery jobs.getQueryResults failed: {e}"))
                    })?;
                if resp.job_complete.unwrap_or(false) {
                    break;
                }
                if started.elapsed() >= IDEMPOTENT_JOB_TIMEOUT {
                    return Err(FaucetError::Sink(format!(
                        "BigQuery job '{job_id}' did not complete within {}s",
                        IDEMPOTENT_JOB_TIMEOUT.as_secs()
                    )));
                }
                // The server long-poll normally blocks until completion, but if
                // it returns early, back off so a still-running job can't turn
                // this into a tight request-hammering loop.
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }

        // Phase 2 — authoritative success check via the job's errorResult.
        //
        // We require an explicit terminal `DONE` state with no `errorResult`.
        // A missing `status`, a non-`DONE` state, or a present `errorResult` all
        // mean we cannot confirm the transaction durably committed — fail safe
        // (returning `Ok` here would advance the bookmark over data that may
        // never have landed, the silent-data-loss failure mode).
        let job = self
            .client
            .job()
            .get_job(&self.config.project_id, &job_id, location.as_deref())
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery jobs.get failed: {e}")))?;
        let status = job.status.ok_or_else(|| {
            FaucetError::Sink(format!(
                "BigQuery job '{job_id}' returned no status; cannot confirm durable commit"
            ))
        })?;
        if let Some(err) = status.error_result {
            return Err(FaucetError::Sink(format!(
                "BigQuery query job '{job_id}' failed: {err}"
            )));
        }
        match status.state.as_deref() {
            Some("DONE") => Ok(()),
            other => Err(FaucetError::Sink(format!(
                "BigQuery job '{job_id}' is in state {other:?}, not DONE; cannot confirm durable commit"
            ))),
        }
    }

    /// Extract `(job_id, location)` from a query response's job reference.
    fn job_reference(qr: &QueryResponse) -> Result<(String, Option<String>), FaucetError> {
        let r = qr.job_reference.as_ref().ok_or_else(|| {
            FaucetError::Sink("BigQuery query response missing jobReference".to_string())
        })?;
        let job_id = r
            .job_id
            .clone()
            .ok_or_else(|| FaucetError::Sink("BigQuery jobReference missing jobId".to_string()))?;
        Ok((job_id, r.location.clone()))
    }

    /// Run a planned upsert/delete page as one BigQuery multi-statement
    /// transaction. When `token` is `Some((scope, tok))` the watermark `MERGE`
    /// is appended inside the same transaction (exactly-once + upsert).
    ///
    /// The caller must have already validated `plan.failed` is empty (and, for
    /// the exactly-once path, ensured the commit table exists). Returns the
    /// number of rows applied (upserts + deletes).
    async fn run_upsert_script(
        &self,
        plan: &faucet_core::WritePlan,
        token: Option<(&str, &str)>,
    ) -> Result<usize, FaucetError> {
        let columns = self.target_schema().await?;
        merge::validate_keys_present(columns, &self.config.write.key)?;

        let has_upserts = !plan.upserts.is_empty();
        let has_deletes = !plan.deletes.is_empty();
        if !has_upserts && !has_deletes {
            // Nothing planned; for the exactly-once path the bookmark still
            // needs its token, so emit the watermark MERGE alone.
            if token.is_none() {
                return Ok(0);
            }
        }

        let key = &self.config.write.key;
        let (project, dataset, table) = (
            &self.config.project_id,
            &self.config.dataset_id,
            &self.config.table_id,
        );
        let sql = match token {
            Some(_) => merge::build_upsert_idempotent_sql(
                columns,
                key,
                has_upserts,
                has_deletes,
                project,
                dataset,
                table,
            ),
            None => merge::build_upsert_transaction_sql(
                columns,
                key,
                has_upserts,
                has_deletes,
                project,
                dataset,
                table,
            ),
        };

        let mut params = Vec::new();
        if has_upserts {
            let payload = serde_json::to_string(&plan.upserts).map_err(|e| {
                FaucetError::Sink(format!("bigquery upsert: serialize payload: {e}"))
            })?;
            params.push(Self::string_param("payload", &payload));
        }
        if has_deletes {
            let deletes = deletes_to_payload(&plan.deletes);
            params.push(Self::string_param("deletes", &deletes));
        }
        if let Some((scope, tok)) = token {
            params.push(Self::string_param("scope", scope));
            params.push(Self::string_param("token", tok));
        }

        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        // MERGE-by-key is idempotent, so a retried request is harmless; for the
        // exactly-once path a deterministic request_id additionally suppresses
        // duplicate jobs from a retried HTTP request within BigQuery's window.
        if let Some((scope, tok)) = token {
            req.request_id = Some(idempotent::build_request_id(scope, tok));
        }
        req.query_parameters = Some(params);

        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("bigquery upsert write failed: {e}")))?;
        self.await_query_complete(resp).await?;

        Ok(plan.upserts.len() + plan.deletes.len())
    }
}

#[async_trait]
impl faucet_core::Sink for BigQuerySink {
    fn connector_name(&self) -> &'static str {
        "bigquery"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(BigQuerySinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "bigquery://{}.{}.{}",
            self.config.project_id, self.config.dataset_id, self.config.table_id
        )
    }

    /// Preflight check (`faucet doctor`).
    ///
    /// Runs a single read-only `tables.get` against the configured
    /// `project_id.dataset_id.table_id` using the already-authenticated
    /// client built in [`BigQuerySink::new`]. This mints/uses the access
    /// token and confirms the credentials can read the target table's
    /// metadata — without inserting any rows. Auth, missing-dataset,
    /// missing-table, and permission errors all surface as a `Fail` probe
    /// with a remediation hint. The access token is never included in the
    /// reason or hint.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let fqn = format!(
            "{}.{}.{}",
            self.config.project_id, self.config.dataset_id, self.config.table_id
        );

        let result = tokio::time::timeout(
            ctx.timeout,
            self.client.table().get(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                Some(vec!["tableReference"]),
            ),
        )
        .await;

        let probe = match result {
            Ok(Ok(_table)) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!("BigQuery tables.get on {fqn} failed: {e}"),
                "Verify the service account has roles/bigquery.dataViewer (or \
                 read access) on the dataset and that the project, dataset, and \
                 table IDs are correct.",
            ),
            Err(_elapsed) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!(
                    "BigQuery tables.get on {fqn} timed out after {:?}",
                    ctx.timeout
                ),
                "Check network reachability to bigquery.googleapis.com and that \
                 credentials can be minted within the timeout.",
            ),
        };

        Ok(CheckReport::single(probe))
    }

    /// Write records to BigQuery.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` rows and
    /// each chunk is sent as a separate `tabledata.insertAll` call. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// `insertAll` request — useful when upstream `StreamPage`s are already
    /// sized for BigQuery's per-request limits.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "bigquery {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.run_upsert_script(&plan, None).await;
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: pass the entire upstream page through in a single
            // insertAll call. Subject to BigQuery's ~10MB request limit.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0;
        for chunk in chunks {
            total += self.insert_batch(chunk).await?;
        }

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            ),
            rows = total,
            "BigQuery write complete"
        );
        Ok(total)
    }

    /// Write records to BigQuery, returning a per-row outcome vector.
    ///
    /// Unlike [`write_batch`](faucet_core::Sink::write_batch), which collapses all
    /// `insertErrors` into a single `FaucetError`, this method maps each row
    /// to `Ok(())` if BigQuery accepted it or `Err(FaucetError::Sink(...))` if
    /// BigQuery reported a per-row error for it. This allows the pipeline's DLQ
    /// router to quarantine only the rows that BigQuery actually rejected while
    /// keeping already-committed siblings out of the dead-letter queue.
    ///
    /// Transport-level or HTTP-level failures (e.g. network errors, 4xx/5xx
    /// responses) are still returned as an outer `Err` because no rows can be
    /// considered committed in that case.
    ///
    /// Chunking follows the same `batch_size` semantics as `write_batch`:
    /// `batch_size == 0` sends the entire slice in one call; `batch_size > 0`
    /// splits the slice into chunks and concatenates the per-row outcomes in
    /// input order.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        use std::collections::HashMap;

        if records.is_empty() {
            return Ok(Vec::new());
        }

        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            self.run_upsert_script(&plan, None).await?;
            let mut outcomes: Vec<faucet_core::RowOutcome> =
                records.iter().map(|_| Ok(())).collect();
            for (idx, msg) in &plan.failed {
                outcomes[*idx] = Err(FaucetError::Sink(format!(
                    "bigquery {}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
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
            // `skipInvalidRows=true`: BigQuery commits every valid row and
            // returns `insertErrors` only for the rejected ones. This is what
            // makes mapping the flagged indices to `Err` and the rest to
            // `Ok(())` correct — the unflagged rows really were committed, so
            // the DLQ router quarantines only the bad rows and the bookmark
            // advances over genuinely-persisted data.
            let response = self.insert_chunk_raw(chunk, true).await?;

            // Build a set of failed row indices → first error message.
            let failed: HashMap<usize, String> = response
                .insert_errors
                .unwrap_or_default()
                .into_iter()
                .filter_map(|e| {
                    let idx = e.index? as usize;
                    let msg = e
                        .errors
                        .as_ref()
                        .and_then(|v| v.first())
                        .map(|er| er.message.clone().unwrap_or_default())
                        .unwrap_or_default();
                    Some((idx, msg))
                })
                .collect();

            for i in 0..chunk.len() {
                match failed.get(&i) {
                    Some(msg) => outcomes.push(Err(FaucetError::Sink(format!(
                        "BigQuery row rejected: {msg}"
                    )))),
                    None => outcomes.push(Ok(())),
                }
            }
        }

        Ok(outcomes)
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
        ]
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Read the last durably-committed token for `scope` from the watermark
    /// table, so the pipeline can skip already-committed pages on resume.
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;
        let mut req = QueryRequest::new(idempotent::build_select_token(
            &self.config.project_id,
            &self.config.dataset_id,
        ));
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        req.query_parameters = Some(vec![Self::string_param("scope", scope)]);

        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery token read failed: {e}")))?;

        // The watermark is a single tiny row, so `jobs.query` returns it inline.
        // If BigQuery did not complete the read synchronously, fail safe: a
        // wrong `None` here would re-run an already-committed page and produce
        // duplicates, defeating exactly-once.
        if !resp.job_complete.unwrap_or(false) {
            return Err(FaucetError::Sink(
                "BigQuery watermark read did not complete synchronously".to_string(),
            ));
        }
        // `ResultSet` only yields rows when the response carries a schema; a
        // completed `SELECT` always returns one. If it is somehow absent we
        // cannot tell "no committed token" from "row present but unreadable",
        // and a wrong `None` would replay committed pages — fail safe instead.
        if resp.schema.is_none() {
            return Err(FaucetError::Sink(
                "BigQuery watermark read returned no schema; cannot trust the token result"
                    .to_string(),
            ));
        }

        let mut rs = ResultSet::new_from_query_response(resp);
        if rs.next_row() {
            rs.get_string_by_name(COMMIT_TOKEN_TOKEN_COL)
                .map_err(|e| FaucetError::Sink(format!("BigQuery token decode failed: {e}")))
        } else {
            Ok(None)
        }
    }

    /// Atomically write `records` and record `token` for `scope` in one BigQuery
    /// multi-statement transaction: a typed `INSERT … SELECT FROM
    /// UNNEST(JSON_QUERY_ARRAY(@payload))` plus a watermark `MERGE`. Either both
    /// the rows and the token commit, or neither does — so a crash/resume skips
    /// the already-committed page (zero duplicates) and a failed page replays
    /// cleanly.
    ///
    /// The entire page is one atomic unit (no `batch_size` re-chunking — core
    /// issues exactly one token per page), so the page must serialize within
    /// BigQuery's ~10 MB `jobs.query` request limit.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_commit_table().await?;

        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "bigquery {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.run_upsert_script(&plan, Some((scope, token))).await;
        }

        let columns = self.target_schema().await?;

        let payload = serde_json::to_string(records).map_err(|e| {
            FaucetError::Sink(format!(
                "BigQuery exactly-once: serialize page payload: {e}"
            ))
        })?;

        let sql = idempotent::build_transaction_sql(
            columns,
            &self.config.project_id,
            &self.config.dataset_id,
            &self.config.table_id,
        );
        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        req.request_id = Some(idempotent::build_request_id(scope, token));
        req.query_parameters = Some(vec![
            Self::string_param("payload", &payload),
            Self::string_param("scope", scope),
            Self::string_param("token", token),
        ]);

        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery idempotent write failed: {e}")))?;
        self.await_query_complete(resp).await?;

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            ),
            rows = records.len(),
            token = %token,
            "BigQuery exactly-once page committed"
        );
        Ok(records.len())
    }
}

#[cfg(test)]
mod tests {
    use super::deletes_to_payload;
    use faucet_core::KeyTuple;
    use serde_json::json;

    // dataset_uri test is skipped: BigQuerySink::new() requires GCP credentials
    // (build_client fetches auth in new()), and from_parts() requires a
    // gcp_bigquery_client::Client which cannot be constructed without auth.

    #[test]
    fn deletes_to_payload_preserves_number_type() {
        // The delete payload must keep an integer key as a JSON number (not the
        // string "2"), so the matching `CAST(JSON_VALUE(d, '$.id') AS INT64)`
        // semi-join compares like-for-like.
        let p = deletes_to_payload(&[KeyTuple(vec![("id".into(), json!(2))])]);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v, json!([{"id": 2}]));
        assert!(v[0]["id"].is_number(), "id must serialize as a number: {p}");
    }

    #[test]
    fn deletes_to_payload_composite_key_roundtrips() {
        let p = deletes_to_payload(&[KeyTuple(vec![
            ("tenant".into(), json!("acme")),
            ("id".into(), json!(7)),
        ])]);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v, json!([{"tenant": "acme", "id": 7}]));
    }

    #[test]
    fn deletes_to_payload_multiple_rows() {
        let p = deletes_to_payload(&[
            KeyTuple(vec![("id".into(), json!(1))]),
            KeyTuple(vec![("id".into(), json!(2))]),
        ]);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v, json!([{"id": 1}, {"id": 2}]));
    }
}
