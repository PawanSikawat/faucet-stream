//! BigQuery streaming insert sink.

use crate::config::BigQuerySinkConfig;
use crate::idempotent;
use async_trait::async_trait;
use faucet_common_bigquery::build_client;
use faucet_core::FaucetError;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::query_parameter::QueryParameter;
use gcp_bigquery_client::model::query_parameter_type::QueryParameterType;
use gcp_bigquery_client::model::query_parameter_value::QueryParameterValue;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::QueryResponse;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_data_insert_all_response::TableDataInsertAllResponse;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::OnceCell;

/// Max wall-clock spent polling an idempotent-write / token-read job to
/// completion before giving up. Exactly-once pages are small, so this is a
/// generous safety cap, not a steady-state wait.
#[allow(dead_code)]
const IDEMPOTENT_JOB_TIMEOUT: Duration = Duration::from_secs(120);

/// Server-side long-poll window per `getQueryResults` completion check —
/// BigQuery holds the connection open up to this long, so we don't busy-wait.
#[allow(dead_code)]
const JOB_POLL_LONG_POLL_MS: i32 = 10_000;

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
    // Exactly-once helpers (wired into trait hooks in a later task)
    // -----------------------------------------------------------------------

    /// Build a NAMED STRING query parameter.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
                    .map(|fs| fs.iter().map(idempotent::FieldSpec::from_table_field).collect())
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
    #[allow(dead_code)]
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql = idempotent::build_create_commit_table(
            &self.config.project_id,
            &self.config.dataset_id,
        );
        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("BigQuery commit-table create failed: {e}"))
            })?;
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
    #[allow(dead_code)]
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
            }
        }

        // Phase 2 — authoritative success check via the job's errorResult.
        let job = self
            .client
            .job()
            .get_job(&self.config.project_id, &job_id, location.as_deref())
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery jobs.get failed: {e}")))?;
        match job.status.and_then(|s| s.error_result) {
            Some(err) => Err(FaucetError::Sink(format!(
                "BigQuery query job '{job_id}' failed: {err}"
            ))),
            None => Ok(()),
        }
    }

    /// Extract `(job_id, location)` from a query response's job reference.
    #[allow(dead_code)]
    fn job_reference(qr: &QueryResponse) -> Result<(String, Option<String>), FaucetError> {
        let r = qr.job_reference.as_ref().ok_or_else(|| {
            FaucetError::Sink("BigQuery query response missing jobReference".to_string())
        })?;
        let job_id = r.job_id.clone().ok_or_else(|| {
            FaucetError::Sink("BigQuery jobReference missing jobId".to_string())
        })?;
        Ok((job_id, r.location.clone()))
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
}

#[cfg(test)]
mod tests {
    // dataset_uri test is skipped: BigQuerySink::new() requires GCP credentials
    // (build_client fetches auth in new()), and from_parts() requires a
    // gcp_bigquery_client::Client which cannot be constructed without auth.
}
