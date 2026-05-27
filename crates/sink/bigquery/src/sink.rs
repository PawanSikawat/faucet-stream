//! BigQuery streaming insert sink.

use crate::config::BigQuerySinkConfig;
use async_trait::async_trait;
use faucet_bigquery_common::build_client;
use faucet_core::FaucetError;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_data_insert_all_response::TableDataInsertAllResponse;
use serde_json::Value;

/// A sink that writes JSON records to a BigQuery table using the streaming
/// insert API (`tabledata.insertAll`).
pub struct BigQuerySink {
    config: BigQuerySinkConfig,
    client: Client,
}

impl BigQuerySink {
    /// Create a new BigQuery sink from the given configuration.
    ///
    /// This initialises the BigQuery client and authenticates with GCP.
    /// Returns a [`FaucetError::Auth`] if authentication fails.
    pub async fn new(config: BigQuerySinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let client = build_client(&config.credentials).await?;
        Ok(Self { config, client })
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
        Self { config, client }
    }

    /// Issue a single `tabledata.insertAll` call and return the raw response.
    ///
    /// Returns `Err` only on transport-level or HTTP-level failures. Per-row
    /// `insertErrors` in the response body are surfaced to the caller as-is;
    /// it is the caller's responsibility to inspect them.
    async fn insert_chunk_raw(
        &self,
        rows: &[Value],
    ) -> Result<TableDataInsertAllResponse, FaucetError> {
        let mut insert_request = TableDataInsertAllRequest::new();
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

        let response = self.insert_chunk_raw(rows).await?;

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
            let response = self.insert_chunk_raw(chunk).await?;

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
