//! BigQuery streaming insert sink.

use crate::config::{BigQueryCredentials, BigQuerySinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
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
    /// Returns a [`FaucetError::Sink`] if authentication fails.
    pub async fn new(config: BigQuerySinkConfig) -> Result<Self, FaucetError> {
        let client = match &config.credentials {
            BigQueryCredentials::ServiceAccountKeyPath(path) => {
                Client::from_service_account_key_file(path)
                    .await
                    .map_err(|e| FaucetError::Sink(format!("BigQuery auth failed: {e}")))?
            }
            BigQueryCredentials::ServiceAccountKey(json) => {
                let sa_key = serde_json::from_str(json)
                    .map_err(|e| FaucetError::Sink(format!("invalid service account JSON: {e}")))?;
                Client::from_service_account_key(sa_key, false)
                    .await
                    .map_err(|e| FaucetError::Sink(format!("BigQuery auth failed: {e}")))?
            }
            BigQueryCredentials::ApplicationDefault => {
                Client::from_application_default_credentials()
                    .await
                    .map_err(|e| FaucetError::Sink(format!("BigQuery auth failed: {e}")))?
            }
        };

        Ok(Self { config, client })
    }

    /// Insert a single batch of rows (up to `batch_size`).
    async fn insert_batch(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut insert_request = TableDataInsertAllRequest::new();
        for row in rows {
            insert_request.add_row(None, row).map_err(|e| {
                FaucetError::Sink(format!("failed to serialize row for BigQuery: {e}"))
            })?;
        }

        let response = self
            .client
            .tabledata()
            .insert_all(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                insert_request,
            )
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery insertAll failed: {e}")))?;

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
    /// Write records to BigQuery, splitting into batches of `config.batch_size`.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut total = 0;
        for chunk in records.chunks(self.config.batch_size) {
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
}
