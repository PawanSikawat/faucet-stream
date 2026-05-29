//! PostgreSQL → BigQuery — full builder showcase for both connectors.
//!
//! Postgres source uses a parameterised query plus a pool sized to the
//! workload. BigQuery sink shows the inline-credential variant and batch
//! sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_bigquery \
//!     --features "source-postgres sink-bigquery"
//! ```

use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(
        PostgresSourceConfig::new(
            "postgres://user:pass@localhost/app",
            "SELECT id, created_at, payload FROM orders WHERE created_at > $1 AND status = $2",
        )
        .params(vec![json!("2026-01-01T00:00:00Z"), json!("completed")])
        .with_max_connections(16),
    )
    .await?;

    let sink = BigQuerySink::new(
        BigQuerySinkConfig::new(
            "my-gcp-project",
            "warehouse",
            "orders",
            BigQueryCredentials::ServiceAccountKey {
                json: std::env::var("GCP_KEY_JSON")?,
            },
        )
        .with_batch_size(1000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} orders into BigQuery", result.records_written);
    Ok(())
}
