//! PostgreSQL query → Google BigQuery.
//!
//! Classic OLTP → DW move: pull rows out of an operational Postgres database
//! and stream them into a BigQuery table.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_bigquery \
//!     --features "source-postgres sink-bigquery"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(PostgresSourceConfig::new(
        "postgres://user:pass@localhost/app",
        "SELECT id, created_at, payload FROM orders WHERE created_at > NOW() - INTERVAL '1 day'",
    ))
    .await?;

    let sink = BigQuerySink::new(BigQuerySinkConfig::new(
        "my-gcp-project",
        "warehouse",
        "orders",
        BigQueryCredentials::ServiceAccountKeyPath("service-account.json".into()),
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} orders into BigQuery", result.records_written);
    Ok(())
}
