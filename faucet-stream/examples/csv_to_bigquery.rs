//! CSV file → Google BigQuery (one-shot CSV upload to DW).
//!
//! The most common destination for an ad-hoc CSV upload. For very large
//! files prefer BigQuery's native `bq load` path; this pattern is best for
//! repeated programmatic loads or continuous flows.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_bigquery \
//!     --features "source-csv sink-bigquery"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::csv::{CsvSource, CsvSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = CsvSource::new(CsvSourceConfig::new("transactions.csv"));

    let sink = BigQuerySink::new(BigQuerySinkConfig::new(
        "my-gcp-project",
        "warehouse",
        "transactions",
        BigQueryCredentials::ServiceAccountKeyPath("service-account.json".into()),
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} CSV rows into BigQuery", result.records_written);
    Ok(())
}
