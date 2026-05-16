//! AWS S3 → Google BigQuery (data-lake → DW).
//!
//! Reads JSONL objects from an S3 prefix and streams them into a BigQuery
//! table. A canonical lakehouse loading pattern.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_bigquery \
//!     --features "source-s3 sink-bigquery"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::s3::{S3Source, S3SourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = S3Source::new(S3SourceConfig::new("my-data-lake")).await?;

    let sink = BigQuerySink::new(BigQuerySinkConfig::new(
        "my-gcp-project",
        "raw",
        "events",
        BigQueryCredentials::ServiceAccountKeyPath("service-account.json".into()),
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "loaded {} records from S3 into BigQuery",
        result.records_written
    );
    Ok(())
}
