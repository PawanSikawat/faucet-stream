//! AWS S3 → Google BigQuery — full builder showcase for both connectors.
//!
//! S3 source uses prefix scoping, region, JsonLines format, and parallel
//! reads. BigQuery sink shows the inline-credential variant and batch
//! sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_bigquery \
//!     --features "source-s3 sink-bigquery"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::s3::{S3FileFormat, S3Source, S3SourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = S3Source::new(
        S3SourceConfig::new("my-data-lake")
            .prefix("raw/events/")
            .region("us-east-1")
            .file_format(S3FileFormat::JsonLines)
            .max_objects(usize::MAX)
            .concurrency(16),
    )
    .await?;

    let sink = BigQuerySink::new(
        BigQuerySinkConfig::new(
            "my-gcp-project",
            "raw",
            "events",
            BigQueryCredentials::ServiceAccountKey(std::env::var("GCP_KEY_JSON")?),
        )
        .batch_size(2000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "loaded {} records from S3 into BigQuery",
        result.records_written
    );
    Ok(())
}
