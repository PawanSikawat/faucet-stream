//! AWS S3 → PostgreSQL — full builder showcase for both connectors.
//!
//! S3 source uses prefix scoping, an explicit region, a custom endpoint
//! (MinIO/LocalStack), `JsonLines` file format, max-object limit, and
//! parallel-read concurrency. Postgres sink uses the JSONB column mapping
//! with batch sizing and pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_postgres \
//!     --features "source-s3 sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use faucet_stream::source::s3::{S3FileFormat, S3Source, S3SourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = S3Source::new(
        S3SourceConfig::new("my-data-lake")
            .prefix("events/2026/")
            .region("us-east-1")
            .endpoint_url("https://s3.us-east-1.amazonaws.com")
            .file_format(S3FileFormat::JsonLines)
            .max_objects(1000)
            .concurrency(16),
    )
    .await?;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new("postgres://user:pass@localhost/warehouse", "events_raw")
            .column_mapping(PostgresColumnMapping::Jsonb {
                column: "payload".into(),
            })
            .with_batch_size(1000)
            .max_connections(10),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "loaded {} records from S3 to Postgres",
        result.records_written
    );
    Ok(())
}
