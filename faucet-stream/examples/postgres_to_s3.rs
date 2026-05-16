//! PostgreSQL → S3 — full builder showcase for both connectors.
//!
//! Postgres source uses a parameterised cutoff query. S3 sink shows
//! prefix, region, custom endpoint (MinIO-compatible), file extension,
//! sharding and parallel-upload concurrency.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_s3 \
//!     --features "source-postgres sink-s3"
//! ```

use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(
        PostgresSourceConfig::new(
            "postgres://user:pass@localhost/app",
            "SELECT * FROM events WHERE created_at < $1",
        )
        .params(vec![json!("2026-01-01T00:00:00Z")])
        .with_max_connections(12),
    )
    .await?;

    let sink = S3Sink::new(
        S3SinkConfig::new("my-archive-bucket")
            .prefix("events/2025/")
            .region("us-east-1")
            .endpoint_url("https://s3.us-east-1.amazonaws.com")
            .file_extension(".jsonl")
            .max_records_per_file(50_000)
            .concurrency(16),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "archived {} rows to s3://my-archive-bucket/",
        result.records_written
    );
    Ok(())
}
