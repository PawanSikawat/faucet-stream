//! PostgreSQL → AWS S3 (DB archive / cold-storage offload).
//!
//! Runs a query against Postgres and writes the result rows as JSONL objects
//! to an S3 prefix. Common for ageing rows out of an operational DB.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_s3 \
//!     --features "source-postgres sink-s3"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(PostgresSourceConfig::new(
        "postgres://user:pass@localhost/app",
        "SELECT * FROM events WHERE created_at < NOW() - INTERVAL '90 days'",
    ))
    .await?;

    let sink = S3Sink::new(S3SinkConfig::new("my-archive-bucket")).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "archived {} rows to s3://my-archive-bucket/",
        result.records_written
    );
    Ok(())
}
