//! AWS S3 → PostgreSQL.
//!
//! Reads JSONL objects from an S3 prefix (one record per line) and writes
//! them into a Postgres table. AWS credentials come from the standard chain.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_postgres \
//!     --features "source-s3 sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresSink, PostgresSinkConfig};
use faucet_stream::source::s3::{S3Source, S3SourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = S3Source::new(S3SourceConfig::new("my-data-lake")).await?;

    let sink = PostgresSink::new(PostgresSinkConfig::new(
        "postgres://user:pass@localhost/warehouse",
        "events_raw",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "loaded {} records from S3 to Postgres",
        result.records_written
    );
    Ok(())
}
