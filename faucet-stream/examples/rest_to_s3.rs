//! REST API → AWS S3 (data-lake landing zone).
//!
//! Pulls records from a REST endpoint and writes them as JSONL files to an
//! S3 prefix. Auth comes from the standard AWS credential chain.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_s3 \
//!     --features "source-rest sink-s3"
//! ```

use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::{Pipeline, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(RestStreamConfig::new(
        "https://api.example.com",
        "/v1/events",
    ))?;

    let sink = S3Sink::new(S3SinkConfig::new("my-data-lake")).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "landed {} records into s3://my-data-lake/",
        result.records_written
    );
    Ok(())
}
