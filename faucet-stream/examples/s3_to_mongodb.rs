//! AWS S3 → MongoDB.
//!
//! Useful when a data-lake landing zone (S3 JSONL) feeds a document store.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_mongodb \
//!     --features "source-s3 sink-mongodb"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mongodb::{MongoSink, MongoSinkConfig};
use faucet_stream::source::s3::{S3Source, S3SourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = S3Source::new(S3SourceConfig::new("my-data-lake")).await?;

    let sink = MongoSink::new(MongoSinkConfig::new(
        "mongodb://localhost:27017",
        "warehouse",
        "events",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} records into MongoDB", result.records_written);
    Ok(())
}
