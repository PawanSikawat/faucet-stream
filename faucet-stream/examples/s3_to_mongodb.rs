//! AWS S3 → MongoDB — full builder showcase for both connectors.
//!
//! S3 source uses prefix scoping and the `JsonArray` format (one whole
//! file = one JSON array of records). MongoDB sink shows batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_mongodb \
//!     --features "source-s3 sink-mongodb"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mongodb::{MongoSink, MongoSinkConfig};
use faucet_stream::source::s3::{S3FileFormat, S3Source, S3SourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = S3Source::new(
        S3SourceConfig::new("my-data-lake")
            .prefix("snapshots/users/")
            .region("us-west-2")
            .file_format(S3FileFormat::JsonArray)
            .max_objects(500)
            .concurrency(8),
    )
    .await?;

    let sink = MongoSink::new(
        MongoSinkConfig::new("mongodb://localhost:27017", "warehouse", "users").batch_size(2000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} user docs into MongoDB", result.records_written);
    Ok(())
}
