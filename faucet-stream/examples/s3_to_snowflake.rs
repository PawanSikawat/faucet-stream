//! AWS S3 → Snowflake — full builder showcase for both connectors.
//!
//! S3 source uses prefix scoping, region, JsonLines format, and parallel
//! reads. Snowflake sink shows the key-pair auth variant and batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_snowflake \
//!     --features "source-s3 sink-snowflake"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
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

    let sink = SnowflakeSink::new(
        SnowflakeSinkConfig::new(
            "xy12345.us-east-1",
            "LOAD_WH",
            "ANALYTICS",
            "RAW",
            "EVENTS",
            SnowflakeAuth::KeyPair {
                user: "LOADER".into(),
                private_key_pem: std::fs::read_to_string("snowflake_key.pem")?,
            },
        )
        .with_batch_size(1000),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "loaded {} records from S3 into Snowflake",
        result.records_written
    );
    Ok(())
}
