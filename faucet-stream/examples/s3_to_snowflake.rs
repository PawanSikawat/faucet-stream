//! AWS S3 → Snowflake (data-lake → DW, alternate target).
//!
//! Reads JSONL objects from S3 and loads them into a Snowflake table via the
//! SQL REST API. For very large volumes consider Snowflake's native
//! COPY-from-S3 path; this pattern is best for small/medium continuous flows.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example s3_to_snowflake \
//!     --features "source-s3 sink-snowflake"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use faucet_stream::source::s3::{S3Source, S3SourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = S3Source::new(S3SourceConfig::new("my-data-lake")).await?;

    let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
        "xy12345.us-east-1",
        "LOAD_WH",
        "ANALYTICS",
        "RAW",
        "EVENTS",
        SnowflakeAuth::KeyPair {
            user: "LOADER".into(),
            private_key_pem: std::fs::read_to_string("snowflake_key.pem")?,
        },
    ));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "loaded {} records from S3 into Snowflake",
        result.records_written
    );
    Ok(())
}
