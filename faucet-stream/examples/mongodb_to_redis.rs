//! MongoDB → Redis stream.
//!
//! Pushes each MongoDB document onto a Redis stream so downstream consumers
//! can `XREAD` from it. Swap to `RedisSinkType::List` for a list, or
//! `KeyValue` to write one key per record.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mongodb_to_redis \
//!     --features "source-mongodb sink-redis"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use faucet_stream::source::mongodb::{MongoSource, MongoSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MongoSource::new(MongoSourceConfig::new(
        "mongodb://localhost:27017",
        "events",
        "raw",
    ))
    .await?;

    let sink = RedisSink::new(RedisSinkConfig::new(
        "redis://localhost:6379",
        RedisSinkType::Stream {
            key: "events:raw".into(),
        },
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "pushed {} docs onto events:raw stream",
        result.records_written
    );
    Ok(())
}
