//! MongoDB → Redis stream — full builder showcase for both connectors.
//!
//! MongoDB source uses filter, sort, limit, and cursor-batch-size for the cursor.
//! Redis sink pushes onto a stream (swap `RedisSinkType::List` for a list,
//! or `KeyValue { key_field }` to write one key per record) and tunes the
//! pipeline batch size.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mongodb_to_redis \
//!     --features "source-mongodb sink-redis"
//! ```

use faucet_stream::sink::redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use faucet_stream::source::mongodb::{MongoSource, MongoSourceConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MongoSource::new(
        MongoSourceConfig::new("mongodb://localhost:27017", "events", "raw")
            .filter(json!({ "processed": false }))
            .sort(json!({ "created_at": 1 }))
            .limit(50_000)
            .cursor_batch_size(500),
    )
    .await?;

    let sink = RedisSink::new(
        RedisSinkConfig::new(
            "redis://localhost:6379",
            RedisSinkType::Stream {
                key: "events:raw".into(),
            },
        )
        .batch_size(1000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "pushed {} docs onto events:raw stream",
        result.records_written
    );
    Ok(())
}
