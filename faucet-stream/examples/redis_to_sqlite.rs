//! Redis list → SQLite — full builder showcase for both connectors.
//!
//! Redis source uses the `List` variant (drain all items via `LRANGE`) with
//! a max-records cap. SQLite sink demonstrates the `AutoMap` column
//! mapping plus batch and pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example redis_to_sqlite \
//!     --features "source-redis sink-sqlite"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use faucet_stream::source::redis::{RedisSource, RedisSourceConfig, RedisSourceType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RedisSource::new(
        RedisSourceConfig::new(
            "redis://localhost:6379",
            RedisSourceType::List {
                key: "jobs:pending".into(),
            },
        )
        .max_records(10_000),
    );

    let sink = SqliteSink::new(
        SqliteSinkConfig::new("sqlite:./cache.db", "jobs")
            .column_mapping(SqliteColumnMapping::AutoMap)
            .with_batch_size(500)
            .max_connections(4),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("persisted {} jobs into SQLite", result.records_written);
    Ok(())
}
