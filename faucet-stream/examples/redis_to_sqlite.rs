//! Redis list → SQLite (local cache).
//!
//! Drains items pushed to a Redis list and persists them in a local SQLite
//! database. Good fit for resilient single-node caches and edge agents.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example redis_to_sqlite \
//!     --features "source-redis sink-sqlite"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::sqlite::{SqliteSink, SqliteSinkConfig};
use faucet_stream::source::redis::{RedisSource, RedisSourceConfig, RedisSourceType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RedisSource::new(RedisSourceConfig::new(
        "redis://localhost:6379",
        RedisSourceType::List {
            key: "jobs:pending".into(),
        },
    ));

    let sink = SqliteSink::new(SqliteSinkConfig::new("sqlite:./cache.db", "jobs")).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("persisted {} jobs into SQLite", result.records_written);
    Ok(())
}
