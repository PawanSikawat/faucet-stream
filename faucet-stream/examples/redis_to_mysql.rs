//! Redis stream → MySQL.
//!
//! Drain messages off a Redis stream and persist them into MySQL using the
//! default JSON column mapping. Useful for durable archiving of in-flight
//! events.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example redis_to_mysql \
//!     --features "source-redis sink-mysql"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mysql::{MysqlSink, MysqlSinkConfig};
use faucet_stream::source::redis::{RedisSource, RedisSourceConfig, RedisSourceType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RedisSource::new(RedisSourceConfig::new(
        "redis://localhost:6379",
        RedisSourceType::Stream {
            key: "events:raw".into(),
            group: Some("archiver".into()),
            consumer: Some("worker-1".into()),
            count: Some(1000),
        },
    ));

    let sink = MysqlSink::new(MysqlSinkConfig::new(
        "mysql://user:pass@localhost/archive",
        "events_raw",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "archived {} stream messages into MySQL",
        result.records_written
    );
    Ok(())
}
