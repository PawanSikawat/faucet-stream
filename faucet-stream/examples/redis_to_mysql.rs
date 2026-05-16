//! Redis stream → MySQL — full builder showcase for both connectors.
//!
//! Redis source uses `Stream` consumer-group reads (the other variants are
//! `List { key }` and `Keys { pattern }`) with a max-records cap. MySQL
//! sink demonstrates the JSON column mapping plus batch and pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example redis_to_mysql \
//!     --features "source-redis sink-mysql"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use faucet_stream::source::redis::{RedisSource, RedisSourceConfig, RedisSourceType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RedisSource::new(
        RedisSourceConfig::new(
            "redis://localhost:6379",
            RedisSourceType::Stream {
                key: "events:raw".into(),
                group: Some("archiver".into()),
                consumer: Some("worker-1".into()),
                count: Some(1000),
            },
        )
        .max_records(100_000),
    );

    let sink = MysqlSink::new(
        MysqlSinkConfig::new("mysql://user:pass@localhost/archive", "events_raw")
            .column_mapping(MysqlColumnMapping::Json {
                column: "payload".into(),
            })
            .batch_size(1000)
            .max_connections(10),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "archived {} stream messages into MySQL",
        result.records_written
    );
    Ok(())
}
