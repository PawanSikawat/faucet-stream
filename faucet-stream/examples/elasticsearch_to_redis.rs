//! Elasticsearch → Redis (key-value cache warm-up).
//!
//! Reads every doc from an Elasticsearch index and writes each one into Redis
//! as a key-value pair, using the document's `id` field as the Redis key.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example elasticsearch_to_redis \
//!     --features "source-elasticsearch sink-redis"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use faucet_stream::source::elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = ElasticsearchSource::new(ElasticsearchSourceConfig::new(
        "http://localhost:9200",
        "products",
    ));

    let sink = RedisSink::new(RedisSinkConfig::new(
        "redis://localhost:6379",
        RedisSinkType::KeyValue {
            key_field: "id".into(),
        },
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("cached {} products into Redis", result.records_written);
    Ok(())
}
