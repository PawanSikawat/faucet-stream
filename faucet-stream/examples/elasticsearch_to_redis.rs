//! Elasticsearch → Redis (key-value cache) — full builder showcase.
//!
//! Elasticsearch source uses query, scroll tuning, Basic auth, and a
//! max-pages cap. Redis sink uses the `KeyValue` variant (one Redis key
//! per record, keyed by a doc field) with a tuned pipeline batch size.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example elasticsearch_to_redis \
//!     --features "source-elasticsearch sink-redis"
//! ```

use faucet_stream::sink::redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use faucet_stream::source::elasticsearch::{
    ElasticsearchAuth, ElasticsearchSource, ElasticsearchSourceConfig,
};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = ElasticsearchSource::new(
        ElasticsearchSourceConfig::new("https://es.example.com:9200", "products")
            .query(json!({ "term": { "available": true } }))
            .scroll_timeout("1m")
            .with_batch_size(1000)
            .auth(ElasticsearchAuth::Basic {
                username: std::env::var("ES_USER")?,
                password: std::env::var("ES_PASS")?,
            })
            .max_pages(500),
    )?;

    let sink = RedisSink::new(
        RedisSinkConfig::new(
            "redis://localhost:6379",
            RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
        )
        .with_batch_size(2000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("cached {} products into Redis", result.records_written);
    Ok(())
}
