//! MongoDB → Elasticsearch — full builder showcase for both connectors.
//!
//! MongoDB source uses a filter, projection, sort, limit, and tuned cursor
//! batch size. Elasticsearch sink shows Basic auth, batch sizing, and the
//! `id_field` knob (uses each doc's `_id`-like field as the ES `_id`).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mongodb_to_elasticsearch \
//!     --features "source-mongodb sink-elasticsearch"
//! ```

use faucet_stream::sink::elasticsearch::{
    ElasticsearchSink, ElasticsearchSinkAuth, ElasticsearchSinkConfig,
};
use faucet_stream::source::mongodb::{MongoSource, MongoSourceConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MongoSource::new(
        MongoSourceConfig::new("mongodb://localhost:27017", "shop", "products")
            .filter(json!({ "available": true }))
            .projection(json!({ "_id": 1, "name": 1, "description": 1, "tags": 1 }))
            .sort(json!({ "updated_at": -1 }))
            .limit(100_000)
            .batch_size(1000),
    )
    .await?;

    let sink = ElasticsearchSink::new(
        ElasticsearchSinkConfig::new("https://es.example.com:9200", "products")
            .auth(ElasticsearchSinkAuth::Basic {
                username: std::env::var("ES_USER")?,
                password: std::env::var("ES_PASS")?,
            })
            .batch_size(1000)
            .id_field("_id"),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "indexed {} products into Elasticsearch",
        result.records_written
    );
    Ok(())
}
