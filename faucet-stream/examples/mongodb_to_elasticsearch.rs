//! MongoDB → Elasticsearch.
//!
//! Mirror a MongoDB collection into an Elasticsearch index for full-text
//! search. The source's default options return every document with no filter
//! or projection; constrain it with `filter` / `projection` for a partial
//! mirror.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mongodb_to_elasticsearch \
//!     --features "source-mongodb sink-elasticsearch"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use faucet_stream::source::mongodb::{MongoSource, MongoSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MongoSource::new(MongoSourceConfig::new(
        "mongodb://localhost:27017",
        "shop",
        "products",
    ))
    .await?;

    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new(
        "http://localhost:9200",
        "products",
    ));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "indexed {} products into Elasticsearch",
        result.records_written
    );
    Ok(())
}
