//! Elasticsearch → AWS S3 — full builder showcase for both connectors.
//!
//! Elasticsearch source uses a query, custom scroll timeout, scroll size,
//! API-key auth, and a max-pages cap. S3 sink shows prefix, region,
//! file extension, sharding, and parallel-upload concurrency.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example elasticsearch_to_s3 \
//!     --features "source-elasticsearch sink-s3"
//! ```

use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::source::elasticsearch::{
    ElasticsearchAuth, ElasticsearchSource, ElasticsearchSourceConfig,
};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = ElasticsearchSource::new(
        ElasticsearchSourceConfig::new("https://es.example.com:9200", "logs-2026-05")
            .query(json!({ "match": { "level": "error" } }))
            .scroll_timeout("2m")
            .with_batch_size(2000)
            .auth(ElasticsearchAuth::ApiKey {
                key: std::env::var("ES_API_KEY")?,
            })
            .max_pages(usize::MAX),
    );

    let sink = S3Sink::new(
        S3SinkConfig::new("my-es-backups")
            .prefix("logs/2026-05/")
            .region("us-east-1")
            .file_extension(".jsonl")
            .max_records_per_file(50_000)
            .concurrency(16),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "backed up {} docs to s3://my-es-backups/",
        result.records_written
    );
    Ok(())
}
