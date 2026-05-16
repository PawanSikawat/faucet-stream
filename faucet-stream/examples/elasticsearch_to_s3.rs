//! Elasticsearch → AWS S3 (index backup).
//!
//! Scroll an Elasticsearch index out to JSONL files in S3 — a common
//! lightweight backup / archival pattern. The source supports the scroll
//! API; tune `scroll_size` for throughput.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example elasticsearch_to_s3 \
//!     --features "source-elasticsearch sink-s3"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::source::elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = ElasticsearchSource::new(ElasticsearchSourceConfig::new(
        "http://localhost:9200",
        "logs-2026-05",
    ));

    let sink = S3Sink::new(S3SinkConfig::new("my-es-backups")).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "backed up {} docs to s3://my-es-backups/",
        result.records_written
    );
    Ok(())
}
