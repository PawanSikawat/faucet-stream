//! PostgreSQL → Elasticsearch (DB → search backend).
//!
//! Materialise rows from Postgres into an Elasticsearch index so they can
//! power full-text search. Tune `batch_size` on the sink for indexing
//! throughput; the default is 500 per bulk request.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_elasticsearch \
//!     --features "source-postgres sink-elasticsearch"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(PostgresSourceConfig::new(
        "postgres://user:pass@localhost/app",
        "SELECT id, title, body, tags FROM articles",
    ))
    .await?;

    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new(
        "http://localhost:9200",
        "articles",
    ));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "indexed {} articles into Elasticsearch",
        result.records_written
    );
    Ok(())
}
