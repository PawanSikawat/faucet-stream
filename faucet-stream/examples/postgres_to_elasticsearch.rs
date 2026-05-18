//! PostgreSQL → Elasticsearch — full builder showcase for both connectors.
//!
//! Postgres source uses a parameterised query and a tuned pool. ES sink
//! shows API-key auth, batch sizing, and `id_field` (the source column
//! used as the Elasticsearch `_id`).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_elasticsearch \
//!     --features "source-postgres sink-elasticsearch"
//! ```

use faucet_stream::sink::elasticsearch::{
    ElasticsearchSink, ElasticsearchSinkAuth, ElasticsearchSinkConfig,
};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(
        PostgresSourceConfig::new(
            "postgres://user:pass@localhost/app",
            "SELECT id, title, body, tags FROM articles WHERE published = $1",
        )
        .params(vec![json!(true)])
        .with_max_connections(8),
    )
    .await?;

    let sink = ElasticsearchSink::new(
        ElasticsearchSinkConfig::new("https://es.example.com:9200", "articles")
            .auth(ElasticsearchSinkAuth::ApiKey {
                key: std::env::var("ES_API_KEY")?,
            })
            .batch_size(500)
            .id_field("id"),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "indexed {} articles into Elasticsearch",
        result.records_written
    );
    Ok(())
}
