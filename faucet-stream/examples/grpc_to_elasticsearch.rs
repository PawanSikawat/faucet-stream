//! gRPC → Elasticsearch — full builder showcase for both connectors.
//!
//! gRPC source uses a request body, Metadata auth, explicit TLS, and a
//! records-path. Elasticsearch sink exercises Basic auth, batch sizing,
//! and `id_field` (the doc field used as the `_id` in Elasticsearch).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example grpc_to_elasticsearch \
//!     --features "source-grpc sink-elasticsearch"
//! ```

use faucet_stream::sink::elasticsearch::{
    ElasticsearchSink, ElasticsearchSinkAuth, ElasticsearchSinkConfig,
};
use faucet_stream::source::grpc::{GrpcAuth, GrpcStream, GrpcStreamConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = GrpcStream::new(
        GrpcStreamConfig::new(
            "https://grpc.example.com:443",
            "events.EventService",
            "ListEvents",
            "proto/events.bin",
        )
        .request(json!({ "since": "2026-01-01T00:00:00Z", "page_size": 1000 }))
        .auth(GrpcAuth::Metadata(vec![
            ("x-api-key".into(), std::env::var("GRPC_API_KEY")?),
            ("x-tenant".into(), "acme".into()),
        ]))
        .tls(true)
        .records_path("$.events[*]"),
    )?;

    let sink = ElasticsearchSink::new(
        ElasticsearchSinkConfig::new("https://es.example.com:9200", "events")
            .auth(ElasticsearchSinkAuth::Basic {
                username: std::env::var("ES_USER")?,
                password: std::env::var("ES_PASS")?,
            })
            .batch_size(1000)
            .id_field("event_id"),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "indexed {} events into Elasticsearch",
        result.records_written
    );
    Ok(())
}
