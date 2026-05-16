//! gRPC → HTTP — full builder showcase for both connectors.
//!
//! gRPC source uses a request body, Bearer-token metadata auth, TLS, and a
//! records-path. The HTTP sink exercises:
//!
//! - `.method(...)` — POST/PUT/PATCH/etc. (default POST)
//! - `.headers(...)` — extra headers
//! - `.auth(...)` — Bearer / Basic / Custom (default None)
//! - `.batch_mode(...)` — `Individual` (one POST per record) or `Array`
//! - `.max_retries(...)` / `.concurrency(...)` — throughput tuning
//!
//! Query params aren't a separate knob — bake them into the URL.
//! The request body is always the source record(s).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example grpc_to_http \
//!     --features "source-grpc sink-http"
//! ```

use faucet_stream::sink::http::{HttpBatchMode, HttpSink, HttpSinkAuth, HttpSinkConfig};
use faucet_stream::source::grpc::{GrpcAuth, GrpcStream, GrpcStreamConfig};
use faucet_stream::{Pipeline, json};
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = GrpcStream::new(
        GrpcStreamConfig::new(
            "https://grpc.example.com:443",
            "metrics.MetricsService",
            "ListMetrics",
            "proto/metrics.bin",
        )
        .request(json!({ "window": "1h" }))
        .auth(GrpcAuth::Bearer(std::env::var("GRPC_TOKEN")?))
        .tls(true)
        .records_path("$.metrics[*]"),
    )?;

    let mut headers = HeaderMap::new();
    headers.insert("X-Source", HeaderValue::from_static("faucet-stream"));
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));

    let sink = HttpSink::new(
        HttpSinkConfig::new("https://ingest.example.com/v1/events?tenant=acme")
            .method(reqwest::Method::POST)
            .auth(HttpSinkAuth::Bearer(std::env::var("INGEST_TOKEN")?))
            .headers(headers)
            .batch_mode(HttpBatchMode::Array)
            .max_retries(3)
            .concurrency(8),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "forwarded {} records to ingest endpoint",
        result.records_written
    );
    Ok(())
}
