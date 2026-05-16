//! gRPC → HTTP fan-out — full HTTP sink builder surface.
//!
//! Reads from a gRPC service and forwards records to an HTTP endpoint. This
//! example exercises the configurable knobs on `HttpSinkConfig`:
//!
//! - `.method(...)` — defaults to POST; set to PUT/PATCH/etc.
//! - `.auth(...)` — Bearer / Basic / Custom-header auth (default None)
//! - `.headers(...)` — extra request headers
//! - `.batch_mode(...)` — `Individual` (one request per record, default) or
//!   `Array` (whole batch as a single JSON array body)
//! - `.max_retries(...)` / `.concurrency(...)` — throughput tuning
//!
//! Query parameters aren't a separate knob on the HTTP sink — bake them
//! into the URL. The request body is always the source record(s).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example grpc_to_http \
//!     --features "source-grpc sink-http"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::http::{HttpBatchMode, HttpSink, HttpSinkAuth, HttpSinkConfig};
use faucet_stream::source::grpc::{GrpcStream, GrpcStreamConfig};
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = GrpcStream::new(GrpcStreamConfig::new(
        "https://grpc.example.com:443",
        "metrics.MetricsService",
        "ListMetrics",
        "proto/metrics.bin",
    ))?;

    let mut headers = HeaderMap::new();
    headers.insert("X-Source", HeaderValue::from_static("faucet-stream"));

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
