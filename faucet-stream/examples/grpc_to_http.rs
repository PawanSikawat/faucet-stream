//! gRPC → HTTP POST fan-out.
//!
//! Reads from a gRPC service and forwards each record as a separate HTTP POST
//! to the configured endpoint.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example grpc_to_http \
//!     --features "source-grpc sink-http"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::http::{HttpSink, HttpSinkConfig};
use faucet_stream::source::grpc::{GrpcStream, GrpcStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = GrpcStream::new(GrpcStreamConfig::new(
        "https://grpc.example.com:443",
        "metrics.MetricsService",
        "ListMetrics",
        "proto/metrics.bin",
    ))?;

    let sink = HttpSink::new(HttpSinkConfig::new("https://ingest.example.com/v1/events"));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "forwarded {} records to ingest endpoint",
        result.records_written
    );
    Ok(())
}
