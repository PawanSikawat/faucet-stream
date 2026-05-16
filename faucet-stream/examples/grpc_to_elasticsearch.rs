//! gRPC → Elasticsearch bulk index.
//!
//! Required: a gRPC service + a compiled protobuf descriptor set
//! (`protoc --descriptor_set_out=svc.bin ...`), plus an Elasticsearch host.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example grpc_to_elasticsearch \
//!     --features "source-grpc sink-elasticsearch"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use faucet_stream::source::grpc::{GrpcStream, GrpcStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = GrpcStream::new(GrpcStreamConfig::new(
        "https://grpc.example.com:443",
        "events.EventService",
        "ListEvents",
        "proto/events.bin",
    ))?;

    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new(
        "http://localhost:9200",
        "events",
    ));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "indexed {} events into Elasticsearch",
        result.records_written
    );
    Ok(())
}
