//! REST API → JSONL using streaming mode.
//!
//! Unlike `rest_to_jsonl`, this example writes each page to the sink as soon
//! as it arrives instead of accumulating every record in memory first. Use
//! streaming mode for very large datasets.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_streaming \
//!     --features "source-rest sink-jsonl"
//! ```

use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::{RestStream, RestStreamConfig, run_stream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(RestStreamConfig::new(
        "https://jsonplaceholder.typicode.com",
        "/comments",
    ))?;
    let sink = JsonlSink::new(JsonlSinkConfig::new("/tmp/comments.jsonl"));

    let pages = source.stream_pages();
    let result = run_stream(pages, &sink).await?;

    println!(
        "streamed {} records to /tmp/comments.jsonl",
        result.records_written
    );
    Ok(())
}
