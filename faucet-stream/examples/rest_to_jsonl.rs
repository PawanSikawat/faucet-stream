//! REST API source → JSONL file sink.
//!
//! Fetches all posts from the public `jsonplaceholder.typicode.com` API and
//! writes each one as a line of JSON to `/tmp/posts.jsonl`.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_jsonl \
//!     --features "source-rest sink-jsonl"
//! ```

use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::{Pipeline, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(RestStreamConfig::new(
        "https://jsonplaceholder.typicode.com",
        "/posts",
    ))?;

    let sink = JsonlSink::new(JsonlSinkConfig::new("/tmp/posts.jsonl"));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "wrote {} records to /tmp/posts.jsonl",
        result.records_written
    );
    Ok(())
}
