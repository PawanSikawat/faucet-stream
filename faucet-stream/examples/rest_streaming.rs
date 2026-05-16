//! REST API → JSONL via `run_stream` — page-by-page, bounded memory.
//!
//! Same parameter showcase as `rest_to_jsonl`, but pages are written to the
//! sink as they arrive instead of buffering. Use this for very large
//! datasets where you can't hold every record in RAM. Note: `stream_pages`
//! does not honour `partitions` — use batch mode for multi-partition feeds.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_streaming \
//!     --features "source-rest sink-jsonl transforms"
//! ```

use std::time::Duration;

use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::{
    Auth, PaginationStyle, RecordTransform, RestStream, RestStreamConfig, run_stream,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/comments")
            .name("comments")
            .auth(Auth::ApiKey {
                header: "X-API-Key".into(),
                value: std::env::var("API_KEY")?,
            })
            .header("Accept", "application/json")
            .pagination(PaginationStyle::LinkHeader)
            .records_path("$.items[*]")
            .max_pages(usize::MAX)
            .timeout(Duration::from_secs(60))
            .max_retries(5)
            .retry_backoff(Duration::from_secs(2))
            .tolerate_http_error(429)
            .request_delay(Duration::from_millis(100))
            .add_transform(RecordTransform::Flatten {
                separator: "__".into(),
            }),
    )?;

    let sink = JsonlSink::new(
        JsonlSinkConfig::new("comments.jsonl")
            .append(false)
            .pretty(false),
    );

    let pages = source.stream_pages();
    let result = run_stream(pages, &sink).await?;
    println!(
        "streamed {} comments to comments.jsonl",
        result.records_written
    );
    Ok(())
}
