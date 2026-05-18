//! Webhook receiver → HTTP forwarder — full builder showcase.
//!
//! Webhook source uses listen-addr, path, max-payloads, and timeout knobs.
//! HTTP sink exercises method, headers, auth, batch mode, retries, and
//! concurrency.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example webhook_to_http \
//!     --features "source-webhook sink-http"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::http::{HttpBatchMode, HttpSink, HttpSinkAuth, HttpSinkConfig};
use faucet_stream::source::webhook::{WebhookSource, WebhookSourceConfig};
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr("0.0.0.0:8080")
            .path("/webhook")
            .max_payloads(10_000)
            .timeout_secs(60),
    );

    let mut headers = HeaderMap::new();
    headers.insert("X-Source", HeaderValue::from_static("faucet-stream"));

    let sink = HttpSink::new(
        HttpSinkConfig::new("https://downstream.example.com/ingest")
            .method(reqwest::Method::POST)
            .auth(HttpSinkAuth::Bearer {
                token: std::env::var("INGEST_TOKEN")?,
            })
            .headers(headers)
            .batch_mode(HttpBatchMode::Individual)
            .max_retries(3)
            .concurrency(16),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("forwarded {} webhook payloads", result.records_written);
    Ok(())
}
