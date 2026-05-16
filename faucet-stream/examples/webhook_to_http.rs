//! Webhook receiver → HTTP POST forwarder.
//!
//! Stands up a temporary HTTP server, collects POST payloads sent to it, and
//! forwards each one to a downstream HTTP endpoint. The webhook source exits
//! after `timeout_secs` of inactivity (default 30s) or `max_payloads`.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example webhook_to_http \
//!     --features "source-webhook sink-http"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::http::{HttpSink, HttpSinkConfig};
use faucet_stream::source::webhook::{WebhookSource, WebhookSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = WebhookSource::new(WebhookSourceConfig::new());

    let sink = HttpSink::new(HttpSinkConfig::new("https://downstream.example.com/ingest"));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("forwarded {} webhook payloads", result.records_written);
    Ok(())
}
