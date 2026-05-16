//! Webhook receiver → CSV file.
//!
//! Spin up a webhook server briefly and dump every received payload as a row
//! in a CSV file. Headers are inferred from the first record's keys.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example webhook_to_csv \
//!     --features "source-webhook sink-csv"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::csv::{CsvSink, CsvSinkConfig};
use faucet_stream::source::webhook::{WebhookSource, WebhookSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = WebhookSource::new(WebhookSourceConfig::new());

    let sink = CsvSink::new(CsvSinkConfig::new("webhooks.csv"));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "captured {} webhook payloads into webhooks.csv",
        result.records_written
    );
    Ok(())
}
