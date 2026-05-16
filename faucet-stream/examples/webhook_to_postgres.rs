//! Webhook receiver → PostgreSQL (durable capture).
//!
//! Stands up a temporary HTTP server, collects every POST payload sent to
//! it, and persists each one as a row in Postgres. Use this to durably
//! capture inbound webhooks for replay or audit.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example webhook_to_postgres \
//!     --features "source-webhook sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresSink, PostgresSinkConfig};
use faucet_stream::source::webhook::{WebhookSource, WebhookSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = WebhookSource::new(WebhookSourceConfig::new());

    let sink = PostgresSink::new(PostgresSinkConfig::new(
        "postgres://user:pass@localhost/inbox",
        "webhook_events",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "captured {} webhook payloads into Postgres",
        result.records_written
    );
    Ok(())
}
