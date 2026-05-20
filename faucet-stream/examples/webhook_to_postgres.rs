//! Webhook receiver → PostgreSQL — full builder showcase for both connectors.
//!
//! Webhook source uses listen-addr, path, max-payloads, and timeout knobs.
//! Postgres sink uses the JSONB column mapping with batch + pool tuning —
//! ideal for durably capturing webhook bodies without flattening them.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example webhook_to_postgres \
//!     --features "source-webhook sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use faucet_stream::source::webhook::{WebhookSource, WebhookSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr("0.0.0.0:8080")
            .path("/inbox")
            .max_payloads(50_000)
            .timeout_secs(300),
    );

    let sink = PostgresSink::new(
        PostgresSinkConfig::new("postgres://user:pass@localhost/inbox", "webhook_events")
            .column_mapping(PostgresColumnMapping::Jsonb {
                column: "body".into(),
            })
            .with_batch_size(500)
            .max_connections(8),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "captured {} webhook payloads into Postgres",
        result.records_written
    );
    Ok(())
}
