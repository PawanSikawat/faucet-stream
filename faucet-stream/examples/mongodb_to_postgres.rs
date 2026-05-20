//! MongoDB → PostgreSQL — full builder showcase for both connectors.
//!
//! MongoDB source uses a filter, projection, sort, and tuned cursor batch size.
//! Postgres sink uses the JSONB column mapping with batch + pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mongodb_to_postgres \
//!     --features "source-mongodb sink-postgres"
//! ```

use faucet_stream::sink::postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use faucet_stream::source::mongodb::{MongoSource, MongoSourceConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MongoSource::new(
        MongoSourceConfig::new("mongodb://localhost:27017", "shop", "orders")
            .filter(json!({ "status": "completed" }))
            .projection(json!({ "_id": 1, "customer_id": 1, "total": 1, "items": 1 }))
            .sort(json!({ "created_at": 1 }))
            .limit(500_000)
            .cursor_batch_size(1000),
    )
    .await?;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new("postgres://user:pass@localhost/warehouse", "orders_mirror")
            .column_mapping(PostgresColumnMapping::Jsonb {
                column: "payload".into(),
            })
            .with_batch_size(1000)
            .max_connections(10),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("mirrored {} orders into Postgres", result.records_written);
    Ok(())
}
