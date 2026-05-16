//! MongoDB → PostgreSQL (document → relational mirror).
//!
//! Streams MongoDB documents into Postgres using the default JSONB column
//! mapping (one row per document, payload in the `data` column). Useful
//! when promoting MongoDB content into a relational system of record.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mongodb_to_postgres \
//!     --features "source-mongodb sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresSink, PostgresSinkConfig};
use faucet_stream::source::mongodb::{MongoSource, MongoSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MongoSource::new(MongoSourceConfig::new(
        "mongodb://localhost:27017",
        "shop",
        "orders",
    ))
    .await?;

    let sink = PostgresSink::new(PostgresSinkConfig::new(
        "postgres://user:pass@localhost/warehouse",
        "orders_mirror",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("mirrored {} orders into Postgres", result.records_written);
    Ok(())
}
