//! REST API → PostgreSQL — the canonical API → operational-DB ELT.
//!
//! Pulls records from a REST endpoint and writes each row into a Postgres
//! table using the default JSONB column mapping. Swap to
//! `PostgresColumnMapping::AutoMap` to write into typed columns.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_postgres \
//!     --features "source-rest sink-postgres"
//! ```

use faucet_stream::sink::postgres::{PostgresSink, PostgresSinkConfig};
use faucet_stream::{Pipeline, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(RestStreamConfig::new(
        "https://api.example.com",
        "/v1/customers",
    ))?;

    let sink = PostgresSink::new(PostgresSinkConfig::new(
        "postgres://user:pass@localhost/app",
        "customers_raw",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "ingested {} customers into Postgres",
        result.records_written
    );
    Ok(())
}
