//! MySQL → PostgreSQL migration — full builder showcase for both connectors.
//!
//! MySQL source uses a tuned pool. Postgres sink demonstrates the
//! `AutoMap` column mapping (each source column written into a matching
//! Postgres column) plus batch size and pool sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mysql_to_postgres \
//!     --features "source-mysql sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use faucet_stream::source::mysql::{MysqlSource, MysqlSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MysqlSource::new(
        MysqlSourceConfig::new(
            "mysql://user:pass@localhost/legacy",
            "SELECT id, name, address, created_at FROM customers ORDER BY id",
        )
        .with_max_connections(16),
    )
    .await?;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new(
            "postgres://user:pass@localhost/modern",
            "customers_imported",
        )
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(1000)
        .max_connections(10),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "migrated {} customers from MySQL to Postgres",
        result.records_written
    );
    Ok(())
}
