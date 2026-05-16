//! MySQL → PostgreSQL migration.
//!
//! Reads from a MySQL source query and writes to a Postgres table using the
//! default JSONB column mapping (one row per source row, payload in `data`).
//! Swap to `PostgresColumnMapping::AutoMap` to write into explicit columns.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mysql_to_postgres \
//!     --features "source-mysql sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresSink, PostgresSinkConfig};
use faucet_stream::source::mysql::{MysqlSource, MysqlSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MysqlSource::new(MysqlSourceConfig::new(
        "mysql://user:pass@localhost/legacy",
        "SELECT id, name, address, created_at FROM customers",
    ))
    .await?;

    let sink = PostgresSink::new(PostgresSinkConfig::new(
        "postgres://user:pass@localhost/modern",
        "customers_imported",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "migrated {} customers from MySQL to Postgres",
        result.records_written
    );
    Ok(())
}
