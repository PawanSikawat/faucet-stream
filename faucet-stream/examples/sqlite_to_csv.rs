//! SQLite query → CSV file.
//!
//! Reads rows out of a local SQLite database and writes them as a CSV file
//! with headers (taken from the first record's keys).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example sqlite_to_csv \
//!     --features "source-sqlite sink-csv"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::csv::{CsvSink, CsvSinkConfig};
use faucet_stream::source::sqlite::{SqliteSource, SqliteSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SqliteSource::new(SqliteSourceConfig::new(
        "sqlite:local.db",
        "SELECT id, name, price FROM products ORDER BY id",
    ))
    .await?;

    let sink = CsvSink::new(CsvSinkConfig::new("products.csv"));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "wrote {} product rows to products.csv",
        result.records_written
    );
    Ok(())
}
