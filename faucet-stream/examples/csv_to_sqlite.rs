//! CSV file → SQLite (local persistence).
//!
//! Imports a CSV file into a local SQLite database. The default JSON column
//! mapping stores each row as a JSON blob in a `data` column.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_sqlite \
//!     --features "source-csv sink-sqlite"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::sqlite::{SqliteSink, SqliteSinkConfig};
use faucet_stream::source::csv::{CsvSource, CsvSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = CsvSource::new(CsvSourceConfig::new("inventory.csv"));

    let sink = SqliteSink::new(SqliteSinkConfig::new("sqlite:./inventory.db", "inventory")).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "imported {} inventory rows into SQLite",
        result.records_written
    );
    Ok(())
}
