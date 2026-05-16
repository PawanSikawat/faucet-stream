//! CSV file → MySQL.
//!
//! Bulk-load a CSV file into MySQL using the default JSON column mapping.
//! Switch to `MysqlColumnMapping::AutoMap` to write each CSV column into a
//! matching SQL column.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_mysql \
//!     --features "source-csv sink-mysql"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mysql::{MysqlSink, MysqlSinkConfig};
use faucet_stream::source::csv::{CsvSource, CsvSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = CsvSource::new(CsvSourceConfig::new("customers.csv"));

    let sink = MysqlSink::new(MysqlSinkConfig::new(
        "mysql://user:pass@localhost/crm",
        "customers_imported",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} customer rows into MySQL", result.records_written);
    Ok(())
}
