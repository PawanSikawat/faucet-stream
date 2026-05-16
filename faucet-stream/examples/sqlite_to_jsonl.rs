//! SQLite query → JSON Lines file.
//!
//! Useful for one-shot exports of a local SQLite database into a portable
//! line-delimited JSON dump.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example sqlite_to_jsonl \
//!     --features "source-sqlite sink-jsonl"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::source::sqlite::{SqliteSource, SqliteSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SqliteSource::new(SqliteSourceConfig::new(
        "sqlite:./app.db",
        "SELECT * FROM events ORDER BY ts",
    ))
    .await?;

    let sink = JsonlSink::new(JsonlSinkConfig::new("events.jsonl"));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("dumped {} events to events.jsonl", result.records_written);
    Ok(())
}
