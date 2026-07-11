//! Proves `faucet-source-csv` upholds the shared connector contract by invoking
//! the reusable `faucet-conformance` battery (checks 1, 2 & 6).
//!
//! CSV is a full-table source (no bookmark) and not a sink, so check 3
//! (bookmark round-trip) and checks 4/5 (sink capabilities) do not apply.

use std::io::Write;

use faucet_source_csv::{CsvSource, CsvSourceConfig};

#[test]
fn conformance_config_schema_valid() {
    let source = CsvSource::new(CsvSourceConfig::new("/tmp/does-not-matter.csv"));
    faucet_conformance::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn conformance_bounded_memory() {
    // Generate a CSV large enough that one-big-page buffering would be caught.
    let total = 5_000;
    let batch = 250;
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "id,name").unwrap();
    for i in 0..total {
        writeln!(file, "{i},row-{i}").unwrap();
    }
    file.flush().unwrap();

    let config = CsvSourceConfig {
        batch_size: batch,
        ..CsvSourceConfig::new(file.path().to_string_lossy().to_string())
    };
    let source = CsvSource::new(config);
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // A missing file must surface a typed FaucetError, never a panic.
    let source = CsvSource::new(CsvSourceConfig::new(
        "/nonexistent/faucet-conformance/missing.csv",
    ));
    faucet_conformance::assert_errors_not_panics(&source).await;
}
