//! MySQL → BigQuery — full builder showcase.
//!
//! MySQL source uses a tuned pool. BigQuery sink shows the inline-credential
//! variant and batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mysql_to_bigquery \
//!     --features "source-mysql sink-bigquery"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::mysql::{MysqlSource, MysqlSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MysqlSource::new(
        MysqlSourceConfig::new(
            "mysql://user:pass@localhost/sales",
            "SELECT order_id, customer_id, total, ordered_at FROM orders",
        )
        .with_max_connections(16),
    )
    .await?;

    let sink = BigQuerySink::new(
        BigQuerySinkConfig::new(
            "my-gcp-project",
            "warehouse",
            "orders",
            BigQueryCredentials::ServiceAccountKeyPath("service-account.json".into()),
        )
        .with_batch_size(1000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} orders into BigQuery", result.records_written);
    Ok(())
}
