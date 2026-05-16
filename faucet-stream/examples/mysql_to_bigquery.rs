//! MySQL query → Google BigQuery (MySQL → DW).
//!
//! Pulls rows from MySQL and streams them into a BigQuery table. The mirror
//! of `postgres_to_bigquery` for shops on MySQL.
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
    let source = MysqlSource::new(MysqlSourceConfig::new(
        "mysql://user:pass@localhost/sales",
        "SELECT order_id, customer_id, total, ordered_at FROM orders",
    ))
    .await?;

    let sink = BigQuerySink::new(BigQuerySinkConfig::new(
        "my-gcp-project",
        "warehouse",
        "orders",
        BigQueryCredentials::ServiceAccountKeyPath("service-account.json".into()),
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} orders into BigQuery", result.records_written);
    Ok(())
}
