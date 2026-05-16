//! REST API → Google BigQuery streaming insert.
//!
//! Required: a BigQuery dataset and table you can write to, plus a service
//! account JSON key file at `GOOGLE_APPLICATION_CREDENTIALS` (or another
//! path; pass it as the credential).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_bigquery \
//!     --features "source-rest sink-bigquery"
//! ```

use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::{Pipeline, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(RestStreamConfig::new(
        "https://api.example.com",
        "/v1/events",
    ))?;

    let sink = BigQuerySink::new(BigQuerySinkConfig::new(
        "my-gcp-project",
        "analytics",
        "events",
        BigQueryCredentials::ServiceAccountKeyPath("service-account.json".into()),
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} rows into BigQuery", result.records_written);
    Ok(())
}
