//! REST API → BigQuery — REST + BigQuery sink knob showcase.
//!
//! REST side exercises Basic auth, page-number pagination, retries,
//! tolerated errors, throttling, and primary-key declaration. BigQuery
//! sink demonstrates `batch_size` tuning and the credential variants.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_bigquery \
//!     --features "source-rest sink-bigquery"
//! ```

use std::time::Duration;

use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::{Auth, PaginationStyle, Pipeline, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/events")
            .name("events")
            .auth(Auth::Basic {
                username: std::env::var("API_USER")?,
                password: std::env::var("API_PASS")?,
            })
            .header("Accept", "application/json")
            .query("type", "purchase")
            .records_path("$.events[*]")
            .pagination(PaginationStyle::PageNumber {
                param_name: "page".into(),
                start_page: 1,
                page_size: Some(500),
                page_size_param: Some("per_page".into()),
            })
            .max_pages(200)
            .request_delay(Duration::from_millis(100))
            .timeout(Duration::from_secs(45))
            .max_retries(5)
            .retry_backoff(Duration::from_secs(2))
            .tolerate_http_error(404)
            .primary_keys(vec!["event_id".into()]),
    )?;

    let sink = BigQuerySink::new(
        BigQuerySinkConfig::new(
            "my-gcp-project",
            "analytics",
            "events",
            BigQueryCredentials::ServiceAccountKeyPath("service-account.json".into()),
        )
        .batch_size(1000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} events into BigQuery", result.records_written);
    Ok(())
}
