//! REST API → PostgreSQL — REST + Postgres sink knob showcase.
//!
//! REST uses next-link pagination, an API-key in the query string, and
//! incremental replication. Postgres sink demonstrates both column
//! mappings (`Jsonb` here) plus `batch_size` and pool sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_postgres \
//!     --features "source-rest sink-postgres"
//! ```

use std::time::Duration;

use faucet_stream::sink::postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use faucet_stream::{
    Auth, PaginationStyle, Pipeline, ReplicationMethod, RestStream, RestStreamConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/customers")
            .name("customers")
            .auth(Auth::ApiKeyQuery {
                param: "api_key".into(),
                value: std::env::var("API_KEY")?,
            })
            .header("Accept", "application/json")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::NextLinkInBody {
                next_link_path: "$.links.next".into(),
            })
            .max_pages(usize::MAX)
            .request_delay(Duration::from_millis(150))
            .timeout(Duration::from_secs(30))
            .max_retries(4)
            .retry_backoff(Duration::from_secs(1))
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .primary_keys(vec!["id".into()]),
    )?;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new("postgres://user:pass@localhost/app", "customers_raw")
            .column_mapping(PostgresColumnMapping::Jsonb {
                column: "payload".into(),
            })
            .with_batch_size(1000)
            .max_connections(10),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "ingested {} customers into Postgres",
        result.records_written
    );
    Ok(())
}
