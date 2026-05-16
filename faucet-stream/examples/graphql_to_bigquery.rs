//! GraphQL API → Google BigQuery.
//!
//! Required: a BigQuery dataset/table and a service account key.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example graphql_to_bigquery \
//!     --features "source-graphql sink-bigquery"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::graphql::{GraphqlStream, GraphqlStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = GraphqlStream::new(GraphqlStreamConfig::new(
        "https://api.example.com/graphql",
        "query { orders { id total status } }",
    ));

    let sink = BigQuerySink::new(BigQuerySinkConfig::new(
        "my-gcp-project",
        "raw",
        "orders",
        BigQueryCredentials::ApplicationDefault,
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} orders into BigQuery", result.records_written);
    Ok(())
}
