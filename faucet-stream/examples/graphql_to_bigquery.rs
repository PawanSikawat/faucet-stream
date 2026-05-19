//! GraphQL → BigQuery — full builder showcase for both connectors.
//!
//! GraphQL source uses custom-header auth, variables, Relay pagination, and
//! a records path. BigQuery sink demonstrates batch sizing and the
//! `ApplicationDefault` credential variant.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example graphql_to_bigquery \
//!     --features "source-graphql sink-bigquery"
//! ```

use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::graphql::{
    GraphqlAuth, GraphqlPagination, GraphqlStream, GraphqlStreamConfig,
};
use faucet_stream::{Pipeline, json};
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query = r#"
        query Orders($after: String, $first: Int!) {
          orders(first: $first, after: $after) {
            edges { node { id total status createdAt } }
            pageInfo { endCursor hasNextPage }
          }
        }
    "#;

    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Api-Key",
        HeaderValue::from_str(&std::env::var("API_KEY")?)?,
    );
    headers.insert("X-Tenant", HeaderValue::from_static("acme"));

    let source = GraphqlStream::new(
        GraphqlStreamConfig::new("https://api.example.com/graphql", query)
            .variables(json!({ "first": 200 }))
            .auth(GraphqlAuth::Custom {
                headers: [
                    ("X-Api-Key".to_string(), std::env::var("API_KEY")?),
                    ("X-Tenant".to_string(), "acme".to_string()),
                ]
                .into_iter()
                .collect(),
            })
            .headers(headers)
            .records_path("$.data.orders.edges[*].node")
            .pagination(GraphqlPagination {
                has_next_page_path: "$.data.orders.pageInfo.hasNextPage".into(),
                cursor_path: "$.data.orders.pageInfo.endCursor".into(),
                cursor_variable: "after".into(),
                page_size_variable: "first".into(),
            })
            .with_batch_size(200)
            .max_pages(500),
    );

    let sink = BigQuerySink::new(
        BigQuerySinkConfig::new(
            "my-gcp-project",
            "raw",
            "orders",
            BigQueryCredentials::ApplicationDefault,
        )
        .batch_size(1000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} orders into BigQuery", result.records_written);
    Ok(())
}
