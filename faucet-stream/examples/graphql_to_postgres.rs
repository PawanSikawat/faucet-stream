//! GraphQL → PostgreSQL — full builder showcase for both connectors.
//!
//! GraphQL source uses variables, custom auth headers, Relay cursor
//! pagination, and a records-path. Postgres sink demonstrates both column
//! mappings (`AutoMap` here) plus batch size and pool sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example graphql_to_postgres \
//!     --features "source-graphql sink-postgres"
//! ```

use faucet_stream::sink::postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use faucet_stream::source::graphql::{
    GraphqlAuth, GraphqlPagination, GraphqlStream, GraphqlStreamConfig,
};
use faucet_stream::{Pipeline, json};
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query = r#"
        query Users($after: String, $first: Int!) {
          users(first: $first, after: $after) {
            edges { node { id name email createdAt } }
            pageInfo { endCursor hasNextPage }
          }
        }
    "#;

    let mut headers = HeaderMap::new();
    headers.insert("X-Client", HeaderValue::from_static("faucet-stream"));

    let source = GraphqlStream::new(
        GraphqlStreamConfig::new("https://api.example.com/graphql", query)
            .variables(json!({ "first": 100 }))
            .auth(GraphqlAuth::Bearer {
                token: std::env::var("API_TOKEN")?,
            })
            .headers(headers)
            .records_path("$.data.users.edges[*].node")
            .pagination(GraphqlPagination {
                has_next_page_path: "$.data.users.pageInfo.hasNextPage".into(),
                cursor_path: "$.data.users.pageInfo.endCursor".into(),
                cursor_variable: "after".into(),
                page_size_variable: "first".into(),
            })
            .with_batch_size(100)
            .max_pages(usize::MAX),
    );

    let sink = PostgresSink::new(
        PostgresSinkConfig::new("postgres://user:pass@localhost/app", "users_imported")
            .column_mapping(PostgresColumnMapping::AutoMap)
            .with_batch_size(1000)
            .max_connections(8),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} users into Postgres", result.records_written);
    Ok(())
}
