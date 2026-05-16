//! GraphQL API → PostgreSQL (JSONB column).
//!
//! Required: a Postgres database reachable at `DATABASE_URL`, with a table
//! containing a `jsonb` column called `data` (the default mapping).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example graphql_to_postgres \
//!     --features "source-graphql sink-postgres"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::postgres::{PostgresSink, PostgresSinkConfig};
use faucet_stream::source::graphql::{GraphqlStream, GraphqlStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query = r#"
        query Users {
          users(first: 100) {
            id
            name
            email
          }
        }
    "#;

    let source = GraphqlStream::new(GraphqlStreamConfig::new(
        "https://api.example.com/graphql",
        query,
    ));

    let sink = PostgresSink::new(PostgresSinkConfig::new(
        "postgres://user:pass@localhost/mydb",
        "users_raw",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("inserted {} rows into users_raw", result.records_written);
    Ok(())
}
