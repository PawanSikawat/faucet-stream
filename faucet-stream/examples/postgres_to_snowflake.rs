//! PostgreSQL → Snowflake (key-pair auth) — full builder showcase.
//!
//! Postgres source uses parameterised query + tuned pool. Snowflake sink
//! demonstrates the JWT key-pair auth variant plus batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_snowflake \
//!     --features "source-postgres sink-snowflake"
//! ```

use faucet_stream::sink::snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};
use faucet_stream::{Pipeline, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(
        PostgresSourceConfig::new(
            "postgres://user:pass@localhost/app",
            "SELECT id, email, created_at FROM users WHERE tenant_id = $1",
        )
        .params(vec![json!("acme")])
        .with_max_connections(8),
    )
    .await?;

    let sink = SnowflakeSink::new(
        SnowflakeSinkConfig::new(
            "xy12345.us-east-1",
            "INGEST_WH",
            "ANALYTICS",
            "RAW",
            "USERS",
            SnowflakeAuth::KeyPair {
                user: "INGEST_USER".into(),
                private_key_pem: std::fs::read_to_string("snowflake_key.pem")?,
            },
        )
        .batch_size(500),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} users into Snowflake", result.records_written);
    Ok(())
}
