//! PostgreSQL query → Snowflake (key-pair auth).
//!
//! Required: a Snowflake account with key-pair authentication configured. The
//! private key PEM is passed inline — in production, load it from a file or
//! secret manager.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example postgres_to_snowflake \
//!     --features "source-postgres sink-snowflake"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use faucet_stream::source::postgres::{PostgresSource, PostgresSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PostgresSource::new(PostgresSourceConfig::new(
        "postgres://user:pass@localhost/app",
        "SELECT id, email, created_at FROM users",
    ))
    .await?;

    let auth = SnowflakeAuth::KeyPair {
        user: "INGEST_USER".into(),
        private_key_pem: std::fs::read_to_string("snowflake_key.pem")?,
    };

    let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
        "xy12345.us-east-1",
        "INGEST_WH",
        "ANALYTICS",
        "RAW",
        "USERS",
        auth,
    ));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} users into Snowflake", result.records_written);
    Ok(())
}
