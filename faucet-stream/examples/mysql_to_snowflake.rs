//! MySQL → Snowflake (OAuth auth) — full builder showcase.
//!
//! MySQL source uses a tuned pool. Snowflake sink demonstrates the OAuth
//! auth variant and batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example mysql_to_snowflake \
//!     --features "source-mysql sink-snowflake"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use faucet_stream::source::mysql::{MysqlSource, MysqlSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = MysqlSource::new(
        MysqlSourceConfig::new(
            "mysql://user:pass@localhost/sales",
            "SELECT order_id, customer_id, total, ordered_at FROM orders",
        )
        .with_max_connections(16),
    )
    .await?;

    let sink = SnowflakeSink::new(
        SnowflakeSinkConfig::new(
            "xy12345.us-east-1",
            "LOAD_WH",
            "ANALYTICS",
            "STAGING",
            "ORDERS",
            SnowflakeAuth::OAuth {
                token: std::env::var("SNOWFLAKE_OAUTH_TOKEN")?,
            },
        )
        .with_batch_size(1000),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} orders into Snowflake", result.records_written);
    Ok(())
}
