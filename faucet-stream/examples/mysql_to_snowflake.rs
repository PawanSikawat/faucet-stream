//! MySQL query → Snowflake (OAuth auth).
//!
//! Required: a Snowflake account using OAuth (token from your IdP) and a
//! reachable MySQL instance.
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
    let source = MysqlSource::new(MysqlSourceConfig::new(
        "mysql://user:pass@localhost/sales",
        "SELECT order_id, customer_id, total, ordered_at FROM orders",
    ))
    .await?;

    let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
        "xy12345.us-east-1",
        "LOAD_WH",
        "ANALYTICS",
        "STAGING",
        "ORDERS",
        SnowflakeAuth::OAuth {
            token: std::env::var("SNOWFLAKE_OAUTH_TOKEN")?,
        },
    ));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} orders into Snowflake", result.records_written);
    Ok(())
}
