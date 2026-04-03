//! # faucet-sink-snowflake
//!
//! Snowflake sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a Snowflake table using the
//! [Snowflake SQL REST API](https://docs.snowflake.com/en/developer-guide/sql-api/reference).

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{SnowflakeAuth, SnowflakeSinkConfig};
pub use sink::SnowflakeSink;
