//! # faucet-source-snowflake
//!
//! Snowflake query source connector for the faucet-stream ecosystem.
//!
//! Executes a SQL statement against Snowflake via the
//! [SQL REST API](https://docs.snowflake.com/en/developer-guide/sql-api/intro)
//! and streams the result set as `serde_json::Value` records, one JSON
//! object per row keyed by lowercased column name.

pub mod config;
pub mod convert;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{SnowflakeAuth, SnowflakeSourceConfig};
pub use stream::SnowflakeSource;
