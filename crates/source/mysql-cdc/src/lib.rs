#![cfg_attr(docsrs, feature(doc_cfg))]
//! MySQL binlog (CDC) source for the faucet-stream ecosystem.
//!
//! Tails the MySQL binary log via row-based replication and emits per-row
//! change events as a CDC envelope, resumable via a `{file,pos}` or
//! `{gtid_set}` bookmark.

mod config;
mod convert;
mod state;
mod stream;

pub use config::{CdcTls, MysqlCdcSourceConfig, StartPosition};
pub use state::{state_key, Bookmark};
pub use stream::MysqlCdcSource;
