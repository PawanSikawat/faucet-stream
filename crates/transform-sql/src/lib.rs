#![cfg_attr(docsrs, feature(doc_cfg))]
//! SQL-as-transform for faucet-stream, backed by embedded DuckDB.

mod config;

pub use config::{RelationSource, RelationSpec, SqlTransformConfig};
