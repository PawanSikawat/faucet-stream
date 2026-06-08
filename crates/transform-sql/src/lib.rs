#![cfg_attr(docsrs, feature(doc_cfg))]
//! SQL-as-transform for faucet-stream, backed by embedded DuckDB.
//!
//! Each pipeline page is exposed to a SQL query as the relation `batch`; the
//! result set becomes the new page. [`SqlTransformConfig`] is the user-facing
//! config (the `query` plus optional reference [`RelationSpec`]s, whose data
//! comes from a [`RelationSource`]). The compiled runtime that runs the query
//! per page is added in a later module.

mod config;
mod shovel;

pub use config::{RelationSource, RelationSpec, SqlTransformConfig};
