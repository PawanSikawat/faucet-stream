#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod config;
pub(crate) mod catalog;
pub(crate) mod schema;
pub(crate) mod writer;
pub mod sink;

pub use config::{CatalogConfig, IcebergSinkConfig, ParquetOpts, PartitionField, WriteMode};
pub use sink::IcebergSink;
