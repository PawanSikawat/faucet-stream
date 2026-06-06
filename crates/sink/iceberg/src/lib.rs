#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod config;
pub(crate) mod catalog;
pub(crate) mod schema;
mod spike;

pub use config::{CatalogConfig, IcebergSinkConfig, ParquetOpts, PartitionField, WriteMode};
