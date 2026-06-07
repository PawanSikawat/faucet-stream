#![cfg_attr(docsrs, feature(doc_cfg))]

pub(crate) mod catalog;
pub mod config;
pub(crate) mod schema;
pub mod sink;
#[cfg(feature = "storage-opendal")]
pub(crate) mod storage_factory;
pub(crate) mod writer;

pub use config::{CatalogConfig, IcebergSinkConfig, ParquetOpts, PartitionField, WriteMode};
pub use sink::IcebergSink;
