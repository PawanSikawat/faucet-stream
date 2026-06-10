#![cfg_attr(docsrs, feature(doc_cfg))]

pub(crate) mod catalog;
pub mod config;
pub(crate) mod schema;
pub mod sink;
#[cfg(any(
    feature = "catalog-glue",
    feature = "catalog-sql",
    feature = "catalog-hms"
))]
pub(crate) mod storage_factory;
pub(crate) mod writer;

pub use config::{CatalogConfig, IcebergSinkConfig, ParquetOpts, PartitionField};
pub use faucet_core::WriteMode;
pub use sink::IcebergSink;
