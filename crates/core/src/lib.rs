#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-core
//!
//! Shared types, traits, and utilities for the faucet-stream ecosystem.
//!
//! This crate provides the common foundation used by all faucet source and
//! sink connectors:
//!
//! - [`FaucetError`] — unified error type
//! - [`Source`] / [`Sink`] — async traits for data connectors
//! - [`RecordTransform`] — record transformation pipeline
//! - [`ReplicationMethod`] — incremental replication support
//! - [`schema::infer_schema`] — JSON Schema inference from record samples

pub mod config;
pub mod dlq;
pub mod error;
pub mod observability;
pub mod pipeline;
pub mod replication;
pub mod retry;
pub mod schema;
pub mod state;
pub mod traits;
pub mod transform;
pub mod transforming_source;
pub mod util;

#[cfg(feature = "compression")]
pub mod compression;

pub use dlq::{DlqConfig, DlqReason, DlqStats, OnBatchError, build_envelope};
pub use error::FaucetError;
pub use observability::{
    DurationGuard, InstallError, InstallReport, InstrumentedSink, InstrumentedSource,
    InstrumentedStateStore, Labels, ObservabilityConfig, PrometheusConfig, RunStreamOptions,
    TracingConfig, install_observability, instrumented_apply_all, register_build_info,
    update_bookmark_lag,
};
pub use pipeline::{
    DEFAULT_BATCH_SIZE, MAX_BATCH_SIZE, Pipeline, PipelineResult, StreamPage, run_stream,
    validate_batch_size,
};
pub use replication::ReplicationMethod;
pub use retry::execute_with_retry;
pub use state::{FileStateStore, MemoryStateStore, StateStore};
pub use traits::{RowOutcome, Sink, Source};
#[cfg(feature = "transform-keys-case")]
pub use transform::KeyCaseMode;
pub use transform::RecordTransform;
#[cfg(feature = "transform-value-case")]
pub use transform::ValueCaseMode;
#[cfg(feature = "transform-cast")]
pub use transform::{CastOnError, CastType};
pub use transforming_source::TransformingSource;

// Re-export dependencies that connector authors need, so they only depend on
// `faucet-core` instead of adding `async-trait` and `serde_json` themselves.
pub use async_stream;
pub use async_trait::async_trait;
pub use futures_core::{self, Stream};
pub use schemars::{self, JsonSchema, schema_for};
pub use serde_json::{self, Value, json};

#[cfg(feature = "compression")]
pub use compression::{Compression, CompressionConfig, compress_buf, warn_mismatch};
