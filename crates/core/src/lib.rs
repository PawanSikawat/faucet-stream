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

pub mod error;
pub mod pipeline;
pub mod replication;
pub mod schema;
pub mod traits;
pub mod transform;

pub use error::FaucetError;
pub use pipeline::{Pipeline, PipelineResult, run_stream};
pub use replication::ReplicationMethod;
pub use traits::{Sink, Source};
pub use transform::RecordTransform;
