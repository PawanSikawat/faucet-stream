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

pub mod adaptive;
pub mod auth;
pub mod check;
pub mod config;
pub mod dlq;
pub mod drift;
pub mod error;
pub mod idempotency;
pub mod observability;
pub mod pipeline;
#[cfg(feature = "quality")]
pub mod quality;
pub mod replication;
pub mod resilience;
pub mod retry;
pub mod schema;
pub mod shard;
pub mod stage;
pub mod state;
pub mod traits;
pub mod transform;
pub mod transforming_source;
pub mod util;
pub mod verify;
pub mod write_mode;

#[cfg(feature = "compression")]
pub mod compression;

pub use adaptive::{
    AdaptiveBatchConfig, AdjustDirection, AdjustReason, Adjustment, AimdController, Observation,
};
pub use auth::{AuthProvider, AuthReference, AuthSpec, Credential, SharedAuthProvider};
pub use check::{CheckContext, CheckReport, Probe, ProbeStatus};
pub use dlq::{DlqConfig, DlqReason, DlqStats, OnBatchError, build_envelope};
pub use drift::{
    ColumnChange, OnDrift, OnIncompatible, SchemaDiff, SchemaDriftPolicy, SchemaDriftSpec,
    SchemaEvolution, SqlBaseType, adds_null, base_widened, json_schema_base_type,
};
pub use error::FaucetError;
pub use idempotency::{DeliveryMode, format_token, parse_token, unwrap_state, wrap_state};
#[cfg(feature = "quality")]
pub use observability::instrumented_apply_quality;
pub use observability::otel::{OtelConfig, OtelProtocol, OtelSignal, shutdown_otel};
pub use observability::{
    DurationGuard, InstallError, InstallReport, InstrumentedSink, InstrumentedSource,
    InstrumentedStateStore, Labels, ObservabilityConfig, PrometheusConfig, RunStreamOptions,
    TracingConfig, install_observability, instrumented_apply_stages, register_build_info,
    update_bookmark_lag,
};
pub use pipeline::{
    DEFAULT_BATCH_SIZE, MAX_BATCH_SIZE, Pipeline, PipelineResult, StreamPage, run_stream,
    validate_batch_size,
};
pub use replication::ReplicationMethod;
pub use resilience::{
    BackoffKind, CircuitBreaker, CircuitBreakerConfig, PoisonAction, PoisonPolicy,
    ResiliencePolicy, RetryClass, RetryClassSet, RetryMetrics, RetryPolicy, classify,
    execute_with_policy, execute_with_policy_metered,
};
pub use retry::execute_with_retry;
pub use shard::ShardSpec;
#[cfg(feature = "transform-cdc-unwrap")]
pub use stage::CdcUnwrapSpec;
#[cfg(feature = "transform-explode")]
pub use stage::{ExplodeSpec, OnMissing};
#[cfg(feature = "transform-filter")]
pub use stage::{FilterOp, FilterSpec};
pub use stage::{TransformStage, compile_stage};
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
pub use util::redact_uri_credentials;
pub use verify::{IntegrityCheck, LengthCheck, VerifyingReader};
pub use write_mode::{
    DeleteMarker, KeyTuple, WriteMode, WritePlan, WriteSpec, key_to_doc_id, key_to_filter,
    plan_writes,
};

// Re-export dependencies that connector authors need, so they only depend on
// `faucet-core` instead of adding `async-trait` and `serde_json` themselves.
pub use async_stream;
pub use async_trait::async_trait;
pub use futures_core::{self, Stream};
pub use schemars::{self, JsonSchema, schema_for};
pub use serde_json::{self, Value, json};
/// Re-exported so callers of [`Pipeline::with_cancel`](pipeline::Pipeline::with_cancel)
/// / [`RunStreamOptions::with_cancel`] can name the token type without adding
/// `tokio-util` themselves.
pub use tokio_util::sync::CancellationToken;

#[cfg(feature = "compression")]
pub use compression::{Compression, CompressionConfig, compress_buf, warn_mismatch};

#[cfg(feature = "quality")]
pub use quality::{
    BatchCheck, CheckTally, CompareOp, CompiledQuality, JsonType, OnFailure, QualityOutcome,
    QualitySpec, QuarantinedRecord, RecordCheck, apply_quality,
};
