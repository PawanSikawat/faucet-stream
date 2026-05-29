//! Data-quality checks: declarative per-record and per-batch assertions that
//! quarantine violating records to the DLQ or abort the run. Pure evaluation;
//! the pipeline wires the DLQ routing in `run_stream`.
//!
//! See `docs/superpowers/specs/2026-05-29-quality-checks-design.md`.

pub mod batch;
pub mod compile;
pub mod config;
pub mod record;

pub use compile::CompiledQuality;
pub use config::{BatchCheck, CompareOp, JsonType, OnFailure, QualitySpec, RecordCheck};
