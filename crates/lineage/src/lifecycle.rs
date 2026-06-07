//! Per-invocation lineage context the executor fills and hands to the emitter.

use crate::column::ColumnLineage;
use crate::config::ParentJob;
use chrono::{DateTime, Utc};

/// Inferred schema: ordered (field name, OL type string) pairs.
#[derive(Debug, Clone, Default)]
pub struct InferredSchema {
    pub fields: Vec<(String, String)>,
}

/// A source/sink dataset identity.
#[derive(Debug, Clone)]
pub struct DatasetRef {
    pub namespace: String,
    pub name: String,
}

/// Everything needed to build a `RunEvent` for one invocation.
#[derive(Debug, Clone)]
pub struct RunLifecycle {
    pub job_namespace: String,
    pub job_name: String,
    pub run_id: String,
    pub parent: Option<ParentJob>,
    pub input: DatasetRef,
    pub output: DatasetRef,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub records: u64,
    pub error: Option<String>,
    pub input_schema: Option<InferredSchema>,
    pub output_schema: Option<InferredSchema>,
    pub column_lineage: Option<ColumnLineage>,
    pub source_code: Option<String>,
}
