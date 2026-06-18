//! Schema-drift detection + policy types (issue #194).
//!
//! Drift is the divergence between an incoming page's inferred top-level shape
//! (via [`crate::schema::infer_schema`]) and the sink's live destination schema
//! (via [`crate::Sink::current_schema`]). The pure [`diff_schema`] classifies
//! each top-level column into one bucket; [`SchemaDriftPolicy`] decides what the
//! pipeline does with the result. Nested objects are treated as a single column.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One column's drift, expressed in JSON-Schema type-fragment terms
/// (e.g. `{"type":"integer"}` or `{"type":["string","null"]}`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChange {
    /// Top-level column name.
    pub name: String,
    /// Destination type fragment; `None` for an addition (not in destination).
    pub from: Option<Value>,
    /// Inferred type fragment from the incoming page.
    pub to: Value,
}

/// Result of diffing a page's inferred shape against the destination schema.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaDiff {
    /// In the page, not in the destination.
    pub additions: Vec<ColumnChange>,
    /// Existing column whose type widened losslessly (e.g. integer→number,
    /// or gained nullability).
    pub widenings: Vec<ColumnChange>,
    /// Existing column whose type changed in a way that cannot be auto-applied
    /// (narrowing / incompatible type swap).
    pub incompatible: Vec<ColumnChange>,
    /// In the destination and NOT NULL, absent from the page — would fail an
    /// insert unless relaxed to nullable.
    pub droppable_required: Vec<String>,
}

impl SchemaDiff {
    /// `true` when no drift of any kind was detected.
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty()
            && self.widenings.is_empty()
            && self.incompatible.is_empty()
            && self.droppable_required.is_empty()
    }

    /// Column names that drifted, for error messages / metrics.
    pub fn changed_columns(&self) -> Vec<String> {
        self.additions
            .iter()
            .chain(&self.widenings)
            .chain(&self.incompatible)
            .map(|c| c.name.clone())
            .chain(self.droppable_required.iter().cloned())
            .collect()
    }
}

/// The applyable subset of a [`SchemaDiff`] handed to [`crate::Sink::evolve_schema`].
/// Never carries `incompatible` columns — those are routed by the policy.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaEvolution {
    pub additions: Vec<ColumnChange>,
    pub widenings: Vec<ColumnChange>,
    /// Columns to relax from NOT NULL to nullable.
    pub relax_nullability: Vec<String>,
}

impl SchemaEvolution {
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.widenings.is_empty() && self.relax_nullability.is_empty()
    }
}
