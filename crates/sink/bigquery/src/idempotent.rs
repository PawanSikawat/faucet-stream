//! Schema-driven SQL generation for the BigQuery exactly-once write path (#215).
//!
//! All functions here are **pure** (no I/O): given the target table's schema as
//! [`FieldSpec`]s, they generate the SQL for the atomic
//! `INSERT … SELECT FROM UNNEST(JSON_QUERY_ARRAY(@payload))` + watermark `MERGE`
//! transaction that makes a page's rows and its commit token land atomically.
//! See `docs/superpowers/specs/2026-06-10-bigquery-exactly-once-design.md`.

#[allow(unused_imports)]
use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL,
};
use gcp_bigquery_client::model::field_type::FieldType;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;

/// A normalized BigQuery column spec — the sink's own mirror of the client's
/// `TableFieldSchema`, so the SQL generator is independent of the client model
/// and trivially constructible in unit tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSpec {
    /// Column / field name (a valid BigQuery identifier, since it came from
    /// BigQuery's own schema).
    pub name: String,
    /// Normalized type.
    pub ty: BqType,
    /// `true` for a `REPEATED` (array) column.
    pub repeated: bool,
    /// Sub-fields for `STRUCT`/`RECORD` columns (empty otherwise).
    pub fields: Vec<FieldSpec>,
}

/// Normalized BigQuery type, collapsing the client's alias variants
/// (`INT64`=`INTEGER`, `FLOAT64`=`FLOAT`, `BOOL`=`BOOLEAN`, `STRUCT`=`RECORD`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BqType {
    String,
    Bytes,
    Int64,
    Float64,
    Numeric,
    BigNumeric,
    Bool,
    Timestamp,
    Date,
    Time,
    Datetime,
    Interval,
    Geography,
    Json,
    Struct,
}

impl BqType {
    /// Map the client's `FieldType` discriminant to a normalized [`BqType`].
    pub fn from_field_type(ft: &FieldType) -> Self {
        match ft {
            FieldType::String => BqType::String,
            FieldType::Bytes => BqType::Bytes,
            FieldType::Integer | FieldType::Int64 => BqType::Int64,
            FieldType::Float | FieldType::Float64 => BqType::Float64,
            FieldType::Numeric => BqType::Numeric,
            FieldType::Bignumeric => BqType::BigNumeric,
            FieldType::Boolean | FieldType::Bool => BqType::Bool,
            FieldType::Timestamp => BqType::Timestamp,
            FieldType::Date => BqType::Date,
            FieldType::Time => BqType::Time,
            FieldType::Datetime => BqType::Datetime,
            FieldType::Interval => BqType::Interval,
            FieldType::Geography => BqType::Geography,
            FieldType::Json => BqType::Json,
            FieldType::Record | FieldType::Struct => BqType::Struct,
        }
    }

    /// The BigQuery SQL type keyword used in a `CAST(... AS <kw>)` / array
    /// element type.
    #[allow(dead_code)]
    fn sql_keyword(&self) -> &'static str {
        match self {
            BqType::String => "STRING",
            BqType::Bytes => "BYTES",
            BqType::Int64 => "INT64",
            BqType::Float64 => "FLOAT64",
            BqType::Numeric => "NUMERIC",
            BqType::BigNumeric => "BIGNUMERIC",
            BqType::Bool => "BOOL",
            BqType::Timestamp => "TIMESTAMP",
            BqType::Date => "DATE",
            BqType::Time => "TIME",
            BqType::Datetime => "DATETIME",
            BqType::Interval => "INTERVAL",
            BqType::Geography => "GEOGRAPHY",
            BqType::Json => "JSON",
            BqType::Struct => "STRUCT",
        }
    }
}

impl FieldSpec {
    /// Convert a client `TableFieldSchema` (possibly nested) into a [`FieldSpec`].
    pub fn from_table_field(f: &TableFieldSchema) -> Self {
        let repeated = f.mode.as_deref() == Some("REPEATED");
        let ty = BqType::from_field_type(&f.r#type);
        let fields = f
            .fields
            .as_ref()
            .map(|sub| sub.iter().map(FieldSpec::from_table_field).collect())
            .unwrap_or_default();
        FieldSpec {
            name: f.name.clone(),
            ty,
            repeated,
            fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar(name: &str, ty: BqType) -> FieldSpec {
        FieldSpec { name: name.into(), ty, repeated: false, fields: vec![] }
    }

    #[test]
    fn field_type_aliases_collapse() {
        assert_eq!(BqType::from_field_type(&FieldType::Integer), BqType::Int64);
        assert_eq!(BqType::from_field_type(&FieldType::Int64), BqType::Int64);
        assert_eq!(BqType::from_field_type(&FieldType::Float), BqType::Float64);
        assert_eq!(BqType::from_field_type(&FieldType::Boolean), BqType::Bool);
        assert_eq!(BqType::from_field_type(&FieldType::Record), BqType::Struct);
        assert_eq!(BqType::from_field_type(&FieldType::Struct), BqType::Struct);
    }

    #[test]
    fn from_table_field_reads_mode_and_nested_fields() {
        let tf = TableFieldSchema {
            name: "addr".into(),
            r#type: FieldType::Record,
            mode: Some("REPEATED".into()),
            fields: Some(vec![TableFieldSchema::string("city")]),
            categories: None,
            description: None,
            policy_tags: None,
        };
        let fs = FieldSpec::from_table_field(&tf);
        assert_eq!(fs, FieldSpec {
            name: "addr".into(),
            ty: BqType::Struct,
            repeated: true,
            fields: vec![scalar("city", BqType::String)],
        });
    }
}
