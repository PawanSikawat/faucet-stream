//! Inbuilt `zip_columns` transform (#551): turn a **columnar payload** —
//! `{ columns: [{name}, …], rows: [[v0, v1, …], …] }` — into one object per row,
//! keyed by column name.
//!
//! Analytics / report APIs (e.g. Shopify's ShopifyQL `tableData`) return results
//! positionally: a list of column descriptors plus a list of value-arrays. This
//! transform zips each row against the column names so downstream stages and
//! sinks see ordinary `{col: value}` records. It is expressible today via the
//! DuckDB `sql` transform, but a small declarative transform is cleaner and
//! needs no embedded engine.
//!
//! The whole module is gated by `#[cfg(feature = "transform-zip-columns")]` at
//! the `mod` site in `lib.rs`. It routes through
//! [`TransformStage::PageFn`](crate::stage::TransformStage) (page-level, 1→0..N,
//! fallible) so a row whose width doesn't match the column count fails loudly
//! rather than silently dropping or misaligning fields.

use crate::FaucetError;
use crate::stage::TransformStage;
use crate::util::extract_records;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

/// User-facing `zip_columns` config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ZipColumnsSpec {
    /// JSONPath to the column **names**. Point it at the name of each column
    /// descriptor (`columns[*].name`) or at a plain array of strings
    /// (`columns`). Every matched value must be a string.
    pub columns_path: String,
    /// JSONPath to the **rows** — an array of positional value-arrays
    /// (`rows` or `rows[*]`). Each row must have exactly as many values as
    /// there are columns.
    pub rows_path: String,
}

impl ZipColumnsSpec {
    /// Validate the spec, returning a reusable [`CompiledZipColumns`].
    pub fn compile(&self) -> Result<CompiledZipColumns, FaucetError> {
        CompiledZipColumns::compile(self)
    }

    /// Compile and wrap as a [`TransformStage::PageFn`] (1→0..N per record,
    /// fallible on a row/column width mismatch).
    pub fn into_stage(&self) -> Result<TransformStage, FaucetError> {
        let compiled = self.compile()?;
        Ok(TransformStage::PageFn(Arc::new(move |page: Vec<Value>| {
            let mut out = Vec::with_capacity(page.len());
            for rec in page {
                out.extend(compiled.apply(&rec)?);
            }
            Ok(out)
        })))
    }
}

/// Validated [`ZipColumnsSpec`] — apply per record with [`CompiledZipColumns::apply`].
#[derive(Debug, Clone)]
pub struct CompiledZipColumns {
    columns_path: String,
    rows_path: String,
}

impl CompiledZipColumns {
    fn compile(spec: &ZipColumnsSpec) -> Result<Self, FaucetError> {
        if spec.columns_path.trim().is_empty() {
            return Err(FaucetError::Config(
                "zip_columns: `columns_path` must not be empty".into(),
            ));
        }
        if spec.rows_path.trim().is_empty() {
            return Err(FaucetError::Config(
                "zip_columns: `rows_path` must not be empty".into(),
            ));
        }
        Ok(Self {
            columns_path: normalize_path(&spec.columns_path),
            rows_path: normalize_path(&spec.rows_path),
        })
    }

    /// Zip one columnar record into one object per row. A record that carries no
    /// rows produces zero output records; a row whose width differs from the
    /// column count is a hard error (never silently misaligned).
    pub fn apply(&self, rec: &Value) -> Result<Vec<Value>, FaucetError> {
        let columns = self.column_names(rec)?;
        let rows = row_candidates(&extract_records(rec, Some(&self.rows_path))?);
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.into_iter().enumerate() {
            let Value::Array(values) = row else {
                return Err(FaucetError::Transform(format!(
                    "zip_columns: row {i} at `{}` is not an array",
                    self.rows_path
                )));
            };
            if values.len() != columns.len() {
                return Err(FaucetError::Transform(format!(
                    "zip_columns: row {i} has {} value(s) but there are {} column(s)",
                    values.len(),
                    columns.len()
                )));
            }
            let obj: Map<String, Value> = columns.iter().cloned().zip(values).collect();
            out.push(Value::Object(obj));
        }
        Ok(out)
    }

    /// Resolve the column names, requiring every matched value to be a string.
    fn column_names(&self, rec: &Value) -> Result<Vec<String>, FaucetError> {
        let matched = extract_records(rec, Some(&self.columns_path))?;
        // A single match that is itself an array (`columns_path: columns` where
        // columns is already a string array) is unwrapped to its elements.
        let candidates = column_candidates(&matched);
        let mut names = Vec::with_capacity(candidates.len());
        for c in candidates {
            match c {
                Value::String(s) => names.push(s),
                other => {
                    return Err(FaucetError::Transform(format!(
                        "zip_columns: column name at `{}` is not a string: {other}",
                        self.columns_path
                    )));
                }
            }
        }
        if names.is_empty() {
            return Err(FaucetError::Transform(format!(
                "zip_columns: `columns_path` `{}` matched no column names",
                self.columns_path
            )));
        }
        Ok(names)
    }
}

/// Accept a bare path (`rows`, `columns[*].name`) by rooting it at `$`, while
/// leaving an already-`$`-rooted expression untouched.
fn normalize_path(path: &str) -> String {
    let p = path.trim();
    if p.starts_with('$') {
        p.to_string()
    } else {
        format!("$.{p}")
    }
}

/// Column candidates: a single array match (`columns_path: columns` pointing at
/// a string array) is unwrapped to its elements; a `columns[*].name`-style match
/// already yields the names directly.
fn column_candidates(matched: &[Value]) -> Vec<Value> {
    match matched {
        [Value::Array(inner)] => inner.clone(),
        other => other.to_vec(),
    }
}

/// Row candidates: `rows` matches the rows array (one match, an array *of
/// arrays*) → unwrap to the rows; `rows[*]` yields each row directly. Unwrapping
/// only when every element is itself an array disambiguates a single-row
/// `rows[*]` (one array of scalars) from the whole rows container.
fn row_candidates(matched: &[Value]) -> Vec<Value> {
    if let [Value::Array(inner)] = matched
        && inner.iter().all(|v| matches!(v, Value::Array(_)))
    {
        return inner.clone();
    }
    matched.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> CompiledZipColumns {
        ZipColumnsSpec {
            columns_path: "columns[*].name".into(),
            rows_path: "rows".into(),
        }
        .compile()
        .unwrap()
    }

    #[test]
    fn zips_columns_into_row_objects() {
        let rec = json!({
            "columns": [{"name": "day"}, {"name": "sessions"}],
            "rows": [["2026-01-01", 12], ["2026-01-02", 7]],
        });
        let out = spec().apply(&rec).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], json!({"day": "2026-01-01", "sessions": 12}));
        assert_eq!(out[1], json!({"day": "2026-01-02", "sessions": 7}));
    }

    #[test]
    fn direct_string_array_columns_and_rows_star() {
        let compiled = ZipColumnsSpec {
            columns_path: "columns".into(),
            rows_path: "rows[*]".into(),
        }
        .compile()
        .unwrap();
        let rec = json!({"columns": ["a", "b"], "rows": [[1, 2]]});
        let out = compiled.apply(&rec).unwrap();
        assert_eq!(out, vec![json!({"a": 1, "b": 2})]);
    }

    #[test]
    fn no_rows_yields_no_records() {
        let rec = json!({"columns": [{"name": "a"}], "rows": []});
        assert!(spec().apply(&rec).unwrap().is_empty());
    }

    #[test]
    fn width_mismatch_errors_clearly() {
        let rec = json!({"columns": [{"name": "a"}, {"name": "b"}], "rows": [[1]]});
        let err = spec().apply(&rec).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("1 value") && msg.contains("2 column"), "{msg}");
    }

    #[test]
    fn non_string_column_name_errors() {
        let rec = json!({"columns": [{"name": 7}], "rows": [[1]]});
        assert!(spec().apply(&rec).is_err());
    }

    #[test]
    fn empty_paths_rejected_at_compile() {
        assert!(
            ZipColumnsSpec {
                columns_path: "".into(),
                rows_path: "rows".into()
            }
            .compile()
            .is_err()
        );
        assert!(
            ZipColumnsSpec {
                columns_path: "columns".into(),
                rows_path: " ".into()
            }
            .compile()
            .is_err()
        );
    }

    #[test]
    fn into_stage_is_pagefn_and_flat_maps() {
        let stage = spec_spec().into_stage().unwrap();
        match stage {
            TransformStage::PageFn(f) => {
                let page = vec![json!({
                    "columns": [{"name": "a"}],
                    "rows": [[1], [2]],
                })];
                let out = f(page).unwrap();
                assert_eq!(out, vec![json!({"a": 1}), json!({"a": 2})]);
            }
            other => panic!("expected PageFn, got {other:?}"),
        }
    }

    fn spec_spec() -> ZipColumnsSpec {
        ZipColumnsSpec {
            columns_path: "columns[*].name".into(),
            rows_path: "rows".into(),
        }
    }
}
