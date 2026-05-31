//! Pure helpers for turning JSON records into MSSQL `INSERT` statements and
//! bound parameters. No I/O — all unit-testable.

use faucet_core::FaucetError;
use faucet_mssql_common::PARAM_LIMIT;
use serde_json::Value;
use tiberius::ToSql;

use crate::config::OnUnknownField;

/// An owned bind parameter, so the `&dyn ToSql` slice handed to `tiberius`
/// borrows from a value that outlives the call.
pub(crate) enum BoundParam {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
    Null(Option<i32>),
}

impl BoundParam {
    pub(crate) fn from_value(v: &Value) -> Self {
        match v {
            Value::String(s) => BoundParam::Str(s.clone()),
            Value::Number(n) if n.is_i64() => BoundParam::I64(n.as_i64().unwrap()),
            Value::Number(n) if n.is_u64() => BoundParam::I64(n.as_u64().unwrap() as i64),
            Value::Number(n) => BoundParam::F64(n.as_f64().unwrap_or(0.0)),
            Value::Bool(b) => BoundParam::Bool(*b),
            Value::Null => BoundParam::Null(None),
            // Arrays/objects are serialized; for a json_column this is the whole
            // record, for auto_columns it's a nested value bound as NVARCHAR.
            other => BoundParam::Str(other.to_string()),
        }
    }

    pub(crate) fn as_tosql(&self) -> &dyn ToSql {
        match self {
            BoundParam::I64(v) => v,
            BoundParam::F64(v) => v,
            BoundParam::Bool(v) => v,
            BoundParam::Str(v) => v,
            BoundParam::Null(v) => v,
        }
    }
}

/// Maximum rows per `INSERT` so `rows * num_cols` stays within
/// [`PARAM_LIMIT`]. Always at least 1.
pub(crate) fn max_rows_per_insert(num_cols: usize) -> usize {
    if num_cols == 0 {
        return 1;
    }
    (PARAM_LIMIT / num_cols).max(1)
}

/// Build a multi-row `INSERT` with `@P`-numbered placeholders:
/// `INSERT INTO <table> (c1, c2) VALUES (@P1, @P2), (@P3, @P4), …`.
///
/// `table_quoted` and the `cols_quoted` entries must already be quoted via
/// `quote_ident_mssql`.
pub(crate) fn build_insert_sql(
    table_quoted: &str,
    cols_quoted: &[String],
    num_rows: usize,
) -> String {
    let num_cols = cols_quoted.len();
    let mut tuples = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let start = row * num_cols + 1;
        let placeholders: Vec<String> = (start..start + num_cols).map(|i| format!("@P{i}")).collect();
        tuples.push(format!("({})", placeholders.join(", ")));
    }
    format!(
        "INSERT INTO {} ({}) VALUES {}",
        table_quoted,
        cols_quoted.join(", "),
        tuples.join(", ")
    )
}

/// Decide the fixed column list for an `auto_columns` batch.
///
/// `insertable` is the set of writable table columns (IDENTITY columns already
/// excluded), in table order. The column list is the insertable columns present
/// in the first record. Record keys that match no insertable column are
/// "unknown" and handled per `on_unknown`.
pub(crate) fn resolve_insert_columns(
    insertable: &[String],
    records: &[Value],
    on_unknown: OnUnknownField,
) -> Result<Vec<String>, FaucetError> {
    let insertable_set: std::collections::HashSet<&str> =
        insertable.iter().map(|s| s.as_str()).collect();

    // Detect unknown keys across the batch.
    let mut unknown: Vec<String> = Vec::new();
    for record in records {
        if let Some(obj) = record.as_object() {
            for key in obj.keys() {
                if !insertable_set.contains(key.as_str()) && !unknown.contains(key) {
                    unknown.push(key.clone());
                }
            }
        }
    }
    if !unknown.is_empty() {
        match on_unknown {
            OnUnknownField::Error => {
                return Err(FaucetError::Sink(format!(
                    "auto_columns: record keys {unknown:?} do not match any writable column"
                )));
            }
            OnUnknownField::Warn => {
                tracing::warn!(?unknown, "auto_columns: dropping record keys with no matching column");
            }
            OnUnknownField::Drop => {}
        }
    }

    // Fix the column set from the first record that has any matching key.
    let first = records.iter().find_map(|r| r.as_object());
    let columns: Vec<String> = match first {
        Some(obj) => insertable
            .iter()
            .filter(|c| obj.contains_key(c.as_str()))
            .cloned()
            .collect(),
        None => Vec::new(),
    };
    Ok(columns)
}

/// Bind one record's values in `columns` order (SQL NULL for missing keys).
pub(crate) fn auto_row_params(record: &Value, columns: &[String]) -> Vec<BoundParam> {
    let obj = record.as_object();
    columns
        .iter()
        .map(|c| {
            let v = obj.and_then(|o| o.get(c)).unwrap_or(&Value::Null);
            BoundParam::from_value(v)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn param_split_respects_2100_limit() {
        // 1 col -> 2100 rows; 3 cols -> 700; 2101 cols -> at least 1.
        assert_eq!(max_rows_per_insert(1), 2100);
        assert_eq!(max_rows_per_insert(3), 700);
        assert_eq!(max_rows_per_insert(0), 1);
        assert_eq!(max_rows_per_insert(5000), 1);
        // a 10-col table never exceeds the limit in one statement
        assert!(max_rows_per_insert(10) * 10 <= PARAM_LIMIT);
    }

    #[test]
    fn insert_sql_numbers_placeholders_across_rows() {
        let sql = build_insert_sql("[dbo].[events]", &["[a]".into(), "[b]".into()], 2);
        assert_eq!(
            sql,
            "INSERT INTO [dbo].[events] ([a], [b]) VALUES (@P1, @P2), (@P3, @P4)"
        );
    }

    #[test]
    fn resolve_columns_fixes_from_first_record() {
        let insertable = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let records = vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})];
        let cols = resolve_insert_columns(&insertable, &records, OnUnknownField::Warn).unwrap();
        assert_eq!(cols, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn resolve_columns_errors_on_unknown_when_configured() {
        let insertable = vec!["id".to_string()];
        let records = vec![json!({"id": 1, "extra": "x"})];
        assert!(resolve_insert_columns(&insertable, &records, OnUnknownField::Error).is_err());
        // Warn/Drop tolerate it.
        assert!(resolve_insert_columns(&insertable, &records, OnUnknownField::Drop).is_ok());
    }

    #[test]
    fn auto_row_params_binds_null_for_missing_keys() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let params = auto_row_params(&json!({"id": 7}), &columns);
        assert_eq!(params.len(), 2);
        assert!(matches!(params[0], BoundParam::I64(7)));
        assert!(matches!(params[1], BoundParam::Null(None)));
    }

    #[test]
    fn bound_param_classifies_json() {
        assert!(matches!(BoundParam::from_value(&json!("s")), BoundParam::Str(_)));
        assert!(matches!(BoundParam::from_value(&json!(7)), BoundParam::I64(7)));
        assert!(matches!(BoundParam::from_value(&json!(1.5)), BoundParam::F64(_)));
        assert!(matches!(BoundParam::from_value(&json!(true)), BoundParam::Bool(true)));
        assert!(matches!(BoundParam::from_value(&Value::Null), BoundParam::Null(None)));
        // nested -> serialized string
        assert!(matches!(BoundParam::from_value(&json!({"k":1})), BoundParam::Str(_)));
    }
}
