//! Pure helpers for turning JSON records into MSSQL `INSERT` statements and
//! bound parameters. No I/O — all unit-testable.

use faucet_common_mssql::{PARAM_LIMIT, quote_ident_mssql};
use faucet_core::FaucetError;
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
            Value::Number(n) if n.is_u64() => match i64::try_from(n.as_u64().unwrap()) {
                Ok(i) => BoundParam::I64(i),
                // A u64 above i64::MAX would wrap to a negative i64; bind it as a
                // string so MSSQL coerces to NUMERIC/DECIMAL instead of corrupting
                // the value.
                Err(_) => BoundParam::Str(n.as_u64().unwrap().to_string()),
            },
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

/// `tiberius` routes parameterized statements through `sp_executesql`, which
/// consumes two of the 2100 parameters itself (the statement text and the
/// parameter-declaration string). So the usable budget for bind values is
/// `PARAM_LIMIT - 2` — sending a full 2100 bind values overflows by two.
const SP_EXECUTESQL_RESERVED: usize = 2;

/// MSSQL's table value constructor (the `VALUES (…), (…), …` clause) allows at
/// most 1000 row expressions per statement — a separate limit from the 2100
/// parameters. For narrow tables (1–2 columns) this binds before the parameter
/// budget does.
const MAX_VALUES_ROWS: usize = 1000;

/// Maximum rows per `INSERT` so the request stays within **both** of MSSQL's
/// limits: the 2100-parameter cap (minus the `sp_executesql` overhead) and the
/// 1000-row-values cap on a `VALUES` clause. Always at least 1.
pub(crate) fn max_rows_per_insert(num_cols: usize) -> usize {
    if num_cols == 0 {
        return 1;
    }
    let by_params = (PARAM_LIMIT.saturating_sub(SP_EXECUTESQL_RESERVED) / num_cols).max(1);
    by_params.min(MAX_VALUES_ROWS)
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
        let placeholders: Vec<String> = (start..start + num_cols)
            .map(|i| format!("@P{i}"))
            .collect();
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
                tracing::warn!(
                    ?unknown,
                    "auto_columns: dropping record keys with no matching column"
                );
            }
            OnUnknownField::Drop => {}
        }
    }

    // Column set = insertable columns present in ANY record (union), preserving
    // the insertable order. Using only the first record's keys would silently
    // drop a field present only in a later record of the batch (audit #146 H1);
    // a row missing a unioned column binds SQL NULL.
    let columns: Vec<String> = insertable
        .iter()
        .filter(|c| {
            records
                .iter()
                .any(|r| r.as_object().is_some_and(|o| o.contains_key(c.as_str())))
        })
        .cloned()
        .collect();
    Ok(columns)
}

/// Build a parameterized `MERGE` upsert. `table` must already be quoted; the
/// `key` / `cols` entries are bare identifiers that this function quotes via
/// [`quote_ident_mssql`].
///
/// Emits `n_rows` `VALUES` groups of `cols.len()` `@PN` params each, numbered
/// row-major so the binding order matches `n_rows` calls of
/// [`auto_row_params(record, cols)`](auto_row_params) concatenated in record
/// order. Joins on every `key` column, `UPDATE`s the non-key columns, and
/// `INSERT`s all columns. When every column is a key there is nothing to
/// update, so the `WHEN MATCHED` clause is omitted entirely. T-SQL requires a
/// terminating `;` on `MERGE`, so one is always appended.
pub(crate) fn build_merge(
    table: &str,
    key: &[String],
    cols: &[String],
    n_rows: usize,
) -> Result<String, FaucetError> {
    let q = |s: &str| quote_ident_mssql(s);
    let quoted_cols: Vec<String> = cols.iter().map(|c| q(c)).collect::<Result<_, _>>()?;
    let quoted_keys: Vec<String> = key.iter().map(|k| q(k)).collect::<Result<_, _>>()?;
    let col_list = quoted_cols.join(", ");

    let mut ph = 1usize;
    let groups: Vec<String> = (0..n_rows)
        .map(|_| {
            let g = cols
                .iter()
                .map(|_| {
                    let p = format!("@P{ph}");
                    ph += 1;
                    p
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("({g})")
        })
        .collect();

    let on = quoted_keys
        .iter()
        .map(|qk| format!("tgt.{qk} = src.{qk}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    // Non-key columns get UPDATE assignments; compare on the *bare* names so a
    // column equal to a key column (after quoting) is correctly excluded.
    let update_set = cols
        .iter()
        .zip(&quoted_cols)
        .filter(|(c, _)| !key.iter().any(|k| k == *c))
        .map(|(_, qc)| format!("tgt.{qc} = src.{qc}"))
        .collect::<Vec<_>>()
        .join(", ");

    let insert_vals = quoted_cols
        .iter()
        .map(|qc| format!("src.{qc}"))
        .collect::<Vec<_>>()
        .join(", ");

    let matched = if update_set.is_empty() {
        String::new()
    } else {
        format!(" WHEN MATCHED THEN UPDATE SET {update_set}")
    };

    Ok(format!(
        "MERGE {table} AS tgt USING (VALUES {}) AS src ({col_list}) ON {on}{matched} \
         WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_vals});",
        groups.join(", ")
    ))
}

/// Build a parameterized `MERGE … WHEN MATCHED THEN DELETE` for composite-key
/// deletes. `table` must already be quoted; `key` entries are bare identifiers
/// quoted here.
///
/// T-SQL has no row-constructor `IN ((a,b), …)`, so a composite-key delete is
/// expressed as a `MERGE` whose source is the `VALUES` list of key tuples.
/// Params are numbered row-major over the key columns, matching how the caller
/// binds each [`KeyTuple`](faucet_core::KeyTuple)'s values in `key` order.
pub(crate) fn build_merge_delete(
    table: &str,
    key: &[String],
    n_rows: usize,
) -> Result<String, FaucetError> {
    let quoted_keys: Vec<String> = key
        .iter()
        .map(|k| quote_ident_mssql(k))
        .collect::<Result<_, _>>()?;
    let key_list = quoted_keys.join(", ");

    let mut ph = 1usize;
    let groups: Vec<String> = (0..n_rows)
        .map(|_| {
            let g = key
                .iter()
                .map(|_| {
                    let p = format!("@P{ph}");
                    ph += 1;
                    p
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("({g})")
        })
        .collect();

    let on = quoted_keys
        .iter()
        .map(|qk| format!("tgt.{qk} = src.{qk}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    Ok(format!(
        "MERGE {table} AS tgt USING (VALUES {}) AS src ({key_list}) ON {on} \
         WHEN MATCHED THEN DELETE;",
        groups.join(", ")
    ))
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
    fn param_split_respects_both_mssql_limits() {
        // Narrow tables are capped by the 1000-row-values limit; wider tables by
        // the parameter budget (PARAM_LIMIT - 2 sp_executesql params).
        assert_eq!(max_rows_per_insert(1), 1000); // 2098 params allowed, but row cap is 1000
        assert_eq!(max_rows_per_insert(2), 1000); // 1049 params allowed, row cap 1000
        assert_eq!(max_rows_per_insert(3), 699); // 2098/3 = 699 < 1000
        assert_eq!(max_rows_per_insert(0), 1);
        assert_eq!(max_rows_per_insert(5000), 1);
        // For any column count, a single INSERT must satisfy BOTH limits:
        // (rows*cols + 2 sp params) <= 2100, and rows <= 1000.
        for cols in 1..=64 {
            let rows = max_rows_per_insert(cols);
            assert!(
                rows <= MAX_VALUES_ROWS,
                "cols={cols} exceeds 1000 row values"
            );
            assert!(
                rows * cols + SP_EXECUTESQL_RESERVED <= PARAM_LIMIT,
                "cols={cols} exceeds the 2100 parameter limit"
            );
        }
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
    fn resolve_columns_unions_keys_across_records() {
        // H1 (audit #146): the column set is the UNION across all records, not
        // just the first record's keys — a field present only in a LATER record
        // must not be dropped. Order follows `insertable` (declared column order).
        let insertable = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        // The first record is the sparsest; `name`/`email` appear only later.
        let records = vec![
            json!({ "id": 1 }),
            json!({ "id": 2, "name": "b", "email": "x@y" }),
        ];
        let cols = resolve_insert_columns(&insertable, &records, OnUnknownField::Warn).unwrap();
        assert_eq!(
            cols,
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
            "later-record-only columns (name, email) must be included"
        );
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
        assert!(matches!(
            BoundParam::from_value(&json!("s")),
            BoundParam::Str(_)
        ));
        assert!(matches!(
            BoundParam::from_value(&json!(7)),
            BoundParam::I64(7)
        ));
        assert!(matches!(
            BoundParam::from_value(&json!(1.5)),
            BoundParam::F64(_)
        ));
        assert!(matches!(
            BoundParam::from_value(&json!(true)),
            BoundParam::Bool(true)
        ));
        assert!(matches!(
            BoundParam::from_value(&Value::Null),
            BoundParam::Null(None)
        ));
        // nested -> serialized string
        assert!(matches!(
            BoundParam::from_value(&json!({"k":1})),
            BoundParam::Str(_)
        ));
    }

    #[test]
    fn mssql_merge_statement_shape() {
        let sql = build_merge(
            "[dbo].[t]",
            &["id".to_string()],
            &["id".to_string(), "name".to_string()],
            1,
        )
        .unwrap();
        assert!(sql.contains("MERGE [dbo].[t] AS tgt"), "{sql}");
        assert!(
            sql.contains("USING (VALUES (@P1, @P2)) AS src ([id], [name])"),
            "{sql}"
        );
        assert!(sql.contains("ON tgt.[id] = src.[id]"), "{sql}");
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET tgt.[name] = src.[name]"),
            "{sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (src.[id], src.[name])"),
            "{sql}"
        );
        assert!(
            sql.trim_end().ends_with(';'),
            "MERGE needs a terminating semicolon: {sql}"
        );
    }

    #[test]
    fn mssql_merge_all_keys_has_no_update_clause() {
        // When every column is a key, there is nothing to UPDATE — emit no WHEN
        // MATCHED clause.
        let sql = build_merge("[t]", &["id".to_string()], &["id".to_string()], 2).unwrap();
        assert!(!sql.contains("WHEN MATCHED"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
        assert!(sql.contains("(@P1), (@P2)"), "two single-col rows: {sql}");
    }

    #[test]
    fn mssql_merge_numbers_params_row_major() {
        // Two rows of two columns → @P1..@P4 in row-major order, matching how
        // `auto_row_params` is concatenated per row.
        let sql = build_merge(
            "[t]",
            &["id".to_string()],
            &["id".to_string(), "name".to_string()],
            2,
        )
        .unwrap();
        assert!(
            sql.contains("VALUES (@P1, @P2), (@P3, @P4)"),
            "row-major param numbering: {sql}"
        );
    }

    #[test]
    fn mssql_merge_composite_key_joins_on_all_key_cols() {
        let sql = build_merge(
            "[t]",
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "b".to_string(), "v".to_string()],
            1,
        )
        .unwrap();
        assert!(
            sql.contains("ON tgt.[a] = src.[a] AND tgt.[b] = src.[b]"),
            "{sql}"
        );
        // Only the non-key column `v` is updated.
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET tgt.[v] = src.[v]"),
            "{sql}"
        );
    }

    #[test]
    fn mssql_merge_delete_statement_shape() {
        let sql = build_merge_delete("[dbo].[t]", &["id".to_string()], 2).unwrap();
        assert!(sql.contains("MERGE [dbo].[t] AS tgt"), "{sql}");
        assert!(
            sql.contains("USING (VALUES (@P1), (@P2)) AS src ([id])"),
            "{sql}"
        );
        assert!(sql.contains("ON tgt.[id] = src.[id]"), "{sql}");
        assert!(sql.contains("WHEN MATCHED THEN DELETE"), "{sql}");
        assert!(sql.trim_end().ends_with(';'), "{sql}");
    }

    #[test]
    fn mssql_merge_delete_composite_key() {
        let sql = build_merge_delete("[t]", &["a".to_string(), "b".to_string()], 1).unwrap();
        assert!(
            sql.contains("USING (VALUES (@P1, @P2)) AS src ([a], [b])"),
            "{sql}"
        );
        assert!(
            sql.contains("ON tgt.[a] = src.[a] AND tgt.[b] = src.[b]"),
            "{sql}"
        );
        assert!(sql.contains("WHEN MATCHED THEN DELETE"), "{sql}");
    }

    #[test]
    fn u64_above_i64_max_binds_as_string_not_wrapped() {
        // A u64 that fits in i64 still binds as I64.
        match BoundParam::from_value(&json!(i64::MAX as u64)) {
            BoundParam::I64(v) => assert_eq!(v, i64::MAX),
            _ => panic!("expected I64 for an in-range u64"),
        }
        // A u64 above i64::MAX must NOT wrap to a negative I64 — bind as a string
        // so MSSQL coerces to NUMERIC/DECIMAL rather than corrupting the value.
        match BoundParam::from_value(&json!(u64::MAX)) {
            BoundParam::Str(s) => assert_eq!(s, "18446744073709551615"),
            BoundParam::I64(v) => panic!("u64::MAX wrapped to negative I64({v})"),
            _ => panic!("expected Str for u64 > i64::MAX"),
        }
    }
}
