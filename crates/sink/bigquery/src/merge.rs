//! Schema-driven SQL generation for the BigQuery upsert/delete write path (#224).
//!
//! All functions are **pure** (no I/O): given the target table's schema as
//! [`crate::idempotent::FieldSpec`]s and the `key` columns, they generate the
//! `MERGE` / `DELETE` / transaction SQL that merges a page in place via
//! `MERGE … USING (SELECT … FROM UNNEST(JSON_QUERY_ARRAY(@payload)))`. No
//! staging table. See `docs/superpowers/specs/2026-06-13-bigquery-upsert-design.md`.

use crate::idempotent::{
    FieldSpec, build_merge_token, column_expr, json_path_segment, quote_ident, table_ref,
};
use faucet_core::FaucetError;

/// Find the `FieldSpec` for a key column by name.
fn key_field<'a>(columns: &'a [FieldSpec], name: &str) -> Option<&'a FieldSpec> {
    columns.iter().find(|f| f.name == name)
}

/// `SELECT <typed expr> AS `col`, … FROM UNNEST(JSON_QUERY_ARRAY(<payload_param>)) AS r`.
///
/// Each column is extracted with the same typed `column_expr` the exactly-once
/// `INSERT … SELECT` uses, then aliased to its name so the MERGE `ON`/`SET`/
/// `INSERT` clauses can reference `S.`col``.
pub(crate) fn build_source_select(columns: &[FieldSpec], payload_param: &str) -> String {
    let exprs = columns
        .iter()
        .map(|f| {
            let path = format!("${}", json_path_segment(&f.name));
            format!("{} AS {}", column_expr(f, "r", &path, 0), quote_ident(&f.name))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {exprs} FROM UNNEST(JSON_QUERY_ARRAY({payload_param})) AS r")
}

/// The in-place upsert `MERGE` over the `@payload` page.
pub(crate) fn build_merge_upsert(
    columns: &[FieldSpec],
    key: &[String],
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let src = build_source_select(columns, "@payload");
    let on = key
        .iter()
        .map(|k| format!("T.{q} = S.{q}", q = quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let insert_cols = columns
        .iter()
        .map(|f| quote_ident(&f.name))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = columns
        .iter()
        .map(|f| format!("S.{}", quote_ident(&f.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let non_key_sets = columns
        .iter()
        .filter(|f| !key.iter().any(|k| k == &f.name))
        .map(|f| format!("{q} = S.{q}", q = quote_ident(&f.name)))
        .collect::<Vec<_>>();
    let matched = if non_key_sets.is_empty() {
        String::new()
    } else {
        format!("WHEN MATCHED THEN UPDATE SET {} ", non_key_sets.join(", "))
    };
    format!(
        "MERGE INTO {t} T USING ({src}) S ON {on} {matched}WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
        t = table_ref(project, dataset, table),
    )
}

/// Keyed `DELETE` via a semi-join over the `@deletes` payload (an array of
/// `{key_col: value, …}` objects). Key columns are typed via `column_expr` so
/// an INT64 key compares as INT64, not STRING.
pub(crate) fn build_delete_by_keys(
    columns: &[FieldSpec],
    key: &[String],
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let preds = key
        .iter()
        .map(|k| {
            let path = format!("${}", json_path_segment(k));
            // Callers run `validate_keys_present` before building any SQL, so
            // every key is guaranteed to be a real column here.
            let fs = key_field(columns, k)
                .unwrap_or_else(|| unreachable!("validate_keys_present runs before build_delete_by_keys"));
            let rhs = column_expr(fs, "d", &path, 0);
            format!("T.{} = {}", quote_ident(k), rhs)
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "DELETE FROM {t} T WHERE EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(@deletes)) AS d WHERE {preds})",
        t = table_ref(project, dataset, table),
    )
}

fn wrap_transaction(stmts: &[String]) -> String {
    format!("BEGIN TRANSACTION;\n{};\nCOMMIT TRANSACTION;", stmts.join(";\n"))
}

/// `BEGIN; [MERGE upsert]; [DELETE]; COMMIT;` — the at-least-once upsert/delete path.
pub(crate) fn build_upsert_transaction_sql(
    columns: &[FieldSpec],
    key: &[String],
    has_upserts: bool,
    has_deletes: bool,
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let mut stmts = Vec::new();
    if has_upserts {
        stmts.push(build_merge_upsert(columns, key, project, dataset, table));
    }
    if has_deletes {
        stmts.push(build_delete_by_keys(columns, key, project, dataset, table));
    }
    wrap_transaction(&stmts)
}

/// As [`build_upsert_transaction_sql`] but with the watermark `MERGE` appended
/// inside the same transaction — the exactly-once + upsert composition.
pub(crate) fn build_upsert_idempotent_sql(
    columns: &[FieldSpec],
    key: &[String],
    has_upserts: bool,
    has_deletes: bool,
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let mut stmts = Vec::new();
    if has_upserts {
        stmts.push(build_merge_upsert(columns, key, project, dataset, table));
    }
    if has_deletes {
        stmts.push(build_delete_by_keys(columns, key, project, dataset, table));
    }
    stmts.push(build_merge_token(project, dataset));
    wrap_transaction(&stmts)
}

/// Validate that every `key` column exists in the target table schema. Returns
/// a `FaucetError::Sink` naming the missing column otherwise.
pub(crate) fn validate_keys_present(
    columns: &[FieldSpec],
    key: &[String],
) -> Result<(), FaucetError> {
    for k in key {
        if !columns.iter().any(|f| &f.name == k) {
            return Err(FaucetError::Sink(format!(
                "bigquery upsert: key column '{k}' is not a column of the target table"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idempotent::{BqType, FieldSpec};

    fn scalar(name: &str, ty: BqType) -> FieldSpec {
        FieldSpec { name: name.into(), ty, repeated: false, fields: vec![] }
    }

    fn id_name_cols() -> Vec<FieldSpec> {
        vec![scalar("id", BqType::Int64), scalar("name", BqType::String)]
    }

    #[test]
    fn source_select_aliases_each_typed_column() {
        let sql = build_source_select(&id_name_cols(), "@payload");
        assert_eq!(
            sql,
            "SELECT CAST(JSON_VALUE(r, '$.id') AS INT64) AS `id`, JSON_VALUE(r, '$.name') AS `name` FROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS r"
        );
    }

    #[test]
    fn merge_upsert_single_key() {
        let sql = build_merge_upsert(&id_name_cols(), &["id".into()], "p", "d", "t");
        assert_eq!(
            sql,
            "MERGE INTO `p.d.t` T USING (SELECT CAST(JSON_VALUE(r, '$.id') AS INT64) AS `id`, JSON_VALUE(r, '$.name') AS `name` FROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS r) S ON T.`id` = S.`id` WHEN MATCHED THEN UPDATE SET `name` = S.`name` WHEN NOT MATCHED THEN INSERT (`id`, `name`) VALUES (S.`id`, S.`name`)"
        );
    }

    #[test]
    fn merge_upsert_composite_key() {
        let cols = vec![
            scalar("tenant", BqType::String),
            scalar("id", BqType::Int64),
            scalar("name", BqType::String),
        ];
        let sql = build_merge_upsert(&cols, &["tenant".into(), "id".into()], "p", "d", "t");
        assert!(sql.contains("ON T.`tenant` = S.`tenant` AND T.`id` = S.`id`"), "got: {sql}");
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET `name` = S.`name`"), "got: {sql}");
        assert!(sql.contains("INSERT (`tenant`, `id`, `name`) VALUES (S.`tenant`, S.`id`, S.`name`)"), "got: {sql}");
    }

    #[test]
    fn merge_upsert_all_columns_are_key_omits_update() {
        // Only a key column: no non-key columns to SET, so emit INSERT-only.
        let sql = build_merge_upsert(&[scalar("id", BqType::Int64)], &["id".into()], "p", "d", "t");
        assert!(!sql.contains("WHEN MATCHED"), "got: {sql}");
        assert!(sql.contains("ON T.`id` = S.`id` WHEN NOT MATCHED THEN INSERT (`id`) VALUES (S.`id`)"), "got: {sql}");
    }

    #[test]
    fn delete_by_keys_typed_single_key() {
        let sql = build_delete_by_keys(&id_name_cols(), &["id".into()], "p", "d", "t");
        assert_eq!(
            sql,
            "DELETE FROM `p.d.t` T WHERE EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(@deletes)) AS d WHERE T.`id` = CAST(JSON_VALUE(d, '$.id') AS INT64))"
        );
    }

    #[test]
    fn delete_by_keys_composite() {
        let cols = vec![scalar("tenant", BqType::String), scalar("id", BqType::Int64)];
        let sql = build_delete_by_keys(&cols, &["tenant".into(), "id".into()], "p", "d", "t");
        assert!(
            sql.contains("WHERE T.`tenant` = JSON_VALUE(d, '$.tenant') AND T.`id` = CAST(JSON_VALUE(d, '$.id') AS INT64)"),
            "got: {sql}"
        );
    }

    #[test]
    fn transaction_upserts_and_deletes() {
        let sql = build_upsert_transaction_sql(&id_name_cols(), &["id".into()], true, true, "p", "d", "t");
        assert!(sql.starts_with("BEGIN TRANSACTION;\n"), "got: {sql}");
        assert!(sql.trim_end().ends_with("COMMIT TRANSACTION;"), "got: {sql}");
        let m = sql.find("MERGE INTO").unwrap();
        let d = sql.find("DELETE FROM").unwrap();
        let c = sql.find("COMMIT TRANSACTION").unwrap();
        assert!(m < d && d < c, "order wrong: {sql}");
        assert!(!sql.contains("_faucet_commit_token"), "no watermark in non-EO path: {sql}");
    }

    #[test]
    fn transaction_upserts_only() {
        let sql = build_upsert_transaction_sql(&id_name_cols(), &["id".into()], true, false, "p", "d", "t");
        assert!(sql.contains("MERGE INTO"), "got: {sql}");
        assert!(!sql.contains("DELETE FROM"), "got: {sql}");
    }

    #[test]
    fn idempotent_transaction_appends_watermark_merge() {
        let sql = build_upsert_idempotent_sql(&id_name_cols(), &["id".into()], true, true, "p", "d", "t");
        let m = sql.find("MERGE INTO `p.d.t`").unwrap();
        let d = sql.find("DELETE FROM").unwrap();
        let w = sql.find("MERGE `p.d._faucet_commit_token`").unwrap();
        let c = sql.find("COMMIT TRANSACTION").unwrap();
        assert!(m < d && d < w && w < c, "order wrong: {sql}");
    }

    #[test]
    fn validate_keys_present_ok_and_err() {
        assert!(validate_keys_present(&id_name_cols(), &["id".into()]).is_ok());
        let err = validate_keys_present(&id_name_cols(), &["missing".into()]).unwrap_err();
        assert!(format!("{err}").contains("missing"), "got: {err}");
    }
}
