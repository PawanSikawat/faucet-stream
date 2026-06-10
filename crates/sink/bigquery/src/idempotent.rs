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

/// SQL string literal for a path/value: single-quoted with `\` and `'` escaped
/// (BigQuery accepts backslash escapes in quoted strings).
fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Backtick-quoted identifier. BigQuery identifiers never contain a backtick
/// (the schema came from BigQuery), so a stray one is stripped defensively.
#[allow(dead_code)]
fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', ""))
}

/// A single JSONPath member segment. Safe BigQuery identifiers use the `.name`
/// form; anything else is bracket-quoted (`['weird.name']`).
fn json_path_segment(name: &str) -> String {
    let safe = match name.chars().next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if safe {
        format!(".{name}")
    } else {
        let esc = name.replace('\\', "\\\\").replace('\'', "\\'");
        format!("['{esc}']")
    }
}

/// Wrap a STRING-typed SQL expression `var` into the column's target type.
/// `Struct` is handled by [`column_expr`], never here.
fn wrap_scalar(ty: &BqType, var: &str) -> String {
    match ty {
        BqType::String => var.to_string(),
        BqType::Bytes => format!("FROM_BASE64({var})"),
        BqType::Geography => format!("ST_GEOGFROMTEXT({var})"),
        BqType::Json => format!("PARSE_JSON({var})"),
        BqType::Struct => unreachable!("struct is handled by column_expr"),
        other => format!("CAST({var} AS {})", other.sql_keyword()),
    }
}

/// Build the `field AS name, …` list for a STRUCT, recursing into each child.
fn struct_field_list(fields: &[FieldSpec], json_var: &str, base_path: &str, depth: usize) -> String {
    fields
        .iter()
        .map(|f| {
            let child_path = format!("{base_path}{}", json_path_segment(&f.name));
            let expr = column_expr(f, json_var, &child_path, depth);
            format!("{expr} AS {}", quote_ident(&f.name))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Generate the SQL extraction expression for one column.
///
/// `json_var` is the SQL expression naming the current JSON value (the row alias
/// `r` at the top level, or an `UNNEST` element alias deeper down). `path` is
/// the JSONPath into `json_var` for this field. `depth` keeps nested `UNNEST`
/// aliases unique (`e{depth}` for struct elements, `x{depth}` for scalars).
fn column_expr(field: &FieldSpec, json_var: &str, path: &str, depth: usize) -> String {
    if field.repeated {
        if field.ty == BqType::Struct {
            let elem = format!("e{depth}");
            let fields = struct_field_list(&field.fields, &elem, "$", depth + 1);
            format!(
                "ARRAY(SELECT AS STRUCT {fields} FROM UNNEST(JSON_QUERY_ARRAY({json_var}, {p})) AS {elem})",
                p = sql_str(path)
            )
        } else {
            let x = format!("x{depth}");
            let src = if field.ty == BqType::Json {
                format!("JSON_QUERY_ARRAY({json_var}, {p})", p = sql_str(path))
            } else {
                format!("JSON_VALUE_ARRAY({json_var}, {p})", p = sql_str(path))
            };
            let elem = wrap_scalar(&field.ty, &x);
            format!("ARRAY(SELECT {elem} FROM UNNEST({src}) AS {x})")
        }
    } else if field.ty == BqType::Struct {
        let fields = struct_field_list(&field.fields, json_var, path, depth + 1);
        format!("STRUCT({fields})")
    } else {
        let raw = if field.ty == BqType::Json {
            format!("JSON_QUERY({json_var}, {p})", p = sql_str(path))
        } else {
            format!("JSON_VALUE({json_var}, {p})", p = sql_str(path))
        };
        wrap_scalar(&field.ty, &raw)
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

    fn repeated(name: &str, ty: BqType) -> FieldSpec {
        FieldSpec { name: name.into(), ty, repeated: true, fields: vec![] }
    }
    fn record(name: &str, repeated: bool, fields: Vec<FieldSpec>) -> FieldSpec {
        FieldSpec { name: name.into(), ty: BqType::Struct, repeated, fields }
    }

    #[test]
    fn scalar_exprs_per_type() {
        assert_eq!(column_expr(&scalar("s", BqType::String), "r", "$.s", 0),
            "JSON_VALUE(r, '$.s')");
        assert_eq!(column_expr(&scalar("n", BqType::Int64), "r", "$.n", 0),
            "CAST(JSON_VALUE(r, '$.n') AS INT64)");
        assert_eq!(column_expr(&scalar("f", BqType::Float64), "r", "$.f", 0),
            "CAST(JSON_VALUE(r, '$.f') AS FLOAT64)");
        assert_eq!(column_expr(&scalar("b", BqType::Bool), "r", "$.b", 0),
            "CAST(JSON_VALUE(r, '$.b') AS BOOL)");
        assert_eq!(column_expr(&scalar("ts", BqType::Timestamp), "r", "$.ts", 0),
            "CAST(JSON_VALUE(r, '$.ts') AS TIMESTAMP)");
        assert_eq!(column_expr(&scalar("by", BqType::Bytes), "r", "$.by", 0),
            "FROM_BASE64(JSON_VALUE(r, '$.by'))");
        assert_eq!(column_expr(&scalar("g", BqType::Geography), "r", "$.g", 0),
            "ST_GEOGFROMTEXT(JSON_VALUE(r, '$.g'))");
        assert_eq!(column_expr(&scalar("j", BqType::Json), "r", "$.j", 0),
            "PARSE_JSON(JSON_QUERY(r, '$.j'))");
    }

    #[test]
    fn repeated_scalar_exprs() {
        assert_eq!(column_expr(&repeated("xs", BqType::String), "r", "$.xs", 0),
            "ARRAY(SELECT x0 FROM UNNEST(JSON_VALUE_ARRAY(r, '$.xs')) AS x0)");
        assert_eq!(column_expr(&repeated("ns", BqType::Int64), "r", "$.ns", 0),
            "ARRAY(SELECT CAST(x0 AS INT64) FROM UNNEST(JSON_VALUE_ARRAY(r, '$.ns')) AS x0)");
        assert_eq!(column_expr(&repeated("js", BqType::Json), "r", "$.js", 0),
            "ARRAY(SELECT PARSE_JSON(x0) FROM UNNEST(JSON_QUERY_ARRAY(r, '$.js')) AS x0)");
    }

    #[test]
    fn nested_struct_expr() {
        let f = record("addr", false, vec![
            scalar("city", BqType::String),
            scalar("zip", BqType::Int64),
        ]);
        assert_eq!(column_expr(&f, "r", "$.addr", 0),
            "STRUCT(JSON_VALUE(r, '$.addr.city') AS `city`, CAST(JSON_VALUE(r, '$.addr.zip') AS INT64) AS `zip`)");
    }

    #[test]
    fn repeated_record_expr_uses_unnest_element() {
        let f = record("items", true, vec![
            scalar("sku", BqType::String),
            scalar("qty", BqType::Int64),
        ]);
        assert_eq!(column_expr(&f, "r", "$.items", 0),
            "ARRAY(SELECT AS STRUCT JSON_VALUE(e0, '$.sku') AS `sku`, CAST(JSON_VALUE(e0, '$.qty') AS INT64) AS `qty` FROM UNNEST(JSON_QUERY_ARRAY(r, '$.items')) AS e0)");
    }

    #[test]
    fn nested_repeated_record_aliases_are_unique() {
        // ARRAY<STRUCT<tags ARRAY<STRING>>> nested inside ARRAY<STRUCT<...>>
        let inner = repeated("tags", BqType::String);
        let f = record("groups", true, vec![inner]);
        let sql = column_expr(&f, "r", "$.groups", 0);
        // Outer element alias e0; the inner repeated scalar uses x1 (depth+1) —
        // distinct from any outer alias.
        assert_eq!(sql,
            "ARRAY(SELECT AS STRUCT ARRAY(SELECT x1 FROM UNNEST(JSON_VALUE_ARRAY(e0, '$.tags')) AS x1) AS `tags` FROM UNNEST(JSON_QUERY_ARRAY(r, '$.groups')) AS e0)");
    }

    #[test]
    fn unsafe_member_name_uses_bracket_path() {
        // A name with a dot would be ambiguous in `$.a.b`; bracket-quote it.
        assert_eq!(json_path_segment("a.b"), "['a.b']");
        assert_eq!(json_path_segment("ok_name"), ".ok_name");
        assert_eq!(json_path_segment("_lead"), "._lead");
        assert_eq!(json_path_segment("1bad"), "['1bad']");
    }
}
