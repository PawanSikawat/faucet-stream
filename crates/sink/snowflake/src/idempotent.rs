//! Pure SQL generation for the Snowflake exactly-once (effectively-once)
//! write path (#291).
//!
//! All functions here are **pure** (no I/O): they generate the SQL text for
//! the atomic page-INSERT + watermark-`MERGE` multi-statement transaction
//! that makes a page's rows and its commit token land atomically, mirroring
//! the BigQuery reference implementation
//! (`crates/sink/bigquery/src/idempotent.rs`).
//!
//! Snowflake specifics:
//! - The SQL REST API executes a multi-statement request only when the body
//!   carries `parameters.MULTI_STATEMENT_COUNT` matching the number of
//!   `;`-separated statements — the counts are exported here as
//!   [`TRANSACTION_STATEMENT_COUNT`] / [`COMMIT_ONLY_STATEMENT_COUNT`] so the
//!   sink and the SQL text can never drift apart.
//! - Bindings are positional **across the whole multi-statement text**: with
//!   an INSERT the payload is `?1` and the MERGE consumes `?2`/`?3`; in the
//!   commit-only (empty page) form the MERGE consumes `?1`/`?2`.
//! - DDL auto-commits in Snowflake, so the watermark table's
//!   `CREATE TABLE IF NOT EXISTS` must run as its **own** request, never
//!   inside the transaction — [`build_create_commit_table`] is submitted
//!   separately (once per sink instance) by the sink.

use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL,
};
use faucet_core::util::quote_ident;

/// Timestamp column recording when the watermark row was last advanced.
const UPDATED_AT_COL: &str = "updated_at";

/// Number of statements in the full page transaction:
/// `BEGIN; INSERT; MERGE; COMMIT;` — the value the sink must send as
/// `parameters.MULTI_STATEMENT_COUNT` alongside
/// [`build_transaction_statement`]'s output.
pub const TRANSACTION_STATEMENT_COUNT: usize = 4;

/// Number of statements in the empty-page (commit-only) transaction:
/// `BEGIN; MERGE; COMMIT;` — the value the sink must send as
/// `parameters.MULTI_STATEMENT_COUNT` alongside
/// [`build_commit_only_statement`]'s output.
pub const COMMIT_ONLY_STATEMENT_COUNT: usize = 3;

/// Fully-qualified, quoted reference to the shared commit-token watermark
/// table in the target database/schema
/// (`"db"."schema"."_faucet_commit_token"`).
fn commit_table_ref(database: &str, schema: &str) -> String {
    format!(
        "{}.{}.{}",
        quote_ident(database),
        quote_ident(schema),
        quote_ident(COMMIT_TOKEN_TABLE)
    )
}

/// `CREATE TABLE IF NOT EXISTS` for the watermark table.
///
/// Must run as its own SQL REST API request (DDL auto-commits in Snowflake,
/// so it can never be part of the data transaction). `TIMESTAMP_NTZ` keeps
/// the audit column timezone-free, matching Snowflake's default timestamp
/// flavor.
pub fn build_create_commit_table(database: &str, schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {t} ({scope} STRING NOT NULL PRIMARY KEY, {token} STRING NOT NULL, {updated} TIMESTAMP_NTZ)",
        t = commit_table_ref(database, schema),
        scope = quote_ident(COMMIT_TOKEN_SCOPE_COL),
        token = quote_ident(COMMIT_TOKEN_TOKEN_COL),
        updated = quote_ident(UPDATED_AT_COL),
    )
}

/// The watermark `MERGE`: upsert exactly one `(scope, token)` row per scope.
/// Consumes two positional `?` bindings — scope, then token.
fn build_merge_token(database: &str, schema: &str) -> String {
    let scope = quote_ident(COMMIT_TOKEN_SCOPE_COL);
    let token = quote_ident(COMMIT_TOKEN_TOKEN_COL);
    let updated = quote_ident(UPDATED_AT_COL);
    format!(
        "MERGE INTO {t} t USING (SELECT ? AS {scope}, ? AS {token}) s ON t.{scope} = s.{scope} \
WHEN MATCHED THEN UPDATE SET t.{token} = s.{token}, t.{updated} = CURRENT_TIMESTAMP() \
WHEN NOT MATCHED THEN INSERT ({scope}, {token}, {updated}) VALUES (s.{scope}, s.{token}, CURRENT_TIMESTAMP())",
        t = commit_table_ref(database, schema),
    )
}

/// The full atomic multi-statement transaction for a non-empty page:
/// `BEGIN; {insert_sql}; {watermark MERGE}; COMMIT;` —
/// [`TRANSACTION_STATEMENT_COUNT`] statements.
///
/// `insert_sql` is the sink's regular parameterized page INSERT
/// (`INSERT … SELECT … FROM TABLE(FLATTEN(input => PARSE_JSON(?)))`), so the
/// positional bindings for the combined text are: `1` = the JSON page
/// payload, `2` = scope, `3` = token.
pub fn build_transaction_statement(insert_sql: &str, database: &str, schema: &str) -> String {
    format!(
        "BEGIN;\n{insert_sql};\n{merge};\nCOMMIT;",
        merge = build_merge_token(database, schema),
    )
}

/// The commit-only transaction for an **empty** page:
/// `BEGIN; {watermark MERGE}; COMMIT;` — [`COMMIT_ONLY_STATEMENT_COUNT`]
/// statements. The token must still advance so a resume does not replay the
/// (empty) page; positional bindings: `1` = scope, `2` = token.
pub fn build_commit_only_statement(database: &str, schema: &str) -> String {
    format!(
        "BEGIN;\n{merge};\nCOMMIT;",
        merge = build_merge_token(database, schema),
    )
}

/// Parameterized read of the last committed token for a scope (binding `1`).
///
/// `LIMIT 1` has no `ORDER BY`: the `MERGE` above maintains exactly one
/// watermark row per scope, so there is never more than one row to choose
/// from.
pub fn build_select_token(database: &str, schema: &str) -> String {
    format!(
        "SELECT {token} FROM {t} WHERE {scope} = ? LIMIT 1",
        token = quote_ident(COMMIT_TOKEN_TOKEN_COL),
        t = commit_table_ref(database, schema),
        scope = quote_ident(COMMIT_TOKEN_SCOPE_COL),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_commit_table_sql_is_exact() {
        assert_eq!(
            build_create_commit_table("MY_DB", "PUBLIC"),
            "CREATE TABLE IF NOT EXISTS \"MY_DB\".\"PUBLIC\".\"_faucet_commit_token\" \
(\"scope\" STRING NOT NULL PRIMARY KEY, \"token\" STRING NOT NULL, \"updated_at\" TIMESTAMP_NTZ)"
        );
    }

    #[test]
    fn create_commit_table_escapes_identifiers() {
        // Database/schema names are config-controlled but still must be
        // quote-safe: an embedded double quote is doubled, never a breakout.
        let sql = build_create_commit_table("we\"ird", "sch\"ema");
        assert!(sql.contains("\"we\"\"ird\".\"sch\"\"ema\""), "sql: {sql}");
    }

    #[test]
    fn select_token_sql_is_exact() {
        assert_eq!(
            build_select_token("MY_DB", "PUBLIC"),
            "SELECT \"token\" FROM \"MY_DB\".\"PUBLIC\".\"_faucet_commit_token\" \
WHERE \"scope\" = ? LIMIT 1"
        );
    }

    #[test]
    fn merge_token_sql_is_exact() {
        assert_eq!(
            build_merge_token("db", "sch"),
            "MERGE INTO \"db\".\"sch\".\"_faucet_commit_token\" t \
USING (SELECT ? AS \"scope\", ? AS \"token\") s ON t.\"scope\" = s.\"scope\" \
WHEN MATCHED THEN UPDATE SET t.\"token\" = s.\"token\", t.\"updated_at\" = CURRENT_TIMESTAMP() \
WHEN NOT MATCHED THEN INSERT (\"scope\", \"token\", \"updated_at\") \
VALUES (s.\"scope\", s.\"token\", CURRENT_TIMESTAMP())"
        );
    }

    #[test]
    fn transaction_statement_wraps_insert_and_merge_in_order() {
        let insert = "INSERT INTO \"db\".\"sch\".\"tbl\" (\"id\") \
SELECT value:\"id\"::string FROM TABLE(FLATTEN(input => PARSE_JSON(?)))";
        let sql = build_transaction_statement(insert, "db", "sch");

        assert!(sql.starts_with("BEGIN;\n"), "sql: {sql}");
        assert!(sql.ends_with("\nCOMMIT;"), "sql: {sql}");
        let i = sql.find("INSERT INTO").expect("insert present");
        let m = sql.find("MERGE INTO").expect("merge present");
        let c = sql.find("COMMIT;").expect("commit present");
        assert!(i < m && m < c, "statement order wrong: {sql}");

        // Exactly TRANSACTION_STATEMENT_COUNT statements (one trailing `;` each).
        assert_eq!(sql.matches(';').count(), TRANSACTION_STATEMENT_COUNT);
        // Bindings are positional across the whole text: payload + scope + token.
        assert_eq!(sql.matches('?').count(), 3, "sql: {sql}");
    }

    #[test]
    fn commit_only_statement_has_no_insert_and_three_statements() {
        let sql = build_commit_only_statement("db", "sch");
        assert!(sql.starts_with("BEGIN;\n"), "sql: {sql}");
        assert!(sql.ends_with("\nCOMMIT;"), "sql: {sql}");
        assert!(!sql.contains("INSERT INTO \"db\".\"sch\".\"tbl\""));
        assert!(sql.contains("MERGE INTO"), "sql: {sql}");
        assert_eq!(sql.matches(';').count(), COMMIT_ONLY_STATEMENT_COUNT);
        // Only the MERGE's scope + token bindings.
        assert_eq!(sql.matches('?').count(), 2, "sql: {sql}");
    }

    #[test]
    fn statement_counts_match_the_generated_text() {
        // The exported counts are what the sink sends as
        // MULTI_STATEMENT_COUNT — they must equal the number of statements
        // in the generated SQL or Snowflake rejects the request.
        assert_eq!(TRANSACTION_STATEMENT_COUNT, 4);
        assert_eq!(COMMIT_ONLY_STATEMENT_COUNT, 3);
    }
}
