//! Config types for the SQL transform. No I/O or DuckDB here.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Configuration for the `sql` transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqlTransformConfig {
    /// The SQL statement. The page's records are the relation `batch`. Must
    /// produce a result set; each result row becomes one output record.
    pub query: String,
    /// Reference relations loaded once at compile time and joinable by name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<RelationSpec>,
    /// Optional DuckDB `memory_limit` pragma (e.g. "1GB"). Default: DuckDB's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<String>,
    /// Optional DuckDB `threads` pragma. Default: DuckDB's own. Set to 1–2 for
    /// high-fan-out matrices to avoid CPU over-subscription across rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threads: Option<usize>,
}

/// A reference relation registered before the first page.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RelationSpec {
    /// Relation name as referenced in the query. Must be a safe SQL identifier
    /// and must not be `batch` (reserved for the page).
    pub name: String,
    /// Where the relation's data comes from.
    pub source: RelationSource,
    /// Re-stat the file's mtime before each page; rebuild + atomic swap if it
    /// changed. Default false. Ignored for `values`.
    #[serde(default)]
    pub reload_on_change: bool,
}

// serde `default = "..."` needs a function, not a literal.
fn default_true() -> bool {
    true
}

/// The data source for a reference relation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RelationSource {
    /// Delimited file loaded via DuckDB `read_csv_auto`.
    Csv {
        /// Filesystem path to the CSV file (absolute, or relative to the working directory).
        path: String,
        /// Whether the first row is a header row. Default: `true`.
        #[serde(default = "default_true")]
        has_header: bool,
    },
    /// Newline-delimited JSON loaded via DuckDB `read_json_auto`.
    Jsonl {
        /// Filesystem path to the JSONL file (absolute, or relative to the working directory).
        path: String,
    },
    /// Inline rows materialized into a table.
    Values {
        /// Column names, in declaration order.
        columns: Vec<String>,
        /// Rows of cell values; each inner row must have the same length as `columns`.
        rows: Vec<Vec<Value>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trips_and_schema_builds() {
        let cfg: SqlTransformConfig = serde_json::from_value(serde_json::json!({
            "query": "SELECT * FROM batch",
            "relations": [
                {"name": "countries",
                 "source": {"type": "csv", "path": "c.csv", "has_header": true}}
            ]
        }))
        .unwrap();
        assert_eq!(cfg.relations.len(), 1);
        assert!(matches!(
            cfg.relations[0].source,
            RelationSource::Csv { .. }
        ));
        // schema_for! must succeed (used by `faucet schema transform sql`).
        let schema = schemars::schema_for!(SqlTransformConfig);
        let json = serde_json::to_value(&schema).unwrap();
        assert!(
            json.get("properties")
                .and_then(|p| p.get("query"))
                .is_some()
        );
    }
}
