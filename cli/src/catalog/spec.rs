//! Serde config types for the top-level `catalog:` block (#279).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_sample_records() -> usize {
    100
}

/// The top-level `catalog:` block: opts a `faucet run` / `schedule` /
/// `replicate` pipeline into recording the Data Movement Catalog after every
/// successful root invocation. `faucet serve` records into its `--history`
/// backend automatically — this block is for the non-serve runtimes (and for
/// `faucet catalog`, which reads the same store).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogSpec {
    /// Where the catalog is stored: `sqlite:<path>` (e.g.
    /// `sqlite:./faucet-catalog.db`), a `postgres://…` URL, or `memory`
    /// (process-lifetime only — useful for tests). SQL backends require the
    /// matching `serve-history-sqlite` / `serve-history-postgres` build
    /// feature. Point `faucet serve --history` at the same URL to browse the
    /// accumulated catalog in the control plane + web console.
    pub url: String,

    /// How many records to sample per run for schema inference (per side).
    /// The sample bounds memory; the schema timeline only ever stores the
    /// inferred schema, never the records.
    #[serde(default = "default_sample_records")]
    pub sample_records: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_block_with_defaults() {
        let spec: CatalogSpec = serde_yaml::from_str("url: sqlite:./cat.db").unwrap();
        assert_eq!(spec.url, "sqlite:./cat.db");
        assert_eq!(spec.sample_records, 100);
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = serde_yaml::from_str::<CatalogSpec>("url: memory\nnope: 1").unwrap_err();
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn schema_generates() {
        let schema = schemars::schema_for!(CatalogSpec);
        let v = serde_json::to_value(&schema).unwrap();
        assert!(v["properties"]["url"].is_object());
    }
}
