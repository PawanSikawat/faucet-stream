//! Configuration types for the Apache Iceberg sink.
//!
//! ## Write mode
//!
//! Only `append` is supported in v1. Overwrite support is tracked in
//! [#179](https://github.com/PawanSikawat/faucet-stream/issues/179) and is
//! blocked on upstream iceberg-rust adding a replace/overwrite transaction action.

use std::collections::HashMap;
use std::fmt;

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Catalog config ────────────────────────────────────────────────────────────

/// Configuration fields shared by every catalog variant.
///
/// Individual variants carry the same fields so `CatalogConfig` stays a
/// well-typed tagged enum without a separate inner struct (which would make
/// the JSON Schema less readable).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CatalogInner {
    /// Catalog endpoint URI.
    ///
    /// For REST: `https://catalog.example.com`.
    /// For HMS: `thrift://hms:9083`.
    /// For SQL: the JDBC/SQLx connection string, e.g. `postgres://…`.
    #[serde(default)]
    pub uri: Option<String>,

    /// Object-storage warehouse root, e.g. `s3://lake/warehouse`.
    #[serde(default)]
    pub warehouse: Option<String>,

    /// REST bearer token or other catalog-specific credential.
    ///
    /// Redacted in `Debug` output — never logged.
    #[serde(default)]
    pub credential: Option<String>,

    /// Arbitrary catalog properties passed through to the catalog builder
    /// (e.g. S3 region, endpoint, access key).
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Iceberg catalog type and its connection settings.
///
/// Uses serde's internally-tagged enum: the JSON/YAML `type` key selects the
/// variant. Each variant carries the same inner fields (`uri`, `warehouse`,
/// `credential`, `properties`); the relevant set differs per catalog type and
/// is documented in each variant.
///
/// | Variant | Cargo feature required   |
/// |---------|--------------------------|
/// | `rest`  | `catalog-rest` (default) |
/// | `glue`  | `catalog-glue`           |
/// | `sql`   | `catalog-sql`            |
/// | `hms`   | `catalog-hms`            |
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogConfig {
    /// Apache Iceberg REST catalog. `uri` is the catalog endpoint; `credential`
    /// becomes the REST bearer token.
    Rest(CatalogInner),

    /// AWS Glue catalog. `warehouse` is the S3 root; AWS credentials are
    /// supplied via `properties` or the default AWS credential chain.
    Glue(CatalogInner),

    /// SQL-backed catalog (e.g. JDBC/postgres). `uri` is the connection string.
    Sql(CatalogInner),

    /// Hive Metastore catalog. `uri` is the Thrift endpoint
    /// (`thrift://hms:9083`).
    Hms(CatalogInner),
}

impl CatalogConfig {
    fn inner(&self) -> &CatalogInner {
        match self {
            CatalogConfig::Rest(i)
            | CatalogConfig::Glue(i)
            | CatalogConfig::Sql(i)
            | CatalogConfig::Hms(i) => i,
        }
    }
}

// Redact credential and URI from Debug output.
impl fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = match self {
            CatalogConfig::Rest(_) => "rest",
            CatalogConfig::Glue(_) => "glue",
            CatalogConfig::Sql(_) => "sql",
            CatalogConfig::Hms(_) => "hms",
        };
        let inner = self.inner();
        // Redact the credential field entirely, and the uri (may contain userinfo/token).
        let uri_display = inner
            .uri
            .as_deref()
            .map(|_| "***")
            .unwrap_or("<none>");
        let cred_display = inner
            .credential
            .as_deref()
            .map(|_| "***")
            .unwrap_or("<none>");
        f.debug_struct("CatalogConfig")
            .field("type", &type_name)
            .field("uri", &uri_display)
            .field("warehouse", &inner.warehouse)
            .field("credential", &cred_display)
            .field("properties_keys", &inner.properties.keys().collect::<Vec<_>>())
            .finish()
    }
}

// ── Partition spec ────────────────────────────────────────────────────────────

/// A single partition field: source column + transform.
///
/// Supported transforms: `identity`, `year`, `month`, `day`, `hour`, `void`,
/// and parameterized forms `bucket[N]` and `truncate[N]` (e.g. `bucket[16]`,
/// `truncate[8]`). Used only when `create_if_missing: true` and the table
/// does not yet exist.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PartitionField {
    /// Source column name in the table schema.
    pub source: String,

    /// Iceberg partition transform. One of: `identity`, `year`, `month`,
    /// `day`, `hour`, `void`, `bucket[N]`, `truncate[N]`.
    pub transform: String,
}

// ── Write mode ────────────────────────────────────────────────────────────────

/// Write semantics for the Iceberg sink.
///
/// Only `append` is available in v1. Overwrite is tracked in
/// [#179](https://github.com/PawanSikawat/faucet-stream/issues/179) and is
/// blocked on upstream iceberg-rust adding an overwrite/replace transaction
/// action (not present in 0.9.1).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Append records as a new snapshot. Existing data is never modified.
    #[default]
    Append,
}

// ── Parquet options ───────────────────────────────────────────────────────────

/// Parquet-level compression and encoding options.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ParquetOpts {
    /// Parquet compression codec. Supported: `snappy` (default), `zstd`,
    /// `gzip`, `lz4`, `none`.
    #[serde(default = "default_compression")]
    pub compression: String,
}

fn default_compression() -> String {
    "snappy".to_string()
}

impl Default for ParquetOpts {
    fn default() -> Self {
        ParquetOpts {
            compression: default_compression(),
        }
    }
}

// ── Top-level sink config ─────────────────────────────────────────────────────

/// Configuration for the Apache Iceberg sink.
///
/// Records are buffered into Arrow batches, written as Parquet data files via
/// the iceberg-rust writer pipeline, and committed as a single snapshot per
/// `flush()` call using `Transaction::fast_append`.
///
/// ## Append-only (v1)
///
/// Only `write_mode: append` is supported. Overwrite is tracked in #179 and
/// is blocked on upstream iceberg-rust. Configuring `write_mode: overwrite`
/// will produce a deserialization error.
///
/// ## Catalog feature gates
///
/// The REST catalog is included in the default build (`catalog-rest`). Glue,
/// SQL, and HMS each require their own Cargo feature (`catalog-glue`,
/// `catalog-sql`, `catalog-hms`). Configuring a disabled catalog type returns
/// `FaucetError::Config` at startup.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IcebergSinkConfig {
    /// Iceberg catalog connection settings.
    pub catalog: CatalogConfig,

    /// Multi-part namespace that contains the target table, e.g.
    /// `["analytics", "events"]`. Must be non-empty; no segment may be empty.
    pub namespace: Vec<String>,

    /// Name of the Iceberg table (without namespace), e.g. `"page_views"`.
    pub table: String,

    /// Create the table if it does not exist, inferring the schema from the
    /// first batch. When `false`, `load_table` is called at startup and an
    /// absent table causes a `FaucetError::Sink` immediately.
    #[serde(default = "default_create_if_missing")]
    pub create_if_missing: bool,

    /// Partition fields applied when creating the table. Ignored on writes to
    /// an existing table (the table's existing spec is used).
    #[serde(default)]
    pub partition_spec: Vec<PartitionField>,

    /// Write semantics. Only `append` is supported in v1; see crate-level docs
    /// for the overwrite roadmap item (#179).
    #[serde(default)]
    pub write_mode: WriteMode,

    /// Roll over to a new Parquet data file when the estimated file size
    /// (uncompressed Arrow bytes × 0.4) exceeds this threshold.
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: u64,

    /// Parquet codec settings.
    #[serde(default)]
    pub parquet: ParquetOpts,

    /// Key-value pairs written into the Iceberg snapshot summary.
    #[serde(default)]
    pub snapshot_properties: HashMap<String, String>,

    /// Maximum records buffered in memory before flushing a write to the
    /// iceberg writer pipeline. `0` = no limit (single batch). Default: 10 000.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_create_if_missing() -> bool {
    true
}

fn default_target_file_size_mb() -> u64 {
    256
}

fn default_batch_size() -> usize {
    10_000
}

// ── Validation ────────────────────────────────────────────────────────────────

/// Known partition transforms (bare names).
const KNOWN_TRANSFORMS: &[&str] = &["identity", "year", "month", "day", "hour", "void"];

/// Returns `true` if the transform string is a valid Iceberg partition transform.
///
/// Accepts:
/// - Bare names: `identity`, `year`, `month`, `day`, `hour`, `void`
/// - Parameterized: `bucket[N]`, `truncate[N]` (N must be a positive integer)
fn is_valid_transform(t: &str) -> bool {
    if KNOWN_TRANSFORMS.contains(&t) {
        return true;
    }
    // bucket[N] or truncate[N]
    for prefix in &["bucket[", "truncate["] {
        if let Some(rest) = t.strip_prefix(prefix)
            && let Some(n_str) = rest.strip_suffix(']')
        {
            return n_str.parse::<u64>().map(|n| n > 0).unwrap_or(false);
        }
    }
    false
}

impl IcebergSinkConfig {
    /// Validate the configuration at load time.
    ///
    /// Checks:
    /// - `namespace` is non-empty and contains no empty segment.
    /// - `table` is non-empty.
    /// - Each `partition_spec[].transform` is a recognised Iceberg transform.
    /// - `batch_size` is within bounds (via [`faucet_core::validate_batch_size`]).
    pub fn validate(&self) -> Result<(), FaucetError> {
        // Namespace
        if self.namespace.is_empty() {
            return Err(FaucetError::Config(
                "iceberg: `namespace` must contain at least one segment".to_string(),
            ));
        }
        for seg in &self.namespace {
            if seg.is_empty() {
                return Err(FaucetError::Config(
                    "iceberg: `namespace` segments must not be empty".to_string(),
                ));
            }
        }

        // Table name
        if self.table.is_empty() {
            return Err(FaucetError::Config(
                "iceberg: `table` must not be empty".to_string(),
            ));
        }

        // Partition transforms
        for (i, pf) in self.partition_spec.iter().enumerate() {
            if pf.source.is_empty() {
                return Err(FaucetError::Config(format!(
                    "iceberg: partition_spec[{i}].source must not be empty"
                )));
            }
            if !is_valid_transform(&pf.transform) {
                return Err(FaucetError::Config(format!(
                    "iceberg: partition_spec[{i}].transform {:?} is not a recognised Iceberg \
                     transform; expected one of: {} or parameterized bucket[N] / truncate[N]",
                    pf.transform,
                    KNOWN_TRANSFORMS.join(", ")
                )));
            }
        }

        // Catalog connection URI. REST / SQL / HMS need an endpoint URI; Glue
        // resolves its endpoint from AWS config (region/credentials in
        // `properties` or the default chain), so it has no required URI. Caught
        // here at config-load time rather than only at connect time.
        let (uri_required, kind) = match &self.catalog {
            CatalogConfig::Rest(_) => (true, "rest"),
            CatalogConfig::Sql(_) => (true, "sql"),
            CatalogConfig::Hms(_) => (true, "hms"),
            CatalogConfig::Glue(_) => (false, "glue"),
        };
        if uri_required
            && self
                .catalog
                .inner()
                .uri
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(FaucetError::Config(format!(
                "iceberg: catalog '{kind}' requires a non-empty `uri`"
            )));
        }

        // Batch size
        faucet_core::validate_batch_size(self.batch_size)?;

        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config_json() -> serde_json::Value {
        serde_json::json!({
            "catalog": { "type": "rest", "uri": "http://localhost:8181" },
            "namespace": ["analytics"],
            "table": "events"
        })
    }

    fn parse(v: serde_json::Value) -> IcebergSinkConfig {
        serde_json::from_value(v).expect("parse failed")
    }

    // ── defaults ──────────────────────────────────────────────────────────────

    #[test]
    fn defaults_are_applied() {
        let cfg = parse(minimal_config_json());
        assert!(cfg.create_if_missing);
        assert_eq!(cfg.target_file_size_mb, 256);
        assert_eq!(cfg.parquet.compression, "snappy");
        assert_eq!(cfg.write_mode, WriteMode::Append);
        assert_eq!(cfg.batch_size, 10_000);
        assert!(cfg.partition_spec.is_empty());
        assert!(cfg.snapshot_properties.is_empty());
    }

    // ── catalog tagged-enum round-trip ────────────────────────────────────────

    #[test]
    fn catalog_rest_round_trip() {
        let v = serde_json::json!({
            "type": "rest",
            "uri": "https://catalog.example.com",
            "warehouse": "s3://lake/wh",
            "credential": "my-token",
            "properties": { "region": "us-east-1" }
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Rest(_)));
        let inner = cat.inner();
        assert_eq!(inner.uri.as_deref(), Some("https://catalog.example.com"));
        assert_eq!(inner.credential.as_deref(), Some("my-token"));
        assert_eq!(inner.properties.get("region").map(String::as_str), Some("us-east-1"));

        // Re-serialize and re-parse.
        let json = serde_json::to_value(&cat).unwrap();
        assert_eq!(json["type"], "rest");
        let _cat2: CatalogConfig = serde_json::from_value(json).unwrap();
    }

    #[test]
    fn catalog_glue_round_trip() {
        let v = serde_json::json!({ "type": "glue", "warehouse": "s3://lake/wh" });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Glue(_)));
    }

    #[test]
    fn catalog_sql_round_trip() {
        let v = serde_json::json!({
            "type": "sql",
            "uri": "postgres://localhost/meta",
            "warehouse": "s3://lake/wh"
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Sql(_)));
    }

    #[test]
    fn catalog_hms_round_trip() {
        let v = serde_json::json!({ "type": "hms", "uri": "thrift://hms:9083" });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Hms(_)));
    }

    // ── write_mode ────────────────────────────────────────────────────────────

    #[test]
    fn write_mode_defaults_to_append() {
        let cfg = parse(minimal_config_json());
        assert_eq!(cfg.write_mode, WriteMode::Append);
    }

    #[test]
    fn write_mode_append_explicit() {
        let mut v = minimal_config_json();
        v["write_mode"] = serde_json::json!("append");
        let cfg = parse(v);
        assert_eq!(cfg.write_mode, WriteMode::Append);
    }

    #[test]
    fn write_mode_overwrite_is_rejected() {
        let mut v = minimal_config_json();
        v["write_mode"] = serde_json::json!("overwrite");
        let err = serde_json::from_value::<IcebergSinkConfig>(v);
        assert!(err.is_err(), "expected error for write_mode=overwrite");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("unknown variant") || msg.contains("overwrite"),
            "error message should mention unknown variant or overwrite: {msg}"
        );
    }

    // ── partition transform validation ────────────────────────────────────────

    #[test]
    fn valid_transforms_accepted() {
        let transforms = ["identity", "year", "month", "day", "hour", "void", "bucket[5]", "bucket[16]", "truncate[8]"];
        for t in transforms {
            assert!(is_valid_transform(t), "{t} should be valid");
        }
    }

    #[test]
    fn invalid_transform_rejected_by_validate() {
        let mut v = minimal_config_json();
        v["partition_spec"] = serde_json::json!([{ "source": "col", "transform": "frobnicate" }]);
        let cfg = parse(v);
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        let msg = err.to_string();
        assert!(msg.contains("frobnicate"), "error should mention the bad transform: {msg}");
    }

    #[test]
    fn bucket_zero_is_rejected() {
        assert!(!is_valid_transform("bucket[0]"));
    }

    #[test]
    fn valid_partition_spec_passes_validate() {
        let mut v = minimal_config_json();
        v["partition_spec"] = serde_json::json!([
            { "source": "event_date", "transform": "day" },
            { "source": "tenant_id",  "transform": "identity" },
            { "source": "bucket_col", "transform": "bucket[16]" }
        ]);
        let cfg = parse(v);
        cfg.validate().expect("should pass");
    }

    // ── namespace / table required ────────────────────────────────────────────

    #[test]
    fn empty_namespace_is_rejected() {
        let mut v = minimal_config_json();
        v["namespace"] = serde_json::json!([]);
        let cfg = parse(v);
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        let msg = err.to_string();
        assert!(msg.contains("namespace"), "should mention namespace: {msg}");
    }

    #[test]
    fn namespace_with_empty_segment_is_rejected() {
        let mut v = minimal_config_json();
        v["namespace"] = serde_json::json!(["analytics", "", "events"]);
        let cfg = parse(v);
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn empty_table_is_rejected() {
        let mut v = minimal_config_json();
        v["table"] = serde_json::json!("");
        let cfg = parse(v);
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        let msg = err.to_string();
        assert!(msg.contains("table"), "should mention table: {msg}");
    }

    // ── catalog uri requirement ───────────────────────────────────────────────

    #[test]
    fn rest_catalog_without_uri_is_rejected() {
        let cfg = parse(serde_json::json!({
            "catalog": { "type": "rest" },
            "namespace": ["analytics"],
            "table": "events"
        }));
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        assert!(err.to_string().contains("uri"), "should mention uri: {err}");
    }

    #[test]
    fn sql_and_hms_without_uri_are_rejected() {
        for ty in ["sql", "hms"] {
            let cfg = parse(serde_json::json!({
                "catalog": { "type": ty },
                "namespace": ["analytics"],
                "table": "events"
            }));
            assert!(cfg.validate().is_err(), "{ty} without uri should fail");
        }
    }

    #[test]
    fn rest_catalog_with_whitespace_uri_is_rejected() {
        let cfg = parse(serde_json::json!({
            "catalog": { "type": "rest", "uri": "   " },
            "namespace": ["analytics"],
            "table": "events"
        }));
        assert!(cfg.validate().is_err(), "whitespace-only uri should fail");
    }

    #[test]
    fn glue_catalog_without_uri_is_allowed() {
        // Glue resolves its endpoint from AWS config, so no uri is required.
        let cfg = parse(serde_json::json!({
            "catalog": { "type": "glue", "warehouse": "s3://lake/wh" },
            "namespace": ["analytics"],
            "table": "events"
        }));
        assert!(cfg.validate().is_ok());
    }

    // ── batch_size bounds ─────────────────────────────────────────────────────

    #[test]
    fn batch_size_above_max_is_rejected() {
        let mut v = minimal_config_json();
        v["batch_size"] = serde_json::json!(1_000_001usize);
        let cfg = parse(v);
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn batch_size_at_max_is_accepted() {
        let mut v = minimal_config_json();
        v["batch_size"] = serde_json::json!(1_000_000usize);
        let cfg = parse(v);
        cfg.validate().expect("max batch_size should be accepted");
    }

    #[test]
    fn batch_size_zero_is_accepted() {
        let mut v = minimal_config_json();
        v["batch_size"] = serde_json::json!(0usize);
        let cfg = parse(v);
        cfg.validate().expect("batch_size=0 sentinel should be accepted");
    }

    // ── Debug redacts credential ──────────────────────────────────────────────

    #[test]
    fn debug_redacts_credential() {
        let v = serde_json::json!({
            "type": "rest",
            "uri": "https://catalog.example.com/api",
            "credential": "super-secret-token"
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        let debug_str = format!("{cat:?}");
        assert!(
            !debug_str.contains("super-secret-token"),
            "credential must be redacted: {debug_str}"
        );
        assert!(
            debug_str.contains("***"),
            "should show *** placeholder: {debug_str}"
        );
    }

    #[test]
    fn debug_redacts_uri() {
        let v = serde_json::json!({
            "type": "rest",
            "uri": "https://user:pass@catalog.example.com"
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        let debug_str = format!("{cat:?}");
        // The URI field is redacted to "***" when present.
        assert!(
            !debug_str.contains("user:pass"),
            "uri userinfo must be redacted: {debug_str}"
        );
    }
}
