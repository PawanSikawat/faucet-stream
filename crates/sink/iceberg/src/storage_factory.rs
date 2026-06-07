//! Iceberg `StorageFactory` selection for the non-REST catalogs.
//!
//! REST catalogs resolve `FileIO` server-side. The SQL / Glue / HMS catalogs
//! build `FileIO` in-process and need an explicit `StorageFactory`:
//!
//! - Local (`file://` / bare path) warehouses → `iceberg::io::LocalFsStorageFactory`.
//! - Cloud (`s3://` / `s3a://` / `gs://`) warehouses → an [`OpendalPropInjector`]
//!   wrapping `iceberg_storage_opendal::OpenDalStorageFactory`.
//!
//! The wrapper exists because `iceberg-catalog-sql` builds its `FileIO` with
//! **empty** properties, so the user's `catalog.properties` (`s3.region`,
//! `s3.endpoint`, `gcs.credentials-json`, …) would otherwise never reach the
//! storage layer. The wrapper re-supplies them, letting the catalog-threaded
//! props (Glue/HMS) overlay on top.

use std::collections::HashMap;
use std::sync::Arc;

use faucet_core::FaucetError;
use iceberg::io::{LocalFsStorageFactory, Storage, StorageConfig, StorageFactory};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};

use crate::config::{warehouse_scheme, CatalogInner, WarehouseScheme};

/// Merge faucet's configured catalog properties (`base`) with the properties
/// the catalog threads into its `FileIO` (`overlay`). The overlay wins on key
/// collisions: for SQL the overlay is empty (faucet props win); for Glue/HMS
/// the overlay carries the catalog's own threaded + enriched props.
fn merge_props(
    base: &HashMap<String, String>,
    overlay: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = base.clone();
    merged.extend(overlay.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged
}

/// A `StorageFactory` that injects faucet's configured `catalog.properties`
/// into the `StorageConfig` before delegating to an OpenDAL-backed factory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OpendalPropInjector {
    inner: OpenDalStorageFactory,
    props: HashMap<String, String>,
}

impl OpendalPropInjector {
    fn new(inner: OpenDalStorageFactory, props: HashMap<String, String>) -> Self {
        Self { inner, props }
    }
}

#[typetag::serde(name = "faucet-opendal-prop-injector")]
impl StorageFactory for OpendalPropInjector {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        let merged = merge_props(&self.props, config.props());
        self.inner.build(&StorageConfig::from_props(merged))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_props_overlay_wins() {
        let base = HashMap::from([
            ("s3.region".to_string(), "us-east-1".to_string()),
            ("s3.endpoint".to_string(), "http://base".to_string()),
        ]);
        let overlay = HashMap::from([("s3.endpoint".to_string(), "http://overlay".to_string())]);
        let merged = merge_props(&base, &overlay);
        assert_eq!(merged.get("s3.region").unwrap(), "us-east-1");
        assert_eq!(merged.get("s3.endpoint").unwrap(), "http://overlay");
    }

    #[test]
    fn merge_props_empty_overlay_keeps_base() {
        let base = HashMap::from([("s3.region".to_string(), "eu-west-1".to_string())]);
        let merged = merge_props(&base, &HashMap::new());
        assert_eq!(merged.get("s3.region").unwrap(), "eu-west-1");
    }
}
