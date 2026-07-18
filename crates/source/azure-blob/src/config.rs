//! Azure Blob source configuration.

use faucet_common_azure::{AzureConnection, AzureCredentials};
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Format of objects stored in the container.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AzureFileFormat {
    /// Each line in the object is a separate JSON record.
    #[default]
    JsonLines,
    /// The entire object is a JSON array of records.
    JsonArray,
    /// Each object becomes a single record with `"key"` and `"content"` fields.
    RawText,
}

/// Configuration for the Azure Blob source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AzureBlobSourceConfig {
    /// Azure connection (container, account, credentials, endpoint, …).
    #[serde(flatten)]
    pub connection: AzureConnection,
    /// Object name prefix filter. Ignored when `object_keys` is set.
    pub prefix: Option<String>,
    /// Explicit object names. When set, listing is skipped and `prefix`
    /// is ignored.
    pub object_keys: Option<Vec<String>>,
    /// File format.
    #[serde(default)]
    pub file_format: AzureFileFormat,
    /// Hard cap on the number of objects read (after listing).
    pub max_objects: Option<usize>,
    /// Maximum concurrent object reads (default: 10).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per emitted `StreamPage`. `batch_size = 0` is the "no batching"
    /// sentinel and emits one page per object.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Compression codec applied to each downloaded object. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) — the
    /// codec is resolved per-object-key, so a single source can read a mix of
    /// compressed and uncompressed objects. Requires the crate-local
    /// `compression` feature.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_concurrency() -> usize {
    10
}

impl AzureBlobSourceConfig {
    /// Create a new config for `container` with sensible defaults.
    pub fn new(container: impl Into<String>) -> Self {
        Self {
            connection: AzureConnection::new(container),
            prefix: None,
            object_keys: None,
            file_format: AzureFileFormat::default(),
            max_objects: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::default(),
        }
    }

    /// Set the storage-account name.
    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.connection = self.connection.account(account);
        self
    }

    /// Set the credential source.
    pub fn auth(mut self, creds: AzureCredentials) -> Self {
        self.connection = self.connection.auth(creds);
        self
    }

    /// Set a custom blob endpoint (emulator / sovereign cloud).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.connection = self.connection.endpoint(endpoint);
        self
    }

    /// Permit plaintext HTTP (required for the Azurite emulator).
    pub fn allow_http(mut self, allow: bool) -> Self {
        self.connection = self.connection.allow_http(allow);
        self
    }

    /// Target the Azurite emulator.
    pub fn use_emulator(mut self, use_emulator: bool) -> Self {
        self.connection = self.connection.use_emulator(use_emulator);
        self
    }

    /// Filter objects by name prefix.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Read an explicit set of object names (skips listing).
    pub fn object_keys(mut self, keys: Vec<String>) -> Self {
        self.object_keys = Some(keys);
        self
    }

    /// Set the object file format.
    pub fn file_format(mut self, format: AzureFileFormat) -> Self {
        self.file_format = format;
        self
    }

    /// Cap the number of objects read.
    pub fn max_objects(mut self, max: usize) -> Self {
        self.max_objects = Some(max);
        self
    }

    /// Set the maximum concurrent object reads.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the records-per-`StreamPage` hint.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }

    /// The container name.
    pub fn container(&self) -> &str {
        &self.connection.container
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = AzureBlobSourceConfig::new("my-container");
        assert_eq!(config.container(), "my-container");
        assert!(config.prefix.is_none());
        assert!(config.object_keys.is_none());
        assert_eq!(config.connection.auth, AzureCredentials::Default);
        assert_eq!(config.file_format, AzureFileFormat::JsonLines);
        assert!(config.max_objects.is_none());
        assert_eq!(config.concurrency, 10);
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn builder_methods() {
        let config = AzureBlobSourceConfig::new("c")
            .account("acct")
            .prefix("data/")
            .file_format(AzureFileFormat::JsonArray)
            .max_objects(5)
            .concurrency(20)
            .with_batch_size(250)
            .auth(AzureCredentials::AccountKey {
                account_key: "k".into(),
            });

        assert_eq!(config.container(), "c");
        assert_eq!(config.connection.account.as_deref(), Some("acct"));
        assert_eq!(config.prefix.as_deref(), Some("data/"));
        assert_eq!(config.file_format, AzureFileFormat::JsonArray);
        assert_eq!(config.max_objects, Some(5));
        assert_eq!(config.concurrency, 20);
        assert_eq!(config.batch_size, 250);
        assert!(matches!(
            config.connection.auth,
            AzureCredentials::AccountKey { .. }
        ));
    }

    #[test]
    fn file_format_default_is_json_lines() {
        assert_eq!(AzureFileFormat::default(), AzureFileFormat::JsonLines);
    }

    #[test]
    fn deserializes_flattened_connection_and_auth() {
        let json = r#"{
            "container": "c",
            "account": "acct",
            "auth": { "type": "account_key", "config": { "account_key": "k" } },
            "prefix": "data/",
            "file_format": "json_array"
        }"#;
        let config: AzureBlobSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.container(), "c");
        assert_eq!(config.connection.account.as_deref(), Some("acct"));
        assert_eq!(config.file_format, AzureFileFormat::JsonArray);
        assert!(matches!(
            config.connection.auth,
            AzureCredentials::AccountKey { .. }
        ));
        // batch_size / concurrency default when omitted.
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert_eq!(config.concurrency, 10);
    }

    #[test]
    fn auth_defaults_to_default_when_absent_from_json() {
        let json = r#"{ "container": "c" }"#;
        let config: AzureBlobSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.connection.auth, AzureCredentials::Default);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = AzureBlobSourceConfig::new("c").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected() {
        let config =
            AzureBlobSourceConfig::new("c").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn schema_generates_without_panicking() {
        let _ = faucet_core::schema_for!(AzureBlobSourceConfig);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = AzureBlobSourceConfig::new("c");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }
}
