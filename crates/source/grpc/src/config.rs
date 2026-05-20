//! gRPC source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

/// A single piece of gRPC request metadata.
///
/// Use a `Vec<MetadataEntry>` rather than a map because gRPC allows duplicate
/// keys and order is occasionally observable.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

/// Authentication for gRPC endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum GrpcAuth {
    /// No authentication.
    None,
    /// Bearer token sent as `authorization` metadata.
    Bearer { token: String },
    /// Custom metadata key-value pairs.
    Metadata { entries: Vec<MetadataEntry> },
}

/// Configuration for the gRPC source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GrpcStreamConfig {
    /// gRPC endpoint URL (e.g. `"http://localhost:50051"`).
    pub endpoint: String,
    /// Fully qualified service name (e.g. `"mypackage.MyService"`).
    pub service_name: String,
    /// Method name (e.g. `"ListUsers"`).
    pub method_name: String,
    /// Request message as JSON. Fields are mapped to protobuf fields
    /// using the `FileDescriptorSet`.
    pub request: Value,
    /// Path to the compiled `FileDescriptorSet` file.
    pub descriptor_set_path: PathBuf,
    /// Authentication method.
    pub auth: GrpcAuth,
    /// Whether to use TLS (detected from `https://` in endpoint by default).
    pub tls: Option<bool>,
    /// JSONPath to extract records from the response.
    /// If not set, the entire response is returned as a single record.
    pub records_path: Option<String>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Defaults
    /// to [`DEFAULT_BATCH_SIZE`].
    ///
    /// Unary gRPC returns one response containing all records, so the source
    /// has no native paging primitive to honour this hint. The default
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages) impl
    /// buffers the full response via `fetch_with_context_incremental` and
    /// then chunks it in memory into `batch_size` pages, which bounds
    /// **sink-side** memory only. For unary RPCs, `batch_size = 0` (the
    /// "no batching" sentinel) and any positive `batch_size` are observably
    /// identical from a wire-protocol standpoint — both buffer the full
    /// result before any page is yielded. Server-streaming gRPC is tracked
    /// separately and will override `stream_pages` to stream per-response.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl GrpcStreamConfig {
    /// Create a new config with the required fields.
    pub fn new(
        endpoint: impl Into<String>,
        service_name: impl Into<String>,
        method_name: impl Into<String>,
        descriptor_set_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            service_name: service_name.into(),
            method_name: method_name.into(),
            request: Value::Object(Default::default()),
            descriptor_set_path: descriptor_set_path.into(),
            auth: GrpcAuth::None,
            tls: None,
            records_path: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the request message as JSON.
    pub fn request(mut self, request: Value) -> Self {
        self.request = request;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, auth: GrpcAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Set the TLS mode explicitly.
    pub fn tls(mut self, tls: bool) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the JSONPath for record extraction from the response.
    pub fn records_path(mut self, path: impl Into<String>) -> Self {
        self.records_path = Some(path.into());
        self
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted in
    /// a single [`StreamPage`](faucet_core::StreamPage). For the unary gRPC
    /// source this is observably identical to any positive `batch_size`,
    /// since the full response is buffered before any page is yielded.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        );
        assert_eq!(config.endpoint, "http://localhost:50051");
        assert_eq!(config.service_name, "users.UserService");
        assert_eq!(config.method_name, "ListUsers");
        assert!(config.records_path.is_none());
        assert!(matches!(config.auth, GrpcAuth::None));
    }

    #[test]
    fn builder_methods() {
        let config =
            GrpcStreamConfig::new("https://grpc.example.com", "svc.Svc", "Get", "desc.bin")
                .request(json!({"page_size": 100}))
                .auth(GrpcAuth::Bearer {
                    token: "tok".into(),
                })
                .tls(true)
                .records_path("$.users[*]");
        assert_eq!(config.request["page_size"], 100);
        assert!(matches!(config.auth, GrpcAuth::Bearer { .. }));
        assert_eq!(config.tls, Some(true));
        assert_eq!(config.records_path.unwrap(), "$.users[*]");
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        );
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        )
        .with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        )
        .with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        )
        .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "endpoint": "http://localhost:50051",
            "service_name": "users.UserService",
            "method_name": "ListUsers",
            "request": {},
            "descriptor_set_path": "proto/descriptor.bin",
            "auth": { "type": "None" },
            "tls": null,
            "records_path": null,
            "batch_size": 250
        }"#;
        let config: GrpcStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_from_json() {
        let json = r#"{
            "endpoint": "http://localhost:50051",
            "service_name": "users.UserService",
            "method_name": "ListUsers",
            "request": {},
            "descriptor_set_path": "proto/descriptor.bin",
            "auth": { "type": "None" },
            "tls": null,
            "records_path": null
        }"#;
        let config: GrpcStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
