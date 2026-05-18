//! gRPC source configuration.

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
}
