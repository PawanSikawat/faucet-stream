//! Confluent Schema Registry client configuration.
//!
//! TODO(Task 4): Implement the full `SchemaRegistryClient` with subject/schema
//! caching (LRU), Avro/Protobuf/JSON Schema codec resolution, and optional
//! basic-auth / bearer-token support against the Schema Registry REST API.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Connection settings for a Confluent Schema Registry instance.
///
/// TODO(Task 4): add `auth`, `tls`, and cache-size fields; implement
/// `fetch_schema(subject, version)` and `register_schema(subject, schema)`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SchemaRegistryConfig {
    /// Base URL of the Schema Registry (e.g. `http://localhost:8081`).
    pub url: String,
    /// Optional bearer token for authentication.
    pub bearer_token: Option<String>,
}
