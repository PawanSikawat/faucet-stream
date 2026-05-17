//! Confluent Schema Registry placeholder. Filled in by Task 4.

use crate::auth::BasicAuth;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// TODO(Task 4): full SchemaRegistryConfig with cache_capacity, request_timeout, validate()
/// Connection settings for a Confluent Schema Registry instance.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SchemaRegistryConfig {
    /// Base URL of the Schema Registry (e.g. `http://localhost:8081`).
    pub url: String,
    /// Optional basic-auth credentials for authenticated Schema Registry deployments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BasicAuth>,
}
