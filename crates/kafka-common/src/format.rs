//! Kafka value/key serialization formats and related error policies.
//!
//! TODO(Task 3): Implement encoding/decoding logic for each `KafkaValueFormat`
//! variant, including Confluent Schema Registry wire-format envelope handling
//! (magic byte 0x00 + 4-byte schema ID prefix).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to handle a message whose key cannot be decoded.
///
/// TODO(Task 3): wire into the source fetch loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnKeyError {
    /// Skip the message and log a warning.
    Skip,
    /// Return a decoding error and abort.
    #[default]
    Fail,
    /// Send the record with no key (librdkafka picks the partition).
    RoundRobin,
}

/// How to handle a message whose value cannot be decoded.
///
/// TODO(Task 3): wire into the source fetch loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnDecodeError {
    /// Skip the message and log a warning.
    Skip,
    /// Return a decoding error and abort.
    #[default]
    Fail,
}

/// Producer-side compression codec applied to message batches.
///
/// TODO(Task 3): map to `rdkafka` `compression.codec` config value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    /// No compression.
    #[default]
    None,
    /// Gzip compression.
    Gzip,
    /// Snappy compression.
    Snappy,
    /// LZ4 compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
}

/// Serialization format used for Kafka message values (and optionally keys).
///
/// TODO(Task 3): implement `encode(&self, record: &Value) -> Result<Vec<u8>>` and
/// `decode(&self, bytes: &[u8]) -> Result<Value>` for each variant, integrating
/// the Schema Registry client when the `schema-registry` feature is enabled.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum KafkaValueFormat {
    /// UTF-8 JSON — decode bytes as a JSON value.
    #[default]
    Json,
    /// Raw UTF-8 string — wrap in `{"value": "<string>"}`.
    RawString,
    /// Raw bytes — base64-encode into `{"value": "<base64>"}`.
    Bytes,
    /// Avro with Confluent Schema Registry wire envelope (magic byte + schema ID).
    ///
    /// Requires the `schema-registry` feature.
    ConfluentAvro {
        /// Schema Registry base URL (e.g. `http://localhost:8081`).
        schema_registry_url: String,
    },
    /// Protobuf with Confluent Schema Registry wire envelope.
    ///
    /// Requires the `schema-registry` feature.
    ConfluentProtobuf {
        /// Schema Registry base URL.
        schema_registry_url: String,
    },
    /// JSON Schema with Confluent Schema Registry validation.
    ///
    /// Requires the `schema-registry` feature.
    ConfluentJsonSchema {
        /// Schema Registry base URL.
        schema_registry_url: String,
    },
}
