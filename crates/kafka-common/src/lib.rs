//! # faucet-kafka-common
//!
//! Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! Kafka source and sink connectors.
//!
//! - [`KafkaAuth`] — authentication modes (None, SASL/PLAIN, SASL/SCRAM, SSL, SASL+SSL)
//! - [`KafkaValueFormat`] — message value serialization (JSON, RawString, Bytes,
//!   ConfluentAvro, ConfluentProtobuf, ConfluentJsonSchema)
//! - [`SchemaRegistryConfig`] — Confluent Schema Registry client settings
//!
//! All types derive `Serialize`, `Deserialize`, and `JsonSchema` so they round-trip
//! through YAML/JSON configs and CLI introspection.

pub mod auth;
pub mod format;

#[cfg(feature = "schema-registry")]
pub mod schema_registry;

pub use auth::{BasicAuth, KafkaAuth, ScramMechanism};
pub use format::{CompressionType, KafkaValueFormat, OnDecodeError, OnKeyError};

#[cfg(feature = "schema-registry")]
pub use schema_registry::SchemaRegistryConfig;
