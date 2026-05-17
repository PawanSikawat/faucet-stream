//! Kafka authentication modes.
//!
//! TODO(Task 2): implement `KafkaAuth::apply()` — fill in the
//! `apply(&self, cfg: &mut ClientConfig)` method that maps each variant to
//! the appropriate `rdkafka::ClientConfig` keys.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// SCRAM hash algorithm used for SASL/SCRAM authentication.
///
/// TODO(Task 2): add rdkafka mechanism string mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScramMechanism {
    /// SCRAM-SHA-256
    Sha256,
    /// SCRAM-SHA-512
    Sha512,
}

/// Basic username/password credentials reused across auth modes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

/// Kafka broker authentication configuration.
///
/// TODO(Task 2): implement `KafkaAuth::apply()` — maps each variant to
/// `rdkafka::ClientConfig` keys (security.protocol, sasl.mechanism, etc.).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KafkaAuth {
    /// No authentication — plaintext brokers only.
    #[default]
    None,
    /// SASL/PLAIN username + password.
    SaslPlain { username: String, password: String },
    /// SASL/SCRAM-SHA-256 or SCRAM-SHA-512.
    SaslScram {
        /// Hash algorithm variant.
        mechanism: ScramMechanism,
        username: String,
        password: String,
    },
    /// SSL/TLS client-certificate authentication (path-based).
    Ssl {
        /// Path to the CA certificate file.
        ca_path: PathBuf,
        /// Path to the client certificate file.
        cert_path: PathBuf,
        /// Path to the client private key file.
        key_path: PathBuf,
        /// Optional passphrase for the private key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_password: Option<String>,
    },
    /// SASL over SSL — combines a SASL mechanism with TLS transport.
    SaslSsl {
        /// Inner SASL auth (must be `SaslPlain` or `SaslScram`).
        sasl: Box<KafkaAuth>,
        /// TLS layer (must be `Ssl`).
        ssl: Box<KafkaAuth>,
    },
}
