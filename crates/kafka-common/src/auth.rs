//! Kafka authentication modes.
//!
//! TODO(Task 2): Fill in the full `KafkaAuth` enum with SASL/PLAIN, SASL/SCRAM,
//! mTLS, and SASL+SSL variants plus `rdkafka::ClientConfig` integration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// SCRAM hash algorithm used for SASL/SCRAM authentication.
///
/// TODO(Task 2): add rdkafka mechanism string mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScramMechanism {
    /// SCRAM-SHA-256
    ScramSha256,
    /// SCRAM-SHA-512
    ScramSha512,
}

/// Basic username/password credentials reused across auth modes.
///
/// TODO(Task 2): integrate with `rdkafka::ClientConfig` setters.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BasicAuth {
    /// Kafka username.
    pub username: String,
    /// Kafka password.
    pub password: String,
}

/// Kafka broker authentication configuration.
///
/// TODO(Task 2): implement `apply_to_client_config(&self, cfg: &mut ClientConfig)`
/// and add all variant fields (TLS certs, SASL+SSL combos, OAuth bearer).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KafkaAuth {
    /// No authentication — plaintext brokers only.
    #[default]
    None,
    /// SASL/PLAIN username + password.
    SaslPlain(BasicAuth),
    /// SASL/SCRAM-SHA-256 or SCRAM-SHA-512.
    SaslScram {
        /// Hash algorithm variant.
        mechanism: ScramMechanism,
        /// Credentials.
        #[serde(flatten)]
        credentials: BasicAuth,
    },
    /// Mutual TLS — client certificate authentication.
    Mtls {
        /// PEM-encoded client certificate (string or path).
        client_cert_pem: String,
        /// PEM-encoded client private key (string or path).
        client_key_pem: String,
        /// Optional PEM-encoded CA certificate to trust.
        ca_cert_pem: Option<String>,
    },
    /// SASL/PLAIN or SASL/SCRAM over SSL (combines SASL credentials with TLS transport).
    SaslSsl {
        /// Inner SASL mechanism (Plain or Scram).
        sasl_mechanism: ScramMechanism,
        /// Credentials.
        #[serde(flatten)]
        credentials: BasicAuth,
        /// Optional CA cert PEM for the TLS layer.
        ca_cert_pem: Option<String>,
    },
}
