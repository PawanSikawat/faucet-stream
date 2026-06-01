#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-elasticsearch
//!
//! Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! Elasticsearch source and sink connectors.
//!
//! - [`ElasticsearchAuth`] — authentication modes (None, Basic, Bearer, ApiKey)
//!
//! The enum derives `Serialize`, `Deserialize`, and `JsonSchema` so it round-trips
//! through YAML/JSON configs and CLI introspection. Its `Debug` impl masks
//! credentials (`password`, `token`, `key`) as `"***"` so accidental logging of a
//! config value never leaks secrets.

use faucet_core::{Credential, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authentication method for Elasticsearch.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by
/// every faucet connector.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum ElasticsearchAuth {
    /// No authentication.
    None,
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// Bearer token authentication.
    Bearer { token: String },
    /// API key authentication (sent as `ApiKey` in the `Authorization` header).
    ApiKey { key: String },
}

/// Map a [`Credential`] from a shared [`faucet_core::AuthProvider`] onto an
/// [`ElasticsearchAuth`] variant.
///
/// Elasticsearch supports `Bearer` and `Basic` credentials. The `Header` and
/// `Token` credential variants have no equivalent Elasticsearch auth mode, so
/// they return [`FaucetError::Auth`].
pub fn credential_to_auth(cred: Credential) -> Result<ElasticsearchAuth, FaucetError> {
    match cred {
        Credential::Bearer(token) => Ok(ElasticsearchAuth::Bearer { token }),
        Credential::Basic { username, password } => {
            Ok(ElasticsearchAuth::Basic { username, password })
        }
        other => Err(FaucetError::Auth(format!(
            "Elasticsearch auth provider must yield a bearer or basic credential, got {other:?}"
        ))),
    }
}

impl std::fmt::Debug for ElasticsearchAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Bearer { .. } => write!(f, "Bearer(***)"),
            Self::ApiKey { .. } => write!(f, "ApiKey(***)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_masks_basic_password() {
        let auth = ElasticsearchAuth::Basic {
            username: "user".into(),
            password: "secret".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("user"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn debug_masks_bearer_token() {
        let auth = ElasticsearchAuth::Bearer {
            token: "my-token".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-token"));
    }

    #[test]
    fn debug_masks_api_key() {
        let auth = ElasticsearchAuth::ApiKey {
            key: "my-key".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-key"));
    }

    #[test]
    fn debug_none_renders_unit() {
        let auth = ElasticsearchAuth::None;
        assert_eq!(format!("{auth:?}"), "None");
    }

    #[test]
    fn serde_round_trip_basic() {
        let auth = ElasticsearchAuth::Basic {
            username: "u".into(),
            password: "p".into(),
        };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(
            json,
            r#"{"type":"basic","config":{"username":"u","password":"p"}}"#
        );
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::Basic { .. }));
    }

    #[test]
    fn serde_round_trip_none() {
        let json = serde_json::to_string(&ElasticsearchAuth::None).unwrap();
        assert_eq!(json, r#"{"type":"none"}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::None));
    }

    #[test]
    fn serde_round_trip_bearer() {
        let auth = ElasticsearchAuth::Bearer { token: "t".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"bearer","config":{"token":"t"}}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::Bearer { .. }));
    }

    #[test]
    fn serde_round_trip_api_key() {
        let auth = ElasticsearchAuth::ApiKey { key: "k".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"api_key","config":{"key":"k"}}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::ApiKey { .. }));
    }
}
