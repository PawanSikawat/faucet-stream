//! # faucet-elasticsearch-common
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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authentication method for Elasticsearch.
///
/// **Wire-format note:** the `#[serde(tag = "type")]` discriminator uses
/// PascalCase variant names (`"None"`, `"Basic"`, `"Bearer"`, `"ApiKey"`)
/// for byte-compatibility with existing YAML configs that predate the
/// extraction of this crate from `faucet-source-elasticsearch` and
/// `faucet-sink-elasticsearch`. Do not add `#[serde(rename_all = "snake_case")]`
/// here — it would silently break those configs.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
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
        let auth = ElasticsearchAuth::Bearer { token: "my-token".into() };
        let debug = format!("{auth:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-token"));
    }

    #[test]
    fn debug_masks_api_key() {
        let auth = ElasticsearchAuth::ApiKey { key: "my-key".into() };
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
        assert_eq!(json, r#"{"type":"Basic","username":"u","password":"p"}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::Basic { .. }));
    }

    #[test]
    fn serde_round_trip_none() {
        let json = serde_json::to_string(&ElasticsearchAuth::None).unwrap();
        assert_eq!(json, r#"{"type":"None"}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::None));
    }

    #[test]
    fn serde_round_trip_bearer() {
        let auth = ElasticsearchAuth::Bearer { token: "t".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"Bearer","token":"t"}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::Bearer { .. }));
    }

    #[test]
    fn serde_round_trip_api_key() {
        let auth = ElasticsearchAuth::ApiKey { key: "k".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"ApiKey","key":"k"}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::ApiKey { .. }));
    }
}
