//! # faucet-snowflake-common
//!
//! Shared configuration types and helpers for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! Snowflake source and sink connectors.
//!
//! - [`SnowflakeAuth`] — JWT key-pair or OAuth bearer authentication.
//! - [`authorization_header`] — produces the `Authorization` header value the
//!   Snowflake SQL REST API expects (JWT for `KeyPair`, `Snowflake Token=...`
//!   for `OAuth`).
//! - [`snowflake_token_type`] — the matching `X-Snowflake-Authorization-Token-Type`
//!   header value (`KEYPAIR_JWT` for `KeyPair`, `OAUTH` for `OAuth`).
//!
//! `SnowflakeAuth` derives `Serialize`, `Deserialize`, and `JsonSchema` so it
//! round-trips through YAML/JSON configs and CLI introspection. Its `Debug`
//! impl masks credentials as `"***"`.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authentication method for Snowflake.
///
/// **Wire-format note:** the `#[serde(tag = "type")]` discriminator uses
/// PascalCase variant names (`"KeyPair"`, `"OAuth"`) for byte-compatibility
/// with existing YAML configs that predate the extraction of this crate
/// from `faucet-sink-snowflake`. Do not add
/// `#[serde(rename_all = "snake_case")]` here — it would silently break those
/// configs.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum SnowflakeAuth {
    /// JWT key-pair authentication.
    ///
    /// Uses an RSA private key (PEM-encoded) to generate JWT tokens for the
    /// Snowflake SQL REST API.
    KeyPair {
        /// The Snowflake user account name.
        user: String,
        /// PEM-encoded RSA private key.
        private_key_pem: String,
    },
    /// OAuth2 bearer token (e.g. from an external identity provider).
    OAuth { token: String },
}

impl std::fmt::Debug for SnowflakeAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyPair { user, .. } => f
                .debug_struct("KeyPair")
                .field("user", user)
                .field("private_key_pem", &"***")
                .finish(),
            Self::OAuth { .. } => f.debug_struct("OAuth").field("token", &"***").finish(),
        }
    }
}

/// Build the `Authorization` header value for a Snowflake SQL REST API request.
///
/// For `KeyPair`, generates a fresh JWT signed with the configured RSA key
/// (issuer/subject set to `{ACCOUNT_UPPER}.{USER_UPPER}`, 1-hour expiry) and
/// wraps it as `Bearer {jwt}`. For `OAuth`, wraps the token as
/// `Snowflake Token="{token}"`.
///
/// `account` is the Snowflake account identifier from the source/sink config
/// (e.g. `"xy12345.us-east-1"`); only its uppercase form is used in the JWT
/// claims.
pub fn authorization_header(auth: &SnowflakeAuth, account: &str) -> Result<String, FaucetError> {
    match auth {
        SnowflakeAuth::KeyPair {
            user,
            private_key_pem,
        } => {
            let account_upper = account.to_uppercase();
            let user_upper = user.to_uppercase();
            let qualified_user = format!("{account_upper}.{user_upper}");

            let now = jsonwebtoken::get_current_timestamp();
            let claims = serde_json::json!({
                "iss": qualified_user,
                "sub": qualified_user,
                "iat": now,
                "exp": now + 3600,
            });

            let key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
                .map_err(|e| FaucetError::Auth(format!("invalid RSA key: {e}")))?;

            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
                &claims,
                &key,
            )
            .map_err(|e| FaucetError::Auth(format!("JWT generation failed: {e}")))?;

            Ok(format!("Bearer {token}"))
        }
        SnowflakeAuth::OAuth { token } => Ok(format!("Snowflake Token=\"{token}\"")),
    }
}

/// The `X-Snowflake-Authorization-Token-Type` header value that pairs with the
/// `Authorization` header produced by [`authorization_header`].
pub fn snowflake_token_type(auth: &SnowflakeAuth) -> &'static str {
    match auth {
        SnowflakeAuth::KeyPair { .. } => "KEYPAIR_JWT",
        SnowflakeAuth::OAuth { .. } => "OAUTH",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_masks_key_pair_private_key() {
        let auth = SnowflakeAuth::KeyPair {
            user: "alice".into(),
            private_key_pem: "PRIVATE-KEY-DATA".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("alice"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("PRIVATE-KEY-DATA"));
    }

    #[test]
    fn debug_masks_oauth_token() {
        let auth = SnowflakeAuth::OAuth {
            token: "my-token".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-token"));
    }

    #[test]
    fn serde_round_trip_oauth() {
        let auth = SnowflakeAuth::OAuth { token: "t".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"OAuth","token":"t"}"#);
        let parsed: SnowflakeAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, SnowflakeAuth::OAuth { .. }));
    }

    #[test]
    fn serde_round_trip_key_pair() {
        let json = r#"{"type":"KeyPair","user":"u","private_key_pem":"k"}"#;
        let parsed: SnowflakeAuth = serde_json::from_str(json).unwrap();
        match parsed {
            SnowflakeAuth::KeyPair {
                user,
                private_key_pem,
            } => {
                assert_eq!(user, "u");
                assert_eq!(private_key_pem, "k");
            }
            _ => panic!("expected KeyPair"),
        }
    }

    #[test]
    fn oauth_authorization_header_uses_snowflake_token_scheme() {
        let auth = SnowflakeAuth::OAuth {
            token: "my-token".into(),
        };
        let header = authorization_header(&auth, "acct").unwrap();
        assert_eq!(header, "Snowflake Token=\"my-token\"");
    }

    #[test]
    fn key_pair_with_invalid_pem_surfaces_auth_error() {
        let auth = SnowflakeAuth::KeyPair {
            user: "u".into(),
            private_key_pem: "not-a-pem".into(),
        };
        let err = authorization_header(&auth, "acct").unwrap_err();
        match err {
            FaucetError::Auth(msg) => assert!(msg.contains("invalid RSA key")),
            other => panic!("expected Auth error, got {other:?}"),
        }
    }

    #[test]
    fn token_type_matches_variant() {
        assert_eq!(
            snowflake_token_type(&SnowflakeAuth::OAuth { token: "t".into() }),
            "OAUTH"
        );
        assert_eq!(
            snowflake_token_type(&SnowflakeAuth::KeyPair {
                user: "u".into(),
                private_key_pem: "k".into()
            }),
            "KEYPAIR_JWT"
        );
    }
}
