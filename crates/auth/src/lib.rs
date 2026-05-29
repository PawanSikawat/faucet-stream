#![cfg_attr(docsrs, feature(doc_cfg))]
//! Shared, single-flight authentication providers for faucet-stream.
//!
//! These implement [`faucet_core::AuthProvider`] — a live entity that owns a
//! token cache and refresh lifecycle. One instance, wrapped in an [`Arc`], is
//! shared across every connector that references it (via the CLI `auth:` catalog
//! and `auth: { ref }`, or by a library caller cloning the `Arc`), so N
//! connectors hitting one identity provider share a single token with
//! single-flight refresh instead of racing.
//!
//! Providers:
//! - [`StaticProvider`] — a fixed, pre-minted credential.
//! - [`OAuth2ClientCredentialsProvider`] — OAuth2 `client_credentials` grant.
//! - [`OAuth2RefreshProvider`] — OAuth2 `refresh_token` grant with rotation
//!   capture (the headline: a single active access token + rotating refresh
//!   token, shared safely).
//! - [`TokenEndpointProvider`] — fetch a token from an arbitrary HTTP endpoint
//!   and extract it via JSONPath.
//!
//! [`build_provider`] constructs one from a `{ type, config }` spec (the shape
//! used by the CLI's top-level `auth:` block).
//!
//! [`Arc`]: std::sync::Arc

mod oauth2;
mod static_provider;
mod token_endpoint;

use std::sync::Arc;

use faucet_core::{FaucetError, SharedAuthProvider};
use serde_json::Value;

pub use oauth2::{OAuth2ClientCredentialsProvider, OAuth2RefreshProvider};
pub use static_provider::StaticProvider;
pub use token_endpoint::TokenEndpointProvider;

/// Default fraction of `expires_in` after which a token is proactively
/// refreshed. A token with `expires_in = 3600` is refreshed after 3240 s.
pub const DEFAULT_EXPIRY_RATIO: f64 = 0.9;

/// Build a shared [`AuthProvider`](faucet_core::AuthProvider) from a
/// `{ type, config }` spec — the shape used by the CLI's top-level `auth:`
/// catalog.
///
/// Supported `type` values: `static`, `oauth2` (client-credentials),
/// `oauth2_refresh`, `token_endpoint`.
pub fn build_provider(spec: &Value) -> Result<SharedAuthProvider, FaucetError> {
    let kind = spec
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| FaucetError::Config("auth provider: missing `type`".into()))?;
    let config = spec.get("config").cloned().unwrap_or(Value::Null);

    match kind {
        "static" => Ok(Arc::new(StaticProvider::from_config(&config)?)),
        "oauth2" => Ok(Arc::new(OAuth2ClientCredentialsProvider::from_config(&config)?)),
        "oauth2_refresh" => Ok(Arc::new(OAuth2RefreshProvider::from_config(&config)?)),
        "token_endpoint" => Ok(Arc::new(TokenEndpointProvider::from_config(&config)?)),
        other => Err(FaucetError::Config(format!(
            "auth provider: unknown type `{other}` (expected one of: static, oauth2, oauth2_refresh, token_endpoint)"
        ))),
    }
}

/// Compute the instant at which a token fetched now (with the given
/// server-reported `expires_in`, in seconds) should be treated as expired,
/// applying `expiry_ratio`. Returns `None` when the server gave no expiry.
pub(crate) fn expiry_instant(
    expires_in: Option<u64>,
    expiry_ratio: f64,
) -> Option<tokio::time::Instant> {
    expires_in.map(|secs| {
        let effective = (secs as f64 * expiry_ratio) as u64;
        tokio::time::Instant::now() + std::time::Duration::from_secs(effective)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_provider_static() {
        let spec = serde_json::json!({
            "type": "static",
            "config": { "token": "abc" }
        });
        let p = build_provider(&spec).unwrap();
        assert_eq!(p.provider_name(), "static");
    }

    #[test]
    fn build_provider_unknown_type_errors() {
        let spec = serde_json::json!({ "type": "magic", "config": {} });
        let err = build_provider(&spec).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn build_provider_missing_type_errors() {
        let spec = serde_json::json!({ "config": {} });
        assert!(build_provider(&spec).is_err());
    }
}
