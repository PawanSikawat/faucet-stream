//! Shared, connector-agnostic authentication abstraction.
//!
//! Multiple connectors that authenticate against the **same** system (e.g. four
//! matrix rows reading from one Snowflake account, or four endpoints of one REST
//! API) can share a single [`AuthProvider`]. A provider is a live entity that
//! owns the token cache and refresh lifecycle; connectors hold an [`Arc`] to it
//! and ask for the current [`Credential`] per request, so N connectors share one
//! token with single-flight refresh instead of racing to refresh it.
//!
//! - [`Credential`] — a resolved credential (bearer token, header, basic auth).
//! - [`AuthProvider`] — an object-safe trait yielding credentials, with
//!   single-flight refresh implemented by the provider.
//! - [`AuthSpec`] — a connector config field that is **either** inline auth
//!   `{ type, config }` **or** a `{ ref: <name> }` pointer to a shared provider.
//!
//! The HTTP-based provider implementations (OAuth2, token-endpoint) live in the
//! separate `faucet-auth` crate so `faucet-core` stays free of an HTTP-client
//! dependency.

use crate::FaucetError;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Arc;

/// A resolved credential produced by an [`AuthProvider`] or built from inline
/// auth config. Connectors map this onto their wire protocol (HTTP header, gRPC
/// metadata, …).
///
/// Intentionally **not** `#[non_exhaustive]`: connectors must map every variant,
/// so adding one should be a compile error that forces correct handling rather
/// than a silently-ignored fallback.
#[derive(Clone, PartialEq, Eq)]
pub enum Credential {
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// An explicit header name + value.
    Header {
        /// Header name (e.g. `Authorization`, `X-Api-Key`).
        name: String,
        /// Header value.
        value: String,
    },
    /// HTTP Basic credentials.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// A raw token for connector-specific assembly (e.g. gRPC `authorization`
    /// metadata, Snowflake's `Authorization` with a token-type header).
    Token(String),
}

// `Debug` is hand-written (not derived) so a `{:?}` of a credential — or of any
// struct that embeds one, e.g. a logged connector config or a `StaticProvider` —
// never prints the secret in clear. The secret-bearing fields render as `"***"`;
// the non-secret identifiers (header name, basic-auth username) stay visible so
// the output is still useful for diagnostics. `Credential` is a 1.0-frozen public
// type, so this redaction is part of its contract.
impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credential::Bearer(_) => f.debug_tuple("Bearer").field(&"***").finish(),
            Credential::Header { name, .. } => f
                .debug_struct("Header")
                .field("name", name)
                .field("value", &"***")
                .finish(),
            Credential::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Credential::Token(_) => f.debug_tuple("Token").field(&"***").finish(),
        }
    }
}

impl Credential {
    /// The value to use for an `Authorization` header, when this credential maps
    /// to one. Returns `None` for credentials that are applied differently
    /// (e.g. [`Credential::Basic`], which connectors apply via basic-auth, or
    /// [`Credential::Header`], which carries its own name).
    pub fn authorization_value(&self) -> Option<String> {
        match self {
            Credential::Bearer(t) => Some(format!("Bearer {t}")),
            Credential::Token(t) => Some(t.clone()),
            Credential::Header { .. } | Credential::Basic { .. } => None,
        }
    }
}

/// A live, shareable source of credentials.
///
/// One instance is shared (via [`Arc`]) across all connectors that reference it,
/// giving single-flight refresh: concurrent callers during a refresh await the
/// one in-flight fetch rather than each refreshing independently.
///
/// Object-safe — no generics or associated types, so it can be held as
/// `Arc<dyn AuthProvider>` ([`SharedAuthProvider`]).
#[async_trait]
pub trait AuthProvider: Send + Sync + std::fmt::Debug {
    /// Return a currently-valid credential, refreshing if needed.
    async fn credential(&self) -> Result<Credential, FaucetError>;

    /// Force a refresh **iff** the cached credential still equals `stale`
    /// (compare-and-swap). Multiple connectors that hit a `401` with the same
    /// token collapse into a single refresh; callers holding an already-rotated
    /// token get the new one without triggering another fetch.
    ///
    /// The default delegates to [`AuthProvider::credential`]; providers that
    /// support refresh override it.
    async fn invalidate(&self, _stale: &Credential) -> Result<Credential, FaucetError> {
        self.credential().await
    }

    /// Stable, non-empty name for diagnostics and metrics.
    fn provider_name(&self) -> &'static str;
}

/// A shared [`AuthProvider`] handle. Cloning it shares the one live provider
/// (and its single token cache) across connectors.
pub type SharedAuthProvider = Arc<dyn AuthProvider>;

/// A `{ ref: <name> }` pointer to a named provider in the top-level `auth:`
/// catalog. The only permitted key is `ref`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthReference {
    /// Name of the provider in the top-level `auth:` catalog.
    #[serde(rename = "ref")]
    pub name: String,
}

/// A connector's `auth:` field: **either** an inline auth definition `A`
/// (the `{ type, config }` shape), **or** a `{ ref: <name> }` reference to a
/// shared provider defined in the top-level `auth:` catalog.
///
/// `ref` is mutually exclusive with inline fields — supplying both is a
/// deserialization error.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum AuthSpec<A> {
    /// Inline auth, spelled out on the connector.
    Inline(A),
    /// A reference to a shared provider in the top-level `auth:` catalog.
    Reference(AuthReference),
}

impl<A: Default> Default for AuthSpec<A> {
    fn default() -> Self {
        AuthSpec::Inline(A::default())
    }
}

impl<A> AuthSpec<A> {
    /// The inline auth, if this is not a reference.
    pub fn inline(&self) -> Option<&A> {
        match self {
            AuthSpec::Inline(a) => Some(a),
            AuthSpec::Reference(_) => None,
        }
    }

    /// The referenced provider name, if this is a reference.
    pub fn reference_name(&self) -> Option<&str> {
        match self {
            AuthSpec::Reference(r) => Some(&r.name),
            AuthSpec::Inline(_) => None,
        }
    }
}

// Manual `Deserialize` enforces the `ref`-XOR-inline rule, which a plain
// `#[serde(untagged)]` derive cannot (it would silently ignore extra keys).
impl<'de, A> Deserialize<'de> for AuthSpec<A>
where
    A: serde::de::DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let has_ref = value.get("ref").is_some();
        if has_ref {
            let has_other = value
                .as_object()
                .map(|o| o.keys().any(|k| k != "ref"))
                .unwrap_or(false);
            if has_other {
                return Err(serde::de::Error::custom(
                    "auth: `ref` cannot be combined with inline auth fields (type/config)",
                ));
            }
            let r: AuthReference =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            return Ok(AuthSpec::Reference(r));
        }
        let inner: A = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(AuthSpec::Inline(inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(tag = "type", content = "config", rename_all = "snake_case")]
    enum StubAuth {
        None,
        Bearer { token: String },
    }

    #[test]
    fn credential_authorization_value() {
        assert_eq!(
            Credential::Bearer("abc".into()).authorization_value(),
            Some("Bearer abc".to_string())
        );
        assert_eq!(
            Credential::Token("Custom xyz".into()).authorization_value(),
            Some("Custom xyz".to_string())
        );
        assert_eq!(
            Credential::Basic {
                username: "u".into(),
                password: "p".into()
            }
            .authorization_value(),
            None
        );
        assert_eq!(
            Credential::Header {
                name: "X-Api-Key".into(),
                value: "k".into()
            }
            .authorization_value(),
            None
        );
    }

    #[test]
    fn authspec_parses_inline() {
        let j = serde_json::json!({"type": "bearer", "config": {"token": "t"}});
        let s: AuthSpec<StubAuth> = serde_json::from_value(j).unwrap();
        match s {
            AuthSpec::Inline(StubAuth::Bearer { token }) => assert_eq!(token, "t"),
            other => panic!("expected inline bearer, got {other:?}"),
        }
    }

    #[test]
    fn authspec_parses_inline_unit_variant() {
        let j = serde_json::json!({"type": "none"});
        let s: AuthSpec<StubAuth> = serde_json::from_value(j).unwrap();
        assert!(matches!(s, AuthSpec::Inline(StubAuth::None)));
    }

    #[test]
    fn authspec_parses_ref() {
        let j = serde_json::json!({"ref": "sf"});
        let s: AuthSpec<StubAuth> = serde_json::from_value(j).unwrap();
        assert_eq!(s.reference_name(), Some("sf"));
    }

    #[test]
    fn authspec_rejects_ref_plus_inline() {
        let j = serde_json::json!({"ref": "sf", "type": "bearer"});
        let r: Result<AuthSpec<StubAuth>, _> = serde_json::from_value(j);
        assert!(r.is_err(), "ref + inline must be rejected");
    }

    #[derive(Debug)]
    struct Fixed(Credential);

    #[async_trait]
    impl AuthProvider for Fixed {
        async fn credential(&self) -> Result<Credential, FaucetError> {
            Ok(self.0.clone())
        }
        fn provider_name(&self) -> &'static str {
            "fixed"
        }
    }

    #[test]
    fn credential_debug_redacts_secrets() {
        // Bearer / Token fully redact the secret value.
        let b = format!("{:?}", Credential::Bearer("supersecrettoken".into()));
        assert!(!b.contains("supersecrettoken"), "bearer token leaked: {b}");
        assert!(b.contains("***"), "bearer token not masked: {b}");

        let t = format!("{:?}", Credential::Token("tok-supersecretxyz".into()));
        assert!(!t.contains("tok-supersecretxyz"), "raw token leaked: {t}");
        assert!(t.contains("***"), "raw token not masked: {t}");

        // Basic redacts the password but keeps the (non-secret) username.
        let basic = format!(
            "{:?}",
            Credential::Basic {
                username: "alice".into(),
                password: "hunter2secret".into(),
            }
        );
        assert!(!basic.contains("hunter2secret"), "password leaked: {basic}");
        assert!(
            basic.contains("alice"),
            "username should stay visible for diagnostics: {basic}"
        );

        // Header redacts the value but keeps the (non-secret) header name.
        let header = format!(
            "{:?}",
            Credential::Header {
                name: "X-Api-Key".into(),
                value: "secretkeyvalue".into(),
            }
        );
        assert!(
            !header.contains("secretkeyvalue"),
            "header value leaked: {header}"
        );
        assert!(
            header.contains("X-Api-Key"),
            "header name should stay visible for diagnostics: {header}"
        );
    }

    #[tokio::test]
    async fn auth_provider_default_invalidate_returns_current() {
        let p = Fixed(Credential::Bearer("x".into()));
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("x".into())
        );
        // Default invalidate just returns the current credential.
        assert_eq!(
            p.invalidate(&Credential::Bearer("old".into()))
                .await
                .unwrap(),
            Credential::Bearer("x".into())
        );
    }
}
