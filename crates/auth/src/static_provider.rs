//! A provider that always returns a fixed, pre-minted credential.

use async_trait::async_trait;
use faucet_core::{AuthProvider, Credential, FaucetError};
use serde_json::Value;

/// Returns a fixed [`Credential`] forever. Useful for pre-minted tokens whose
/// lifetime exceeds the run, or for sharing one static bearer token across many
/// connectors.
#[derive(Debug, Clone)]
pub struct StaticProvider {
    credential: Credential,
}

impl StaticProvider {
    /// A static `Authorization: Bearer <token>`.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            credential: Credential::Bearer(token.into()),
        }
    }

    /// A static credential of any [`Credential`] shape.
    pub fn new(credential: Credential) -> Self {
        Self { credential }
    }

    /// Build from a `{ token }` / `{ header, value }` / `{ username, password }`
    /// config object.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        if let Some(token) = config.get("token").and_then(Value::as_str) {
            return Ok(Self::bearer(token));
        }
        if let (Some(name), Some(value)) = (
            config.get("header").and_then(Value::as_str),
            config.get("value").and_then(Value::as_str),
        ) {
            return Ok(Self::new(Credential::Header {
                name: name.to_string(),
                value: value.to_string(),
            }));
        }
        if let (Some(username), Some(password)) = (
            config.get("username").and_then(Value::as_str),
            config.get("password").and_then(Value::as_str),
        ) {
            return Ok(Self::new(Credential::Basic {
                username: username.to_string(),
                password: password.to_string(),
            }));
        }
        Err(FaucetError::Config(
            "static auth provider: config must contain `token`, `header`+`value`, or `username`+`password`".into(),
        ))
    }
}

#[async_trait]
impl AuthProvider for StaticProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(self.credential.clone())
    }

    fn provider_name(&self) -> &'static str {
        "static"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bearer_round_trips() {
        let p = StaticProvider::bearer("t");
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("t".into())
        );
    }

    #[tokio::test]
    async fn from_config_bearer() {
        let p = StaticProvider::from_config(&serde_json::json!({"token": "abc"})).unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("abc".into())
        );
    }

    #[tokio::test]
    async fn from_config_header() {
        let p =
            StaticProvider::from_config(&serde_json::json!({"header": "X-Api-Key", "value": "k"}))
                .unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Header {
                name: "X-Api-Key".into(),
                value: "k".into()
            }
        );
    }

    #[test]
    fn from_config_empty_errors() {
        assert!(StaticProvider::from_config(&serde_json::json!({})).is_err());
    }
}
