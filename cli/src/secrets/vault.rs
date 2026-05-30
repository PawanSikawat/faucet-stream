//! HashiCorp Vault KV v2 resolver (`${vault:<path>[#field]}`).
//!
//! Auth: `VAULT_ADDR` + `VAULT_TOKEN`, optional `VAULT_NAMESPACE`. Pure HTTP
//! via `reqwest` — no Vault client dependency.

use super::{extract_field, split_field, SecretResolver};
use crate::error::{CliError, CliResult};
use async_trait::async_trait;
use serde_json::Value;

pub struct VaultResolver {
    addr: String,
    token: String,
    namespace: Option<String>,
    client: reqwest::Client,
}

impl VaultResolver {
    /// Build from `VAULT_ADDR` / `VAULT_TOKEN` (+ optional `VAULT_NAMESPACE`).
    pub fn from_env() -> CliResult<Self> {
        let addr = std::env::var("VAULT_ADDR").map_err(|_| CliError::SecretAuthFailed {
            scheme: "vault".into(),
            hint: "set VAULT_ADDR (e.g. https://vault.example.com:8200)".into(),
        })?;
        let token = std::env::var("VAULT_TOKEN").map_err(|_| CliError::SecretAuthFailed {
            scheme: "vault".into(),
            hint: "set VAULT_TOKEN".into(),
        })?;
        Ok(Self {
            addr: addr.trim_end_matches('/').to_owned(),
            token,
            namespace: std::env::var("VAULT_NAMESPACE").ok(),
            client: reqwest::Client::new(),
        })
    }
}

#[async_trait]
impl SecretResolver for VaultResolver {
    fn scheme(&self) -> &'static str {
        "vault"
    }

    async fn resolve(&self, reference: &str) -> CliResult<String> {
        let (path, field) = split_field(reference);
        let url = format!("{}/v1/{}", self.addr, path);
        let mut req = self.client.get(&url).header("X-Vault-Token", &self.token);
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns);
        }
        let resp = req.send().await.map_err(|source| CliError::SecretFetchFailed {
            scheme: "vault".into(),
            reference: reference.into(),
            source: Box::new(source),
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(CliError::SecretNotFound {
                scheme: "vault".into(),
                reference: reference.into(),
            });
        }
        if resp.status() == reqwest::StatusCode::FORBIDDEN
            || resp.status() == reqwest::StatusCode::UNAUTHORIZED
        {
            return Err(CliError::SecretAuthFailed {
                scheme: "vault".into(),
                hint: "VAULT_TOKEN was rejected (403/401) — check the token and its policy".into(),
            });
        }
        let resp = resp.error_for_status().map_err(|source| CliError::SecretFetchFailed {
            scheme: "vault".into(),
            reference: reference.into(),
            source: Box::new(source),
        })?;
        let body: Value = resp.json().await.map_err(|source| CliError::SecretFetchFailed {
            scheme: "vault".into(),
            reference: reference.into(),
            source: Box::new(source),
        })?;
        // KV v2: secret map lives at .data.data
        let data = &body["data"]["data"];
        match field {
            Some(f) => extract_field("vault", reference, &data.to_string(), f),
            None => Ok(data.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn reads_kv_v2_field() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/app"))
            .and(header("X-Vault-Token", "test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "data": { "token": "s3cr3t-value", "other": "x" } }
            })))
            .mount(&server)
            .await;

        let resolver = VaultResolver {
            addr: server.uri(),
            token: "test-token".into(),
            namespace: None,
            client: reqwest::Client::new(),
        };
        let v = resolver.resolve("secret/data/app#token").await.unwrap();
        assert_eq!(v, "s3cr3t-value");
    }

    #[tokio::test]
    async fn missing_secret_is_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/nope"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let resolver = VaultResolver {
            addr: server.uri(),
            token: "t".into(),
            namespace: None,
            client: reqwest::Client::new(),
        };
        match resolver.resolve("secret/data/nope#x").await.unwrap_err() {
            CliError::SecretNotFound { .. } => {}
            other => panic!("expected SecretNotFound, got {other:?}"),
        }
    }
}
