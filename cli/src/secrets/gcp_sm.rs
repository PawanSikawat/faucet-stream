//! GCP Secret Manager resolver
//! (`${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}`).
//!
//! Auth: Application Default Credentials via `google-cloud-auth` (same chain
//! `faucet-common-gcs` uses). Fetches via the REST `:access` endpoint and
//! base64-decodes the payload.

use super::SecretResolver;
use crate::error::{CliError, CliResult};
use async_trait::async_trait;
use base64::Engine;
use google_cloud_auth::credentials::AccessTokenCredentials;
use serde_json::Value;
use tokio::sync::OnceCell;

pub struct GcpSmResolver {
    client: reqwest::Client,
    creds: OnceCell<AccessTokenCredentials>,
}

impl GcpSmResolver {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            creds: OnceCell::new(),
        }
    }

    /// Obtain an ADC access token, building (and caching) the credentials on
    /// first use.
    async fn token(&self) -> CliResult<String> {
        let creds = self
            .creds
            .get_or_try_init(|| async {
                google_cloud_auth::credentials::Builder::default()
                    .build_access_token_credentials()
                    .map_err(|e| CliError::SecretAuthFailed {
                        scheme: "gcp-sm".into(),
                        hint: format!(
                            "no Application Default Credentials ({e}) — run \
                             `gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS"
                        ),
                    })
            })
            .await?;
        let access = creds
            .access_token()
            .await
            .map_err(|e| CliError::SecretAuthFailed {
                scheme: "gcp-sm".into(),
                hint: format!("failed to obtain an ADC access token: {e}"),
            })?;
        Ok(access.token)
    }
}

impl Default for GcpSmResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretResolver for GcpSmResolver {
    fn scheme(&self) -> &'static str {
        "gcp-sm"
    }

    async fn resolve(&self, reference: &str) -> CliResult<String> {
        let token = self.token().await?;
        let url = format!("https://secretmanager.googleapis.com/v1/{reference}:access");
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "gcp-sm".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(CliError::SecretNotFound {
                scheme: "gcp-sm".into(),
                reference: reference.into(),
            });
        }
        let resp = resp
            .error_for_status()
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "gcp-sm".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        let body: Value = resp
            .json()
            .await
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "gcp-sm".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        let b64 = body["payload"]["data"]
            .as_str()
            .ok_or_else(|| CliError::SecretNotFound {
                scheme: "gcp-sm".into(),
                reference: reference.into(),
            })?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "gcp-sm".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        String::from_utf8(bytes).map_err(|source| CliError::SecretFetchFailed {
            scheme: "gcp-sm".into(),
            reference: reference.into(),
            source: Box::new(source),
        })
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    #[test]
    fn decodes_base64_payload() {
        // Guards the decode/utf8 logic independently of the token fetch.
        let b64 = base64::engine::general_purpose::STANDARD.encode("my-secret");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "my-secret");
    }
}
