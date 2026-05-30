//! Azure Key Vault resolver (`${azure-kv:<vault>/<secret>[/<version>]}`).
//!
//! Auth: chained credential that tries, in order:
//!   1. `ClientSecretCredential` if AZURE_TENANT_ID + AZURE_CLIENT_ID +
//!      AZURE_CLIENT_SECRET are all set.
//!   2. `ManagedIdentityCredential` (Azure VM / App Service / AKS pod identity).
//!   3. `DeveloperToolsCredential` (Azure CLI / Azure Developer CLI).
//!
//! No emulator exists; a live test gated on `AZURE_TEST=1` may be added later.

use super::SecretResolver;
use crate::error::{CliError, CliResult};
use async_trait::async_trait;
use azure_core::credentials::{AccessToken, Secret, TokenCredential, TokenRequestOptions};
use azure_identity::{
    ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
};
use std::sync::Arc;

// ── Chained credential ────────────────────────────────────────────────────────

/// A lightweight credential chain: tries each source in order, returning the
/// first successful token or aggregating errors if all fail.
struct ChainedCredential {
    sources: Vec<Arc<dyn TokenCredential>>,
}

impl ChainedCredential {
    fn new(sources: Vec<Arc<dyn TokenCredential>>) -> Arc<Self> {
        Arc::new(Self { sources })
    }
}

impl std::fmt::Debug for ChainedCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainedCredential")
            .field("sources_len", &self.sources.len())
            .finish()
    }
}

#[async_trait]
impl TokenCredential for ChainedCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        let mut errors = Vec::new();
        for source in &self.sources {
            match source.get_token(scopes, options.clone()).await {
                Ok(token) => return Ok(token),
                Err(e) => errors.push(e.to_string()),
            }
        }
        Err(azure_core::Error::with_message(
            azure_core::error::ErrorKind::Credential,
            format!(
                "All Azure credential sources failed:\n{}",
                errors.join("\n")
            ),
        ))
    }
}

// ── Credential builder ────────────────────────────────────────────────────────

/// Build the default Azure credential chain for this resolver.
///
/// Returns `CliError::SecretAuthFailed` if even the chain cannot be constructed
/// (which should only happen if the Azure identity library itself is broken).
fn build_credential() -> CliResult<Arc<dyn TokenCredential>> {
    let mut sources: Vec<Arc<dyn TokenCredential>> = Vec::new();

    // 1. Service-principal via env vars (AZURE_TENANT_ID + AZURE_CLIENT_ID +
    //    AZURE_CLIENT_SECRET). Try only when all three are present.
    if let (Ok(tenant_id), Ok(client_id), Ok(client_secret)) = (
        std::env::var("AZURE_TENANT_ID"),
        std::env::var("AZURE_CLIENT_ID"),
        std::env::var("AZURE_CLIENT_SECRET"),
    ) {
        match ClientSecretCredential::new(
            &tenant_id,
            client_id,
            Secret::new(client_secret),
            None,
        ) {
            Ok(cred) => sources.push(cred),
            Err(e) => {
                tracing::debug!("Azure ClientSecretCredential build failed: {e}");
            }
        }
    }

    // 2. Managed identity (VM / App Service / AKS).
    match ManagedIdentityCredential::new(None) {
        Ok(cred) => sources.push(cred),
        Err(e) => tracing::debug!("Azure ManagedIdentityCredential build failed: {e}"),
    }

    // 3. Developer tools (Azure CLI / Azure Developer CLI).
    match DeveloperToolsCredential::new(None) {
        Ok(cred) => sources.push(cred),
        Err(e) => tracing::debug!("Azure DeveloperToolsCredential build failed: {e}"),
    }

    if sources.is_empty() {
        return Err(CliError::SecretAuthFailed {
            scheme: "azure-kv".into(),
            hint: "no Azure credentials available — set AZURE_TENANT_ID / AZURE_CLIENT_ID / \
                   AZURE_CLIENT_SECRET, use a managed identity, or run `az login`"
                .into(),
        });
    }

    Ok(ChainedCredential::new(sources))
}

// ── Resolver ──────────────────────────────────────────────────────────────────

pub struct AzureKvResolver;

impl AzureKvResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AzureKvResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretResolver for AzureKvResolver {
    fn scheme(&self) -> &'static str {
        "azure-kv"
    }

    async fn resolve(&self, reference: &str) -> CliResult<String> {
        // reference = "<vault>/<secret>[/<version>]"
        let mut parts = reference.splitn(3, '/');

        let vault = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
            CliError::SecretFetchFailed {
                scheme: "azure-kv".into(),
                reference: reference.into(),
                source: "expected '<vault>/<secret>[/<version>]'".into(),
            }
        })?;
        let secret_name = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
            CliError::SecretFetchFailed {
                scheme: "azure-kv".into(),
                reference: reference.into(),
                source: "expected '<vault>/<secret>[/<version>]'".into(),
            }
        })?;
        let version = parts.next().filter(|s| !s.is_empty());

        let credential = build_credential()?;

        let vault_url = format!("https://{vault}.vault.azure.net/");
        let client =
            azure_security_keyvault_secrets::SecretClient::new(&vault_url, credential, None)
                .map_err(|e| CliError::SecretFetchFailed {
                    scheme: "azure-kv".into(),
                    reference: reference.into(),
                    source: Box::new(e),
                })?;

        // Version goes in the options struct, NOT a positional arg.
        let options = version.map(|v| {
            azure_security_keyvault_secrets::models::SecretClientGetSecretOptions {
                secret_version: Some(v.to_string()),
                ..Default::default()
            }
        });

        let resp = client
            .get_secret(secret_name, options)
            .await
            .map_err(|e| CliError::SecretFetchFailed {
                scheme: "azure-kv".into(),
                reference: reference.into(),
                source: Box::new(e),
            })?;

        // `Response<Secret>::into_model()` is sync and deserializes the body
        // into the typed `Secret` model (the JSON format is inferred from F).
        let secret = resp
            .into_model()
            .map_err(|e| CliError::SecretFetchFailed {
                scheme: "azure-kv".into(),
                reference: reference.into(),
                source: Box::new(e),
            })?;

        secret.value.ok_or_else(|| CliError::SecretNotFound {
            scheme: "azure-kv".into(),
            reference: reference.into(),
        })
    }
}
