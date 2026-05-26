//! # faucet-bigquery-common
//!
//! Shared credential configuration and client construction for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) BigQuery source
//! and sink connectors.
//!
//! - [`BigQueryCredentials`] — service-account key file, inline service-account
//!   JSON, or Application Default Credentials.
//! - [`build_client`] — async helper that turns a [`BigQueryCredentials`] into
//!   a ready-to-use [`gcp_bigquery_client::Client`].
//!
//! `BigQueryCredentials` derives `Serialize`, `Deserialize`, and `JsonSchema`
//! so it round-trips through YAML/JSON configs and CLI introspection. Its
//! `Debug` impl masks inline JSON as `"***"` while leaving the key path
//! visible.

use faucet_core::FaucetError;
use gcp_bigquery_client::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to authenticate with Google BigQuery.
///
/// **Wire-format note:** the `#[serde(tag = "type", content = "value")]`
/// discriminator uses PascalCase variant names (`"ServiceAccountKeyPath"`,
/// `"ServiceAccountKey"`, `"ApplicationDefault"`) for byte-compatibility with
/// existing YAML configs that predate the extraction of this crate from
/// `faucet-sink-bigquery`. Do not add `#[serde(rename_all = "snake_case")]`
/// here — it would silently break those configs.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "value")]
pub enum BigQueryCredentials {
    /// Path to a service account JSON key file.
    ServiceAccountKeyPath(String),
    /// Inline service account JSON key content.
    ServiceAccountKey(String),
    /// Use application default credentials (e.g. workload identity, `gcloud auth`).
    ApplicationDefault,
}

impl std::fmt::Debug for BigQueryCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServiceAccountKeyPath(path) => {
                f.debug_tuple("ServiceAccountKeyPath").field(path).finish()
            }
            Self::ServiceAccountKey(_) => write!(f, "ServiceAccountKey(***)"),
            Self::ApplicationDefault => write!(f, "ApplicationDefault"),
        }
    }
}

/// Build a [`gcp_bigquery_client::Client`] from a faucet credential spec.
///
/// Returns [`FaucetError::Auth`] on authentication failures and on inline
/// service-account JSON that fails to parse.
pub async fn build_client(creds: &BigQueryCredentials) -> Result<Client, FaucetError> {
    match creds {
        BigQueryCredentials::ServiceAccountKeyPath(path) => {
            Client::from_service_account_key_file(path)
                .await
                .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}")))
        }
        BigQueryCredentials::ServiceAccountKey(json) => {
            let sa_key = serde_json::from_str(json)
                .map_err(|e| FaucetError::Auth(format!("invalid service account JSON: {e}")))?;
            Client::from_service_account_key(sa_key, false)
                .await
                .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}")))
        }
        BigQueryCredentials::ApplicationDefault => Client::from_application_default_credentials()
            .await
            .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_masks_inline_service_account_key() {
        let creds = BigQueryCredentials::ServiceAccountKey("secret-json".into());
        let debug = format!("{creds:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret-json"));
    }

    #[test]
    fn debug_does_not_mask_service_account_key_path() {
        let creds = BigQueryCredentials::ServiceAccountKeyPath("/path/to/key.json".into());
        let debug = format!("{creds:?}");
        assert!(debug.contains("/path/to/key.json"));
    }

    #[test]
    fn debug_application_default_is_plain() {
        let creds = BigQueryCredentials::ApplicationDefault;
        assert_eq!(format!("{creds:?}"), "ApplicationDefault");
    }

    #[test]
    fn serde_round_trip_application_default() {
        let json = serde_json::to_string(&BigQueryCredentials::ApplicationDefault).unwrap();
        let parsed: BigQueryCredentials = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, BigQueryCredentials::ApplicationDefault));
    }

    #[test]
    fn serde_round_trip_service_account_key_path() {
        let creds = BigQueryCredentials::ServiceAccountKeyPath("/k.json".into());
        let json = serde_json::to_string(&creds).unwrap();
        assert_eq!(
            json,
            r#"{"type":"ServiceAccountKeyPath","value":"/k.json"}"#
        );
        let parsed: BigQueryCredentials = serde_json::from_str(&json).unwrap();
        match parsed {
            BigQueryCredentials::ServiceAccountKeyPath(p) => assert_eq!(p, "/k.json"),
            _ => panic!("expected ServiceAccountKeyPath"),
        }
    }

    #[tokio::test]
    async fn build_client_with_invalid_inline_json_surfaces_auth_error() {
        let creds = BigQueryCredentials::ServiceAccountKey("not-json".into());
        match build_client(&creds).await {
            Ok(_) => panic!("expected auth error"),
            Err(FaucetError::Auth(_)) => {}
            Err(other) => panic!("expected Auth error, got {other:?}"),
        }
    }
}
