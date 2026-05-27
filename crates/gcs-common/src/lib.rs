#![cfg_attr(docsrs, feature(doc_cfg))]

//! Shared GCS credential and client construction for faucet source and
//! sink connectors.

use faucet_core::FaucetError;
use google_cloud_storage::client::{Storage, StorageControl};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Credential source for a GCS client.
///
/// Tagged enum so YAML/JSON configs read naturally:
/// `{ method: "service_account_json_file", path: "/run/secrets/sa.json" }`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum GcsCredentials {
    /// Path to a service-account JSON key file on disk.
    ServiceAccountJsonFile { path: String },
    /// Service-account JSON key as an inline string. Useful for
    /// environment-variable injection via `${env:GCP_SA_JSON}` in CLI
    /// configs.
    ServiceAccountJsonInline { json: String },
    /// Application Default Credentials — honours
    /// `GOOGLE_APPLICATION_CREDENTIALS`, gcloud user creds, and the
    /// GCE/GKE metadata server, in that order.
    #[default]
    ApplicationDefault,
    /// Anonymous credentials. Use this with emulators (e.g.
    /// `fake-gcs-server`) that do not validate bearer tokens — the SDK
    /// otherwise tries to fetch ADC tokens at request time and fails in
    /// environments without GCP credentials.
    Anonymous,
}

/// Build a `google_cloud_auth::credentials::Credentials` from a faucet
/// credential spec. All failures map to `FaucetError::Auth`.
pub async fn build_credentials(
    creds: &GcsCredentials,
) -> Result<google_cloud_auth::credentials::Credentials, FaucetError> {
    match creds {
        GcsCredentials::ApplicationDefault => google_cloud_auth::credentials::Builder::default()
            .build()
            .map_err(|e| FaucetError::Auth(format!("GCS auth (ADC): {e}"))),
        GcsCredentials::Anonymous => {
            Ok(google_cloud_auth::credentials::anonymous::Builder::new().build())
        }
        GcsCredentials::ServiceAccountJsonFile { path } => {
            let bytes = tokio::fs::read(path).await.map_err(|e| {
                FaucetError::Auth(format!(
                    "GCS auth: could not read service-account key from '{path}': {e}"
                ))
            })?;
            let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                FaucetError::Auth(format!(
                    "GCS auth: service-account key at '{path}' is not valid JSON: {e}"
                ))
            })?;
            google_cloud_auth::credentials::service_account::Builder::new(value)
                .build()
                .map_err(|e| FaucetError::Auth(format!("GCS auth (service account): {e}")))
        }
        GcsCredentials::ServiceAccountJsonInline { json } => {
            let value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
                FaucetError::Auth(format!(
                    "GCS auth: inline service-account key is not valid JSON: {e}"
                ))
            })?;
            google_cloud_auth::credentials::service_account::Builder::new(value)
                .build()
                .map_err(|e| FaucetError::Auth(format!("GCS auth (service account): {e}")))
        }
    }
}

/// Build a data-plane [`Storage`] client. Accepts an optional storage-host
/// override for integration tests (e.g. fake-gcs-server at
/// `http://localhost:4443`).
pub async fn build_storage(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<Storage, FaucetError> {
    let credentials = build_credentials(creds).await?;
    let mut builder = Storage::builder().with_credentials(credentials);
    if let Some(host) = storage_host {
        builder = builder.with_endpoint(host.to_string());
    }
    builder
        .build()
        .await
        .map_err(|e| FaucetError::Auth(format!("GCS client build failed: {e}")))
}

/// Build a control-plane [`StorageControl`] client.
pub async fn build_storage_control(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<StorageControl, FaucetError> {
    let credentials = build_credentials(creds).await?;
    let mut builder = StorageControl::builder().with_credentials(credentials);
    if let Some(host) = storage_host {
        builder = builder.with_endpoint(host.to_string());
    }
    builder
        .build()
        .await
        .map_err(|e| FaucetError::Auth(format!("GCS control client build failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn credentials_serde_application_default() {
        let creds = GcsCredentials::ApplicationDefault;
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v, json!({"method": "application_default"}));
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(matches!(back, GcsCredentials::ApplicationDefault));
    }

    #[test]
    fn credentials_serde_service_account_json_file() {
        let creds = GcsCredentials::ServiceAccountJsonFile {
            path: "/run/secrets/sa.json".into(),
        };
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(
            v,
            json!({"method": "service_account_json_file", "path": "/run/secrets/sa.json"})
        );
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(
            matches!(back, GcsCredentials::ServiceAccountJsonFile { path } if path == "/run/secrets/sa.json")
        );
    }

    #[test]
    fn credentials_serde_service_account_json_inline() {
        let creds = GcsCredentials::ServiceAccountJsonInline {
            json: "{\"client_email\":\"x@y\"}".into(),
        };
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v["method"], "service_account_json_inline");
        assert!(v["json"].as_str().unwrap().contains("client_email"));
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(matches!(
            back,
            GcsCredentials::ServiceAccountJsonInline { .. }
        ));
    }

    #[test]
    fn credentials_default_is_application_default() {
        let creds = GcsCredentials::default();
        assert!(matches!(creds, GcsCredentials::ApplicationDefault));
    }

    #[test]
    fn credentials_serde_anonymous() {
        let creds = GcsCredentials::Anonymous;
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v, json!({"method": "anonymous"}));
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(matches!(back, GcsCredentials::Anonymous));
    }

    #[tokio::test]
    async fn build_credentials_anonymous_succeeds() {
        let creds = build_credentials(&GcsCredentials::Anonymous).await.unwrap();
        // Smoke check: the call returns a real Credentials handle. We
        // can't easily assert on its internal type, but the fact that
        // it returned `Ok` (vs. needing GCP creds in the environment)
        // is the point of the variant.
        let _ = creds;
    }

    #[tokio::test]
    async fn build_credentials_rejects_missing_file() {
        let creds = GcsCredentials::ServiceAccountJsonFile {
            path: "/definitely/does/not/exist/sa.json".into(),
        };
        let err = build_credentials(&creds).await.unwrap_err();
        assert!(matches!(err, FaucetError::Auth(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("could not read") || msg.contains("No such file"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn build_credentials_rejects_invalid_inline_json() {
        let creds = GcsCredentials::ServiceAccountJsonInline {
            json: "not-json".into(),
        };
        let err = build_credentials(&creds).await.unwrap_err();
        assert!(matches!(err, FaucetError::Auth(_)));
    }
}
