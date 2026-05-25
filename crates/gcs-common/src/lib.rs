//! Shared GCS credential and client construction for faucet source and
//! sink connectors.

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
}
