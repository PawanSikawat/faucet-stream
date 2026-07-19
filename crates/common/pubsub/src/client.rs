//! Pub/Sub client construction. **This is the only module in the crate that
//! touches the `gcloud-pubsub` / `gcloud-auth` SDK**, so a real-compile fixup
//! (if the pinned SDK version's API differs) is localised here.

use crate::config::{PubsubConnection, PubsubCredentials};
use faucet_core::FaucetError;
use gcloud_auth::credentials::CredentialsFile;
use gcloud_pubsub::client::{Client, ClientConfig};

fn auth_err(context: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Auth(format!("pubsub auth ({context}): {e}"))
}

/// Build a Pub/Sub [`Client`] from a [`PubsubConnection`].
///
/// * When an emulator host is configured (config `emulator_host` or the
///   `PUBSUB_EMULATOR_HOST` env var), auth is skipped entirely and the client
///   talks plaintext to the emulator.
/// * Otherwise credentials are resolved per [`PubsubCredentials`] — ADC, a
///   service-account key file, or an inline key — and all failures map to
///   [`FaucetError::Auth`].
///
/// Construction is I/O-light: ADC token acquisition may touch the metadata
/// server, but gRPC channels connect lazily on first RPC.
pub async fn build_client(conn: &PubsubConnection) -> Result<Client, FaucetError> {
    // The SDK reads `PUBSUB_EMULATOR_HOST` when assembling `ClientConfig`.
    // Export an explicit config value so both signals converge on one code
    // path. Connectors build their client once at startup, before spawning
    // any worker task, so this process-env write is not racing readers.
    if let Some(host) = &conn.emulator_host
        && std::env::var_os("PUBSUB_EMULATOR_HOST").is_none()
    {
        // SAFETY: single-threaded startup path (no concurrent env access).
        unsafe {
            std::env::set_var("PUBSUB_EMULATOR_HOST", host);
        }
    }

    let mut config = ClientConfig::default();
    if let Some(project) = &conn.project_id {
        config.project_id = Some(project.clone());
    }
    if let Some(endpoint) = &conn.endpoint {
        config.endpoint = endpoint.clone();
    }

    // Emulator (or explicitly anonymous): no credentials.
    let use_auth = conn.effective_emulator_host().is_none()
        && conn.credentials != PubsubCredentials::Anonymous;

    let config = if use_auth {
        match &conn.credentials {
            PubsubCredentials::Anonymous => config, // unreachable: filtered above
            PubsubCredentials::ApplicationDefault => config
                .with_auth()
                .await
                .map_err(|e| auth_err("application default", e))?,
            PubsubCredentials::ServiceAccountJsonFile { path } => {
                let cf = CredentialsFile::new_from_file(path.clone())
                    .await
                    .map_err(|e| auth_err("service-account file", e))?;
                config
                    .with_credentials(cf)
                    .await
                    .map_err(|e| auth_err("service-account file", e))?
            }
            PubsubCredentials::ServiceAccountJsonInline { json } => {
                let cf = CredentialsFile::new_from_str(json)
                    .await
                    .map_err(|e| auth_err("inline service-account key", e))?;
                config
                    .with_credentials(cf)
                    .await
                    .map_err(|e| auth_err("inline service-account key", e))?
            }
        }
    } else {
        config
    };

    Client::new(config)
        .await
        .map_err(|e| FaucetError::Source(format!("pubsub: client build failed: {e}")))
}
