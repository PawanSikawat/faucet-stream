//! OAuth2 client credentials flow.

use crate::error::FaucetError;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    #[serde(default)]
    expires_in: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    token_type: Option<String>,
}

/// Fetch an OAuth2 token using the client credentials grant.
pub async fn fetch_oauth2_token(
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
) -> Result<String, FaucetError> {
    let client = Client::new();
    let resp: TokenResponse = client
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", &scopes.join(" ")),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(resp.access_token)
}
