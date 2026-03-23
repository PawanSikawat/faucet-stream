//! Authentication strategies for REST APIs.

pub mod api_key;
pub mod basic;
pub mod bearer;
pub mod custom;
pub mod oauth2;

use crate::error::FaucetError;
use reqwest::header::HeaderMap;

/// Supported authentication methods.
#[derive(Debug, Clone)]
pub enum Auth {
    None,
    Bearer(String),
    Basic {
        username: String,
        password: String,
    },
    ApiKey {
        header: String,
        value: String,
    },
    OAuth2 {
        token_url: String,
        client_id: String,
        client_secret: String,
        scopes: Vec<String>,
    },
    Custom(HeaderMap),
}

impl Auth {
    pub fn apply(&self, headers: &mut HeaderMap) -> Result<(), FaucetError> {
        match self {
            Auth::None => Ok(()),
            Auth::Bearer(token) => {
                bearer::apply(headers, token);
                Ok(())
            }
            Auth::Basic { username, password } => {
                basic::apply(headers, username, password);
                Ok(())
            }
            Auth::ApiKey { header, value } => api_key::apply(headers, header, value),
            Auth::OAuth2 { .. } => Ok(()),
            Auth::Custom(h) => {
                custom::apply(headers, h);
                Ok(())
            }
        }
    }
}

pub use oauth2::fetch_oauth2_token;
