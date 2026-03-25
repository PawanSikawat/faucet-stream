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
    /// API key sent in a request header.
    ApiKey {
        header: String,
        value: String,
    },
    /// API key sent as a query parameter (e.g. `?api_key=secret`).
    ///
    /// Some APIs require the key in the URL rather than a header. The `param`
    /// field is the query parameter name, and `value` is the key itself.
    ApiKeyQuery {
        param: String,
        value: String,
    },
    OAuth2 {
        token_url: String,
        client_id: String,
        client_secret: String,
        scopes: Vec<String>,
        /// Fraction of `expires_in` after which the cached token is considered
        /// expired and a new one is fetched. Must be in `(0.0, 1.0]`.
        /// Defaults to `0.9` (refresh after 90 % of the token lifetime).
        expiry_ratio: f64,
    },
    Custom(HeaderMap),
}

impl Auth {
    /// Apply header-based auth to the request headers.
    ///
    /// `ApiKeyQuery` is a no-op here — it is applied as a query parameter by
    /// `RestStream::execute_request` instead.
    pub fn apply(&self, headers: &mut HeaderMap) -> Result<(), FaucetError> {
        match self {
            Auth::None | Auth::ApiKeyQuery { .. } => Ok(()),
            Auth::Bearer(token) => bearer::apply(headers, token),
            Auth::Basic { username, password } => basic::apply(headers, username, password),
            Auth::ApiKey { header, value } => api_key::apply(headers, header, value),
            // OAuth2 is resolved to Auth::Bearer by RestStream before apply() is called.
            // If apply() is reached with an OAuth2 variant, it means the caller bypassed
            // RestStream — return a clear error rather than silently sending no auth.
            Auth::OAuth2 { .. } => Err(FaucetError::Auth(
                "OAuth2 auth must be resolved to a bearer token before applying; \
                 use RestStream (which resolves it automatically) or call \
                 fetch_oauth2_token() and use Auth::Bearer"
                    .into(),
            )),
            Auth::Custom(h) => {
                custom::apply(headers, h);
                Ok(())
            }
        }
    }
}

pub use oauth2::fetch_oauth2_token;
