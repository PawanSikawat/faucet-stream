//! HTTP sink executor.

use crate::config::{HttpBatchMode, HttpSinkAuth, HttpSinkConfig};
use async_trait::async_trait;
use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider};
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;
use std::collections::HashMap;

/// Map a [`Credential`] from a shared provider onto the [`HttpSinkAuth`]
/// representation so the existing header-application path can be reused.
fn credential_to_auth(cred: Credential) -> HttpSinkAuth {
    match cred {
        Credential::Bearer(token) => HttpSinkAuth::Bearer { token },
        Credential::Token(token) => HttpSinkAuth::Custom {
            headers: HashMap::from([("Authorization".to_string(), token)]),
        },
        Credential::Basic { username, password } => HttpSinkAuth::Basic { username, password },
        Credential::Header { name, value } => HttpSinkAuth::Custom {
            headers: HashMap::from([(name, value)]),
        },
    }
}

/// An HTTP sink that sends records to an HTTP endpoint.
pub struct HttpSink {
    config: HttpSinkConfig,
    client: reqwest::Client,
    /// Optional shared auth provider. When set, it takes precedence over inline
    /// auth. Set via [`HttpSink::with_auth_provider`].
    auth_provider: Option<SharedAuthProvider>,
}

impl HttpSink {
    /// Create a new HTTP sink from the given configuration.
    pub fn new(config: HttpSinkConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            auth_provider: None,
        }
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking
    /// precedence over inline auth), so several sinks can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and
    /// by library callers who construct one provider and inject it into many
    /// sinks.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Resolve the effective auth for the current batch. The provider (if any)
    /// takes precedence; otherwise inline auth is used. A bare
    /// `AuthSpec::Reference` with no provider is an error.
    async fn resolve_auth(&self) -> Result<HttpSinkAuth, FaucetError> {
        if let Some(provider) = &self.auth_provider {
            Ok(credential_to_auth(provider.credential().await?))
        } else {
            match &self.config.auth {
                AuthSpec::Inline(a) => Ok(a.clone()),
                AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                    "auth references provider '{}' but no provider was supplied; \
                     set one via the CLI `auth:` catalog or `with_auth_provider`",
                    r.name
                ))),
            }
        }
    }

    /// Build an HTTP request with auth and headers applied.
    fn apply_auth(
        &self,
        mut req: reqwest::RequestBuilder,
        auth: &HttpSinkAuth,
    ) -> Result<reqwest::RequestBuilder, FaucetError> {
        match auth {
            HttpSinkAuth::None => {}
            HttpSinkAuth::Bearer { token } => {
                req = req.bearer_auth(token);
            }
            HttpSinkAuth::Basic { username, password } => {
                req = req.basic_auth(username, Some(password));
            }
            HttpSinkAuth::Custom { headers } => {
                let mut hm = reqwest::header::HeaderMap::new();
                for (name, value) in headers {
                    let n =
                        reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                            FaucetError::Auth(format!("invalid custom header name {name:?}: {e}"))
                        })?;
                    let v = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                        FaucetError::Auth(format!("invalid custom header value for {name:?}: {e}"))
                    })?;
                    hm.insert(n, v);
                }
                req = req.headers(hm);
            }
        }
        Ok(req)
    }

    /// Build an HTTP request with the given pre-resolved auth and body.
    fn build_request_with_auth(
        &self,
        body: &Value,
        auth: &HttpSinkAuth,
    ) -> Result<reqwest::RequestBuilder, FaucetError> {
        let req = self
            .client
            .request(self.config.method.clone(), &self.config.url)
            .headers(self.config.headers.clone())
            .json(body);
        self.apply_auth(req, auth)
    }

    /// Send a single request with retry logic, using the pre-resolved `auth`.
    async fn send_with_retry(
        &self,
        body: &Value,
        auth: &HttpSinkAuth,
    ) -> Result<(), FaucetError> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            let req = self.build_request_with_auth(body, auth)?;

            match req.send().await {
                Ok(resp) => match check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        if attempt < self.config.max_retries && e.is_retriable() {
                            tracing::warn!(
                                attempt = attempt + 1,
                                max_retries = self.config.max_retries,
                                error = %e,
                                "retrying request"
                            );
                            last_error = Some(e);
                            continue;
                        }
                        return Err(e);
                    }
                },
                Err(e) => {
                    let faucet_err = FaucetError::Http(e);
                    if attempt < self.config.max_retries && faucet_err.is_retriable() {
                        tracing::warn!(
                            attempt = attempt + 1,
                            max_retries = self.config.max_retries,
                            error = %faucet_err,
                            "retrying request"
                        );
                        last_error = Some(faucet_err);
                        continue;
                    }
                    return Err(faucet_err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| FaucetError::Sink("max retries exhausted".into())))
    }
}

#[async_trait]
impl faucet_core::Sink for HttpSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(HttpSinkConfig))
            .expect("schema serialization")
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Resolve auth once per batch (provider-first, then inline).
        let auth = self.resolve_auth().await?;

        match &self.config.batch_mode {
            HttpBatchMode::Individual => {
                // Run `send_with_retry` for every record with at most
                // `concurrency` in-flight at once. We drive a
                // `FuturesUnordered` directly, refilling it as each future
                // completes, instead of acquiring permits up-front the way
                // the previous semaphore-based code did — that approach
                // deadlocked because permits were acquired sequentially in
                // a loop before any future actually ran, so after
                // `concurrency` iterations the next `acquire_owned().await`
                // would block forever (closes #59).
                let concurrency = self.config.concurrency.max(1);
                let mut in_flight = FuturesUnordered::new();
                let mut iter = records.iter();
                for record in iter.by_ref().take(concurrency) {
                    in_flight.push(self.send_with_retry(record, &auth));
                }
                while let Some(result) = in_flight.next().await {
                    result?;
                    if let Some(record) = iter.next() {
                        in_flight.push(self.send_with_retry(record, &auth));
                    }
                }

                tracing::debug!(records = records.len(), "HTTP individual batch written");
                Ok(records.len())
            }
            HttpBatchMode::Array => {
                // `batch_size = 0` is the "no batching" sentinel: forward
                // whatever upstream handed us as a single JSON-array POST,
                // preserving `StreamPage` framing. Otherwise re-chunk into
                // `batch_size` slices and issue one POST per chunk.
                let effective_chunk = if self.config.batch_size == 0 {
                    records.len()
                } else {
                    self.config.batch_size
                };

                let mut total = 0;
                for chunk in records.chunks(effective_chunk) {
                    let array = Value::Array(chunk.to_vec());
                    self.send_with_retry(&array, &auth).await?;
                    total += chunk.len();
                }
                tracing::debug!(
                    records = total,
                    batch_size = self.config.batch_size,
                    "HTTP array batch written"
                );
                Ok(total)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HttpSinkConfig;

    #[test]
    fn creates_sink() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest");
        let _sink = HttpSink::new(config);
    }

    #[test]
    fn build_request_applies_bearer_auth() {
        let auth = HttpSinkAuth::Bearer {
            token: "my-token".into(),
        };
        let config = HttpSinkConfig::new("https://api.example.com/ingest").auth(auth.clone());
        let sink = HttpSink::new(config);

        let req = sink
            .build_request_with_auth(&serde_json::json!({"test": true}), &auth)
            .unwrap()
            .build()
            .unwrap();

        let auth_header = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("Bearer "));
        assert!(auth_header.contains("my-token"));
    }

    #[test]
    fn build_request_applies_basic_auth() {
        let auth = HttpSinkAuth::Basic {
            username: "user".into(),
            password: "pass".into(),
        };
        let config = HttpSinkConfig::new("https://api.example.com/ingest").auth(auth.clone());
        let sink = HttpSink::new(config);

        let req = sink
            .build_request_with_auth(&serde_json::json!({"test": true}), &auth)
            .unwrap()
            .build()
            .unwrap();

        let auth_header = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("Basic "));
    }

    #[test]
    fn build_request_uses_configured_method() {
        let config =
            HttpSinkConfig::new("https://api.example.com/ingest").method(reqwest::Method::PUT);
        let sink = HttpSink::new(config);

        let req = sink
            .build_request_with_auth(&serde_json::json!({"test": true}), &HttpSinkAuth::None)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(req.method(), reqwest::Method::PUT);
    }
}
