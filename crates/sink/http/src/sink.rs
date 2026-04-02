//! HTTP sink executor.

use crate::config::{HttpBatchMode, HttpSinkAuth, HttpSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use serde_json::Value;

/// An HTTP sink that sends records to an HTTP endpoint.
pub struct HttpSink {
    config: HttpSinkConfig,
    client: reqwest::Client,
}

impl HttpSink {
    /// Create a new HTTP sink from the given configuration.
    pub fn new(config: HttpSinkConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Build an HTTP request with auth and headers applied.
    fn build_request(&self, body: &Value) -> reqwest::RequestBuilder {
        let mut req = self
            .client
            .request(self.config.method.clone(), &self.config.url)
            .headers(self.config.headers.clone())
            .json(body);

        match &self.config.auth {
            HttpSinkAuth::None => {}
            HttpSinkAuth::Bearer(token) => {
                req = req.bearer_auth(token);
            }
            HttpSinkAuth::Basic { username, password } => {
                req = req.basic_auth(username, Some(password));
            }
            HttpSinkAuth::Custom(headers) => {
                req = req.headers(headers.clone());
            }
        }

        req
    }

    /// Send a single request with retry logic.
    async fn send_with_retry(&self, body: &Value) -> Result<(), FaucetError> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            let req = self.build_request(body);

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
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        match &self.config.batch_mode {
            HttpBatchMode::Individual => {
                let concurrency = self.config.concurrency.max(1);
                let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
                let mut handles = Vec::with_capacity(records.len());

                for record in records {
                    let permit =
                        semaphore.clone().acquire_owned().await.map_err(|e| {
                            FaucetError::Sink(format!("semaphore acquire failed: {e}"))
                        })?;
                    let fut = self.send_with_retry(record);
                    handles.push(async move {
                        let result = fut.await;
                        drop(permit);
                        result
                    });
                }

                futures::future::try_join_all(handles).await?;

                tracing::debug!(records = records.len(), "HTTP individual batch written");
                Ok(records.len())
            }
            HttpBatchMode::Array => {
                let array = Value::Array(records.to_vec());
                self.send_with_retry(&array).await?;
                tracing::debug!(records = records.len(), "HTTP array batch written");
                Ok(records.len())
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
        let config = HttpSinkConfig::new("https://api.example.com/ingest")
            .auth(HttpSinkAuth::Bearer("my-token".into()));
        let sink = HttpSink::new(config);

        let req = sink
            .build_request(&serde_json::json!({"test": true}))
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
        let config =
            HttpSinkConfig::new("https://api.example.com/ingest").auth(HttpSinkAuth::Basic {
                username: "user".into(),
                password: "pass".into(),
            });
        let sink = HttpSink::new(config);

        let req = sink
            .build_request(&serde_json::json!({"test": true}))
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
            .build_request(&serde_json::json!({"test": true}))
            .build()
            .unwrap();

        assert_eq!(req.method(), reqwest::Method::PUT);
    }
}
