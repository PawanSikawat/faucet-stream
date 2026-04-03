//! Webhook source stream executor.

use crate::config::WebhookSourceConfig;
use async_trait::async_trait;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use faucet_core::FaucetError;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// Shared state for the webhook HTTP handler.
struct AppState {
    records: Mutex<Vec<Value>>,
    max_payloads: Option<usize>,
    done: Notify,
}

/// A webhook receiver source that starts a temporary HTTP server and
/// collects incoming POST payloads as records.
pub struct WebhookSource {
    config: WebhookSourceConfig,
}

impl WebhookSource {
    /// Create a new webhook source from the given configuration.
    pub fn new(config: WebhookSourceConfig) -> Self {
        Self { config }
    }

    /// Start the webhook server, collect payloads, and return them.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let state = Arc::new(AppState {
            records: Mutex::new(Vec::new()),
            max_payloads: self.config.max_payloads,
            done: Notify::new(),
        });

        let app = Router::new()
            .route(&self.config.path, post(webhook_handler))
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind(&self.config.listen_addr)
            .await
            .map_err(|e| {
                FaucetError::Config(format!(
                    "failed to bind to {}: {e}",
                    self.config.listen_addr
                ))
            })?;

        tracing::info!(
            addr = %self.config.listen_addr,
            path = %self.config.path,
            "webhook server listening"
        );

        let timeout = tokio::time::sleep(std::time::Duration::from_secs(self.config.timeout_secs));
        let done_notified = state.done.notified();

        tokio::select! {
            result = axum::serve(listener, app).into_future() => {
                if let Err(e) = result {
                    return Err(FaucetError::Config(format!("webhook server error: {e}")));
                }
            }
            () = timeout => {
                tracing::info!("webhook timeout reached");
            }
            () = done_notified => {
                tracing::info!("max payloads reached");
            }
        }

        let records = state.records.lock().await.clone();
        tracing::info!(records = records.len(), "webhook fetch complete");
        Ok(records)
    }
}

/// Axum handler for incoming webhook POST requests.
async fn webhook_handler(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> StatusCode {
    let value = match serde_json::from_slice::<Value>(&body) {
        Ok(v) => v,
        Err(_) => {
            // If the body is not valid JSON, wrap it as a string.
            match String::from_utf8(body.to_vec()) {
                Ok(s) => Value::String(s),
                Err(_) => return StatusCode::BAD_REQUEST,
            }
        }
    };

    let mut records = state.records.lock().await;
    records.push(value);

    if let Some(max) = state.max_payloads
        && records.len() >= max
    {
        state.done.notify_one();
    }

    StatusCode::OK
}

#[async_trait]
impl faucet_core::Source for WebhookSource {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        WebhookSource::fetch_all(self).await
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(WebhookSourceConfig))
            .expect("schema serialization")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn webhook_collects_payloads() {
        // Use port 0 to get a random available port.
        let config = WebhookSourceConfig::new()
            .listen_addr("127.0.0.1:0")
            .max_payloads(2)
            .timeout_secs(5);

        let state = Arc::new(AppState {
            records: Mutex::new(Vec::new()),
            max_payloads: config.max_payloads,
            done: Notify::new(),
        });

        let server_state = Arc::clone(&state);
        let app = Router::new()
            .route(&config.path, post(webhook_handler))
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind(&config.listen_addr)
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let done_notified = server_state.done.notified();
            tokio::select! {
                result = axum::serve(listener, app).into_future() => {
                    if let Err(e) = result {
                        panic!("server error: {e}");
                    }
                }
                () = done_notified => {}
            }
        });

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/webhook");

        // Send two payloads.
        let resp1 = client
            .post(&url)
            .json(&json!({"event": "created", "id": 1}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.status(), 200);

        let resp2 = client
            .post(&url)
            .json(&json!({"event": "updated", "id": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.status(), 200);

        // Wait for the server to notice max_payloads.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        server_handle.abort();

        let records = state.records.lock().await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["event"], "created");
        assert_eq!(records[1]["event"], "updated");
    }

    #[tokio::test]
    async fn webhook_handles_non_json_body() {
        let state = Arc::new(AppState {
            records: Mutex::new(Vec::new()),
            max_payloads: Some(1),
            done: Notify::new(),
        });

        let server_state = Arc::clone(&state);
        let app = Router::new()
            .route("/webhook", post(webhook_handler))
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let done_notified = server_state.done.notified();
            tokio::select! {
                result = axum::serve(listener, app).into_future() => {
                    if let Err(e) = result {
                        panic!("server error: {e}");
                    }
                }
                () = done_notified => {}
            }
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{addr}/webhook"))
            .body("plain text body")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        server_handle.abort();

        let records = state.records.lock().await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], Value::String("plain text body".into()));
    }
}
