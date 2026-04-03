//! Webhook source configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the webhook receiver source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct WebhookSourceConfig {
    /// Address to bind the HTTP server to (default: `"0.0.0.0:8080"`).
    pub listen_addr: String,
    /// Endpoint path for receiving webhooks (default: `"/webhook"`).
    pub path: String,
    /// Stop after receiving this many payloads.
    pub max_payloads: Option<usize>,
    /// How long to listen before returning, in seconds (default: 30).
    pub timeout_secs: u64,
}

impl Default for WebhookSourceConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".into(),
            path: "/webhook".into(),
            max_payloads: None,
            timeout_secs: 30,
        }
    }
}

impl WebhookSourceConfig {
    /// Create a new config with sensible defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the listen address.
    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.listen_addr = addr.into();
        self
    }

    /// Set the webhook endpoint path.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Stop after receiving this many payloads.
    pub fn max_payloads(mut self, max: usize) -> Self {
        self.max_payloads = Some(max);
        self
    }

    /// Set the timeout in seconds.
    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = WebhookSourceConfig::new();
        assert_eq!(config.listen_addr, "0.0.0.0:8080");
        assert_eq!(config.path, "/webhook");
        assert!(config.max_payloads.is_none());
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn builder_methods() {
        let config = WebhookSourceConfig::new()
            .listen_addr("127.0.0.1:9090")
            .path("/hooks/incoming")
            .max_payloads(10)
            .timeout_secs(60);
        assert_eq!(config.listen_addr, "127.0.0.1:9090");
        assert_eq!(config.path, "/hooks/incoming");
        assert_eq!(config.max_payloads, Some(10));
        assert_eq!(config.timeout_secs, 60);
    }
}
