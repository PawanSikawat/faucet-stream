//! Cached HTTP client for the Confluent Schema Registry REST API.

use crate::schema_registry::SchemaRegistryConfig;
use faucet_core::FaucetError;
use lru::LruCache;
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Raw schema document returned by the registry.
#[derive(Debug, Clone, Deserialize)]
pub struct RegistrySchema {
    pub schema: String,
    #[serde(default = "default_schema_type")]
    pub schema_type: String,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

fn default_schema_type() -> String {
    "AVRO".into()
}

/// HTTP client that fetches and caches Schema Registry entries.
///
/// Cloning is cheap (`Arc`); the cache is shared across clones.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    http: reqwest::Client,
    base_url: String,
    auth: Option<crate::BasicAuth>,
    cache: Arc<Mutex<LruCache<u32, RegistrySchema>>>,
}

impl SchemaRegistryClient {
    pub fn new(config: &SchemaRegistryConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| FaucetError::Config(format!("schema-registry HTTP client: {e}")))?;
        Ok(Self {
            http,
            base_url: config.url.trim_end_matches('/').to_string(),
            auth: config.auth.clone(),
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(config.cache_capacity).unwrap(),
            ))),
        })
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(a) => req.basic_auth(&a.username, Some(&a.password)),
            None => req,
        }
    }

    /// Fetch the schema by ID, consulting the LRU first.
    pub async fn get_schema(&self, schema_id: u32) -> Result<RegistrySchema, FaucetError> {
        {
            let mut cache = self.cache.lock().await;
            if let Some(hit) = cache.get(&schema_id) {
                return Ok(hit.clone());
            }
        }

        let url = format!("{}/schemas/ids/{schema_id}", self.base_url);
        let resp = self
            .apply_auth(self.http.get(&url))
            .send()
            .await
            .map_err(FaucetError::Http)?;
        if !resp.status().is_success() {
            return Err(FaucetError::Source(format!(
                "schema registry GET {url} returned {}",
                resp.status()
            )));
        }
        let schema: RegistrySchema = resp
            .json()
            .await
            .map_err(|e| FaucetError::Source(format!("schema registry JSON decode: {e}")))?;

        let mut cache = self.cache.lock().await;
        cache.put(schema_id, schema.clone());
        Ok(schema)
    }

    /// Register a schema under `subject`, returning the registry-assigned ID.
    pub async fn register_schema(
        &self,
        subject: &str,
        schema_type: &str,
        schema_text: &str,
    ) -> Result<u32, FaucetError> {
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            urlencoding::encode(subject)
        );
        let body = serde_json::json!({
            "schemaType": schema_type,
            "schema": schema_text,
        });
        let resp = self
            .apply_auth(
                self.http
                    .post(&url)
                    .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .json(&body),
            )
            .send()
            .await
            .map_err(FaucetError::Http)?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "schema registry POST {url} returned {status}: {body}"
            )));
        }
        #[derive(Deserialize)]
        struct RegisterResp {
            id: u32,
        }
        let parsed: RegisterResp = resp
            .json()
            .await
            .map_err(|e| FaucetError::Sink(format!("schema registry register JSON decode: {e}")))?;
        Ok(parsed.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn get_schema_caches_after_first_fetch() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": "{\"type\":\"string\"}",
                "schemaType": "AVRO",
            })))
            .expect(1) // exactly one network call
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let first = client.get_schema(1).await.unwrap();
        let second = client.get_schema(1).await.unwrap();
        assert_eq!(first.schema, second.schema);
    }

    #[tokio::test]
    async fn get_schema_returns_error_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/99"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        assert!(client.get_schema(99).await.is_err());
    }

    #[tokio::test]
    async fn register_schema_returns_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/test-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 7})))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let id = client
            .register_schema("test-value", "AVRO", "\"string\"")
            .await
            .unwrap();
        assert_eq!(id, 7);
    }

    #[test]
    fn validate_accepts_http_url() {
        let c = SchemaRegistryConfig::new("http://localhost:8081");
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_non_http_scheme() {
        let mut c = SchemaRegistryConfig::new("ftp://localhost");
        c.cache_capacity = 1024;
        c.request_timeout = std::time::Duration::from_secs(10);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_cache_capacity() {
        let mut c = SchemaRegistryConfig::new("http://localhost");
        c.cache_capacity = 0;
        assert!(c.validate().is_err());
    }
}
