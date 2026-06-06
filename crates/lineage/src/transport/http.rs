//! HTTP transport — POST each event to an OpenLineage endpoint.

use super::Transport;
use crate::config::HttpAuth;
use async_trait::async_trait;
use faucet_core::FaucetError;
use std::time::Duration;

pub struct HttpTransport {
    client: reqwest::Client,
    url: String,
    auth: Option<HttpAuth>,
}

impl HttpTransport {
    pub fn new(url: String, timeout: Duration, auth: Option<HttpAuth>) -> Result<Self, FaucetError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| FaucetError::Custom(Box::new(e)))?;
        Ok(Self { client, url, auth })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, event_json: Vec<u8>) -> Result<(), FaucetError> {
        let mut req = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(event_json);
        if let Some(HttpAuth::Bearer { token }) = &self.auth {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e| FaucetError::Custom(Box::new(e)))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FaucetError::HttpStatus {
                status: status.as_u16(),
                url: self.url.clone(),
                body: resp.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HttpAuth;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn posts_event_with_bearer_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/lineage"))
            .and(header("authorization", "Bearer tok"))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&server)
            .await;
        let t = HttpTransport::new(
            format!("{}/api/v1/lineage", server.uri()),
            std::time::Duration::from_secs(5),
            Some(HttpAuth::Bearer { token: "tok".into() }),
        )
        .unwrap();
        t.send(b"{\"eventType\":\"START\"}".to_vec()).await.unwrap();
    }

    #[tokio::test]
    async fn non_2xx_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let t = HttpTransport::new(server.uri(), std::time::Duration::from_secs(5), None).unwrap();
        assert!(t.send(b"{}".to_vec()).await.is_err());
    }
}
