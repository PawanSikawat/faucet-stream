//! The main REST stream executor.

use crate::config::RestStreamConfig;
use crate::error::FaucetError;
use crate::extract;
use crate::pagination::{PaginationState, PaginationStyle};
use crate::retry;
use futures_core::Stream;
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;

/// A configured REST API stream that handles pagination, auth, and extraction.
pub struct RestStream {
    config: RestStreamConfig,
    client: Client,
}

impl RestStream {
    /// Create a new stream from the given configuration.
    pub fn new(config: RestStreamConfig) -> Result<Self, FaucetError> {
        let mut builder = Client::builder();
        if let Some(t) = config.timeout {
            builder = builder.timeout(t);
        }
        Ok(Self {
            config,
            client: builder.build()?,
        })
    }

    /// Fetch all records across all pages as raw JSON values.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut state = PaginationState::default();
        let mut pages_fetched = 0usize;

        loop {
            if let Some(max) = self.config.max_pages
                && pages_fetched >= max
            {
                tracing::warn!("max pages ({max}) reached");
                break;
            }

            let mut params = self.config.query_params.clone();
            self.config.pagination.apply_params(&mut params, &state);

            // For LinkHeader pagination, subsequent requests use the full URL from the
            // Link header rather than constructing from base_url + path.
            let url_override = match &self.config.pagination {
                PaginationStyle::LinkHeader => state.next_link.clone(),
                _ => None,
            };

            let params_clone = params.clone();
            let (body, resp_headers) = retry::execute_with_retry(
                self.config.max_retries,
                self.config.retry_backoff,
                || self.execute_request(&params_clone, url_override.as_deref()),
            )
            .await?;

            let records = extract::extract_records(&body, self.config.records_path.as_deref())?;
            let count = records.len();
            all_records.extend(records);

            let has_next =
                self.config
                    .pagination
                    .advance(&body, &resp_headers, &mut state, count)?;
            pages_fetched += 1;
            if !has_next {
                break;
            }

            if let Some(delay) = self.config.request_delay {
                tokio::time::sleep(delay).await;
            }
        }

        tracing::info!(
            "fetched {} total records across {} page(s)",
            all_records.len(),
            pages_fetched
        );
        Ok(all_records)
    }

    /// Fetch all records and deserialize into typed structs.
    pub async fn fetch_all_as<T: for<'de> Deserialize<'de>>(&self) -> Result<Vec<T>, FaucetError> {
        let values = self.fetch_all().await?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(FaucetError::Json))
            .collect()
    }

    /// Stream records page-by-page, yielding one `Vec<Value>` per page as it arrives.
    ///
    /// Unlike [`fetch_all`](Self::fetch_all), this does not wait for all pages to be fetched
    /// before returning — callers can process each page immediately.
    ///
    /// ```rust,no_run
    /// use faucet_stream::{RestStream, RestStreamConfig};
    /// use futures::StreamExt;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stream = RestStream::new(RestStreamConfig::new("https://api.example.com", "/items"))?;
    /// let mut pages = stream.stream_pages();
    /// while let Some(page) = pages.next().await {
    ///     let records = page?;
    ///     println!("got {} records", records.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream_pages(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Value>, FaucetError>> + '_>> {
        Box::pin(async_stream::try_stream! {
            let mut state = PaginationState::default();
            let mut pages_fetched = 0usize;

            loop {
                if let Some(max) = self.config.max_pages
                    && pages_fetched >= max
                {
                    tracing::warn!("max pages ({max}) reached");
                    break;
                }

                let mut params = self.config.query_params.clone();
                self.config.pagination.apply_params(&mut params, &state);

                let url_override = match &self.config.pagination {
                    PaginationStyle::LinkHeader => state.next_link.clone(),
                    _ => None,
                };

                let params_clone = params.clone();
                let (body, resp_headers) = retry::execute_with_retry(
                    self.config.max_retries,
                    self.config.retry_backoff,
                    || self.execute_request(&params_clone, url_override.as_deref()),
                )
                .await?;

                let records = extract::extract_records(&body, self.config.records_path.as_deref())?;
                let count = records.len();

                yield records;

                let has_next = self
                    .config
                    .pagination
                    .advance(&body, &resp_headers, &mut state, count)?;
                pages_fetched += 1;
                if !has_next {
                    break;
                }

                if let Some(delay) = self.config.request_delay {
                    tokio::time::sleep(delay).await;
                }
            }
        })
    }

    /// Execute a single HTTP request and return the response body and headers.
    ///
    /// When `url_override` is `Some`, that URL is used directly (no query params are
    /// appended — the override URL already encodes them, as with Link header pagination).
    async fn execute_request(
        &self,
        params: &HashMap<String, String>,
        url_override: Option<&str>,
    ) -> Result<(Value, HeaderMap), FaucetError> {
        let use_override = url_override.is_some();
        let url = match url_override {
            Some(u) => u.to_string(),
            None => format!(
                "{}/{}",
                self.config.base_url,
                self.config.path.trim_start_matches('/')
            ),
        };

        let mut headers = self.config.headers.clone();
        self.config.auth.apply(&mut headers)?;

        let mut req = self
            .client
            .request(self.config.method.clone(), &url)
            .headers(headers);

        if !use_override {
            req = req.query(params);
        }

        if let Some(body) = &self.config.body {
            req = req.json(body);
        }

        let resp = req.send().await?.error_for_status()?;
        let resp_headers = resp.headers().clone();
        let body: Value = resp.json().await?;
        Ok((body, resp_headers))
    }
}
