//! XML stream executor.

use crate::config::{XmlAuth, XmlPagination, XmlStreamConfig};
use crate::convert;
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::{self, DEFAULT_ERROR_BODY_MAX_LEN};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;

/// A configured XML API source that handles pagination and extraction.
pub struct XmlStream {
    config: XmlStreamConfig,
    client: Client,
}

impl XmlStream {
    /// Create a new XML stream from the given configuration.
    pub fn new(config: XmlStreamConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    /// Fetch all records across all pages.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut pages_fetched = 0usize;
        let mut offset = 0usize;
        let mut page_number = None;
        let mut prev_record_count: Option<usize> = None;

        // Initialize pagination state.
        if let Some(XmlPagination::PageNumber { start_page, .. }) = &self.config.pagination {
            page_number = Some(*start_page);
        }

        loop {
            if let Some(max) = self.config.max_pages
                && pages_fetched >= max
            {
                tracing::warn!("max pages ({max}) reached");
                break;
            }

            let mut params = self.config.query_params.clone();
            self.apply_pagination_params(&mut params, page_number, offset);

            let xml_text = self.execute_request(&params).await?;
            let json = convert::xml_to_json(&xml_text)?;

            let records = match &self.config.records_element_path {
                Some(path) => convert::extract_at_path(&json, path),
                None => vec![json],
            };

            let record_count = records.len();
            all_records.extend(records);
            pages_fetched += 1;

            // Advance pagination or stop.
            match &self.config.pagination {
                Some(XmlPagination::PageNumber { page_size, .. }) => {
                    if record_count == 0 {
                        break;
                    }
                    // Stop if page_size is set and we got fewer records than the page size.
                    if let Some(size) = page_size
                        && record_count < *size
                    {
                        break;
                    }
                    page_number = page_number.map(|p| p + 1);
                }
                Some(XmlPagination::Offset { limit, .. }) => {
                    if record_count < *limit {
                        break;
                    }
                    // Loop detection: stop if record count is identical and offset hasn't advanced.
                    if prev_record_count == Some(record_count) && record_count == 0 {
                        tracing::warn!("offset pagination loop detected, stopping");
                        break;
                    }
                    offset += record_count;
                }
                None => break,
            }
            prev_record_count = Some(record_count);
        }

        tracing::info!(
            records = all_records.len(),
            pages = pages_fetched,
            "XML fetch complete"
        );
        Ok(all_records)
    }

    fn apply_pagination_params(
        &self,
        params: &mut HashMap<String, String>,
        page_number: Option<usize>,
        offset: usize,
    ) {
        match &self.config.pagination {
            Some(XmlPagination::PageNumber {
                param_name,
                page_size,
                page_size_param,
                ..
            }) => {
                if let Some(page) = page_number {
                    params.insert(param_name.clone(), page.to_string());
                }
                if let (Some(size), Some(param)) = (page_size, page_size_param) {
                    params.insert(param.clone(), size.to_string());
                }
            }
            Some(XmlPagination::Offset {
                offset_param,
                limit_param,
                limit,
            }) => {
                params.insert(offset_param.clone(), offset.to_string());
                params.insert(limit_param.clone(), limit.to_string());
            }
            None => {}
        }
    }

    async fn execute_request(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<String, FaucetError> {
        let url = format!(
            "{}/{}",
            self.config.base_url,
            self.config.path.trim_start_matches('/')
        );

        let mut req = self
            .client
            .request(self.config.method.clone(), &url)
            .headers(self.config.headers.clone())
            .query(params);

        // Apply auth.
        match &self.config.auth {
            XmlAuth::None => {}
            XmlAuth::Bearer(token) => {
                req = req.bearer_auth(token);
            }
            XmlAuth::Basic { username, password } => {
                req = req.basic_auth(username, Some(password));
            }
            XmlAuth::Custom(headers) => {
                req = req.headers(headers.clone());
            }
        }

        // Set body for POST requests (SOAP).
        if let Some(body) = &self.config.body {
            req = req
                .header("Content-Type", "text/xml; charset=utf-8")
                .body(body.clone());
        }

        let resp = req.send().await.map_err(FaucetError::Http)?;
        let resp = util::check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        resp.text().await.map_err(FaucetError::Http)
    }
}

#[async_trait]
impl faucet_core::Source for XmlStream {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        XmlStream::fetch_all(self).await
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(XmlStreamConfig))
            .expect("schema serialization")
    }
}
