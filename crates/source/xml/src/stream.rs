//! XML stream executor.

use crate::config::{XmlAuth, XmlPagination, XmlStreamConfig};
use crate::convert;
use async_trait::async_trait;
use faucet_core::util::{self, DEFAULT_ERROR_BODY_MAX_LEN};
use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider};
use faucet_core::{Stream, StreamPage};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// Content fingerprint of a fetched page, used as a pagination loop guard: a
/// server that ignores the page/offset parameter (or clamps to the last page)
/// returns the same non-empty page on every request, which would otherwise loop
/// forever. Stopping when two consecutive pages fingerprint identically mirrors
/// the REST source's body-fingerprint guard (audit #146 H4/H5).
fn page_fingerprint(records: &[Value]) -> u64 {
    use std::hash::{Hash, Hasher};
    // `serde_json::Value` is not `Hash`; hash its canonical string form.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    records.len().hash(&mut hasher);
    for r in records {
        r.to_string().hash(&mut hasher);
    }
    hasher.finish()
}

/// Retries on transient (5xx / connection) failures before giving up.
const RETRY_MAX_ATTEMPTS: u32 = 3;
/// Base exponential-backoff delay between retries.
const RETRY_BASE_BACKOFF: Duration = Duration::from_millis(500);

/// A configured XML API source that handles pagination and extraction.
pub struct XmlStream {
    config: XmlStreamConfig,
    client: Client,
    /// Optional shared auth provider. When present it takes precedence over
    /// inline auth, so several sources can share one token with single-flight
    /// refresh. Used by the CLI to resolve `auth: { ref }`, and by library
    /// callers who construct one provider and inject it into many sources.
    auth_provider: Option<SharedAuthProvider>,
}

/// Map a [`Credential`] from a shared provider onto the XML [`XmlAuth`]
/// representation so the existing header-application path can be reused.
fn credential_to_auth(cred: Credential) -> XmlAuth {
    match cred {
        Credential::Bearer(token) => XmlAuth::Bearer { token },
        Credential::Token(token) => XmlAuth::Custom {
            headers: std::iter::once(("Authorization".to_string(), token)).collect(),
        },
        Credential::Basic { username, password } => XmlAuth::Basic { username, password },
        Credential::Header { name, value } => XmlAuth::Custom {
            headers: std::iter::once((name, value)).collect(),
        },
    }
}

impl XmlStream {
    /// Create a new XML stream from the given configuration.
    pub fn new(config: XmlStreamConfig) -> Self {
        Self {
            config,
            client: Client::new(),
            auth_provider: None,
        }
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking precedence
    /// over inline auth), so several sources can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and by
    /// library callers who construct one provider and inject it into many
    /// sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Fetch all records across all pages.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.fetch_all_with_context(&HashMap::new()).await
    }

    /// Fetch all records, substituting parent context into path, query_params, and body.
    async fn fetch_all_with_context(
        &self,
        context: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut pages_fetched = 0usize;
        let mut offset = 0usize;
        let mut page_number = None;
        let mut prev_fingerprint: Option<u64> = None;

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

            let xml_text = self.execute_request(&params, context).await?;
            let json = convert::xml_to_json(&xml_text)?;

            let records = match &self.config.records_element_path {
                Some(path) => convert::extract_at_path(&json, path),
                None => vec![json],
            };

            let record_count = records.len();
            let fingerprint = page_fingerprint(&records);
            all_records.extend(records);
            pages_fetched += 1;

            // Loop guard: a server that ignores the page/offset parameter (or
            // clamps to the last page) returns the same non-empty page forever.
            // Stop when two consecutive pages are identical (audit #146 H4/H5).
            if record_count > 0 && prev_fingerprint == Some(fingerprint) {
                tracing::warn!(
                    "XML pagination returned an identical page; stopping to avoid an infinite loop"
                );
                break;
            }
            prev_fingerprint = Some(fingerprint);

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
                    offset += record_count;
                }
                None => break,
            }
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
        context: &HashMap<String, serde_json::Value>,
    ) -> Result<String, FaucetError> {
        let path = if context.is_empty() {
            self.config.path.clone()
        } else {
            faucet_core::util::substitute_context(&self.config.path, context)
        };

        let url = format!("{}/{}", self.config.base_url, path.trim_start_matches('/'));

        // Substitute context into query parameter values.
        let resolved_params: HashMap<String, String> = if context.is_empty() {
            params.clone()
        } else {
            params
                .iter()
                .map(|(k, v)| (k.clone(), faucet_core::util::substitute_context(v, context)))
                .collect()
        };

        let mut req = self
            .client
            .request(self.config.method.clone(), &url)
            .headers(self.config.headers.clone())
            .query(&resolved_params);

        // Resolve credentials to concrete auth. A shared auth provider
        // (from `auth: { ref }` or injected by a library caller) takes
        // precedence; otherwise inline auth is used.
        let effective_auth: XmlAuth = if let Some(provider) = &self.auth_provider {
            credential_to_auth(provider.credential().await?)
        } else {
            match &self.config.auth {
                AuthSpec::Inline(a) => a.clone(),
                AuthSpec::Reference(r) => {
                    return Err(FaucetError::Auth(format!(
                        "auth references provider '{}' but no provider was supplied; \
                         set one via the CLI `auth:` catalog or `with_auth_provider`",
                        r.name
                    )));
                }
            }
        };

        // Apply auth.
        match &effective_auth {
            XmlAuth::None => {}
            XmlAuth::Bearer { token } => {
                req = req.bearer_auth(token);
            }
            XmlAuth::Basic { username, password } => {
                req = req.basic_auth(username, Some(password));
            }
            XmlAuth::Custom { headers } => {
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

        // Set body for POST requests (SOAP), with context substitution.
        if let Some(body) = &self.config.body {
            let resolved_body = if context.is_empty() {
                body.clone()
            } else {
                faucet_core::util::substitute_context(body, context)
            };
            req = req
                .header("Content-Type", "text/xml; charset=utf-8")
                .body(resolved_body);
        }

        // Retry transient failures (5xx / connection resets) with jittered
        // backoff, matching the REST source's reliability layer (#78/#16).
        // The request body is a String, so `try_clone` always succeeds.
        faucet_core::execute_with_retry(RETRY_MAX_ATTEMPTS, RETRY_BASE_BACKOFF, || {
            let attempt = req.try_clone();
            async move {
                let req = attempt.ok_or_else(|| {
                    FaucetError::Source("xml: request is not cloneable for retry".into())
                })?;
                let resp = req.send().await.map_err(FaucetError::Http)?;
                let resp = util::check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
                resp.text().await.map_err(FaucetError::Http)
            }
        })
        .await
    }
}

#[async_trait]
impl faucet_core::Source for XmlStream {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.fetch_all_with_context(context).await
    }

    /// Stream records from the XML response without materialising the whole
    /// document tree. The event-driven parser only builds JSON values for
    /// elements matching [`XmlStreamConfig::records_element_path`]; other
    /// elements are observed and discarded, so client-side memory is bounded
    /// at `O(batch_size * record_size)` regardless of how large the document
    /// is.
    ///
    /// Records are accumulated into a buffer of
    /// [`XmlStreamConfig::batch_size`] entries and yielded as a
    /// [`StreamPage`] once the buffer is full. The trailing partial buffer
    /// (if any) is emitted after the parser hits EOF and all pagination
    /// rounds drain.
    ///
    /// The trait-level `batch_size` argument is intentionally ignored in
    /// favour of the config field — the config is the user-facing knob the
    /// README documents, and routing the pipeline-supplied hint through it
    /// would silently override an explicit config value. `batch_size = 0`
    /// drains every page into a single emitted page.
    ///
    /// Bookmarks are always `None` — the XML source has no
    /// incremental-replication mode today; pagination only walks the
    /// API's own page-number / offset cursor.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let owned_context = context.clone();

        Box::pin(async_stream::try_stream! {
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;
            let mut pages_fetched = 0usize;
            let mut offset = 0usize;
            let mut page_number = None;
            let mut prev_fingerprint: Option<u64> = None;

            if let Some(XmlPagination::PageNumber { start_page, .. }) =
                &self.config.pagination
            {
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

                let xml_text = self.execute_request(&params, &owned_context).await?;

                // Event-driven extraction: only the matched subtree is
                // ever materialised. The closure pushes into the local
                // buffer; once it crosses `chunk`, the surrounding loop
                // can flush, but we can't `yield` from inside the closure,
                // so we collect this HTTP page's records into a scratch
                // Vec and then iterate them after.
                let mut page_records: Vec<Value> = Vec::new();
                convert::stream_extract(
                    &xml_text,
                    self.config.records_element_path.as_deref(),
                    |rec| page_records.push(rec),
                )?;

                let record_count = page_records.len();
                let fingerprint = page_fingerprint(&page_records);

                for rec in page_records.drain(..) {
                    buffer.push(rec);
                    if buffer.len() >= chunk {
                        let flush = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                        total += flush.len();
                        yield StreamPage { records: flush, bookmark: None };
                    }
                }
                pages_fetched += 1;

                // Loop guard: stop when two consecutive pages are identical — a
                // server ignoring the page/offset parameter (or clamping to the
                // last page) returns the same non-empty page forever (#146 H4/H5).
                if record_count > 0 && prev_fingerprint == Some(fingerprint) {
                    tracing::warn!(
                        "XML pagination returned an identical page; stopping to avoid an infinite loop"
                    );
                    break;
                }
                prev_fingerprint = Some(fingerprint);

                // Advance pagination using the same rules as
                // `fetch_all_with_context`.
                match &self.config.pagination {
                    Some(XmlPagination::PageNumber { page_size, .. }) => {
                        if record_count == 0 {
                            break;
                        }
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
                        offset += record_count;
                    }
                    None => break,
                }
            }

            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                records = total,
                pages = pages_fetched,
                batch_size,
                "XML source stream complete",
            );
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(XmlStreamConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}{}",
            faucet_core::redact_uri_credentials(&self.config.base_url),
            self.config.path
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source;

    #[test]
    fn dataset_uri_combines_base_and_path() {
        let source = XmlStream::new(XmlStreamConfig::new(
            "https://soap.example.com",
            "/api/v1/service",
        ));
        assert_eq!(
            source.dataset_uri(),
            "https://soap.example.com/api/v1/service"
        );
    }

    #[test]
    fn dataset_uri_redacts_credentials() {
        let source = XmlStream::new(XmlStreamConfig::new(
            "https://user:pass@soap.example.com",
            "/svc",
        ));
        assert_eq!(source.dataset_uri(), "https://soap.example.com/svc");
    }
}
