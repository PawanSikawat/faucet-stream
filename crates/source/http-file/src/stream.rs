//! The HTTP file source: download an authed blob and parse CSV/Excel.

use crate::config::{FileFormat, HttpFileAuth, HttpFileSourceConfig};
use async_trait::async_trait;
use faucet_core::{Credential, FaucetError, SharedAuthProvider, Source, Stream, StreamPage};
use reqwest::{Client, RequestBuilder};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// A source that downloads a file from an authenticated HTTP/Graph URL and
/// parses it (CSV always; Excel behind the `excel` feature) into records.
pub struct HttpFileSource {
    config: HttpFileSourceConfig,
    client: Client,
    /// Optional shared auth provider (from `auth: { ref }` or a library caller).
    /// Takes precedence over inline auth.
    auth_provider: Option<SharedAuthProvider>,
}

impl HttpFileSource {
    /// Create a new source from the given config.
    pub fn new(config: HttpFileSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let mut builder = Client::builder();
        if let Some(secs) = config.timeout_secs {
            builder = builder.timeout(Duration::from_secs(secs));
        } else {
            builder = builder.timeout(Duration::from_secs(60));
        }
        let client = builder
            .build()
            .map_err(|e| FaucetError::Config(format!("http-file: build HTTP client: {e}")))?;
        Ok(Self {
            config,
            client,
            auth_provider: None,
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider) (resolved from
    /// `auth: { ref }` by the CLI, or injected by a library caller). Takes
    /// precedence over inline auth.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Download the file bytes, applying auth.
    async fn fetch_bytes(&self, url: &str) -> Result<Vec<u8>, FaucetError> {
        let mut req = self.client.get(url);
        req = if let Some(provider) = &self.auth_provider {
            apply_credential(req, provider.credential().await?)
        } else {
            match self.config.auth.inline() {
                Some(auth) => apply_inline_auth(req, auth),
                None => req, // a `{ ref }` with no provider wired — send unauth'd
            }
        };
        let resp = req
            .send()
            .await
            .map_err(|e| FaucetError::Source(format!("http-file: request to {url} failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let body = body.chars().take(512).collect::<String>();
            return Err(FaucetError::HttpStatus {
                status: status.as_u16(),
                url: url.to_string(),
                body,
            });
        }
        Ok(resp
            .bytes()
            .await
            .map_err(|e| FaucetError::Source(format!("http-file: reading body of {url}: {e}")))?
            .to_vec())
    }

    /// Parse downloaded bytes into records per the resolved format.
    async fn parse(&self, bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
        match self.config.resolved_format() {
            FileFormat::Csv | FileFormat::Auto => {
                parse_csv(bytes, self.config.delimiter, self.config.has_headers).await
            }
            FileFormat::Excel => {
                parse_excel(bytes, self.config.sheet.as_deref(), self.config.header_row)
            }
        }
    }
}

/// Apply an inline [`HttpFileAuth`] to the request.
fn apply_inline_auth(req: RequestBuilder, auth: &HttpFileAuth) -> RequestBuilder {
    match auth {
        HttpFileAuth::None => req,
        HttpFileAuth::Bearer { token } => req.bearer_auth(token),
        HttpFileAuth::Basic { username, password } => req.basic_auth(username, Some(password)),
        HttpFileAuth::ApiKey { header, value } => req.header(header, value),
        HttpFileAuth::Custom { headers } => {
            let mut req = req;
            for (k, v) in headers {
                req = req.header(k, v);
            }
            req
        }
    }
}

/// Apply a shared-provider [`Credential`] to the request.
fn apply_credential(req: RequestBuilder, cred: Credential) -> RequestBuilder {
    match cred {
        Credential::Bearer(token) => req.bearer_auth(token),
        Credential::Token(token) => req.header(reqwest::header::AUTHORIZATION, token),
        Credential::Basic { username, password } => req.basic_auth(username, Some(password)),
        Credential::Header { name, value } => req.header(name, value),
    }
}

/// Parse CSV bytes into records. Header row (when `has_headers`) supplies field
/// names; otherwise fields are named `column_N`. Values are strings (like the
/// `csv` source). Pure (no network).
pub async fn parse_csv(
    bytes: &[u8],
    delimiter: u8,
    has_headers: bool,
) -> Result<Vec<Value>, FaucetError> {
    use futures::StreamExt as _;
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .flexible(true)
        .create_reader(bytes);
    let mut records = rdr.records();
    let mut headers: Option<Vec<String>> = None;
    let mut out = Vec::new();
    while let Some(rec) = records.next().await {
        let rec =
            rec.map_err(|e| FaucetError::Source(format!("http-file: CSV parse error: {e}")))?;
        if has_headers && headers.is_none() {
            headers = Some(rec.iter().map(str::to_string).collect());
            continue;
        }
        let mut obj = Map::new();
        for (i, field) in rec.iter().enumerate() {
            let key = headers
                .as_ref()
                .and_then(|h| h.get(i).cloned())
                .unwrap_or_else(|| format!("column_{i}"));
            obj.insert(key, Value::String(field.to_string()));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// Parse Excel bytes into records. Requires the `excel` feature.
#[cfg(feature = "excel")]
pub fn parse_excel(
    bytes: &[u8],
    sheet: Option<&str>,
    header_row: usize,
) -> Result<Vec<Value>, FaucetError> {
    use calamine::{Data, Reader, Xlsx};
    let cursor = std::io::Cursor::new(bytes.to_vec());
    let mut wb: Xlsx<_> = calamine::open_workbook_from_rs(cursor)
        .map_err(|e| FaucetError::Source(format!("http-file: opening Excel workbook: {e}")))?;
    let names = wb.sheet_names().to_vec();
    let name = match sheet {
        Some(s) if names.iter().any(|n| n == s) => s.to_string(),
        Some(s) => match s.parse::<usize>() {
            Ok(idx) => names.get(idx).cloned().ok_or_else(|| {
                FaucetError::Source(format!("http-file: sheet index {idx} out of range"))
            })?,
            Err(_) => {
                return Err(FaucetError::Source(format!(
                    "http-file: sheet '{s}' not found (available: {})",
                    names.join(", ")
                )));
            }
        },
        None => names
            .first()
            .cloned()
            .ok_or_else(|| FaucetError::Source("http-file: workbook has no worksheets".into()))?,
    };
    let range = wb
        .worksheet_range(&name)
        .map_err(|e| FaucetError::Source(format!("http-file: reading sheet '{name}': {e}")))?;
    let rows: Vec<&[Data]> = range.rows().collect();
    let header = rows.get(header_row).ok_or_else(|| {
        FaucetError::Source(format!(
            "http-file: header_row {header_row} is beyond the sheet ({} rows)",
            rows.len()
        ))
    })?;
    let headers: Vec<String> = header.iter().map(cell_to_string).collect();
    let mut out = Vec::new();
    for row in rows.iter().skip(header_row + 1) {
        let mut obj = Map::new();
        for (i, cell) in row.iter().enumerate() {
            let key = headers
                .get(i)
                .cloned()
                .filter(|k| !k.is_empty())
                .unwrap_or_else(|| format!("column_{i}"));
            obj.insert(key, cell_to_value(cell));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// Stub when the `excel` feature is disabled — errors loudly rather than
/// silently mis-parsing an Excel blob as CSV.
#[cfg(not(feature = "excel"))]
pub fn parse_excel(
    _bytes: &[u8],
    _sheet: Option<&str>,
    _header_row: usize,
) -> Result<Vec<Value>, FaucetError> {
    Err(FaucetError::Config(
        "http-file: Excel parsing requires the `excel` feature — rebuild with `--features excel` \
         (e.g. `cargo install faucet-cli --features source-http-file-excel`)"
            .into(),
    ))
}

#[cfg(feature = "excel")]
fn cell_to_string(cell: &calamine::Data) -> String {
    use calamine::Data;
    match cell {
        Data::String(s) => s.clone(),
        Data::Empty => String::new(),
        other => other.to_string(),
    }
}

#[cfg(feature = "excel")]
fn cell_to_value(cell: &calamine::Data) -> Value {
    use calamine::Data;
    match cell {
        Data::Empty => Value::Null,
        Data::String(s) => Value::String(s.clone()),
        Data::Bool(b) => Value::Bool(*b),
        Data::Int(i) => Value::from(*i),
        Data::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Data::DateTime(dt) => Value::String(dt.to_string()),
        Data::DateTimeIso(s) | Data::DurationIso(s) => Value::String(s.clone()),
        Data::Error(e) => Value::String(format!("{e:?}")),
    }
}

#[async_trait]
impl Source for HttpFileSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let url = if context.is_empty() {
            self.config.url.clone()
        } else {
            faucet_core::util::substitute_context(&self.config.url, context)
        };
        let bytes = self.fetch_bytes(&url).await?;
        self.parse(&bytes).await
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        Box::pin(async_stream::try_stream! {
            // A single blob is downloaded and parsed in full, then re-chunked
            // into pages of `batch_size` (0 = one page).
            let records = self.fetch_with_context(context).await?;
            if batch_size == 0 || records.is_empty() {
                yield StreamPage { records, bookmark: None };
            } else {
                let total = records.len();
                let mut iter = records.into_iter();
                let mut emitted = 0;
                loop {
                    let page: Vec<Value> = iter.by_ref().take(batch_size).collect();
                    if page.is_empty() { break; }
                    emitted += page.len();
                    let is_last = emitted >= total;
                    // No incremental replication → bookmark is always None.
                    yield StreamPage { records: page, bookmark: None };
                    if is_last { break; }
                }
            }
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(HttpFileSourceConfig)).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "http-file"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::AUTHORIZATION;

    fn built_headers(req: RequestBuilder) -> reqwest::header::HeaderMap {
        req.build().unwrap().headers().clone()
    }

    #[test]
    fn inline_auth_variants_apply_headers() {
        let client = Client::new();
        let g = || client.get("http://example.invalid/");

        // None → no Authorization header.
        assert!(
            !built_headers(apply_inline_auth(g(), &HttpFileAuth::None)).contains_key(AUTHORIZATION)
        );

        // Bearer.
        let h = built_headers(apply_inline_auth(
            g(),
            &HttpFileAuth::Bearer {
                token: "tok".into(),
            },
        ));
        assert_eq!(h[AUTHORIZATION], "Bearer tok");

        // Basic → base64-encoded.
        let h = built_headers(apply_inline_auth(
            g(),
            &HttpFileAuth::Basic {
                username: "u".into(),
                password: "p".into(),
            },
        ));
        assert!(h[AUTHORIZATION].to_str().unwrap().starts_with("Basic "));

        // ApiKey → named header.
        let h = built_headers(apply_inline_auth(
            g(),
            &HttpFileAuth::ApiKey {
                header: "X-Api-Key".into(),
                value: "abc".into(),
            },
        ));
        assert_eq!(h["x-api-key"], "abc");

        // Custom → arbitrary headers.
        let mut headers = HashMap::new();
        headers.insert("X-A".to_string(), "1".to_string());
        headers.insert("X-B".to_string(), "2".to_string());
        let h = built_headers(apply_inline_auth(g(), &HttpFileAuth::Custom { headers }));
        assert_eq!(h["x-a"], "1");
        assert_eq!(h["x-b"], "2");
    }

    #[test]
    fn credential_variants_apply_headers() {
        let client = Client::new();
        let g = || client.get("http://example.invalid/");

        assert_eq!(
            built_headers(apply_credential(g(), Credential::Bearer("tok".into())))[AUTHORIZATION],
            "Bearer tok"
        );
        assert_eq!(
            built_headers(apply_credential(g(), Credential::Token("raw-tok".into())))
                [AUTHORIZATION],
            "raw-tok"
        );
        assert!(
            built_headers(apply_credential(
                g(),
                Credential::Basic {
                    username: "u".into(),
                    password: "p".into()
                }
            ))[AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("Basic ")
        );
        let h = built_headers(apply_credential(
            g(),
            Credential::Header {
                name: "X-Auth".into(),
                value: "v".into(),
            },
        ));
        assert_eq!(h["x-auth"], "v");
    }

    #[test]
    fn new_honours_timeout_and_exposes_schema() {
        let mut cfg = HttpFileSourceConfig::new("https://x/f.csv");
        cfg.timeout_secs = Some(5);
        let s = HttpFileSource::new(cfg).unwrap();
        assert_eq!(s.connector_name(), "http-file");
        let schema = s.config_schema();
        assert!(schema.get("properties").is_some(), "schema: {schema}");
    }

    #[test]
    fn new_rejects_invalid_config() {
        assert!(HttpFileSource::new(HttpFileSourceConfig::new("  ")).is_err());
    }

    #[cfg(feature = "excel")]
    #[test]
    fn cell_conversions_cover_all_variants() {
        use calamine::Data;
        assert_eq!(cell_to_value(&Data::Empty), Value::Null);
        assert_eq!(
            cell_to_value(&Data::String("s".into())),
            Value::String("s".into())
        );
        assert_eq!(cell_to_value(&Data::Bool(true)), Value::Bool(true));
        assert_eq!(cell_to_value(&Data::Int(7)), Value::from(7i64));
        assert_eq!(cell_to_value(&Data::Float(1.5)), Value::from(1.5));
        assert!(cell_to_value(&Data::DateTimeIso("2020-01-01".into())).is_string());
        assert!(cell_to_value(&Data::DurationIso("PT1H".into())).is_string());
        assert!(cell_to_value(&Data::Error(calamine::CellErrorType::Div0)).is_string());

        assert_eq!(cell_to_string(&Data::String("k".into())), "k");
        assert_eq!(cell_to_string(&Data::Empty), "");
        assert_eq!(cell_to_string(&Data::Int(3)), "3");
    }

    #[cfg(feature = "excel")]
    #[test]
    fn parse_excel_error_paths() {
        let xlsx = include_bytes!("../tests/fixtures/sample.xlsx");
        // Sheet index out of range.
        assert!(
            parse_excel(xlsx, Some("99"), 0)
                .unwrap_err()
                .to_string()
                .contains("out of range")
        );
        // Named sheet not found.
        assert!(
            parse_excel(xlsx, Some("Nope"), 0)
                .unwrap_err()
                .to_string()
                .contains("not found")
        );
        // header_row beyond the sheet.
        assert!(
            parse_excel(xlsx, None, 9999)
                .unwrap_err()
                .to_string()
                .contains("beyond the sheet")
        );
        // Sheet selected by numeric index.
        let recs = parse_excel(xlsx, Some("1"), 0).unwrap();
        assert_eq!(recs[0]["k"], "x");
    }

    #[tokio::test]
    async fn parse_csv_with_headers() {
        let csv = b"id,name\n1,Alice\n2,Bob\n";
        let recs = parse_csv(csv, b',', true).await.unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["id"], "1");
        assert_eq!(recs[0]["name"], "Alice");
        assert_eq!(recs[1]["name"], "Bob");
    }

    #[tokio::test]
    async fn parse_csv_without_headers_generates_names() {
        let csv = b"1,Alice\n2,Bob\n";
        let recs = parse_csv(csv, b',', false).await.unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["column_0"], "1");
        assert_eq!(recs[0]["column_1"], "Alice");
    }

    #[tokio::test]
    async fn parse_csv_custom_delimiter_and_embedded_newline() {
        let csv = b"a;b\n1;\"x\ny\"\n";
        let recs = parse_csv(csv, b';', true).await.unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["b"], "x\ny");
    }

    #[cfg(not(feature = "excel"))]
    #[test]
    fn parse_excel_without_feature_errors() {
        let err = parse_excel(b"anything", None, 0).unwrap_err();
        assert!(err.to_string().contains("excel"), "{err}");
    }
}
