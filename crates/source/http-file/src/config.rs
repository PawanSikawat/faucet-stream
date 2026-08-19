//! Configuration for the authenticated HTTP file source.

use faucet_core::AuthSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// File format of the downloaded blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// Infer from the URL extension (`.xlsx`/`.xls` → Excel, else CSV). Default.
    #[default]
    Auto,
    /// Comma/delimiter-separated text.
    Csv,
    /// Excel workbook (`.xlsx`/`.xls`). Requires the `excel` crate feature.
    Excel,
}

/// Authentication for the file download. Inline `{ type, config }` or a
/// `{ ref: <name> }` pointer to a shared provider — the same shape every other
/// HTTP connector uses, so a OneDrive/Graph token minted by an `oauth2_refresh`
/// provider can be shared via `auth: { ref }`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum HttpFileAuth {
    /// No authentication.
    #[default]
    None,
    /// `Authorization: Bearer <token>`.
    Bearer { token: String },
    /// HTTP Basic credentials.
    Basic { username: String, password: String },
    /// An API key in a named header (e.g. `X-Api-Key`).
    ApiKey { header: String, value: String },
    /// Arbitrary headers attached to the request.
    Custom { headers: HashMap<String, String> },
}

fn default_delimiter() -> u8 {
    b','
}
fn default_has_headers() -> bool {
    true
}
fn default_header_row() -> usize {
    0
}
fn default_batch_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

/// Configuration for [`HttpFileSource`](crate::HttpFileSource).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HttpFileSourceConfig {
    /// URL of the file to download (e.g. a Microsoft Graph
    /// `/drive/items/{id}/content` endpoint). Supports `{key}` context
    /// substitution.
    pub url: String,

    /// Authentication for the request. Inline or `{ ref: <name> }`.
    #[serde(default)]
    pub auth: AuthSpec<HttpFileAuth>,

    /// File format. `auto` (default) infers from the URL extension.
    #[serde(default)]
    pub format: FileFormat,

    // ── CSV options ─────────────────────────────────────────────────────────
    /// CSV field delimiter (default `,`).
    #[serde(default = "default_delimiter")]
    pub delimiter: u8,
    /// Whether the first CSV row is a header row supplying field names
    /// (default `true`). When `false`, fields are named `column_0`, `column_1`, …
    #[serde(default = "default_has_headers")]
    pub has_headers: bool,

    // ── Excel options ───────────────────────────────────────────────────────
    /// Worksheet to read: a sheet name, or a 0-based index as a string. When
    /// omitted, the first worksheet is used.
    #[serde(default)]
    pub sheet: Option<String>,
    /// 0-based row index of the Excel header row (default `0`). Rows above it
    /// are skipped; the header row supplies field names.
    #[serde(default = "default_header_row")]
    pub header_row: usize,

    // ── Streaming / reliability ─────────────────────────────────────────────
    /// Records per emitted page (default [`DEFAULT_BATCH_SIZE`](faucet_core::DEFAULT_BATCH_SIZE)).
    /// `0` drains the whole file into a single page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Per-request HTTP timeout in seconds (default `60`).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl HttpFileSourceConfig {
    /// Create a config with the required URL and defaults.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth: AuthSpec::Inline(HttpFileAuth::None),
            format: FileFormat::Auto,
            delimiter: b',',
            has_headers: true,
            sheet: None,
            header_row: 0,
            batch_size: faucet_core::DEFAULT_BATCH_SIZE,
            timeout_secs: None,
        }
    }

    /// Resolve `format: auto` against the URL extension.
    pub fn resolved_format(&self) -> FileFormat {
        match self.format {
            FileFormat::Auto => {
                let lower = self
                    .url
                    .split('?')
                    .next()
                    .unwrap_or(&self.url)
                    .to_lowercase();
                if lower.ends_with(".xlsx") || lower.ends_with(".xls") {
                    FileFormat::Excel
                } else {
                    FileFormat::Csv
                }
            }
            other => other,
        }
    }

    /// Validate the config: non-empty URL and a sane `batch_size`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        if self.url.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "http-file: `url` must not be empty".into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_format_infers_from_extension() {
        assert_eq!(
            HttpFileSourceConfig::new("https://x/file.xlsx").resolved_format(),
            FileFormat::Excel
        );
        assert_eq!(
            HttpFileSourceConfig::new("https://x/file.XLS?q=1").resolved_format(),
            FileFormat::Excel
        );
        assert_eq!(
            HttpFileSourceConfig::new("https://x/data.csv").resolved_format(),
            FileFormat::Csv
        );
        // Graph /content has no extension → CSV by default.
        assert_eq!(
            HttpFileSourceConfig::new("https://graph/items/1/content").resolved_format(),
            FileFormat::Csv
        );
    }

    #[test]
    fn explicit_format_wins() {
        let mut c = HttpFileSourceConfig::new("https://x/file.csv");
        c.format = FileFormat::Excel;
        assert_eq!(c.resolved_format(), FileFormat::Excel);
    }

    #[test]
    fn validate_rejects_empty_url() {
        assert!(HttpFileSourceConfig::new("  ").validate().is_err());
        assert!(
            HttpFileSourceConfig::new("https://x/f.csv")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn auth_defaults_to_none_and_parses_ref() {
        let c: HttpFileSourceConfig =
            serde_json::from_value(serde_json::json!({ "url": "https://x/f.csv" })).unwrap();
        assert!(matches!(c.auth, AuthSpec::Inline(HttpFileAuth::None)));
        let r: HttpFileSourceConfig = serde_json::from_value(
            serde_json::json!({ "url": "https://x/f.csv", "auth": { "ref": "graph" } }),
        )
        .unwrap();
        assert_eq!(r.auth.reference_name(), Some("graph"));
    }
}
