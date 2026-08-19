#![cfg_attr(docsrs, feature(doc_cfg))]
//! # faucet-source-http-file
//!
//! An authenticated **HTTP file source** for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem: download
//! a file from an authenticated HTTP(S) URL — a Microsoft Graph / OneDrive /
//! SharePoint `…/content` endpoint, a signed S3 URL, any authed static host —
//! and parse it into records.
//!
//! - **CSV** parsing is always available (streaming RFC-4180 via `csv-async`,
//!   so quoted embedded newlines round-trip).
//! - **Excel** (`.xlsx`/`.xls`) parsing is available behind the `excel` crate
//!   feature (via `calamine`).
//! - **`format: auto`** (the default) infers CSV vs Excel from the URL
//!   extension.
//!
//! Authentication uses the project-wide `{ type, config }` shape and also
//! accepts `auth: { ref: <name> }` to share a provider from the top-level
//! `auth:` catalog — so an `oauth2_refresh` provider minting a Graph access
//! token can be reused across rows.
//!
//! ```no_run
//! use faucet_source_http_file::{HttpFileSource, HttpFileSourceConfig};
//! # async fn run() -> Result<(), faucet_core::FaucetError> {
//! let cfg = HttpFileSourceConfig::new("https://graph.microsoft.com/v1.0/me/drive/items/ID/content");
//! let source = HttpFileSource::new(cfg)?;
//! # let _ = source;
//! # Ok(()) }
//! ```

mod config;
mod stream;

pub use config::{FileFormat, HttpFileAuth, HttpFileSourceConfig};
pub use stream::{HttpFileSource, parse_csv, parse_excel};
