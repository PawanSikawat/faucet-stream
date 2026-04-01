//! # faucet-stream
//!
//! A declarative, config-driven data pipeline with pluggable source and sink
//! connectors.
//!
//! ## Feature flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `source-rest` *(default)* | REST API source with pagination, auth, transforms |
//! | `sink-bigquery` | Google BigQuery streaming insert sink |
//! | `source` | All source connectors |
//! | `sink` | All sink connectors |
//! | `full` | Every connector |

// Always re-export core types and traits.
pub use faucet_core::*;

// ── Source connectors ────────────────────────────────────────────────────────

#[cfg(feature = "source-rest")]
pub mod source {
    pub mod rest {
        pub use faucet_source_rest::*;
    }
}

// Backwards-compatible flat re-exports for existing users who depend on
// `faucet-stream::{RestStream, Auth, ...}` without the `source::rest::` path.
#[cfg(feature = "source-rest")]
pub use faucet_source_rest::{
    Auth, DEFAULT_EXPIRY_RATIO, DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO, PaginationStyle,
    ResponseValidator, RestStream, RestStreamConfig, fetch_oauth2_token, fetch_token_from_endpoint,
};

// ── Sink connectors ──────────────────────────────────────────────────────────

#[cfg(feature = "sink-bigquery")]
pub mod sink {
    pub mod bigquery {
        pub use faucet_sink_bigquery::*;
    }
}
