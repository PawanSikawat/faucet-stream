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
//! | `source-graphql` | GraphQL API source with cursor pagination |
//! | `source-xml` | XML/SOAP API source with XML-to-JSON conversion |
//! | `source-grpc` | gRPC source with dynamic protobuf messages |
//! | `sink-bigquery` | Google BigQuery streaming insert sink |
//! | `sink-postgres` | PostgreSQL sink (jsonb or auto-mapped columns) |
//! | `sink-jsonl` | JSON Lines file sink |
//! | `sink-snowflake` | Snowflake SQL REST API sink |
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

    #[cfg(feature = "source-graphql")]
    pub mod graphql {
        pub use faucet_source_graphql::*;
    }

    #[cfg(feature = "source-xml")]
    pub mod xml {
        pub use faucet_source_xml::*;
    }

    #[cfg(feature = "source-grpc")]
    pub mod grpc {
        pub use faucet_source_grpc::*;
    }
}

// Source modules available without source-rest (when only other sources are enabled).
#[cfg(not(feature = "source-rest"))]
pub mod source {
    #[cfg(feature = "source-graphql")]
    pub mod graphql {
        pub use faucet_source_graphql::*;
    }

    #[cfg(feature = "source-xml")]
    pub mod xml {
        pub use faucet_source_xml::*;
    }

    #[cfg(feature = "source-grpc")]
    pub mod grpc {
        pub use faucet_source_grpc::*;
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

pub mod sink {
    #[cfg(feature = "sink-bigquery")]
    pub mod bigquery {
        pub use faucet_sink_bigquery::*;
    }

    #[cfg(feature = "sink-postgres")]
    pub mod postgres {
        pub use faucet_sink_postgres::*;
    }

    #[cfg(feature = "sink-jsonl")]
    pub mod jsonl {
        pub use faucet_sink_jsonl::*;
    }

    #[cfg(feature = "sink-snowflake")]
    pub mod snowflake {
        pub use faucet_sink_snowflake::*;
    }
}
