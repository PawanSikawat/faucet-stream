#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-stream
//!
//! A declarative, config-driven data pipeline with pluggable source and sink
//! connectors.
//!
//! 📖 **Guide, tutorials & cookbook:** <https://pawansikawat.github.io/faucet-stream/>
//!
//! ## Feature flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `source-rest` *(default)* | REST API source with pagination, auth, transforms |
//! | `source-graphql` | GraphQL API source with cursor pagination |
//! | `source-xml` | XML/SOAP API source with XML-to-JSON conversion |
//! | `source-grpc` | gRPC source with dynamic protobuf messages |
//! | `source-postgres` | PostgreSQL query source |
//! | `source-postgres-cdc` | PostgreSQL CDC source (logical replication) |
//! | `source-mysql` | MySQL query source |
//! | `source-mssql` | Microsoft SQL Server query source |
//! | `source-sqlite` | SQLite query source |

//! | `source-s3` | AWS S3 file source |
//! | `source-mongodb` | MongoDB query source |
//! | `source-mongodb-cdc` | MongoDB CDC source (Change Streams) |
//! | `source-mysql-cdc` | MySQL CDC source (binlog replication) |
//! | `source-redis` | Redis source (streams, lists, keys) |
//! | `source-webhook` | Webhook HTTP receiver source |
//! | `source-websocket` | WebSocket streaming source |
//! | `source-csv` | CSV file source |
//! | `source-elasticsearch` | Elasticsearch search/scroll source |
//! | `source-kafka` | Apache Kafka consumer source |
//! | `source-parquet` | Apache Parquet file source (local, glob, S3) |
//! | `sink-bigquery` | Google BigQuery streaming insert sink |
//! | `sink-iceberg` | Apache Iceberg sink (append-only, REST/Glue/SQL/HMS catalogs) |
//! | `sink-postgres` | PostgreSQL sink (jsonb or auto-mapped columns) |
//! | `sink-jsonl` | JSON Lines file sink |
//! | `sink-snowflake` | Snowflake SQL REST API sink |
//! | `sink-mysql` | MySQL sink |
//! | `sink-mssql` | Microsoft SQL Server sink |
//! | `sink-sqlite` | SQLite sink |

//! | `sink-s3` | AWS S3 file sink |
//! | `sink-mongodb` | MongoDB insert sink |
//! | `sink-redis` | Redis sink (streams, lists, key-value) |
//! | `sink-csv` | CSV file sink |
//! | `sink-elasticsearch` | Elasticsearch bulk index sink |
//! | `sink-http` | HTTP POST sink |
//! | `sink-kafka` | Apache Kafka producer sink |
//! | `sink-parquet` | Apache Parquet file sink (local, S3) |
//! | `kafka-schema-registry` | Schema Registry support for Kafka connectors |
//! | `source` | All source connectors |
//! | `sink` | All sink connectors |
//! | `full` | Every connector |

// Always re-export core types and traits.
pub use faucet_core::*;

// Explicit re-exports for the library-side transforms wrapper and observability
// labels so users can import them via the umbrella path.
pub use faucet_core::TransformingSource;
pub use faucet_core::observability::Labels;

// ── Shared auth providers ────────────────────────────────────────────────────
/// Single-flight OAuth2 / token-endpoint auth providers (enable the `auth`
/// feature). Share one across connectors via `with_auth_provider` or the CLI
/// `auth: { ref }` catalog.
#[cfg(feature = "auth")]
pub mod auth {
    pub use faucet_auth::*;
}

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

    #[cfg(feature = "source-postgres")]
    pub mod postgres {
        pub use faucet_source_postgres::*;
    }

    #[cfg(feature = "source-postgres-cdc")]
    pub mod postgres_cdc {
        pub use faucet_source_postgres_cdc::*;
    }

    #[cfg(feature = "source-mysql")]
    pub mod mysql {
        pub use faucet_source_mysql::*;
    }

    #[cfg(feature = "source-mssql")]
    pub mod mssql {
        pub use faucet_source_mssql::*;
    }

    #[cfg(feature = "source-sqlite")]
    pub mod sqlite {
        pub use faucet_source_sqlite::*;
    }

    #[cfg(feature = "source-s3")]
    pub mod s3 {
        pub use faucet_source_s3::*;
    }

    #[cfg(feature = "source-mongodb")]
    pub mod mongodb {
        pub use faucet_source_mongodb::*;
    }

    #[cfg(feature = "source-mongodb-cdc")]
    pub mod mongodb_cdc {
        pub use faucet_source_mongodb_cdc::*;
    }

    #[cfg(feature = "source-mysql-cdc")]
    pub mod mysql_cdc {
        pub use faucet_source_mysql_cdc::*;
    }

    #[cfg(feature = "source-redis")]
    pub mod redis {
        pub use faucet_source_redis::*;
    }

    #[cfg(feature = "source-webhook")]
    pub mod webhook {
        pub use faucet_source_webhook::*;
    }

    #[cfg(feature = "source-websocket")]
    pub mod websocket {
        pub use faucet_source_websocket::*;
    }

    #[cfg(feature = "source-csv")]
    pub mod csv {
        pub use faucet_source_csv::*;
    }

    #[cfg(feature = "source-elasticsearch")]
    pub mod elasticsearch {
        pub use faucet_source_elasticsearch::*;
    }

    #[cfg(feature = "source-kafka")]
    pub mod kafka {
        pub use faucet_source_kafka::*;
    }

    #[cfg(feature = "source-parquet")]
    pub mod parquet {
        pub use faucet_source_parquet::*;
    }

    #[cfg(feature = "source-gcs")]
    pub mod gcs {
        pub use faucet_source_gcs::*;
    }

    #[cfg(feature = "source-singer")]
    pub mod singer {
        pub use faucet_source_singer::*;
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

    #[cfg(feature = "source-postgres")]
    pub mod postgres {
        pub use faucet_source_postgres::*;
    }

    #[cfg(feature = "source-postgres-cdc")]
    pub mod postgres_cdc {
        pub use faucet_source_postgres_cdc::*;
    }

    #[cfg(feature = "source-mysql")]
    pub mod mysql {
        pub use faucet_source_mysql::*;
    }

    #[cfg(feature = "source-mssql")]
    pub mod mssql {
        pub use faucet_source_mssql::*;
    }

    #[cfg(feature = "source-sqlite")]
    pub mod sqlite {
        pub use faucet_source_sqlite::*;
    }

    #[cfg(feature = "source-s3")]
    pub mod s3 {
        pub use faucet_source_s3::*;
    }

    #[cfg(feature = "source-mongodb")]
    pub mod mongodb {
        pub use faucet_source_mongodb::*;
    }

    #[cfg(feature = "source-mongodb-cdc")]
    pub mod mongodb_cdc {
        pub use faucet_source_mongodb_cdc::*;
    }

    #[cfg(feature = "source-mysql-cdc")]
    pub mod mysql_cdc {
        pub use faucet_source_mysql_cdc::*;
    }

    #[cfg(feature = "source-redis")]
    pub mod redis {
        pub use faucet_source_redis::*;
    }

    #[cfg(feature = "source-webhook")]
    pub mod webhook {
        pub use faucet_source_webhook::*;
    }

    #[cfg(feature = "source-websocket")]
    pub mod websocket {
        pub use faucet_source_websocket::*;
    }

    #[cfg(feature = "source-csv")]
    pub mod csv {
        pub use faucet_source_csv::*;
    }

    #[cfg(feature = "source-elasticsearch")]
    pub mod elasticsearch {
        pub use faucet_source_elasticsearch::*;
    }

    #[cfg(feature = "source-kafka")]
    pub mod kafka {
        pub use faucet_source_kafka::*;
    }

    #[cfg(feature = "source-parquet")]
    pub mod parquet {
        pub use faucet_source_parquet::*;
    }

    #[cfg(feature = "source-gcs")]
    pub mod gcs {
        pub use faucet_source_gcs::*;
    }

    #[cfg(feature = "source-singer")]
    pub mod singer {
        pub use faucet_source_singer::*;
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

    #[cfg(feature = "sink-iceberg")]
    pub mod iceberg {
        pub use faucet_sink_iceberg::*;
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

    #[cfg(feature = "sink-mysql")]
    pub mod mysql {
        pub use faucet_sink_mysql::*;
    }

    #[cfg(feature = "sink-mssql")]
    pub mod mssql {
        pub use faucet_sink_mssql::*;
    }

    #[cfg(feature = "sink-sqlite")]
    pub mod sqlite {
        pub use faucet_sink_sqlite::*;
    }

    #[cfg(feature = "sink-s3")]
    pub mod s3 {
        pub use faucet_sink_s3::*;
    }

    #[cfg(feature = "sink-mongodb")]
    pub mod mongodb {
        pub use faucet_sink_mongodb::*;
    }

    #[cfg(feature = "sink-redis")]
    pub mod redis {
        pub use faucet_sink_redis::*;
    }

    #[cfg(feature = "sink-csv")]
    pub mod csv {
        pub use faucet_sink_csv::*;
    }

    #[cfg(feature = "sink-elasticsearch")]
    pub mod elasticsearch {
        pub use faucet_sink_elasticsearch::*;
    }

    #[cfg(feature = "sink-http")]
    pub mod http {
        pub use faucet_sink_http::*;
    }

    #[cfg(feature = "sink-stdout")]
    pub mod stdout {
        pub use faucet_sink_stdout::*;
    }

    #[cfg(feature = "sink-kafka")]
    pub mod kafka {
        pub use faucet_sink_kafka::*;
    }

    #[cfg(feature = "sink-parquet")]
    pub mod parquet {
        pub use faucet_sink_parquet::*;
    }

    #[cfg(feature = "sink-gcs")]
    pub mod gcs {
        pub use faucet_sink_gcs::*;
    }
}

// ── GCS common types ─────────────────────────────────────────────────────────

#[cfg(any(feature = "source-gcs", feature = "sink-gcs"))]
pub mod common_gcs {
    pub use faucet_common_gcs::*;
}

// ── Kafka common types ───────────────────────────────────────────────────────

#[cfg(any(feature = "source-kafka", feature = "sink-kafka"))]
pub mod common_kafka {
    pub use faucet_common_kafka::*;
}

// ── State-store backends ─────────────────────────────────────────────────────

pub mod state {
    #[cfg(feature = "state-redis")]
    pub mod redis {
        pub use faucet_state_redis::*;
    }

    #[cfg(feature = "state-postgres")]
    pub mod postgres {
        pub use faucet_state_postgres::*;
    }
}

// ── Lineage (OpenLineage emission) ───────────────────────────────────────────
/// OpenLineage event emission for pipeline runs (enable the `lineage` feature;
/// `lineage-kafka` adds the Kafka transport). The CLI wires this automatically
/// from a `lineage:` config block; library callers can build a
/// [`lineage::LineageEmitter`] directly.
#[cfg(feature = "lineage")]
pub use faucet_lineage as lineage;

// ── SQL transform (embedded DuckDB) ──────────────────────────────────────────
/// SQL-as-transform: run DuckDB SQL over each pipeline page (the `batch`
/// relation). Enable the `transform-sql` feature. The CLI wires this via the
/// `sql` transform; library callers build [`transform_sql::SqlTransform`] and
/// attach it with [`TransformingSource`].
#[cfg(feature = "transform-sql")]
pub use faucet_transform_sql as transform_sql;
