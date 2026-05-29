//! Feature-gated dispatch from a string `type` to a concrete connector.
//!
//! Every arm in this file is guarded by the matching `source-*` / `sink-*`
//! Cargo feature so users can build a slim binary with just the connectors
//! they need. The string keys here are the public contract of the CLI's
//! `type:` field in YAML/JSON pipeline configs.

use crate::error::{CliError, CliResult};
use faucet_core::{Sink, Source};
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Build a [`Source`] trait object from a `(kind, config)` pair.
pub async fn build_source(kind: &str, config: Value) -> CliResult<Box<dyn Source>> {
    match kind {
        #[cfg(feature = "source-rest")]
        "rest" => {
            let cfg = decode::<faucet_source_rest::RestStreamConfig>("source", "rest", config)?;
            Ok(Box::new(faucet_source_rest::RestStream::new(cfg)?))
        }
        #[cfg(feature = "source-graphql")]
        "graphql" => {
            let cfg =
                decode::<faucet_source_graphql::GraphqlStreamConfig>("source", "graphql", config)?;
            Ok(Box::new(faucet_source_graphql::GraphqlStream::new(cfg)))
        }
        #[cfg(feature = "source-xml")]
        "xml" => {
            let cfg = decode::<faucet_source_xml::XmlStreamConfig>("source", "xml", config)?;
            Ok(Box::new(faucet_source_xml::XmlStream::new(cfg)))
        }
        #[cfg(feature = "source-grpc")]
        "grpc" => {
            let cfg = decode::<faucet_source_grpc::GrpcStreamConfig>("source", "grpc", config)?;
            Ok(Box::new(faucet_source_grpc::GrpcStream::new(cfg)?))
        }
        #[cfg(feature = "source-postgres")]
        "postgres" => {
            let cfg = decode::<faucet_source_postgres::PostgresSourceConfig>(
                "source", "postgres", config,
            )?;
            Ok(Box::new(
                faucet_source_postgres::PostgresSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-postgres-cdc")]
        "postgres-cdc" => {
            let cfg = decode::<faucet_source_postgres_cdc::PostgresCdcSourceConfig>(
                "source",
                "postgres-cdc",
                config,
            )?;
            Ok(Box::new(
                faucet_source_postgres_cdc::PostgresCdcSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-mysql")]
        "mysql" => {
            let cfg = decode::<faucet_source_mysql::MysqlSourceConfig>("source", "mysql", config)?;
            Ok(Box::new(faucet_source_mysql::MysqlSource::new(cfg).await?))
        }
        #[cfg(feature = "source-sqlite")]
        "sqlite" => {
            let cfg =
                decode::<faucet_source_sqlite::SqliteSourceConfig>("source", "sqlite", config)?;
            Ok(Box::new(
                faucet_source_sqlite::SqliteSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-s3")]
        "s3" => {
            let cfg = decode::<faucet_source_s3::S3SourceConfig>("source", "s3", config)?;
            Ok(Box::new(faucet_source_s3::S3Source::new(cfg).await?))
        }
        #[cfg(feature = "source-mongodb")]
        "mongodb" => {
            let cfg =
                decode::<faucet_source_mongodb::MongoSourceConfig>("source", "mongodb", config)?;
            Ok(Box::new(
                faucet_source_mongodb::MongoSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-redis")]
        "redis" => {
            let cfg = decode::<faucet_source_redis::RedisSourceConfig>("source", "redis", config)?;
            Ok(Box::new(faucet_source_redis::RedisSource::new(cfg)))
        }
        #[cfg(feature = "source-webhook")]
        "webhook" => {
            let cfg =
                decode::<faucet_source_webhook::WebhookSourceConfig>("source", "webhook", config)?;
            Ok(Box::new(faucet_source_webhook::WebhookSource::new(cfg)))
        }
        #[cfg(feature = "source-websocket")]
        "websocket" => {
            let cfg = decode::<faucet_source_websocket::WebsocketSourceConfig>(
                "source",
                "websocket",
                config,
            )?;
            Ok(Box::new(faucet_source_websocket::WebsocketSource::new(
                cfg,
            )?))
        }
        #[cfg(feature = "source-csv")]
        "csv" => {
            let cfg = decode::<faucet_source_csv::CsvSourceConfig>("source", "csv", config)?;
            Ok(Box::new(faucet_source_csv::CsvSource::new(cfg)))
        }
        #[cfg(feature = "source-elasticsearch")]
        "elasticsearch" => {
            let cfg = decode::<faucet_source_elasticsearch::ElasticsearchSourceConfig>(
                "source",
                "elasticsearch",
                config,
            )?;
            Ok(Box::new(
                faucet_source_elasticsearch::ElasticsearchSource::new(cfg),
            ))
        }
        #[cfg(feature = "source-kafka")]
        "kafka" => {
            let cfg = decode::<faucet_source_kafka::KafkaSourceConfig>("source", "kafka", config)?;
            Ok(Box::new(faucet_source_kafka::KafkaSource::new(cfg).await?))
        }
        #[cfg(feature = "source-parquet")]
        "parquet" => {
            let cfg =
                decode::<faucet_source_parquet::ParquetSourceConfig>("source", "parquet", config)?;
            Ok(Box::new(
                faucet_source_parquet::ParquetSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-gcs")]
        "gcs" => {
            let cfg = decode::<faucet_source_gcs::GcsSourceConfig>("source", "gcs", config)?;
            Ok(Box::new(faucet_source_gcs::GcsSource::new(cfg).await?))
        }
        #[cfg(feature = "source-bigquery")]
        "bigquery" => {
            let cfg = decode::<faucet_source_bigquery::BigQuerySourceConfig>(
                "source", "bigquery", config,
            )?;
            Ok(Box::new(
                faucet_source_bigquery::BigQuerySource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-snowflake")]
        "snowflake" => {
            let cfg = decode::<faucet_source_snowflake::SnowflakeSourceConfig>(
                "source",
                "snowflake",
                config,
            )?;
            Ok(Box::new(faucet_source_snowflake::SnowflakeSource::new(cfg)))
        }
        other => Err(unknown(other, "source", source_kinds())),
    }
}

/// Build a [`Sink`] trait object from a `(kind, config)` pair.
pub async fn build_sink(kind: &str, config: Value) -> CliResult<Box<dyn Sink>> {
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => {
            let cfg =
                decode::<faucet_sink_bigquery::BigQuerySinkConfig>("sink", "bigquery", config)?;
            Ok(Box::new(
                faucet_sink_bigquery::BigQuerySink::new(cfg).await?,
            ))
        }
        #[cfg(feature = "sink-postgres")]
        "postgres" => {
            let cfg =
                decode::<faucet_sink_postgres::PostgresSinkConfig>("sink", "postgres", config)?;
            Ok(Box::new(
                faucet_sink_postgres::PostgresSink::new(cfg).await?,
            ))
        }
        #[cfg(feature = "sink-jsonl")]
        "jsonl" => {
            let cfg = decode::<faucet_sink_jsonl::JsonlSinkConfig>("sink", "jsonl", config)?;
            Ok(Box::new(faucet_sink_jsonl::JsonlSink::new(cfg)))
        }
        #[cfg(feature = "sink-snowflake")]
        "snowflake" => {
            let cfg =
                decode::<faucet_sink_snowflake::SnowflakeSinkConfig>("sink", "snowflake", config)?;
            Ok(Box::new(faucet_sink_snowflake::SnowflakeSink::new(cfg)?))
        }
        #[cfg(feature = "sink-mysql")]
        "mysql" => {
            let cfg = decode::<faucet_sink_mysql::MysqlSinkConfig>("sink", "mysql", config)?;
            Ok(Box::new(faucet_sink_mysql::MysqlSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => {
            let cfg = decode::<faucet_sink_sqlite::SqliteSinkConfig>("sink", "sqlite", config)?;
            Ok(Box::new(faucet_sink_sqlite::SqliteSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-s3")]
        "s3" => {
            let cfg = decode::<faucet_sink_s3::S3SinkConfig>("sink", "s3", config)?;
            Ok(Box::new(faucet_sink_s3::S3Sink::new(cfg).await?))
        }
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => {
            let cfg = decode::<faucet_sink_mongodb::MongoSinkConfig>("sink", "mongodb", config)?;
            Ok(Box::new(faucet_sink_mongodb::MongoSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-redis")]
        "redis" => {
            let cfg = decode::<faucet_sink_redis::RedisSinkConfig>("sink", "redis", config)?;
            Ok(Box::new(faucet_sink_redis::RedisSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-csv")]
        "csv" => {
            let cfg = decode::<faucet_sink_csv::CsvSinkConfig>("sink", "csv", config)?;
            Ok(Box::new(faucet_sink_csv::CsvSink::new(cfg)))
        }
        #[cfg(feature = "sink-elasticsearch")]
        "elasticsearch" => {
            let cfg = decode::<faucet_sink_elasticsearch::ElasticsearchSinkConfig>(
                "sink",
                "elasticsearch",
                config,
            )?;
            Ok(Box::new(faucet_sink_elasticsearch::ElasticsearchSink::new(
                cfg,
            )?))
        }
        #[cfg(feature = "sink-kafka")]
        "kafka" => {
            let cfg = decode::<faucet_sink_kafka::KafkaSinkConfig>("sink", "kafka", config)?;
            Ok(Box::new(faucet_sink_kafka::KafkaSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-http")]
        "http" => {
            let cfg = decode::<faucet_sink_http::HttpSinkConfig>("sink", "http", config)?;
            Ok(Box::new(faucet_sink_http::HttpSink::new(cfg)))
        }
        #[cfg(feature = "sink-stdout")]
        "stdout" => {
            let cfg = decode::<faucet_sink_stdout::StdoutSinkConfig>("sink", "stdout", config)?;
            Ok(Box::new(faucet_sink_stdout::StdoutSink::new(cfg)))
        }
        #[cfg(feature = "sink-parquet")]
        "parquet" => {
            let cfg = decode::<faucet_sink_parquet::ParquetSinkConfig>("sink", "parquet", config)?;
            Ok(Box::new(faucet_sink_parquet::ParquetSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-gcs")]
        "gcs" => {
            let cfg = decode::<faucet_sink_gcs::GcsSinkConfig>("sink", "gcs", config)?;
            Ok(Box::new(faucet_sink_gcs::GcsSink::new(cfg).await?))
        }
        other => Err(unknown(other, "sink", sink_kinds())),
    }
}

/// Return the JSON Schema for the named source's config struct.
pub fn source_schema(kind: &str) -> CliResult<Value> {
    match kind {
        #[cfg(feature = "source-rest")]
        "rest" => Ok(schema::<faucet_source_rest::RestStreamConfig>()),
        #[cfg(feature = "source-graphql")]
        "graphql" => Ok(schema::<faucet_source_graphql::GraphqlStreamConfig>()),
        #[cfg(feature = "source-xml")]
        "xml" => Ok(schema::<faucet_source_xml::XmlStreamConfig>()),
        #[cfg(feature = "source-grpc")]
        "grpc" => Ok(schema::<faucet_source_grpc::GrpcStreamConfig>()),
        #[cfg(feature = "source-postgres")]
        "postgres" => Ok(schema::<faucet_source_postgres::PostgresSourceConfig>()),
        #[cfg(feature = "source-postgres-cdc")]
        "postgres-cdc" => Ok(schema::<faucet_source_postgres_cdc::PostgresCdcSourceConfig>()),
        #[cfg(feature = "source-mysql")]
        "mysql" => Ok(schema::<faucet_source_mysql::MysqlSourceConfig>()),
        #[cfg(feature = "source-sqlite")]
        "sqlite" => Ok(schema::<faucet_source_sqlite::SqliteSourceConfig>()),
        #[cfg(feature = "source-s3")]
        "s3" => Ok(schema::<faucet_source_s3::S3SourceConfig>()),
        #[cfg(feature = "source-mongodb")]
        "mongodb" => Ok(schema::<faucet_source_mongodb::MongoSourceConfig>()),
        #[cfg(feature = "source-redis")]
        "redis" => Ok(schema::<faucet_source_redis::RedisSourceConfig>()),
        #[cfg(feature = "source-webhook")]
        "webhook" => Ok(schema::<faucet_source_webhook::WebhookSourceConfig>()),
        #[cfg(feature = "source-websocket")]
        "websocket" => Ok(schema::<faucet_source_websocket::WebsocketSourceConfig>()),
        #[cfg(feature = "source-csv")]
        "csv" => Ok(schema::<faucet_source_csv::CsvSourceConfig>()),
        #[cfg(feature = "source-elasticsearch")]
        "elasticsearch" => Ok(schema::<
            faucet_source_elasticsearch::ElasticsearchSourceConfig,
        >()),
        #[cfg(feature = "source-kafka")]
        "kafka" => Ok(schema::<faucet_source_kafka::KafkaSourceConfig>()),
        #[cfg(feature = "source-parquet")]
        "parquet" => Ok(schema::<faucet_source_parquet::ParquetSourceConfig>()),
        #[cfg(feature = "source-gcs")]
        "gcs" => Ok(schema::<faucet_source_gcs::GcsSourceConfig>()),
        #[cfg(feature = "source-bigquery")]
        "bigquery" => Ok(schema::<faucet_source_bigquery::BigQuerySourceConfig>()),
        #[cfg(feature = "source-snowflake")]
        "snowflake" => Ok(schema::<faucet_source_snowflake::SnowflakeSourceConfig>()),
        other => Err(unknown(other, "source", source_kinds())),
    }
}

/// Check if a source kind is registered (not unknown or disabled by feature gate).
pub fn source_exists(kind: &str) -> bool {
    source_schema(kind).is_ok()
}

/// Check if a sink kind is registered (not unknown or disabled by feature gate).
pub fn sink_exists(kind: &str) -> bool {
    sink_schema(kind).is_ok()
}

/// Return the JSON Schema for the named sink's config struct.
pub fn sink_schema(kind: &str) -> CliResult<Value> {
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => Ok(schema::<faucet_sink_bigquery::BigQuerySinkConfig>()),
        #[cfg(feature = "sink-postgres")]
        "postgres" => Ok(schema::<faucet_sink_postgres::PostgresSinkConfig>()),
        #[cfg(feature = "sink-jsonl")]
        "jsonl" => Ok(schema::<faucet_sink_jsonl::JsonlSinkConfig>()),
        #[cfg(feature = "sink-snowflake")]
        "snowflake" => Ok(schema::<faucet_sink_snowflake::SnowflakeSinkConfig>()),
        #[cfg(feature = "sink-mysql")]
        "mysql" => Ok(schema::<faucet_sink_mysql::MysqlSinkConfig>()),
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => Ok(schema::<faucet_sink_sqlite::SqliteSinkConfig>()),
        #[cfg(feature = "sink-s3")]
        "s3" => Ok(schema::<faucet_sink_s3::S3SinkConfig>()),
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => Ok(schema::<faucet_sink_mongodb::MongoSinkConfig>()),
        #[cfg(feature = "sink-redis")]
        "redis" => Ok(schema::<faucet_sink_redis::RedisSinkConfig>()),
        #[cfg(feature = "sink-csv")]
        "csv" => Ok(schema::<faucet_sink_csv::CsvSinkConfig>()),
        #[cfg(feature = "sink-elasticsearch")]
        "elasticsearch" => Ok(schema::<faucet_sink_elasticsearch::ElasticsearchSinkConfig>()),
        #[cfg(feature = "sink-kafka")]
        "kafka" => Ok(schema::<faucet_sink_kafka::KafkaSinkConfig>()),
        #[cfg(feature = "sink-http")]
        "http" => Ok(schema::<faucet_sink_http::HttpSinkConfig>()),
        #[cfg(feature = "sink-stdout")]
        "stdout" => Ok(schema::<faucet_sink_stdout::StdoutSinkConfig>()),
        #[cfg(feature = "sink-parquet")]
        "parquet" => Ok(schema::<faucet_sink_parquet::ParquetSinkConfig>()),
        #[cfg(feature = "sink-gcs")]
        "gcs" => Ok(schema::<faucet_sink_gcs::GcsSinkConfig>()),
        other => Err(unknown(other, "sink", sink_kinds())),
    }
}

/// One-line summary of every compiled-in source connector. Used by `faucet list`.
#[allow(clippy::vec_init_then_push)]
pub fn source_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v: Vec<(&'static str, &'static str)> = Vec::new();
    #[cfg(feature = "source-rest")]
    v.push(("rest", "REST API source with pagination, auth, transforms"));
    #[cfg(feature = "source-graphql")]
    v.push(("graphql", "GraphQL API source with cursor pagination"));
    #[cfg(feature = "source-xml")]
    v.push(("xml", "XML / SOAP API source with XML→JSON conversion"));
    #[cfg(feature = "source-grpc")]
    v.push(("grpc", "gRPC source with dynamic protobuf"));
    #[cfg(feature = "source-postgres")]
    v.push(("postgres", "PostgreSQL query source"));
    #[cfg(feature = "source-postgres-cdc")]
    v.push((
        "postgres-cdc",
        "PostgreSQL CDC source (logical replication)",
    ));
    #[cfg(feature = "source-mysql")]
    v.push(("mysql", "MySQL query source"));
    #[cfg(feature = "source-sqlite")]
    v.push(("sqlite", "SQLite query source"));
    #[cfg(feature = "source-s3")]
    v.push(("s3", "AWS S3 object source"));
    #[cfg(feature = "source-mongodb")]
    v.push(("mongodb", "MongoDB query source"));
    #[cfg(feature = "source-redis")]
    v.push(("redis", "Redis (streams, lists, keys) source"));
    #[cfg(feature = "source-webhook")]
    v.push(("webhook", "Webhook HTTP receiver source"));
    #[cfg(feature = "source-websocket")]
    v.push((
        "websocket",
        "WebSocket streaming source — connects, subscribes, streams each message as a record",
    ));
    #[cfg(feature = "source-csv")]
    v.push(("csv", "CSV file source"));
    #[cfg(feature = "source-elasticsearch")]
    v.push(("elasticsearch", "Elasticsearch search / scroll source"));
    #[cfg(feature = "source-kafka")]
    v.push(("kafka", "Apache Kafka consumer (rdkafka). Subscribes to topics and drains messages with idle/max-messages termination."));
    #[cfg(feature = "source-parquet")]
    v.push(("parquet", "Apache Parquet file source (local path, glob, or S3). Streams record batches via the Arrow async reader."));
    #[cfg(feature = "source-gcs")]
    v.push((
        "gcs",
        "Google Cloud Storage source — JSONL, JSON array, or raw text",
    ));
    #[cfg(feature = "source-bigquery")]
    v.push((
        "bigquery",
        "Google BigQuery query source (jobs.query + jobs.getQueryResults)",
    ));
    #[cfg(feature = "source-snowflake")]
    v.push((
        "snowflake",
        "Snowflake query source (SQL REST API with partition paging)",
    ));
    v
}

/// One-line summary of every compiled-in sink connector. Used by `faucet list`.
#[allow(clippy::vec_init_then_push)]
pub fn sink_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v: Vec<(&'static str, &'static str)> = Vec::new();
    #[cfg(feature = "sink-bigquery")]
    v.push(("bigquery", "Google BigQuery streaming-insert sink"));
    #[cfg(feature = "sink-postgres")]
    v.push(("postgres", "PostgreSQL sink (JSONB or auto-mapped columns)"));
    #[cfg(feature = "sink-jsonl")]
    v.push(("jsonl", "JSON Lines file sink"));
    #[cfg(feature = "sink-snowflake")]
    v.push(("snowflake", "Snowflake SQL REST API sink"));
    #[cfg(feature = "sink-mysql")]
    v.push(("mysql", "MySQL sink"));
    #[cfg(feature = "sink-sqlite")]
    v.push(("sqlite", "SQLite sink"));
    #[cfg(feature = "sink-s3")]
    v.push(("s3", "AWS S3 object sink"));
    #[cfg(feature = "sink-mongodb")]
    v.push(("mongodb", "MongoDB insert sink"));
    #[cfg(feature = "sink-redis")]
    v.push(("redis", "Redis (streams, lists, key-value) sink"));
    #[cfg(feature = "sink-csv")]
    v.push(("csv", "CSV file sink"));
    #[cfg(feature = "sink-elasticsearch")]
    v.push(("elasticsearch", "Elasticsearch bulk index sink"));
    #[cfg(feature = "sink-kafka")]
    v.push(("kafka", "Apache Kafka producer (rdkafka). FuturesUnordered batched sends with QueueFull retry; supports fixed or per-record topic routing."));
    #[cfg(feature = "sink-http")]
    v.push(("http", "HTTP POST sink (individual or array batch)"));
    #[cfg(feature = "sink-stdout")]
    v.push(("stdout", "Stdout / stderr sink (JSON Lines, pretty, TSV)"));
    #[cfg(feature = "sink-parquet")]
    v.push(("parquet", "Apache Parquet file sink (local path or S3). Schema-inferred, configurable compression, row/byte rollover."));
    #[cfg(feature = "sink-gcs")]
    v.push(("gcs", "Google Cloud Storage sink — JSONL files"));
    v
}

/// Names of every compiled-in source connector.
pub fn source_kinds() -> Vec<&'static str> {
    source_descriptions().into_iter().map(|(k, _)| k).collect()
}

/// Names of every compiled-in sink connector.
pub fn sink_kinds() -> Vec<&'static str> {
    sink_descriptions().into_iter().map(|(k, _)| k).collect()
}

fn decode<T: DeserializeOwned>(kind: &'static str, name: &str, config: Value) -> CliResult<T> {
    serde_json::from_value(config).map_err(|e| CliError::InvalidConnectorConfig {
        kind,
        name: name.to_owned(),
        message: scrub_config_error(&e.to_string()),
    })
}

/// Sanitise a serde deserialization error before it reaches stderr/logs.
///
/// serde_json's `invalid type:` errors echo the offending value as a
/// double-quoted literal — which can be a secret injected via
/// `${secret:...}` / `${env:...}`. Replace every double-quoted run with a
/// placeholder (field/type names use backticks and are preserved for
/// diagnostics) and cap the length so a huge value can't flood the log
/// (#78/#38). Note: `${secret:}` is currently an `${env:}` alias with no
/// at-rest redaction — this only scrubs error *output*.
fn scrub_config_error(msg: &str) -> String {
    const MAX_CHARS: usize = 200;
    let mut out = String::with_capacity(msg.len());
    let mut in_quote = false;
    for c in msg.chars() {
        if c == '"' {
            if !in_quote {
                out.push_str("\"<redacted>\"");
            }
            in_quote = !in_quote;
            continue;
        }
        if !in_quote {
            out.push(c);
        }
    }
    if out.chars().count() > MAX_CHARS {
        let truncated: String = out.chars().take(MAX_CHARS).collect();
        return format!("{truncated}…");
    }
    out
}

fn schema<T: faucet_core::JsonSchema>() -> Value {
    serde_json::to_value(faucet_core::schema_for!(T))
        .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
}

fn unknown(name: &str, kind: &'static str, available: Vec<&'static str>) -> CliError {
    CliError::UnknownConnector {
        kind,
        name: name.to_owned(),
        available: if available.is_empty() {
            "(none — rebuild faucet-cli with the relevant feature enabled)".to_owned()
        } else {
            available.join(", ")
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "source-rest")]
    #[test]
    fn rest_source_appears_in_listings() {
        assert!(source_kinds().contains(&"rest"));
    }

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn jsonl_sink_appears_in_listings() {
        assert!(sink_kinds().contains(&"jsonl"));
    }

    #[tokio::test]
    async fn unknown_source_kind_errors() {
        let err = build_source("nope", serde_json::json!({}))
            .await
            .err()
            .expect("should fail");
        match err {
            CliError::UnknownConnector { kind, name, .. } => {
                assert_eq!(kind, "source");
                assert_eq!(name, "nope");
            }
            other => panic!("expected UnknownConnector, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_sink_kind_errors() {
        let err = build_sink("nope", serde_json::json!({}))
            .await
            .err()
            .expect("should fail");
        assert!(matches!(
            err,
            CliError::UnknownConnector { kind: "sink", .. }
        ));
    }

    #[cfg(feature = "source-rest")]
    #[test]
    fn rest_schema_is_object() {
        let s = source_schema("rest").unwrap();
        assert!(s.is_object());
    }

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn jsonl_schema_is_object() {
        let s = sink_schema("jsonl").unwrap();
        assert!(s.is_object());
    }

    #[test]
    fn scrub_config_error_redacts_quoted_values() {
        // A serde "invalid type" error echoes the offending value in double
        // quotes — must be redacted so a secret can't reach the log (#78/#38).
        let msg =
            r#"invalid type: string "sk-super-secret-123", expected a sequence at line 1 column 9"#;
        let scrubbed = scrub_config_error(msg);
        assert!(!scrubbed.contains("sk-super-secret-123"), "{scrubbed}");
        assert!(scrubbed.contains("<redacted>"), "{scrubbed}");
        // Structural context outside the quotes is preserved.
        assert!(scrubbed.contains("invalid type"), "{scrubbed}");
        assert!(scrubbed.contains("expected a sequence"), "{scrubbed}");
    }

    #[test]
    fn scrub_config_error_truncates_long_messages() {
        let msg = "x".repeat(500);
        let scrubbed = scrub_config_error(&msg);
        assert!(
            scrubbed.chars().count() <= 201,
            "len {}",
            scrubbed.chars().count()
        );
        assert!(scrubbed.ends_with('…'));
    }
}
