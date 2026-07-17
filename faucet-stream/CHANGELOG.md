# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [1.4.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-stream-v1.3.0...faucet-stream-v1.4.0) - 2026-07-17

### Features

- *(databricks)* Databricks SQL query source via Statement Execution API ([#320](https://github.com/PawanSikawat/faucet-stream/pull/320))
- *(delta)* Apache Delta Lake source + sink via delta-rs ([#319](https://github.com/PawanSikawat/faucet-stream/pull/319))
- Encryption at rest for state/DLQ + live TUI for faucet run ([#315](https://github.com/PawanSikawat/faucet-stream/pull/315))
- Google Cloud Spanner source + sink connectors ([#312](https://github.com/PawanSikawat/faucet-stream/pull/312))
- Connector conformance battery + tiers, FCP spec, sink-bound benchmark, sink config fixes ([#307](https://github.com/PawanSikawat/faucet-stream/pull/307))
- *(cli)* Plugin loading, schema config, connector scaffolding + registry, plan/dev, hot reload ([#306](https://github.com/PawanSikawat/faucet-stream/pull/306))
- AWS Kinesis source + sink connectors and shipped Grafana dashboards / Prometheus alerts
- Faucet discover (live source introspection) + faucet backfill (resumable historical replay)

### Testing

- *(conformance)* Promote connectors to Tier-1 with the full conformance battery ([#311](https://github.com/PawanSikawat/faucet-stream/pull/311))

## [1.3.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-stream-v1.2.0...faucet-stream-v1.3.0) - 2026-07-10

### Features

- Typed delivery guarantees, effectively-once coverage expansion, and prebuilt binary distribution ([#294](https://github.com/PawanSikawat/faucet-stream/pull/294))
- Singer tap bridge + conformance battery (+ docs precision & Meltano benchmark) ([#289](https://github.com/PawanSikawat/faucet-stream/pull/289))

### Miscellaneous

- *(dist)* Homebrew tap homebrew-faucet-stream, formula faucet-cli ([#295](https://github.com/PawanSikawat/faucet-stream/pull/295))

## [1.2.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-stream-v1.1.1...faucet-stream-v1.2.0) - 2026-07-08

### Documentation

- Ship interactive local demo (try-local.sh) + quickstart & console screenshots ([#288](https://github.com/PawanSikawat/faucet-stream/pull/288))

### Features

- *(masking)* PII detection + column-level masking policies ([#206](https://github.com/PawanSikawat/faucet-stream/pull/206))
- *(cli)* Depends_on — completion ordering between matrix rows ([#276](https://github.com/PawanSikawat/faucet-stream/pull/276))
- *(cli)* Data-freshness & volume SLA monitoring with anomaly alerts ([#275](https://github.com/PawanSikawat/faucet-stream/pull/275))
- *(cli)* Faucet test — fixture-based offline pipeline testing ([#273](https://github.com/PawanSikawat/faucet-stream/pull/273))
- *(core)* Data contracts — versioned output schema/constraints enforced per page ([#272](https://github.com/PawanSikawat/faucet-stream/pull/272))

## [1.1.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-stream-v1.1.0...faucet-stream-v1.1.1) - 2026-06-22

### Miscellaneous

- Updated the following local packages: faucet-core, faucet-lineage, faucet-common-gcs, faucet-source-rest, faucet-source-graphql, faucet-source-xml, faucet-source-postgres, faucet-source-mysql, faucet-source-mssql, faucet-source-gcs, faucet-source-s3, faucet-source-mongodb-cdc, faucet-source-mysql-cdc, faucet-source-redis, faucet-source-sqlite, faucet-source-csv, faucet-source-parquet, faucet-source-postgres-cdc, faucet-source-snowflake, faucet-sink-bigquery, faucet-sink-postgres, faucet-sink-snowflake, faucet-sink-mysql, faucet-sink-sqlite, faucet-sink-mssql, faucet-sink-csv, faucet-sink-elasticsearch, faucet-sink-http, faucet-sink-kafka, faucet-sink-parquet, faucet-sink-iceberg, faucet-auth, faucet-transform-sql, faucet-common-bigquery, faucet-common-kafka, faucet-common-snowflake, faucet-source-grpc, faucet-source-kafka, faucet-source-mongodb, faucet-source-webhook, faucet-source-websocket, faucet-source-elasticsearch, faucet-source-bigquery, faucet-sink-jsonl, faucet-sink-gcs, faucet-sink-s3, faucet-sink-mongodb, faucet-sink-redis, faucet-sink-stdout, faucet-state-redis, faucet-state-postgres
