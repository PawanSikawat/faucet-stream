# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
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
