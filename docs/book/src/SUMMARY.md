# Summary

[Introduction](./introduction.md)

# Getting Started

- [Installation](./getting-started/installation.md)
- [Your first pipeline](./getting-started/first-pipeline.md)
- [Core concepts](./getting-started/concepts.md)

# Tutorials

- [REST API → BigQuery (incremental)](./tutorials/rest-to-bigquery.md)
- [PostgreSQL CDC → JSONL](./tutorials/postgres-cdc.md)
- [Multi-pipeline DAGs with `matrix`](./tutorials/matrix-dag.md)
- [Embedding faucet as a Rust library](./tutorials/library.md)

# Cookbook

- [Pagination styles](./cookbook/pagination.md)
- [Authentication](./cookbook/auth.md)
- [Incremental replication & state](./cookbook/state.md)
- [Upsert / mirror tables](./cookbook/upsert.md)
- [Dead-letter queues](./cookbook/dlq.md)
- [Data-quality checks](./cookbook/quality.md)
- [Compression](./cookbook/compression.md)
- [Record transforms](./cookbook/transforms.md)
- [SQL transform](./cookbook/sql-transform.md)
- [Secrets-manager interpolation](./cookbook/secrets.md)
- [Config composition](./cookbook/composition.md)
- [Adaptive batching](./cookbook/adaptive-batching.md)
- [Scheduling](./cookbook/scheduling.md)
- [Running faucet as a service](./cookbook/serve.md)
- [Web console (`serve-ui`)](./cookbook/web-console.md)
- [Running a cluster](./cookbook/cluster.md)
- [Lineage (OpenLineage)](./cookbook/lineage.md)
- [Troubleshooting with faucet doctor](./cookbook/troubleshooting.md)

# Reference

- [Connector catalog](./reference/connectors.md)
- [Choosing a connector](./reference/choosing.md)
- [CLI commands](./reference/cli.md)
- [Configuration file format](./reference/config.md)
- [HTTP API (`faucet serve`)](./reference/http-api.md)

# Operations

- [Deploying faucet](./operations/deploying.md)
- [Observability](./operations/observability.md)
- [Performance tuning](./operations/tuning.md)
- [Troubleshooting & FAQ](./operations/troubleshooting.md)

# Extending

- [Authoring a connector](./extending/authoring-connectors.md)
