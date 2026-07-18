# Summary

[Introduction](./introduction.md)

# Getting Started

- [Installation](./getting-started/installation.md)
- [Your first pipeline](./getting-started/first-pipeline.md)
- [Try it locally (interactive demo)](./getting-started/try-it-locally.md)
- [Core concepts](./getting-started/concepts.md)
- [Learn the architecture (interactive)](./getting-started/learn.md)

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
- [Schema drift](./cookbook/schema-drift.md)
- [Replication (snapshot → CDC)](./cookbook/replication.md)
- [Backfill (historical replay)](./cookbook/backfill.md)
- [Source discovery (auto-generate configs)](./cookbook/discover.md)
- [Dead-letter queues](./cookbook/dlq.md)
- [Resilience (retry / circuit breaker / poison-pill)](./cookbook/resilience.md)
- [Data-quality checks](./cookbook/quality.md)
- [Data contracts](./cookbook/contracts.md)
- [PII detection & masking](./cookbook/masking.md)
- [SLA monitoring (freshness & volume)](./cookbook/sla.md)
- [Notifications (Slack / PagerDuty / webhook)](./cookbook/notifications.md)
- [Testing pipelines](./cookbook/testing.md)
- [Compression](./cookbook/compression.md)
- [Record transforms](./cookbook/transforms.md)
- [SQL transform](./cookbook/sql-transform.md)
- [Secrets-manager interpolation](./cookbook/secrets.md)
- [Config composition](./cookbook/composition.md)
- [Adaptive batching](./cookbook/adaptive-batching.md)
- [Throughput tuning](./cookbook/tuning.md)
- [Scheduling](./cookbook/scheduling.md)
- [Running faucet as a service](./cookbook/serve.md)
- [Web console (`serve-ui`)](./cookbook/web-console.md)
- [Running a cluster](./cookbook/cluster.md)
- [Event-driven triggers](./cookbook/triggers.md)
- [Lineage (OpenLineage)](./cookbook/lineage.md)
- [Dashboards & alerts](./cookbook/dashboards.md)
- [Data Movement Catalog](./cookbook/catalog.md)
- [Troubleshooting with faucet doctor](./cookbook/troubleshooting.md)

# Reference

- [Connector catalog](./reference/connectors.md)
- [Choosing a connector](./reference/choosing.md)
- [CLI commands](./reference/cli.md)
- [Configuration file format](./reference/config.md)
- [Editor setup (autocomplete & validation)](./reference/editor-setup.md)
- [HTTP API (`faucet serve`)](./reference/http-api.md)
- [Triggers](./reference/triggers.md)

# Comparisons

- [How faucet-stream compares](./comparison/index.md)
- [vs. Meltano (Singer)](./comparison/meltano.md)
- [vs. Airbyte](./comparison/airbyte.md)
- [vs. Singer](./comparison/singer.md)

# Operations

- [Deploying faucet](./operations/deploying.md)
- [Observability](./operations/observability.md)
- [Performance tuning](./operations/tuning.md)
- [Troubleshooting & FAQ](./operations/troubleshooting.md)

# Extending

- [Authoring a connector](./extending/authoring-connectors.md)
- [Connector protocol (FCP v0)](./spec/faucet-connector-spec-v0.md)
- [Connector marketplace](./extending/marketplace.md)
