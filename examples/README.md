# Examples

Every connector pair ships a ready-to-run config in
[`cli/examples/`](../cli/examples/). This directory adds the **local
infrastructure** to actually run the ones that need a database, broker, or
object store — one `docker compose up` brings up Postgres, MySQL, Kafka
(Redpanda), Redis, MongoDB, Elasticsearch, and MinIO (S3-compatible).

## Quick start

```bash
# 1. Build/install the CLI (full build, all connectors)
cargo install --path cli            # or: cargo install faucet-cli

# 2. Bring up the local stack
docker compose -f examples/docker-compose.yml up -d

# 3. Run an example (Postgres CDC → JSONL)
faucet run cli/examples/postgres_cdc_to_jsonl.yaml

# 4. Tear it down (and wipe volumes)
docker compose -f examples/docker-compose.yml down -v
```

A `Makefile` wraps the common steps: `make demo` (no-infra smoke test),
`make infra-up`, `make infra-down`.

## No infrastructure required

These run immediately after installing the CLI — great for a first smoke test:

| Example | Notes |
|---------|-------|
| `csv_to_jsonl.yaml` | the canonical smoke test (`make demo` runs this) |
| `csv_to_sqlite.yaml` | CSV → local SQLite file |
| `sqlite_to_jsonl.yaml`, `sqlite_to_csv.yaml` | local SQLite → file |
| `rest_to_jsonl.yaml`, `rest_streaming.yaml`, `rest_to_stdout_preview.yaml` | point `base_url` at any HTTP API; preview needs no sink setup |
| `rest_filter_explode_to_stdout.yaml` | `filter` + `explode` + `keys_case` against DummyJSON; demonstrates the v1 JSONPath subset and the merge rule |
| `shared_auth_rest.yaml` | one OAuth2 provider in the top-level `auth:` block shared across four matrix rows via `auth: { ref }` — single token, single-flight refresh (point `base_url` / token endpoint at a real API) |
| `rest_to_jsonl_with_vault.yaml` | Vault KV v2 secret injected as a Bearer token via `${vault:…#field}`; requires `VAULT_ADDR` + `VAULT_TOKEN` and `--features secrets-vault` |
| `websocket_to_jsonl.yaml` | none (live public WS endpoint — Binance BTC/USDT trade stream, no auth) |

> REST / GraphQL / XML / gRPC / webhook source examples hit an external endpoint
> (the configs use a placeholder `base_url`). Edit it to a real API — there's no
> mock server in the stack.

## Covered by the local Docker stack

The service column lists what each example touches; all are provided by
`docker-compose.yml`. **S3** examples use MinIO — set the S3 endpoint to
`http://localhost:9000` with credentials `faucet` / `faucetfaucet`.

| Example | Services |
|---------|----------|
| `postgres_cdc_to_jsonl.yaml` | postgres (logical replication preconfigured) |
| `rest_to_postgres.yaml`, `rest_to_postgres_with_quality.yaml`, `mongodb_to_postgres.yaml`, `graphql_to_postgres.yaml`, `webhook_to_postgres.yaml` | postgres (+ source) |
| `mysql_to_postgres.yaml` | mysql, postgres |
| `csv_to_mysql.yaml`, `redis_to_mysql.yaml` | mysql (+ source) |
| `redis_to_sqlite.yaml`, `mongodb_to_redis.yaml`, `elasticsearch_to_redis.yaml` | redis (+ source) |
| `kafka_to_jsonl.yaml`, `rest_to_kafka.yaml` | redpanda (Kafka API) |
| `mongodb_to_elasticsearch.yaml`, `postgres_to_elasticsearch.yaml`, `grpc_to_elasticsearch.yaml` | elasticsearch (+ source) |
| `elasticsearch_to_s3.yaml`, `postgres_to_s3.yaml`, `rest_to_s3.yaml`, `xml_to_s3.yaml` | minio (S3) (+ source) |
| `s3_to_postgres.yaml`, `s3_to_mongodb.yaml` | minio (S3), target |
| `mongodb_to_redis.yaml`, `xml_to_mongodb.yaml` | mongodb (+ source) |
| `webhook_to_csv.yaml`, `webhook_to_http.yaml`, `grpc_to_http.yaml` | none external beyond the source/HTTP target |
| `dag_users_posts.yaml`, `rest_users_posts_dag.yaml`, `rest_to_bigquery_matrix.yaml`, `templates_*.yaml` | demonstrate matrix / DAG / template syntax (REST source) |

## Cloud credentials required (not in the stack)

BigQuery, Snowflake, and GCS need real cloud projects and credentials, so they
can't run against local Docker. Provide credentials via env vars / service-account
files as each config shows:

`csv_to_bigquery.yaml`, `graphql_to_bigquery.yaml`, `mysql_to_bigquery.yaml`,
`postgres_to_bigquery.yaml`, `rest_to_bigquery.yaml`, `s3_to_bigquery.yaml`,
`mysql_to_snowflake.yaml`, `postgres_to_snowflake.yaml`, `s3_to_snowflake.yaml`.

## Tips

- `faucet validate <config>` checks any config without running it (and without
  infra) — useful in CI.
- `faucet preview <config> --limit 10` runs just the source and prints records.
- Most examples read secrets from the environment via `${env:VAR}`; export them
  or drop a sibling `.env` before running.
- The Postgres service seeds a `users` table and a `faucet_pub` publication (see
  [`infra/postgres-init.sql`](infra/postgres-init.sql)) so the CDC and query
  examples work immediately.
