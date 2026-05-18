# faucet-cli

`faucet` — config-driven runner for [`faucet-stream`](https://crates.io/crates/faucet-stream) pipelines.

Write a YAML or JSON file describing a source, optional transforms, a sink, and (optionally) a state store. Run it with the `faucet` binary. No Rust code required.

## Install

```bash
cargo install faucet-cli
```

To build a slim binary with only the connectors you need:

```bash
cargo install faucet-cli --no-default-features \
    --features source-rest,sink-jsonl,sink-stdout,transforms
```

## Commands

| Command | What it does |
|---------|--------------|
| `faucet run <config>` | Execute the pipeline end-to-end. Supports `--dry-run`, `--limit N`, `--state-path PATH`. |
| `faucet validate <config>` | Parse + validate without running. Exits non-zero on error. |
| `faucet schema source|sink <name>` | Print the JSON Schema for a specific connector's config. |
| `faucet list` | List every compiled-in source, sink, transform, and state-store backend. |
| `faucet preview <config> --limit N` | Run only the source side and emit the first N records to stdout as JSONL. |
| `faucet init <name>` | Scaffold a starter `pipeline.yaml`. |

Pass `--log-level debug` (or set `FAUCET_LOG=debug`) for verbose tracing. Logs are written to stderr; pipeline records and command output go to stdout.

## Config shape

```yaml
version: 1
name: github_to_jsonl
source:
  type: rest
  config:
    base_url: https://api.github.com
    path: /repos/PawanSikawat/faucet-stream/issues
    method: GET
    auth:
      type: ApiKey
      header: Authorization
      value: Bearer ${env:GITHUB_TOKEN}
    query_params: {state: open}
    pagination:
      type: LinkHeader
    max_retries: 3
    retry_backoff: 1
    tolerated_http_errors: []
    replication_method: { type: FullTable }
    primary_keys: ["id"]
    partitions: []
    schema_sample_size: 100
transforms:
  - type: snake_case
sink:
  type: jsonl
  config:
    path: ./out/issues.jsonl
state:
  type: file
  config:
    path: ./.faucet-state
```

### Env / file interpolation

Anywhere in the config, `${env:VAR}` is replaced with the value of the environment variable, and `${file:./path}` with the (trimmed) contents of the file. Interpolation runs before YAML / JSON parsing, so resolved values can become any structured type. `${secret:VAR}` is an alias for `${env:VAR}` today; future releases will plug in a real secrets backend behind that prefix.

### State stores

```yaml
state:
  type: file              # or: memory, redis, postgres
  config:
    path: ./.faucet-state
```

The Redis and PostgreSQL backends ship behind the `state-redis` and `state-postgres` features.

### Transforms

```yaml
transforms:
  - type: snake_case
  - type: rename_keys
    config: { pattern: "^_sdc_", replacement: "" }
  - type: flatten
    config: { separator: "__" }
```

## Running from environment variables (`--from-env`)

`faucet` can build and run a pipeline entirely from `FAUCET_*` environment variables — no YAML file required. This mode is designed for container / Kubernetes / Airflow deployments where every config value naturally flows through the orchestrator's env-var interface.

```bash
faucet run --from-env
```

`--from-env` is mutually exclusive with a positional config path; you pick one source of truth or the other. Mixing them is rejected at argument-parse time.

### Variable schema

| Variable | Purpose |
|---|---|
| `FAUCET_SOURCE` | Source kind — same string keys as the YAML `source.type:` field (`rest`, `csv`, `postgres`, `postgres-cdc`, …). |
| `FAUCET_SOURCE_<KIND>_<FIELD>` | Scalar source-config fields. Scope is keyed by `<KIND>` so two different sources can't collide. |
| `FAUCET_SINK` | Sink kind. |
| `FAUCET_SINK_<KIND>_<FIELD>` | Scalar sink-config fields. |
| `FAUCET_STATE` | Optional. State store kind (`file`, `memory`, `redis`, `postgres`). |
| `FAUCET_STATE_<KIND>_<FIELD>` | State-store config. |
| `FAUCET_TRANSFORM_<N>` | Optional. Indexed transforms — `FAUCET_TRANSFORM_1=snake_case`, `FAUCET_TRANSFORM_2=flatten`. Indices must be contiguous starting at 1. |
| `FAUCET_TRANSFORM_<N>_<FIELD>` | Per-transform config (e.g. `FAUCET_TRANSFORM_2_SEPARATOR=__`). |
| `FAUCET_NAME` | Optional pipeline name (used in log messages). |

Field names are case-insensitive: write env vars in `SCREAMING_SNAKE_CASE`; they are lowercased before being matched against connector field names. Hyphens in connector kinds (e.g. `postgres-cdc`) become underscores in the env scope (`FAUCET_SOURCE_POSTGRES_CDC_*`). Empty values for `FAUCET_SOURCE` / `FAUCET_SINK` / `FAUCET_STATE` / `FAUCET_NAME` are treated as unset.

### Scalar values

Scalar fields go through a JSON-parse-then-string-fallback coercion: `30` is a number, `true` is a bool, `null` is JSON null, and anything that doesn't parse as JSON is treated as a plain string. This matches how the same value would be typed in YAML.

### Nested / tagged-enum fields (`*_JSON` escape hatch)

Tagged-enum config fields (`auth`, `pagination`, `replication_method`, `column_mapping`, …) don't flatten cleanly into env-var names because different variants have different sub-fields. For those, set the entire value as JSON under a `*_JSON` suffix:

```bash
FAUCET_SOURCE=rest \
FAUCET_SOURCE_REST_BASE_URL=https://api.github.com \
FAUCET_SOURCE_REST_PATH=/repos/PawanSikawat/faucet-stream/issues \
FAUCET_SOURCE_REST_AUTH_JSON='{"type":"Bearer","token":"ghp_xxx"}' \
FAUCET_SOURCE_REST_PAGINATION_JSON='{"type":"LinkHeader"}' \
FAUCET_SINK=jsonl \
FAUCET_SINK_JSONL_PATH=./issues.jsonl \
  faucet run --from-env
```

Setting both `FAUCET_SOURCE_REST_AUTH=...` and `FAUCET_SOURCE_REST_AUTH_JSON=...` for the same field is a hard error — pick one. The error names both variables.

### Loading a `.env` file first

Use `--env-file PATH` to load a `.env` file into the process environment before the env walker runs. Existing process-env values always win (12-factor convention). `--env-file` only works together with `--from-env`.

```bash
faucet run --from-env --env-file ./pipeline.env
```

## Examples

[`examples/`](examples/) ships YAML pipelines for every `faucet-stream/examples/*.rs` use case — the same source → sink combinations the library docs cover, expressed as config.

CLI-only smoke tests:

- [`csv_to_jsonl.yaml`](examples/csv_to_jsonl.yaml) — read a CSV, write JSONL (zero external deps)
- [`rest_to_stdout_preview.yaml`](examples/rest_to_stdout_preview.yaml) — pipe REST records into `jq`

Mirrors of the Rust examples (one `.yaml` per `.rs`):

- REST: [`rest_to_jsonl`](examples/rest_to_jsonl.yaml), [`rest_to_bigquery`](examples/rest_to_bigquery.yaml), [`rest_to_postgres`](examples/rest_to_postgres.yaml), [`rest_to_s3`](examples/rest_to_s3.yaml), [`rest_streaming`](examples/rest_streaming.yaml)
- GraphQL: [`graphql_to_bigquery`](examples/graphql_to_bigquery.yaml), [`graphql_to_postgres`](examples/graphql_to_postgres.yaml)
- XML/SOAP: [`xml_to_s3`](examples/xml_to_s3.yaml), [`xml_to_mongodb`](examples/xml_to_mongodb.yaml)
- gRPC: [`grpc_to_elasticsearch`](examples/grpc_to_elasticsearch.yaml), [`grpc_to_http`](examples/grpc_to_http.yaml)
- Databases: [`postgres_to_bigquery`](examples/postgres_to_bigquery.yaml), [`postgres_to_elasticsearch`](examples/postgres_to_elasticsearch.yaml), [`postgres_to_s3`](examples/postgres_to_s3.yaml), [`postgres_to_snowflake`](examples/postgres_to_snowflake.yaml), [`mysql_to_bigquery`](examples/mysql_to_bigquery.yaml), [`mysql_to_postgres`](examples/mysql_to_postgres.yaml), [`mysql_to_snowflake`](examples/mysql_to_snowflake.yaml), [`sqlite_to_jsonl`](examples/sqlite_to_jsonl.yaml), [`sqlite_to_csv`](examples/sqlite_to_csv.yaml)
- Document stores: [`mongodb_to_postgres`](examples/mongodb_to_postgres.yaml), [`mongodb_to_elasticsearch`](examples/mongodb_to_elasticsearch.yaml), [`mongodb_to_redis`](examples/mongodb_to_redis.yaml)
- Search / cache: [`elasticsearch_to_redis`](examples/elasticsearch_to_redis.yaml), [`elasticsearch_to_s3`](examples/elasticsearch_to_s3.yaml), [`redis_to_mysql`](examples/redis_to_mysql.yaml), [`redis_to_sqlite`](examples/redis_to_sqlite.yaml)
- Object storage: [`s3_to_bigquery`](examples/s3_to_bigquery.yaml), [`s3_to_mongodb`](examples/s3_to_mongodb.yaml), [`s3_to_postgres`](examples/s3_to_postgres.yaml), [`s3_to_snowflake`](examples/s3_to_snowflake.yaml)
- CSV in: [`csv_to_bigquery`](examples/csv_to_bigquery.yaml), [`csv_to_mysql`](examples/csv_to_mysql.yaml), [`csv_to_sqlite`](examples/csv_to_sqlite.yaml)
- Webhook receiver: [`webhook_to_csv`](examples/webhook_to_csv.yaml), [`webhook_to_http`](examples/webhook_to_http.yaml), [`webhook_to_postgres`](examples/webhook_to_postgres.yaml)
- DAG parent leg: [`dag_users_posts`](examples/dag_users_posts.yaml) — parent only (multi-node DAGs require the library API today)

Every auth shape — Bearer, Basic, API key, OAuth2, custom headers, gRPC metadata — round-trips through YAML/JSON, so the YAML examples are 1:1 with the Rust ones.

## License

MIT OR Apache-2.0
