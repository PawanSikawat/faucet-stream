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

### Config + `.env` auto-discovery

`run`, `validate`, and `preview` all auto-discover their inputs from the current directory:

| What | Behaviour |
|------|-----------|
| Config path omitted | Probe `faucet.yaml` → `faucet.yml` → `faucet.json` in cwd; first match wins. |
| `.env` in cwd | Loaded automatically before any `${env:VAR}` interpolation runs. |
| `--env-file <path>` | Forces a specific file. The file must exist or the command errors. Works in both YAML mode and `--from-env`. |
| `--no-env-file` | Disables `.env` auto-loading. Cannot be combined with `--env-file`. |
| Process env vs `.env` | Process env always wins — `.env` only fills in unset variables. |

So `cd into-your-project && faucet run` is the short form for `faucet run --env-file .env faucet.yaml` whenever both files are present.

## Config shape

```yaml
version: 1
name: github_to_jsonl

pipeline:
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

`pipeline:` is the only required block. Anything you would have written at the top level pre-#54 (`source:`, `transforms:`, `sink:`, `state:`) now lives one level deeper inside `pipeline:`. Validation rejects the old shape with a clear hint.

### Matrix mode — run many invocations from one config

Add a `matrix:` block to run multiple invocations from the same base. Each row is **deep-merged** into `pipeline:` (objects merge recursively, arrays replace wholesale, scalars replace). Rows with `parent:` become children that fan out one invocation per record produced by the parent row.

```yaml
version: 1
name: api_to_warehouse

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      auth: { type: Bearer, token: ${env:API_TOKEN} }
      pagination: { type: PageNumber, param_name: page, page_size: 100 }
  sink:
    type: bigquery
    config:
      service_account_key_path: ${env:GCP_SA_PATH}
      project_id: my-project

matrix:
  # Independent roots — different paths/tables, shared auth + sink type.
  - id: users
    source: { config: { path: /v1/users } }
    sink:   { config: { dataset: raw, table: users } }
  - id: products
    source: { config: { path: /v1/products } }
    sink:   { config: { dataset: raw, table: products } }

  # DAG fan-out — one child invocation per parent record.
  - id: user_posts
    parent: users
    source: { config: { path: /v1/users/${users.id}/posts } }
    sink:   { config: { dataset: raw, table: user_posts } }

execution:
  max_concurrent: 8
  on_error: continue   # or `stop`
```

#### Deep-merge rules

- Objects merge recursively (overlay keys win on collision).
- Arrays replace wholesale — no element-merging, no concat. If a row needs to add to an inherited list, redeclare it.
- Scalars / `null` / numbers / booleans replace.

#### Two-stage interpolation

Tokens are resolved in two passes:

| Token | When |
|-------|------|
| `${env:VAR}` | Load-time, before YAML parsing. |
| `${file:./path}` | Load-time. File contents trimmed of trailing whitespace. |
| `${secret:VAR}` | Load-time. Alias for `${env:VAR}` today. |
| `${row_id.dotted.path}` | Run-time, per parent record. The `row_id` must be the id of another matrix row. |

`$${` escapes a literal `${`. Reserved row ids that can never appear in `matrix.id`: `env`, `file`, `secret`, `matrix`, `pipeline`.

#### Execution

- `max_concurrent` bounds total in-flight invocations (roots + per-parent-record children compete for one budget). Default: `min(num_cpus, 4)`.
- `on_error: continue` (default) — a failed invocation is logged, its subtree is skipped, every sibling already running keeps running to completion. The process exits non-zero if any invocation failed.
- `on_error: stop` — first failure halts the entire run. Pending invocations waiting on a concurrency permit are dropped before they start; in-flight invocations running in parallel are cancelled at their next `.await` point. Honours `max_concurrent` like `continue` does, so the failure is detected as quickly as the slowest in-flight task can be polled.

> **Caveat for `stop`:** cancelling a task mid-write may leave partial state in the sink — a half-flushed file, an open transaction, a connection that closed before the server's response was read. Idempotent sinks (JSONL append, S3 put with a fixed key, BigQuery streaming insert with `insertId`, upsert-style writes) handle re-runs cleanly. Non-idempotent sinks (`HTTP POST` without dedupe headers, `INSERT` with auto-id) may double-write on retry. If you can't tolerate that, prefer `on_error: continue` and reconcile failed rows after the fact.

#### State keys

- Root invocations: `{name}::{row_id}`.
- Child invocations: `{name}::{row_id}::{parent_record_key}` where `parent_record_key` is the value at `parent_key` (default `id`) in the parent record.

A state-key collision among siblings sharing a parent is detected upfront and errors with both offenders named.

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

## Observability (Prometheus + tracing)

Optional top-level block in `faucet.yaml`:

```yaml
version: 1
name: github-issues-sync
observability:
  prometheus:
    listen: "127.0.0.1:9464"        # recommended bind; 0.0.0.0 is opt-in
    buckets: [0.001, 0.01, 0.1, 1.0, 10.0, 60.0]  # optional; sensible defaults if unset
  tracing:
    level: "info"                   # falls back to RUST_LOG / FAUCET_LOG / --log-level
pipeline: { ... }
```

When `prometheus.listen` is set, `faucet run` exposes a `/metrics` HTTP endpoint at that address using `metrics-exporter-prometheus`. **The endpoint is unauthenticated** — bind to `127.0.0.1` (the default in examples) and put a reverse proxy or network ACL in front if you need to expose it to other hosts.

**Default histogram buckets** (when `buckets` is unset): `0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0` seconds. Covers sub-millisecond writes through five-minute batch loads.

**Per-command behavior:**

| Command | Installs Prometheus? | Installs `tracing-subscriber`? | Notes |
|---------|----------------------|-------------------------------|-------|
| `run` | Yes (when `prometheus.listen` set) | Yes | The only command that runs pipelines. |
| `validate` | No | Yes (basic fmt layer) | Short-lived; metrics meaningless. |
| `preview` | No | Yes | Short-lived. |
| `schema`, `list`, `init` | No | Yes | Pure metadata commands. |

**Tracing level precedence:** `--log-level` flag > `FAUCET_LOG` env > `RUST_LOG` env > YAML `observability.tracing.level` > default.

### Bridging to OpenTelemetry

`faucet-stream` emits stable `tracing` spans (`faucet.pipeline.run`, `faucet.source.page`, `faucet.sink.write`, `faucet.transform.apply`, `faucet.state.get|put|delete`). To export them to an OTel collector, install `tracing-opentelemetry` + `opentelemetry-otlp` in your own binary:

```rust
use tracing_subscriber::prelude::*;
let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .install_batch(opentelemetry_sdk::runtime::Tokio)?;
let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
tracing_subscriber::registry().with(otel_layer).init();
// then call faucet_cli::run_main(...) (or run_from_yaml_str) as usual
```

Faucet does not bundle an OTel exporter — wire your own to keep dependencies minimal.

## License

MIT OR Apache-2.0
