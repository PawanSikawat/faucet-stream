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

A handful of YAMLs note specific limitations in their header comment — chiefly that `Auth::Bearer(String)` and similar newtype-in-tagged-enum variants can't currently round-trip through serde. The [tracking issue](https://github.com/PawanSikawat/faucet-stream/issues) will replace those with struct variants so the YAML versions become 1:1 with the Rust ones.

## License

MIT OR Apache-2.0
