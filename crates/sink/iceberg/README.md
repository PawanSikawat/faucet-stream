# faucet-sink-iceberg

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-iceberg.svg)](https://crates.io/crates/faucet-sink-iceberg)
[![Docs.rs](https://docs.rs/faucet-sink-iceberg/badge.svg)](https://docs.rs/faucet-sink-iceberg)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-iceberg.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-iceberg.svg)](https://github.com/PawanSikawat/faucet-stream#license)

Apache Iceberg sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records as Parquet data files and commits them as Iceberg snapshots
via `Transaction::fast_append`. Catalog connectivity is pluggable — REST
(default), AWS Glue, SQL-backed, or Hive Metastore — selected by Cargo feature.

## Append-only (v1)

Only `write_mode: append` is supported. Each `flush()` call commits the
buffered data files as a new snapshot; existing data is never modified.
Overwrite / replace support is tracked in
[#179](https://github.com/PawanSikawat/faucet-stream/issues/179) and is blocked
on upstream `iceberg-rust` 0.9.x, which does not yet expose an overwrite
transaction action.

## Catalog support

| Catalog | `catalog.type` | Cargo feature | Included by default? |
|---------|---------------|--------------|----------------------|
| REST (Polaris, Nessie, Tabular, …) | `rest` | `catalog-rest` | **yes** |
| AWS Glue | `glue` | `catalog-glue` | no |
| SQL-backed (Postgres, SQLite, …) | `sql` | `catalog-sql` | no |
| Hive Metastore | `hms` | `catalog-hms` | no |

**Warehouse storage:** all catalogs support both cloud object stores and local
filesystems. The **REST** catalog resolves FileIO server-side from the catalog
config + `s3.*` properties. The **SQL / Glue / HMS** catalogs select an
OpenDAL-backed storage factory from the `warehouse` URI scheme:

| Warehouse scheme | Storage |
|---|---|
| `file://…` / bare path | local filesystem |
| `s3://…` / `s3a://…` | Amazon S3 / S3-compatible (MinIO, …) |
| `gs://…` | Google Cloud Storage |

Cloud storage is enabled automatically when you enable a non-REST catalog
feature (each of `catalog-glue` / `catalog-sql` / `catalog-hms` also enables the
`storage-opendal` feature). Pass storage credentials and options through
`catalog.properties`:

- **S3:** `s3.region`, `s3.endpoint` (for S3-compatible stores), `s3.access-key-id`,
  `s3.secret-access-key`, `s3.path-style-access`, `s3.disable-config-load`.
- **GCS:** `gcs.credentials-json`, `gcs.no-auth`, `gcs.service.path`.

Schemes without a built-in factory (e.g. `oss://`, `abfss://`) are rejected at
config-load time; use the REST catalog for those object stores.

Install only what you need:

```bash
# REST catalog (default):
cargo add faucet-sink-iceberg

# Glue catalog:
cargo add faucet-sink-iceberg --features catalog-glue

# All catalogs:
cargo add faucet-sink-iceberg --features catalog-glue,catalog-sql,catalog-hms
```

Via the umbrella crate, catalog forwarding features are available:

```bash
cargo add faucet-stream --features sink-iceberg                    # REST only
cargo add faucet-stream --features sink-iceberg,sink-iceberg-glue  # REST + Glue
```

## Quick start

```rust
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configs are normally loaded from YAML/JSON; deserialize one here.
    let config: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": {
            "type": "rest",
            "uri": "http://localhost:8181",
            "warehouse": "s3://warehouse/"
        },
        "namespace": ["analytics"],
        "table": "events",
        "create_if_missing": true,
        "batch_size": 10000
    }))?;

    let sink = IcebergSink::new(config).await?;

    sink.write_batch(&[
        json!({"user_id": "u123", "event": "page_view", "ts": "2026-01-02T10:00:00Z"}),
        json!({"user_id": "u456", "event": "click",     "ts": "2026-01-02T10:01:00Z"}),
    ]).await?;

    // Required: writes the Parquet footer and commits the Iceberg snapshot.
    sink.flush().await?;
    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog` | `CatalogConfig` | *(required)* | Catalog type and connection settings |
| `namespace` | `Vec<String>` | *(required)* | Multi-part namespace, e.g. `["analytics", "events"]`. Must be non-empty; no segment may be empty |
| `table` | `String` | *(required)* | Table name within the namespace |
| `create_if_missing` | `bool` | `true` | Create the table (inferring schema from the first batch) if it does not exist. When `false`, `new()` fails immediately if the table is absent |
| `partition_spec` | `Vec<PartitionField>` | `[]` | Partition fields used **only when creating** the table. Ignored for existing tables |
| `write_mode` | `WriteMode` | `append` | Write semantics. Only `append` is supported in v1 |
| `target_file_size_mb` | `u64` | `256` | Soft target for Parquet data-file size (MB). The sink rolls over to a new file when the estimated in-memory size exceeds this threshold |
| `parquet.compression` | `String` | `"snappy"` | Parquet compression codec: `snappy`, `zstd`, `gzip`, `lz4`, `none` |
| `snapshot_properties` | `HashMap<String,String>` | `{}` | Key-value pairs written into the Iceberg snapshot summary |
| `batch_size` | `usize` | `10000` | Records buffered before each Arrow writer flush. `0` = no limit (entire page in one batch) |

### Catalog config

Every catalog variant shares the same inner fields:

| Field | Description |
|-------|-------------|
| `uri` | Catalog endpoint. Required for `rest`, `sql`, `hms`; resolved from AWS config for `glue` |
| `warehouse` | Object-storage warehouse root, e.g. `s3://lake/warehouse/` |
| `credential` | REST bearer token or other catalog-specific credential. Redacted in `Debug` output |
| `properties` | Arbitrary key-value pairs forwarded to the catalog builder (e.g. `s3.region`, `s3.endpoint`) |

Example — SQL catalog (Postgres metadata) writing to an S3 warehouse:

```yaml
sink:
  type: iceberg
  config:
    catalog:
      type: sql
      uri: "postgres://meta:meta@localhost:5432/iceberg"
      warehouse: "s3://lake/warehouse"
      properties:
        s3.region: "us-east-1"
        s3.access-key-id: "${env:AWS_ACCESS_KEY_ID}"
        s3.secret-access-key: "${env:AWS_SECRET_ACCESS_KEY}"
    namespace: ["analytics"]
    table: "events"
    create_if_missing: true
```

### Partition transforms

Supported transforms for `partition_spec[*].transform`:

`identity`, `year`, `month`, `day`, `hour`, `void`, `bucket[N]`, `truncate[N]`
(where `N` is a positive integer, e.g. `bucket[16]`, `truncate[8]`).

## Schema inference

When `create_if_missing: true` and the table is new, the Iceberg schema is
inferred from the first Arrow batch: every JSON field becomes a nullable
column, typed by its first non-null value. Subsequent batches use the
table's existing schema so the writer and the table schema stay in sync.

Iceberg assigns **field IDs** sequentially starting from `1`. IDs are
stable once the table is created; renaming or reordering JSON keys in later
runs does not change them.

## Arrow / Parquet version note

`faucet-sink-iceberg` links Arrow 57 to match `iceberg-rust` 0.9.x, which
pins the same major. This version does not affect the workspace's other
connectors — the Parquet source/sink use Arrow 58 (workspace) and Cargo
resolves both majors simultaneously.

## Flush semantics and commit-failure caveat

`flush()` does two things in sequence:

1. **Closes the Parquet data file** — writes the Parquet footer and uploads
   it to object storage.
2. **Commits the Iceberg snapshot** — calls `Transaction::fast_append` to
   atomically register the new data files as a snapshot.

If step 2 fails (e.g. a concurrent writer caused an optimistic-concurrency
conflict that iceberg's internal retry could not resolve), the data files
from step 1 are already in object storage but never referenced by any
snapshot — they are **orphaned**. The error propagates, the run aborts, the
bookmark is not advanced, and the re-run writes fresh files and commits
them.

Orphaned files accumulate over time. Run Iceberg's standard
`remove_orphan_files` maintenance (e.g. via Spark, pyiceberg, or your
catalog's UI) to clean them up. The sink does **not** auto-delete on
failure, because re-committing after an ambiguous commit could duplicate
data.

## Streaming and batching

| Config | Default | Meaning |
|--------|---------|---------|
| `batch_size` | `10000` | Records buffered per Arrow write pass |
| `batch_size = 0` | n/a | "No batching" — pass the entire upstream page in one go |

The pipeline calls `flush()` once per `StreamPage`, so each page becomes
exactly one Iceberg snapshot. For high-throughput pipelines, use a large
upstream `batch_size` (e.g. `100000`) and leave the sink's `batch_size` at
its default so Arrow batches stay within memory limits while the snapshot
amortises catalog-commit overhead across many rows.

## YAML config example

```yaml
version: 1
name: postgres-to-iceberg
pipeline:
  source:
    type: postgres
    config:
      connection_url: "postgres://faucet:faucet@localhost:5432/appdb"
      query: "SELECT * FROM users"
  sink:
    type: iceberg
    config:
      catalog:
        type: rest
        uri: "http://localhost:8181"
        warehouse: "s3://warehouse/"
      namespace: ["analytics"]
      table: "users"
      create_if_missing: true
      partition_spec:
        - source: created_at
          transform: day
      batch_size: 10000
```

## Exactly-once delivery

`IcebergSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes `records` and records the `token` as Iceberg **snapshot summary properties** (`faucet.commit-scope` and `faucet.commit-token`) on the committed snapshot, so both are durable in the same atomic catalog operation.
- `last_committed_token(scope)` — scans recent snapshots for a matching `faucet.commit-scope` entry to let the pipeline skip already-committed pages on resume.

To use exactly-once delivery, set `delivery: exactly_once` in your pipeline config and pair this sink with one of the CDC sources (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in exactly-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: iceberg
    config:
      catalog:
        type: rest
        uri: http://catalog.example.com
        warehouse: s3://lake/warehouse
      namespace: ["analytics"]
      table: change_events
      create_if_missing: true
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

See the [Exactly-once delivery cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/state.html#exactly-once-delivery) for full rationale and the supported source/sink set.

## Lineage dataset URI

`iceberg://<catalog_type>/<namespace>.<table>` — e.g. `iceberg://rest/prod.events` (catalog type is `rest`, `glue`, `sql`, or `hms`).

## License

Licensed under MIT or Apache-2.0.
