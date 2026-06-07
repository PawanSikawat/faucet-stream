# faucet-lineage

OpenLineage event emission for
[`faucet-stream`](https://github.com/PawanSikawat/faucet-stream).

Every pipeline run emits OpenLineage `RunEvent`s
(`START` / `RUNNING` / `COMPLETE` / `ABORT` / `FAIL`) describing the job, run,
input/output datasets, inferred schemas, and column-level lineage to a
configured HTTP, file, or Kafka backend. Wired automatically by the `faucet`
CLI via a top-level `lineage:` block; the types here are also usable directly by
library callers.

**OpenLineage spec version: 2.0.2.** Every event carries the pinned schema URL
[`event::OL_SCHEMA_URL`]
(`https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent`).

## Emission never fails a run

Lineage is observability, not a data path. The [`LineageEmitter::emit`] method
returns no error: a transport failure (HTTP non-2xx, unreachable file, Kafka
send error), a serialization failure, or a disabled event is logged and counted,
then dropped. A broken lineage backend can never abort or stall a pipeline.

## Configuration

The `lineage:` block deserializes into [`LineageConfig`]:

```yaml
lineage:
  type: openlineage              # only "openlineage" in v1 (default)
  namespace: prod.warehouse      # OpenLineage namespace for job + datasets
  transport:
    type: http                   # http | file | kafka
    url: https://marquez/api/v1/lineage
    timeout_secs: 10             # default 10s
    auth:
      type: bearer
      token: ${env:MARQUEZ_TOKEN}
  job_name: "${name}::${row_id}" # template; resolved per matrix row (default)
  parent_job:                    # optional orchestrator linkage
    namespace: airflow
    name: warehouse_dag.load_orders
    run_id: 0190-abc...          # optional
  include_schema_facet: true     # emit inferred dataset schemas (default false)
  include_column_lineage: true   # emit column-lineage facets (default false)
  include_source_code_facet: false  # emit resolved config (default false; warns)
  emit_on:                       # which lifecycle events to send
    start: true                  # default true
    running: false               # default false (RUNNING heartbeat)
    complete: true               # default true
    fail: true                   # default true
    abort: true                  # default true
  sample_records: 100            # records sampled for schema/column facets
  heartbeat_interval: 30         # RUNNING heartbeat seconds (when running:true)
```

## Transports

| `type`  | Backend                                   | Notes                                            |
|---------|-------------------------------------------|--------------------------------------------------|
| `http`  | OpenLineage HTTP endpoint (e.g. Marquez)  | `POST` per event; optional `bearer` auth; `timeout_secs`. |
| `file`  | Local JSON Lines file                     | One JSON object per line; parent dirs created.   |
| `kafka` | Kafka topic                               | One JSON message per event. Requires the `transport-kafka` feature. |

The Kafka transport is gated behind the **`transport-kafka`** Cargo feature
(pulls in `rdkafka`); `http` and `file` are always available.

## Schema facets

When `include_schema_facet` is set, the source/sink wrappers
([`SamplingSource`] / [`SamplingSink`]) clone up to `sample_records` records and
infer an ordered `(field, type)` schema attached to the input/output datasets on
terminal events. Inferred OpenLineage types: `null`, `boolean`, `integer`,
`number`, `string`, `array`, `object`.

## Column-level lineage

When `include_column_lineage` is set, the declared transform chain is folded
over the input field set ([`derive_column_lineage`]) to produce per-output-field
input edges. Lineage is **only** emitted when every transform in the chain is
mappable — it is never fabricated.

| Transform                                                | Column lineage |
|----------------------------------------------------------|----------------|
| `rename_field`, `select`, `drop`, `set`                  | Mapped (explicit key relationships). |
| `cast`, `redact`, `value_case`, `spell_symbols`, `filter`| Mapped (identity — keys unchanged).  |
| `flatten`, `explode`, `keys_case`, `rename_keys`, custom | Opaque — no facet emitted for the run. |

If any transform in the chain is opaque, [`derive_column_lineage`] returns
`None` and **no** column-lineage facet is attached.

## Metrics

Emitted via the [`metrics`](https://docs.rs/metrics) facade:

- `faucet_lineage_events_total{event_type, outcome}` — events sent, `outcome` ∈ `ok|err`.
- `faucet_lineage_dropped_total{reason}` — dropped events, `reason` ∈ `disabled|transport_error`.
- `faucet_lineage_emit_duration_seconds{event_type}` — per-event send latency histogram.

## License

MIT OR Apache-2.0.
