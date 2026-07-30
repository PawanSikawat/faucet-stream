# Topology mode (tee / merge / join)

The default pipeline moves records from one source to one sink. **Topology
mode** generalizes that to an explicit graph of typed nodes, so a single run
can:

- **fan-out (tee)** — fetch a source once and route the same records to several
  sinks (no refetch, no divergence);
- **fan-in (merge)** — concatenate several sources into one sink;
- **join** — enrich one stream with fields looked up from another by key.

Declare `pipeline.nodes` (a map of node id → node) and `pipeline.edges`
(producer → consumer connections). Topology mode is **mutually exclusive** with
`matrix:`.

## Node kinds

| `kind` | in | out | fields |
|--------|----|-----|--------|
| `source` | 0 | 1 | `ref:` (a `pipeline.sources` template) + optional `type` / `config` overrides |
| `transform` | 1 | 1 | `transforms:` (the usual transform list) |
| `tee` | 1 | N | `channel_capacity` (default 4), optional `fanout` sanity-check |
| `merge` | N | 1 | — |
| `join` | 2 | 1 | see [Joins](#joins) |
| `sink` | 1 | 0 | `ref:` (a `pipeline.sinks` template) + optional `type` / `config` overrides |

## Fan-out (tee)

```yaml
version: 1
name: fan_out
pipeline:
  sources:
    orders: { type: csv, config: { path: ./data/orders.csv } }
  sinks:
    warehouse: { type: jsonl, config: { path: ./out/warehouse.jsonl } }
    archive:   { type: jsonl, config: { path: ./out/archive.jsonl } }
  nodes:
    src:  { kind: source, ref: orders }
    norm: { kind: transform, transforms: [ { type: keys_case, config: { mode: snake } } ] }
    fan:  { kind: tee, channel_capacity: 4, fanout: 2 }
    w1:   { kind: sink, ref: warehouse }
    w2:   { kind: sink, ref: archive }
  edges:
    - { from: src,  to: norm }
    - { from: norm, to: fan }
    - { from: fan,  to: w1 }
    - { from: fan,  to: w2 }
```

Nodes run concurrently, connected by bounded channels: the slowest sink paces
its producer (backpressure). The `tee` clones each page to every downstream
edge.

## Fan-in (merge)

```yaml
  nodes:
    a: { kind: source, ref: orders }
    b: { kind: source, ref: returns }
    m: { kind: merge }
    w: { kind: sink, ref: combined }
  edges:
    - { from: a, to: m }
    - { from: b, to: m }
    - { from: m, to: w }
```

`merge` forwards pages from all inputs in arrival order.

## Joins

A `join` node hash-joins two upstreams. The **build** (right) side is buffered
into an in-memory index keyed by `build.key`; then the **probe** (left) side is
streamed and each record enriched with the `project`ed fields of its match. The
join's two incoming edges carry `as:` labels that match `build.edge` /
`probe.edge`.

```yaml
  nodes:
    fetch_customers: { kind: source, ref: customers }
    fetch_orders:    { kind: source, ref: orders }
    enrich:
      kind: join
      mode: left                 # `inner` drops non-matches; `left` keeps them
      build: { edge: customers_in, key: id }
      probe: { edge: orders_in,    key: customer_id }
      project:
        - { from: tier, as: customer_tier }
      on_missing: null           # left-mode fill when there is no match
      on_duplicate: first        # or `cartesian` (one output row per build match)
      on_collision: overwrite    # or `skip` / `error`
      key_normalize: preserve    # or `stringify` so "42" matches 42
      max_build_records: 10000000
    write: { kind: sink, ref: warehouse }
  edges:
    - { from: fetch_customers, to: enrich, as: customers_in }
    - { from: fetch_orders,    to: enrich, as: orders_in }
    - { from: enrich,          to: write }
```

The build side is fully materialized before probing begins, so pair a large
dimension table with a fast local source (SQLite / Parquet) rather than a slow
remote API, and keep `max_build_records` as a guardrail.

## State and errors

Each terminal sink owns a bookmark under `{name}::{node_id}`. On restart the
source resumes from the **minimum** across every sink's stored bookmark (only
when all sinks have one), so a lagging sink is never skipped — sinks whose
bookmarks diverge must be idempotent.

`execution.on_error: stop` aborts the whole topology on the first failure;
`continue` lets healthy branches finish and reports the failures at the end.

## Observability

Topology runs emit the standard sink/transform/state metrics plus
`faucet_tee_records_total`, `faucet_merge_records_total`, and the
`faucet_join_*` family (`build_records`, `probe_records`, `matches`, `misses`,
`duplicates`, `build_nulls`, `project_misses`, `build_duration_seconds`),
labelled `pipeline` + `node`.

## Runnable examples

- `cli/examples/topology_tee_users.yaml` — fan-out to three sinks.
- `cli/examples/topology_merge_files.yaml` — fan-in of two CSV sources.
- `cli/examples/topology_join_orders_countries.yaml` — left-join enrichment.

```bash
faucet validate cli/examples/topology_join_orders_countries.yaml
faucet run      cli/examples/topology_join_orders_countries.yaml
```
