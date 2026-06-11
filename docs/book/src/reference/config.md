# Configuration file format

A faucet config is a YAML or JSON document with this top-level shape:

```yaml
version: 1                 # required, must be 1
name: my_pipeline          # optional; used in state keys and metrics
vars: {}                   # optional; reusable values referenced as ${vars.X}
auth: {}                   # optional; named shared auth providers (see below)
schedule: {}               # optional; cron schedule for faucet schedule (see below)
pipeline:                  # required
  source: { type: …, config: { … } }
  transforms: []           # optional list
  sink:   { type: …, config: { … } }
  state:  { type: …, config: { … } }   # optional
  dlq:    { … }            # optional dead-letter queue
matrix: []                 # optional per-row overrides / DAG
execution:                 # optional
  max_concurrent: 4
  on_error: continue       # continue | stop
```

> **Unknown keys are rejected.** The structural blocks (`pipeline`, each
> `source`/`sink`/`transform`/`state` spec, `matrix` rows, `execution`) reject
> unrecognized fields, so a typo like `transorms:` or `parnet:` is a load-time
> error rather than a silently-ignored field. A connector's own `config: { … }`
> object is still passed through verbatim to that connector.

## `pipeline`

`source` and `sink` each take a `type` (the connector name) and a `config`
object whose fields are that connector's schema — see `faucet schema source
<name>`. `transforms` is an ordered list applied to every record. `state`
attaches a [state store](../cookbook/state.md); `dlq` attaches a
[dead-letter queue](../cookbook/dlq.md).

### Transforms layering

Transforms can be declared at three layers and are resolved additively per
matrix row in lifecycle order:

```
final = T_pipeline ++ T_source ++ T_row
```

- `pipeline.transforms` — cross-cutting policy, runs first on every row.
- `pipeline.sources.<name>.transforms` — bound to a source template; runs for
  every row that resolves to this source.
- `matrix[i].transforms` — row-specific extras, runs last.

Each declaring layer (source template, matrix row) carries an
`inherit_transforms: bool` (default `true`); setting it `false` drops every
upstream layer for that scope.

Sinks reject both `transforms:` and `inherit_transforms:` at expand time —
destination shaping belongs at the pipeline or row layer. See the
[transforms cookbook](../cookbook/transforms.md) for the full model and
worked examples.

### Available transforms

The full catalogue (with shapes and worked examples) lives in the
[transforms cookbook](../cookbook/transforms.md); `faucet list` prints the
same set, and `faucet schema transform <name>` returns the JSON schema for
each. Highlights:

- `filter` — keep records where a JSONPath predicate is true. See the cookbook for the operator set and path syntax.
- `explode` — expand an array field into one record per element. See the cookbook for the merge rule and `on_missing` semantics.

## Config composition

Three top-level mechanisms let a config be assembled from reusable pieces.
They are resolved when the file is read, **before** any `${...}` interpolation.

| Mechanism | Form | Effect |
|-----------|------|--------|
| `extends:` | `extends: ./base.yaml` or a list | Inherit one or more base files; the child deep-merges on top. |
| `profiles:` | `profiles: { dev: {…}, prod: {…} }` | Named overlays, selected at run time with `--profile NAME` / `FAUCET_PROFILE`. |
| `!include` | `key: !include ./frag.yaml` | Substitute a YAML fragment at any node (**YAML only**). |

```yaml
# app.yaml — inherits a base and pulls in a transform fragment.
extends: ./base.yaml          # single path, or a list (merged left-to-right)
pipeline:
  transforms: !include ./transforms.yaml
```

```yaml
# base.yaml — shared connection + sink, with named per-environment overlays.
version: 1
name: composed-pipeline
pipeline:
  source: { type: csv,   config: { path: ./data/input.csv } }
  sink:   { type: jsonl, config: { path: ./out/dev.jsonl } }
profiles:
  dev:  { pipeline: { sink: { config: { path: ./out/dev.jsonl } } } }
  prod: { pipeline: { sink: { config: { path: ./out/prod.jsonl } } } }
```

- **`extends`** — relative paths resolve against the directory of the file that
  declares them. A list of bases merges left-to-right; the child document
  overrides them all. Bases may themselves `extends:` further files (depth-capped,
  cycle-detected).
- **`profiles`** — nothing is applied unless a profile is selected. Select with
  `--profile prod` or `FAUCET_PROFILE=prod`; **the flag overrides the env var**.
  An undeclared name is a load-time error.
- **`!include`** — a YAML tag (no JSON equivalent) that replaces the tagged node
  with the parsed contents of another YAML file (sequence, mapping, or scalar).
  Paths resolve against the including file's directory.

**Merge rule and precedence.** Everything composes with the same deep-merge used
by `matrix` rows (objects merge recursively, arrays replace wholesale, scalars
replace). Lowest-to-highest priority (last wins):

```
extended base(s)  →  child document  →  selected profile  →  matrix row
```

**Load-time ordering.** Composition runs first, then interpolation:

1. **Composition** — `extends` / `!include` stitched, then the selected
   `profile` overlaid; the `extends:` / `profiles:` metadata keys are stripped.
2. `${env:…}` / `${file:…}` / `${secret:…}`, then `${vars.X}` and
   `${sources.X}` / `${sinks.X}` (see [Interpolation](#interpolation)).
3. Secrets-manager directives (`${vault:…}` etc.).
4. `matrix` expansion.

**Inspect the result** with `faucet validate --show-composed` — it prints the
fully composed document (bases merged, profile applied, fragments substituted,
metadata stripped) before interpolation.

> **Composition is file-loads-only.** `extends` / `profiles` / `!include` apply
> to configs faucet reads from disk (`run`, `validate`, `preview`, `doctor`,
> `schedule`). They are **not** honored for configs submitted to `faucet serve`
> over HTTP — a submitted body is a single self-contained document with no
> filesystem access. See the [config-composition cookbook](../cookbook/composition.md).

## Interpolation

Three stages resolve placeholders:

- **Load time:** `${env:VAR}`, `${file:PATH}`, `${secret:VAR}` are resolved when
  the file is read. `${vars.X}` resolves against the top-level `vars:` block;
  `${sources.NAME.PATH}` / `${sinks.NAME.PATH}` resolve against named templates.
  Secret-manager directives (see below) run as the final load-time stage.
- **Runtime:** `${row_id.dotted.path}` tokens are resolved per parent record in
  DAG runs. `${now.*}` tokens are resolved per invocation at run time (see
  below).

Reference cycles surface as a clear `InterpolationCycle` error.

### `${now.*}` — run-clock interpolation

`${now.*}` tokens inject the current wall time into **source and sink config
values**. Each invocation evaluates them once at run time:

| Token | Example output | Notes |
|-------|---------------|-------|
| `${now.date}` | `2026-03-08` | `YYYY-MM-DD` |
| `${now.datetime}` | `2026-03-08T14:05:09+00:00` | RFC 3339; alias: `${now.iso}` |
| `${now.iso}` | `2026-03-08T14:05:09+00:00` | Alias for `${now.datetime}` |
| `${now.year}` | `2026` | Zero-padded 4-digit year |
| `${now.month}` | `03` | Zero-padded month (01–12) |
| `${now.day}` | `08` | Zero-padded day (01–31) |
| `${now.hour}` | `14` | Zero-padded hour (00–23) |
| `${now.minute}` | `05` | Zero-padded minute (00–59) |
| `${now.second}` | `09` | Zero-padded second (00–59) |
| `${now.unix}` | `1741442709` | Unix epoch seconds |
| `${now.strftime.<fmt>}` | `2026/03/08/14` | Arbitrary [chrono strftime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) — e.g. `${now.strftime.%Y/%m/%d/%H}` |

An unknown token (e.g. `${now.foo}`) is a config error at run time. An invalid
strftime format produces a clean config error rather than a panic.

**Clock source:**

- **`faucet run`** — the process start time in UTC. Override with
  `--clock <value>` for backfills: an RFC 3339 timestamp
  (`2026-03-01T00:00:00Z`) or a bare date (`2026-03-01`, treated as midnight
  UTC). See the [`run` command reference](cli.md#run).
- **`faucet schedule`** — the tick's **scheduled time**, rendered in the
  schedule's `timezone`. `${now.date}` therefore reflects the date in the
  timezone the cron fires in (e.g. `America/Los_Angeles`), not UTC. Queued
  runs use their original scheduled time; `--once` uses the current wall clock.

**Scope:** `${now.*}` tokens (and `${row_id.path}` parent-record references) are
resolved only in source and sink config values. Using one in a `state:`,
`dlq:`, or `transforms:` config is a **config error at validate/expand time** —
it is rejected rather than silently passed to the connector as a literal
`${…}` string. (`${env:…}` / `${vars.X}` / `${sources.X}` still resolve
everywhere.)

**Reserved id:** `now` is a reserved matrix row id — a matrix row cannot be
named `now`.

**SQL caveat:** `${now.*}` substitutes as plain text into config values — the
same semantics as `${row_id.path}` tokens. For SQL sources that interpolate
`${now.*}` into a query string, prefer the connector's bind-parameter path
(`substitute_context_bind_params`) over raw text substitution to avoid
injection risk.

### Secrets-manager directives

Four additional load-time schemes pull values from external secrets managers.
Each requires the matching build feature (`--features secrets-vault`, etc.;
`--features secrets` enables all four). Values are fetched concurrently and
de-duplicated; they are never written to disk.

| Directive | Backend | Auth |
|-----------|---------|------|
| `${vault:<path>[#field]}` | HashiCorp Vault KV v2 | `VAULT_ADDR` + `VAULT_TOKEN` (+ optional `VAULT_NAMESPACE`) |
| `${aws-sm:<name-or-ARN>[#field]}` | AWS Secrets Manager | `aws-config` default chain (env / profile / instance / web-identity) |
| `${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}` | GCP Secret Manager (`versions/latest` ok) | Application Default Credentials |
| `${azure-kv:<vault>/<secret>[/<version>]}` | Azure Key Vault | `AZURE_*` env / managed identity / `az login` |

The `#field` selector (Vault and AWS only) parses the secret body as a JSON
object and extracts a single key. Use `faucet schema secrets` for the machine-readable
grammar reference and `faucet validate --no-secrets` to check grammar offline.

See the [secrets cookbook](../cookbook/secrets.md) for full examples, the
redaction guarantee, and the known limitation around the `auth:` catalog.

## `matrix`

Each row is deep-merged onto `pipeline` (scalars replace, objects merge, arrays
replace). A row with `parent:` runs once per parent record. See the
[matrix DAG tutorial](../tutorials/matrix-dag.md). For DRY configs with many
rows, define named templates under `pipeline.sources` / `pipeline.sinks` and
select them per row with `ref:`.

## `auth`

A map of named auth providers, each `{ type, config }` (`type` ∈ `static` /
`oauth2` / `oauth2_refresh` / `token_endpoint`). A connector references one with
`auth: { ref: <name> }` instead of inline auth; faucet builds each provider once
and shares it across every connector that references it (one token, single-flight
refresh). See the [authentication cookbook](../cookbook/auth.md).

```yaml
auth:
  api:
    type: oauth2_refresh
    config:
      token_url: ${env:API_TOKEN_URL}
      client_id: ${secret:API_CLIENT_ID}
      client_secret: ${secret:API_CLIENT_SECRET}
      refresh_token: ${secret:API_REFRESH_TOKEN}
```

## `delivery`

Controls the delivery guarantee for every pipeline row.

```yaml
delivery: at_least_once   # default — no behaviour change
# or:
delivery: exactly_once
```

| Value | Behaviour |
|-------|-----------|
| `at_least_once` | Default. A crash between the sink write and the bookmark persist causes the page to be re-delivered on the next run. Downstream must tolerate duplicates. |
| `exactly_once` | The sink durably records a per-page commit token atomically with the data inside its own transaction. On resume the pipeline reads the sink's last committed token and skips already-committed pages, giving zero duplicates after a crash. |

**Per-row override:** set `delivery:` directly on a matrix row to override the top-level value for that row.

```yaml
delivery: at_least_once    # top-level default

matrix:
  - id: critical_row
    delivery: exactly_once  # this row uses exactly-once
  - id: best_effort_row
    # inherits top-level at_least_once
```

### Requirements for `exactly_once`

All four conditions are validated at config-load time (`faucet validate` and `faucet run`). A violation is a hard `config error` naming the offending row — no run is started.

1. **Deterministic-replay source** — the source must be one of: `postgres-cdc`, `mysql-cdc`, `mongodb-cdc`. Non-CDC sources are rejected because a different page content on replay would cause the pipeline to silently skip records it never wrote.
2. **Idempotent sink** — the sink must be one of: `sqlite`, `postgres`, `mysql`, `mssql`, `iceberg`, `bigquery`. These sinks atomically commit both the data and a watermark token inside the same transaction or snapshot.
3. **State store** — a `state:` block is required. The pipeline stores the per-page sequence number alongside the bookmark so it can detect already-committed pages on resume.
4. **No DLQ** — a `dlq:` block is incompatible with `exactly_once` in this version. Per-row error routing and the idempotency watermark interact in ways not yet resolved.

See the [Exactly-once delivery cookbook](../cookbook/state.md#exactly-once-delivery) for a worked example and the full rationale.

## `execution`

- `max_concurrent` — one shared concurrency budget across roots and child
  fan-outs.
- `on_error` — `continue` (siblings finish; failed subtree skipped) or `stop`
  (abort pending and in-flight work on first failure).

### Adaptive batch sizing

The optional `adaptive_batch_size:` sub-block enables the AIMD controller that
auto-tunes the effective write batch size from observed sink latency and error
rate. Default `enabled: false` (opt-in).

```yaml
execution:
  adaptive_batch_size:
    enabled: true          # master switch
    controller: aimd       # only "aimd" is supported in v1
    min: 100               # lower bound (rows)
    max: 50000             # upper bound; inert above the source page size
    increase_step: 250     # additive growth per clean batch
    decrease_factor: 0.5   # multiplicative shrink on error/high latency  (0, 1)
    cooldown_batches: 5    # batches to skip after a shrink
    target_latency_ms: null  # optional write-latency target (ms)
    latency_window: 10     # rolling window size for p50 latency
    error_threshold: 0.01  # per-batch error rate that triggers a shrink
    respect_source_max: true  # cap at source page size (see Caveats)
    log_every: 50          # tracing::info every N adjustments
```

**Key caveats:**

- **Error-driven shrink requires a `dlq:` block.** Without one the controller
  sees no per-row errors; only `target_latency_ms` can drive shrinks.
- **Effective ceiling = source page size.** In v1 the controller reslices pages
  in-memory — it cannot buffer across pages. Setting `max` higher than the
  source `batch_size` is harmless but inert. Raise the source `batch_size` to
  allow bigger write batches.
- **No-op for per-record sinks.** `jsonl`, `csv`, and `stdout` write one record
  at a time; the controller adjusts normally but the write granularity is
  unchanged.

See the [Adaptive batching cookbook](../cookbook/adaptive-batching.md) for a
full worked example, the AIMD trajectory, and the four Prometheus metrics
(`faucet_pipeline_adaptive_batch_*`).

## `schedule`

Present only when you run `faucet schedule`. Absent configs are rejected by that
command with a hint to use `faucet run` instead. All fields except `cron` are
optional.

```yaml
schedule:
  cron: "0 2 * * *"               # REQUIRED. Standard 5-field cron, or 6-field with leading seconds.
  timezone: "UTC"                 # IANA timezone name. Default UTC.
  overlap_policy: skip            # skip | queue | forbid. Default skip.
  max_runs: null                  # null = run forever; N = exit 0 after N successful runs.
  max_consecutive_failures: null  # null = never exit on failure; N = exit non-zero after N straight failures.
  on_failure: continue            # continue | stop. Default continue.
  start_immediately: false        # Run once on startup before waiting for the first tick. Default false.
  run_timeout_secs: null          # Per-run wall-clock kill switch (seconds). Timed-out runs count as failed.
  shutdown_grace_secs: 30         # SIGTERM: wait this long for the in-flight run before aborting. Default 30.
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cron` | string | **required** | 5-field standard Unix cron (`MIN HOUR DOM MON DOW`) or 6-field with a leading seconds field (`SEC MIN HOUR DOM MON DOW`). Validated at load time. |
| `timezone` | string | `"UTC"` | IANA timezone name (e.g. `"America/Los_Angeles"`, `"Europe/Berlin"`). Affects how the cron expression is interpreted. |
| `overlap_policy` | `skip` \| `queue` \| `forbid` | `skip` | What to do when a tick fires while a run is already in flight. `skip` drops the tick; `queue` buffers one missed tick (in-memory only, lost on restart); `forbid` exits non-zero. |
| `max_runs` | integer \| null | `null` | Stop the scheduler cleanly (exit 0) after this many *successful* runs. `null` means run forever. `0` is rejected as a config error. |
| `max_consecutive_failures` | integer \| null | `null` | Exit non-zero after this many consecutive failed runs without a success in between. A successful run resets the counter. `null` means never exit on failures alone. |
| `on_failure` | `continue` \| `stop` | `continue` | `stop` exits non-zero immediately after the first failed run. `continue` keeps scheduling; use `max_consecutive_failures` to bound sustained outages. |
| `start_immediately` | bool | `false` | When `true`, the first run fires right on startup before the cron clock reaches its first tick. |
| `run_timeout_secs` | integer \| null | `null` | Per-run time limit in seconds. A run that exceeds this is killed and counts as a failure. `null` means no timeout. |
| `shutdown_grace_secs` | integer | `30` | On SIGTERM/SIGINT, wait this many seconds for the in-flight run to finish before forcibly aborting it. |

**Validation:** `faucet validate pipeline.yaml` checks the `schedule:` block at parse time — bad cron
syntax, unknown timezone names, `max_runs: 0`, and a cron expression that can never fire all produce
a clear `config error: schedule: …` message before any run starts.

See the [scheduling cookbook](../cookbook/scheduling.md) for worked examples, the DST/timezone
details, the overlap-policy decision tree, and the full Prometheus metric set.

## `lineage`

Optional. When present, every pipeline run emits [OpenLineage](https://openlineage.io/) `RunEvent`s
describing the job, its input/output datasets, inferred schemas, and column-level lineage. Emission
**never fails a run** — transport errors are logged and counted but do not propagate.

```yaml
lineage:
  namespace: prod.warehouse      # REQUIRED. Logical namespace for all jobs and datasets.
  transport:                     # REQUIRED. Where to send events.
    type: http                   # http | file | kafka (kafka requires lineage-kafka feature)
    config:
      url: ${env:MARQUEZ_URL}
  job_name: ${name}::${row_id}   # Default. Resolved per matrix row at run time.
  include_schema_facet: false    # Emit DatasetFacets.schema (inferred from a sample).
  include_column_lineage: false  # Emit column-level lineage where statically derivable.
  include_source_code_facet: false  # Emit resolved config as a sourceCode job facet (warns; may expose secrets).
  emit_on:
    start: true
    running: false               # RUNNING heartbeats; see heartbeat_interval.
    complete: true
    fail: true
    abort: true
  sample_records: 100            # Max records sampled for schema/column facets.
  heartbeat_interval: 30         # Seconds between RUNNING heartbeats (when emit_on.running is true).
```

See the [Lineage cookbook](../cookbook/lineage.md) for the full field reference, the three
transports (HTTP, file, Kafka), the column-lineage support matrix, schema-facet behavior, and
the Prometheus metrics (`faucet_lineage_events_total`, etc.).

## Discovery & env files

`run` / `validate` / `preview` / `schedule` auto-discover `faucet.yaml` → `.yml` → `.json` in
the current directory, and load a sibling `.env` unless `--no-env-file` is given
(`--env-file PATH` points elsewhere).

> The authoritative, exhaustive grammar — including every matrix and template
> edge case — is in
> [`cli/README.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/README.md).
