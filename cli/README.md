# faucet-cli

[![Crates.io](https://img.shields.io/crates/v/faucet-cli.svg)](https://crates.io/crates/faucet-cli)
[![Docs.rs](https://docs.rs/faucet-cli/badge.svg)](https://docs.rs/faucet-cli)
[![MSRV](https://img.shields.io/crates/msrv/faucet-cli.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-cli.svg)](https://github.com/PawanSikawat/faucet-stream#license)

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
| `faucet schema source|sink|transform <name>` | Print the JSON Schema for a connector's or transform's config. |
| `faucet schema dlq` | Print the JSON Schema for the dead-letter-queue spec. |
| `faucet list` | List every compiled-in source, sink, transform, and state-store backend. |
| `faucet preview <config> --limit N` | Run only the source side and emit the first N records to stdout as JSONL. |
| `faucet init [name] [--source X] [--sink Y]` | Scaffold a pipeline.yaml from each connector's JSON Schema. |
| `faucet doctor <config> [--timeout-secs N] [--json]` | Probe every connector (auth/network/permissions/reachability) and print a checklist. Exits with the failed-probe count. |
| `faucet schedule <config> [--once]` | Run a pipeline on a cron schedule (long-running foreground process). Requires a `schedule:` block. |

Pass `--log-level debug` (or set `FAUCET_LOG=debug`) for verbose tracing. Logs are written to stderr; pipeline records and command output go to stdout.

### `faucet doctor`

`faucet doctor <config>` runs a fast, **non-mutating** preflight against every connector in a config before you commit to a real run — so a misconfigured credential, an unreachable host, or a missing permission surfaces in seconds with a clear remediation hint, instead of failing mid-run and polluting your metrics.

For each root invocation it probes the source, sink, and state store:

- **Sources** reuse the real read path — the probe pulls a *single page* (DNS + TLS + auth + the first request + first-record decode) and stops, never paginating the full dataset. A handful of sources whose first page would block or have side effects use a targeted probe instead: `webhook` checks the port is bindable, `websocket` does a TCP connect, `postgres-cdc` checks the replication slot is reachable, `kafka` fetches cluster metadata.
- **Sinks** run a non-mutating connect/auth/metadata call (e.g. `SELECT 1`, `HeadBucket`, `PING`, `tables.get`, cluster health, `fetch_metadata`) — never a real write. File sinks check the target directory is writable; `stdout` always passes.
- **State stores** do a sentinel `put`/`get`/`delete` round-trip that leaves no residue.

```bash
faucet doctor pipeline.yaml                      # checklist, exit code = # of failed probes
faucet doctor pipeline.yaml --timeout-secs 5     # per-probe timeout (default 10)
faucet doctor pipeline.yaml --json               # machine-readable, for CI gating
```

Example output:

```text
✓ Config parses and interpolates                                 8 ms
✓ Matrix expands to 2 invocations                    0 skipped (children)

▸ Invocation default::us-east  (source=postgres, sink=bigquery)
  ✓ source [postgres] read                                      42 ms
  ✗ sink   [bigquery] auth (dataset us_east not found)         410 ms
        hint: check bigquery credentials and that the dataset exists

Summary: 1 passed, 1 failed, 0 skipped       total elapsed 0.5s
```

Flags:

| Flag | Purpose |
|------|---------|
| `--timeout-secs <N>` | Per-probe timeout in seconds (default 10). |
| `--json` | Emit a `{ config, invocations, summary }` JSON document instead of the checklist. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

**Exit code** = the number of failed probes, clamped to 255 (so `0` means all probes passed). **Child invocations** (parent/child matrix rows) are listed but not probed — their configs depend on parent records that only exist at run time. Probe `reason`/`hint` text is scrubbed for resolved secrets before printing, but third-party connectors should never place credentials in a probe message.

**Probe contract for connector authors:** `Source::check` / `Sink::check` / `StateStore::check` (in `faucet-core`) default to a generic probe (source) or "not implemented" skip (sink / state). Override them with a probe that is **idempotent and side-effect-free** and never echoes credentials. Return probe-level failures as `ProbeStatus::Fail` inside an `Ok(CheckReport)`; reserve `Err` for "couldn't run any probe".

### `faucet schedule`

`faucet schedule <config>` runs a pipeline on a cron schedule in a **long-running foreground process**. Stop it with Ctrl-C or SIGTERM; an in-flight run drains gracefully before the process exits.

```bash
faucet schedule pipeline.yaml                        # run on cron schedule, foreground
faucet schedule pipeline.yaml --once                 # run exactly once now, then exit
faucet schedule pipeline.yaml --env-file prod.env    # inject env before loading config
faucet schedule pipeline.yaml --no-env-file          # disable .env auto-loading
```

The config must contain a top-level `schedule:` block (a config without one is rejected with a clear hint pointing to `faucet run`). Requires the `schedule` Cargo feature (included in the default `full` build).

#### `schedule:` block grammar

```yaml
schedule:
  cron: "0 2 * * *"               # REQUIRED. 5-field standard Unix cron, or 6-field with leading seconds.
  timezone: "UTC"                 # IANA name (e.g. America/Los_Angeles). Default UTC.
  overlap_policy: skip            # skip (default) | queue | forbid
  max_runs: null                  # null = forever; N = stop cleanly (exit 0) after N *successful* runs
  max_consecutive_failures: null  # null = never exit on failure; N = exit non-zero after N straight failures
  on_failure: continue            # continue (default) | stop (exit non-zero on first failed run)
  start_immediately: false        # run once on startup before waiting for the first tick
  run_timeout_secs: null          # optional per-run kill switch (seconds); a timed-out run counts as failed
  shutdown_grace_secs: 30         # SIGTERM: await the in-flight run this long, then abort
```

**Cron syntax:** standard 5-field Unix cron (`MIN HOUR DOM MON DOW`) or 6-field with a leading seconds field (`SEC MIN HOUR DOM MON DOW`). Examples:

| Expression | Meaning |
|------------|---------|
| `0 2 * * *` | Every night at 02:00 |
| `*/15 * * * *` | Every 15 minutes |
| `0 9 * * 1-5` | Weekdays at 09:00 |
| `*/30 * * * * *` | Every 30 seconds (6-field) |

Bad cron expressions, unknown timezones, `max_runs: 0`, and a cron that can never fire all fail fast with a clear `config error: schedule: …` message.

**Timezone and DST:** all ticks are computed on UTC instants with timezone-correct wall times via `chrono-tz`. Fall-back repeated hours fire once; spring-forward skipped hours roll to the next valid time. The loop re-checks the wall clock every ≤30 s so NTP steps and VM freezes can't drift a fire by more than ~30 s.

**Missed ticks:** skipped, not backfilled. If a run ran long or the box was down, the scheduler fires at the next due time — no catch-up storm.

#### Overlap policy

| Policy | Behaviour |
|--------|-----------|
| `skip` (default) | Drop the tick if a run is already in flight. Increment `faucet_schedule_overlaps_total`. |
| `queue` | Buffer one missed tick; run it when the current run finishes. Further misses collapse to one queued tick (in-memory only — lost on restart). |
| `forbid` | Exit non-zero immediately if a second run would overlap. |

#### Failure model

Two independent knobs control what happens when a run fails:

| `on_failure` | `max_consecutive_failures` | Behaviour |
|---|---|---|
| `continue` (default) | `null` | Tolerates all failures; never exits on failure alone. Alert on `faucet_schedule_consecutive_failures`. |
| `continue` | `N` | Tolerates up to N−1 straight failures; exits non-zero after N consecutive failures (a success resets the counter). Pair with a supervisor (`systemd Restart=on-failure`, Kubernetes) for "tolerate blips, restart on sustained outage". |
| `stop` | any | Exits non-zero on the **first** failed run. |

#### Connectors and state

Connectors are rebuilt fresh per run so no idle connections pool up. The shared `auth:` catalog (cached tokens) is reused across ticks. Resumability rides faucet's existing per-page `StateStore` bookmark: the scheduler itself keeps no run-state across restarts, so a crash or SIGKILL resume from the last persisted bookmark on the next start.

#### Metrics

All metrics carry the `{pipeline}` label. Register a Prometheus listener via the `observability:` block as usual.

| Metric | Type | Description |
|--------|------|-------------|
| `faucet_schedule_runs_total{pipeline,outcome}` | Counter | `outcome` ∈ `{ok, err, skipped}` |
| `faucet_schedule_overlaps_total{pipeline,policy}` | Counter | Overlap events; `policy` ∈ `{skip, queue, forbid}` |
| `faucet_schedule_next_tick_unix_seconds{pipeline}` | Gauge | Unix timestamp of the next scheduled tick |
| `faucet_schedule_runs_in_flight{pipeline}` | Gauge | 0 or 1 — whether a run is currently executing |
| `faucet_schedule_consecutive_failures{pipeline}` | Gauge | Resets to 0 on a successful run |
| `faucet_schedule_heartbeat_unix_seconds{pipeline}` | Gauge | Updated every loop wake (≤30 s); alert `time() − heartbeat > 90` to detect a stuck scheduler |
| `faucet_schedule_last_run_started_unix_seconds{pipeline}` | Gauge | |
| `faucet_schedule_last_run_completed_unix_seconds{pipeline}` | Gauge | |
| `faucet_schedule_last_run_duration_seconds{pipeline}` | Gauge | |
| `faucet_schedule_run_lateness_seconds{pipeline}` | Histogram | `actual_start − scheduled_for` — how late the run fired |

Each run also emits a `faucet.schedule.run` tracing span (attributes: `run_ordinal`, `scheduled_for_unix_seconds`, `tick_unix_seconds`) wrapping the inner pipeline spans.

#### Exit codes

| Condition | Exit code |
|-----------|-----------|
| `max_runs` reached | 0 |
| SIGTERM / SIGINT graceful drain | 0 |
| `on_failure: stop` — first run failed | non-zero |
| `max_consecutive_failures` reached | non-zero |
| `overlap_policy: forbid` overlap | non-zero |
| Bad cron / timezone / config | non-zero |

#### Build feature

```bash
cargo install faucet-cli                         # schedule included (in full)
cargo install faucet-cli --features schedule     # explicit, no-default-features build
```

### `faucet serve`

`faucet serve` runs a **long-running HTTP control plane**: it accepts pipeline configs over REST, executes them under bounded concurrency (reusing the same executor as `faucet run`), and exposes submit / poll / list / cancel / SSE-log endpoints plus `/healthz`, `/readyz`, and `/metrics`. It takes **no config file** — configs arrive per request.

```bash
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 0.0.0.0:8080      # bearer auth (preferred)
faucet serve --no-auth                                                 # explicit no-auth opt-in (required if no token)
faucet serve --history sqlite:/var/lib/faucet/runs.db                  # durable run history
faucet serve --default-config defaults.yaml                            # merge workspace defaults under every run
```

Auth is mandatory: without `--auth-token`/`FAUCET_SERVE_AUTH_TOKEN` **and** without `--no-auth`, startup fails (an unauthenticated server is never accidental). The default bind is loopback.

| Flag | Purpose |
|------|---------|
| `--listen <addr>` | Bind address (default `127.0.0.1:8080`; env `FAUCET_SERVE_LISTEN`). |
| `--auth-token <t>` / `--no-auth` | Bearer token (prefer the env var) or explicit no-auth opt-in. |
| `--max-concurrent-runs` / `--max-queued-runs` | Concurrency + queue caps (submit past the queue → 429 + `Retry-After`). |
| `--history <url>` | `postgres://…` / `sqlite:…` for durable history (`serve-history-postgres` / `serve-history-sqlite`; default in-memory). |
| `--default-config <path>` | Workspace defaults merged **under** every submitted run. |
| `--cors-origin <o>` | Allow-list a browser origin (repeatable; CORS off by default). |
| `--lease-ttl-secs <n>` | Run-ownership lease TTL (default `30`). Set above your worst-case GC/IO stall to avoid false-reclaim of paused instances. |
| `--cluster` | Enable clustered execution: all instances sharing the same `--history` DB pull-balance `pending` runs and provide crash-failover. Requires a persistent `--history` backend. See the [cluster cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/cluster.html). |
| `--cluster-poll-secs <n>` | Claim-loop poll interval in seconds (default `2`). Also the maximum cross-instance cancel propagation lag. |
| `--cluster-max-attempts <n>` | Maximum attempts per run (including crash-failovers) before it is poisoned as `failed` (default `3`). |
| `--triggers <path>` | Path to a triggers file (YAML) defining event-driven watchers. Requires the `triggers` Cargo feature. See [Event-driven triggers](#event-driven-triggers-triggers). |
| `--body-limit-bytes`, `--shutdown-grace-secs`, `--retain-terminal-runs-secs`, `--idempotency-retention-secs`, `--probe-timeout-secs` | Tuning knobs. |

#### Event-driven triggers (`--triggers`)

`--triggers <file>` loads a static triggers file at startup and spawns long-lived watcher tasks.
When a watcher fires, it enqueues a run through the same pipeline as `POST /v1/runs`, reusing the
full queue / idempotency / history / metrics machinery.

Three trigger types are available:

| Type | What it watches | Requires feature |
|------|----------------|-----------------|
| `object_arrival` | New S3 or GCS objects under a prefix | `triggers-object-store` |
| `webhook` | `POST /v1/triggers/{name}` (bearer-gated) | `triggers` |
| `queue_depth` | Redis list/stream depth or Kafka consumer-group lag | `triggers-redis` / `triggers-kafka` |

```yaml
# triggers.yaml
version: 1
triggers:
  # Fire for every new S3 object; ${trigger.object_key} is injected into the run config
  - name: load-files
    type: object_arrival
    config: ./my_pipeline.yaml
    store: { type: s3, bucket: my-bucket, prefix: incoming/, region: us-east-1 }
    poll_interval_secs: 30
    mode: per_object
    start_at: now

  # Fire when POST /v1/triggers/sync-hook is called
  - name: sync-hook
    type: webhook
    config: ./csv_to_jsonl.yaml
    dedupe_header: Idempotency-Key

  # Fire when a Redis list depth reaches >= 1
  - name: drain-jobs
    type: queue_depth
    config: ./redis_to_sqlite.yaml
    queue: { type: redis, url: redis://localhost:6379, key: jobs, kind: list }
    threshold: 1
    poll_interval_secs: 15
```

```bash
# Start with event-driven triggers
FAUCET_SERVE_AUTH_TOKEN=s3cret \
faucet serve --listen 0.0.0.0:8080 --triggers triggers.yaml

# Fire the webhook trigger manually
curl -XPOST http://localhost:8080/v1/triggers/sync-hook \
     -H "Authorization: Bearer s3cret" \
     -H "Idempotency-Key: run-001" -d '{}'

# Print the JSON Schema for the triggers file format
faucet schema triggers
```

Each trigger emits `faucet_serve_triggers_fired_total{trigger,type}`,
`faucet_serve_trigger_healthy{trigger,type}`, and related Prometheus metrics.
`GET /readyz` includes a `triggers` array showing per-watcher health.

See the [triggers cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/triggers.html)
for detailed walkthroughs and the [triggers reference](https://pawansikawat.github.io/faucet-stream/reference/triggers.html)
for the full field reference.

Requires the `triggers` feature family (included in `full`):

```bash
cargo install faucet-cli --features "triggers,triggers-object-store,triggers-redis,triggers-kafka"
cargo install faucet-cli --features full    # all features
```

#### Clustered execution (`--cluster`)

`--cluster` turns a fleet of `faucet serve` processes into a pull-balanced, self-healing
cluster. Instances share a single Postgres or SQLite history database; submissions are written
as `pending` in the shared DB and any instance with spare capacity atomically claims and runs
them. If an instance crashes, a survivor's lease loop detects the expired-lease run and
re-queues it (up to `--cluster-max-attempts`). All instances must be **homogeneous** — same
container image, env vars, and secrets access — because the claiming instance re-resolves
`${env:…}`/`${secret:…}` directives with its own credentials at execution time.

```bash
# Node A
FAUCET_SERVE_AUTH_TOKEN=s3cret \
faucet serve --cluster \
             --history 'postgres://faucet:pw@db/faucet' \
             --listen 0.0.0.0:8080

# Node B (same DB)
FAUCET_SERVE_AUTH_TOKEN=s3cret \
faucet serve --cluster \
             --history 'postgres://faucet:pw@db/faucet' \
             --listen 0.0.0.0:8081
```

See the [cluster cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/cluster.html)
for the full lifecycle, delivery guarantees, and Kubernetes deployment notes.

Submit a run:

```bash
curl -XPOST localhost:8080/v1/runs -H "Authorization: Bearer s3cret" \
  -H 'content-type: application/json' \
  -d '{"config":"version: 1\npipeline:\n  source: {type: csv, config: {path: in.csv}}\n  sink: {type: jsonl, config: {path: out.jsonl}}\n","name":"adhoc","idempotency_key":"k1"}'
```

> ⚠️ **Security:** `serve` executes arbitrary client-supplied configs with the server's identity — secrets, files, and network egress (SSRF). Run single-tenant, authenticated, behind egress controls; terminate TLS at a proxy. See the [serve cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/serve.html) and [HTTP API reference](https://pawansikawat.github.io/faucet-stream/reference/http-api.html).

Requires the `serve` Cargo feature (included in `full`):

```bash
cargo install faucet-cli --features serve
cargo install faucet-cli --features "serve,serve-history-postgres,serve-history-sqlite"
```

#### Optional embedded web console (`serve-ui`)

Build with `serve-ui` to serve a browser-based web console at `/` alongside the
REST API. The console gives you a Runs dashboard, Run detail with live SSE logs,
a Submit view (raw YAML/JSON editor + schema-driven wizard), and a Schemas
explorer — all backed by the same bearer-gated `/v1` API.

```bash
cargo install faucet-cli --features serve-ui    # serve-ui implies serve
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 127.0.0.1:8080
# Open http://127.0.0.1:8080/ in a browser; paste the bearer token when prompted.
```

Pass `--no-ui` to disable the console at runtime without rebuilding. The `serve-ui`
feature also adds three bearer-gated endpoints: `GET /v1/schemas` (connector
catalog), `GET /v1/schemas/{kind}/{name}` (one JSON Schema), and `POST /v1/doctor`
(validate + probe a config without running it). These endpoints are available
regardless of `--no-ui`.

See the [web console guide](https://pawansikawat.github.io/faucet-stream/cookbook/web-console.html)
for the full walkthrough.

### `faucet init`

`faucet init` writes a starter `pipeline.yaml` by walking each selected connector's JSON Schema. Required fields are surfaced with a `# REQUIRED` comment and a typed placeholder (`""`, `0`, `false`, `[]`, `{}`); optional fields are commented out so connector-level defaults stay in force. Enum-typed fields list valid values in the trailing comment. Tagged-enum blocks (the `#[serde(tag = "type")]` shape used by `auth:`, `pagination:`, BigQuery `credentials:`, etc.) inline the chosen variant and emit every other variant as a commented-out "Alternative variants" block right below it — so users can switch auth modes (or pagination, or credentials) without leaving the file to consult `faucet schema`. Run `faucet init --interactive` (requires `--features cli-interactive`) to be prompted for each variant up front.

```bash
faucet init                                              # rest → jsonl, name = my-pipeline
faucet init my-job                                       # rest → jsonl, name = my-job
faucet init my-job --source postgres --sink bigquery     # postgres → bigquery
faucet init --source rest --sink jsonl -o config.yaml    # custom output path
faucet init --force                                      # overwrite pipeline.yaml in cwd
faucet init --interactive                                # TTY prompts (requires --features cli-interactive)
```

Flags:

| Flag | Purpose |
|------|---------|
| `name` (positional) | Pipeline name written to the generated file's `name:`. Defaults to `my-pipeline`. |
| `--source <kind>` | Source connector to scaffold (e.g. `rest`, `postgres`, `s3`). Defaults to `rest`. |
| `--sink <kind>` | Sink connector to scaffold (e.g. `jsonl`, `bigquery`). Defaults to `jsonl`. |
| `--output, -o <path>` | Output file path. Defaults to `pipeline.yaml`. |
| `--force` | Overwrite an existing file at the output path. |
| `--interactive` | Prompt for kinds via `inquire` on a TTY; falls back to `--source`/`--sink` otherwise. Requires the `cli-interactive` build feature. |

Run `faucet list` to see every kind that's compiled into your build of `faucet`. Use `faucet schema source <kind>` (or `sink <kind>`, or `transform <name>`) to see the full JSON Schema if a field's truncated description doesn't tell you enough.

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

## Named source and sink templates

Declare reusable connector definitions under `pipeline.sources` and
`pipeline.sinks`, then pick from them per matrix row via `ref: <name>`.
Combined with the top-level `vars:` block, this is the recommended shape
for any config with more than one matrix row.

```yaml
version: 1
name: api_ingest

vars:                                # optional shared constants
  api_base: https://api.example.com
  api_token: ${env:API_TOKEN}

pipeline:
  sources:                           # named source templates
    api:
      type: rest
      config:
        base_url: ${vars.api_base}
        auth: { type: Bearer, token: ${vars.api_token} }
        records_path: $.data[*]
  sinks:                             # named sink templates
    archive:
      type: jsonl
      config: { append: false }

matrix:
  - id: users
    source: { ref: api, config: { path: /v1/users } }
    sink:   { ref: archive, config: { path: users.jsonl } }
  - id: orders
    source: { ref: api, config: { path: /v1/orders } }
    sink:   { ref: archive, config: { path: orders.jsonl } }
```

### Resolution order

Load-time interpolation runs in this order:

1. `${env:VAR}` / `${file:PATH}` / `${secret:VAR}` — resolved during the raw text pass.
2. `${vars.X}` — resolved against the top-level `vars:` block. Vars may reference other vars; cycles surface as `InterpolationCycle`.
3. `${sources.NAME.PATH}` and `${sinks.NAME.PATH}` — resolved against the post-vars-substitution template bodies. Useful for copying constants between templates without restating them. A template may reference another template (including across the source/sink namespaces), and such chains are followed to their terminal value; mutual or circular references surface as `InterpolationCycle` rather than resolving to literal token text.
4. `${row_id.path}` — left literal; resolved at runtime against parent records (per-record fan-out).

### Backwards compatibility

The legacy singular `pipeline.source:` / `pipeline.sink:` continues to work
unchanged. Internally they register as a template named `default`. A
matrix row without a `ref:` field inherits the `default` template
(matching the pre-templates merge semantics). You can mix the two
styles — declare some templates via `pipeline.sources.*` and a
fallback via `pipeline.source:` — but the `default` slot can only be
defined once.

See [`examples/templates_dry_rest.yaml`](examples/templates_dry_rest.yaml) and
[`examples/templates_users_posts.yaml`](examples/templates_users_posts.yaml) for
end-to-end examples of this pattern.

## Config composition

Factor shared connection / sink / transform pieces out of each file and
recombine them at load time. Three mechanisms, all resolved when the file is
read (**before** any `${...}` interpolation):

| Mechanism | Form | Effect |
|-----------|------|--------|
| `extends:` | `extends: ./base.yaml` (or a list) | Inherit one or more base files; the child deep-merges on top. |
| `profiles:` | `profiles: { dev: {…}, prod: {…} }` | Named overlays, selected with `--profile NAME` / `FAUCET_PROFILE` (flag wins). |
| `!include` | `key: !include ./frag.yaml` | Substitute a YAML fragment at any node (**YAML only**). |

```yaml
# app.yaml — inherits a base, then pulls in a reusable transform chain.
extends: ./base.yaml
pipeline:
  transforms: !include ./transforms.yaml
```

```bash
faucet run app.yaml --profile prod                 # select an overlay
faucet validate app.yaml --show-composed --profile prod   # print the merged config
```

Precedence (last wins): `extended base → child document → profile → matrix row`,
all via the same deep-merge as `matrix` rows. `faucet validate --show-composed`
prints the fully composed document (bases merged, profile applied, fragments
substituted, `extends:`/`profiles:` metadata stripped) before interpolation.

**Composition is file-loads-only** — `extends`/`profiles`/`!include` apply to
configs read from disk (`run`/`validate`/`preview`/`doctor`/`schedule`), **not**
to configs submitted to `faucet serve` over HTTP (a submitted body is a single
self-contained document with no filesystem access). See
[`examples/compose/`](examples/compose/) for an end-to-end example, and the
[docs-site composition cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/composition.html)
for the full walkthrough.

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
    - type: keys_case
      config: { mode: snake }
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
| `${file:./path}` | Load-time. File contents trimmed of trailing whitespace. Capped at 1 MiB — this is for small token/secret/cert files, not bulk data. |
| `${secret:VAR}` | Load-time. Alias for `${env:VAR}` today (no at-rest redaction). |
| `${vault:<path>[#field]}` | Load-time. HashiCorp Vault KV v2. Requires `VAULT_ADDR` + `VAULT_TOKEN`. `#field` extracts one key from a JSON secret. Build with `--features secrets-vault`. |
| `${aws-sm:<name-or-ARN>[#field]}` | Load-time. AWS Secrets Manager. Auth: `aws-config` default chain (env / profile / instance / web-identity). Build with `--features secrets-aws-sm`. |
| `${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}` | Load-time. GCP Secret Manager (`versions/latest` ok). Auth: Application Default Credentials. Build with `--features secrets-gcp-sm`. |
| `${azure-kv:<vault>/<secret>[/<version>]}` | Load-time. Azure Key Vault. Auth: `AZURE_*` env / managed identity / `az login`. Build with `--features secrets-azure-kv`. |
| `${row_id.dotted.path}` | Run-time, per parent record. The `row_id` must be the id of another matrix row. |
| `${now.*}` | Run-time, per invocation. Injects the run's wall time into source and sink config values. See below. |

A token's form decides its meaning: a **colon** marks a load-time directive (`${env:VAR}`), while a **dot or nothing** marks a deferred row-id reference (`${users.id}`). The same rule is used by both `faucet validate` and `faucet run`, so a token like `${env.foo}` (a dot, not a colon) is consistently treated as a reference to row id `env` and rejected at validate-time rather than failing only at run-time.

`$${` escapes a literal `${`. Reserved row ids that can never appear in `matrix.id`: `env`, `file`, `secret`, `matrix`, `pipeline`, `now`.

#### `${now.*}` — run-clock interpolation

Inject the invocation's wall time into any **source or sink** config value. Common use case: writing to a dated output path so each scheduled run lands in its own partition.

| Token | Example | Notes |
|-------|---------|-------|
| `${now.date}` | `2026-03-08` | `YYYY-MM-DD` |
| `${now.datetime}` / `${now.iso}` | `2026-03-08T14:05:09+00:00` | RFC 3339 |
| `${now.year}` | `2026` | Zero-padded |
| `${now.month}` | `03` | Zero-padded (01–12) |
| `${now.day}` | `08` | Zero-padded (01–31) |
| `${now.hour}` | `14` | Zero-padded (00–23) |
| `${now.minute}` | `05` | Zero-padded (00–59) |
| `${now.second}` | `09` | Zero-padded (00–59) |
| `${now.unix}` | `1741442709` | Epoch seconds |
| `${now.strftime.<fmt>}` | `2026/03/08/14` | Arbitrary chrono strftime, e.g. `${now.strftime.%Y/%m/%d/%H}` |

An unknown token (e.g. `${now.foo}`) is a config error at run time. `${now.*}` is **not** resolved in `state:`, `dlq:`, `transforms:`, or the `auth:` / `vars:` blocks.

**Clock source:**

- `faucet run` — process start time in UTC, or `--clock <RFC3339|YYYY-MM-DD>` for backfills (a bare date means midnight UTC).
- `faucet schedule` — the tick's scheduled time in the schedule's `timezone`; `${now.date}` therefore matches the timezone the cron fires in, not UTC.

**Backfills:**

```bash
faucet run --clock 2026-03-01 pipeline.yaml          # midnight UTC
faucet run --clock 2026-03-01T02:00:00-08:00 pipeline.yaml  # precise timestamp
```

**Local file sinks** (JSONL, CSV) create missing parent directories automatically, so dated subdirectory paths like `./data/dt=${now.date}/part.jsonl` work without pre-creating the tree.

**Security note.** Pipeline configs are trusted input: `${file:...}` reads any path the process can access (capped at 1 MiB), and `${env:}`/`${secret:}` inject process environment values. Connector-config deserialization errors are scrubbed (double-quoted values redacted, length-capped) before they reach logs so an injected secret can't leak through an error message, but treat configs and their resolved values as sensitive.

#### Execution

- `max_concurrent` bounds total in-flight invocations (roots + per-parent-record children compete for one budget). Default: `min(num_cpus, 4)`.
- `on_error: continue` (default) — a failed invocation is logged, its subtree is skipped, every sibling already running keeps running to completion. The process exits non-zero if any invocation failed.
- `on_error: stop` — first failure halts the entire run. In-flight invocations are **cooperatively cancelled**: each stops at its next page boundary and flushes its sink, so a buffered sink (e.g. Parquet, whose footer is only written on flush) commits the rows written so far rather than orphaning the whole file (#146 H16). Any invocation still stuck *mid-write* after a short flush grace is then hard-aborted, and pending invocations waiting on a permit stop before doing real work. Honours `max_concurrent` like `continue` does.

> **Caveat for `stop`:** even with the cooperative flush, cancelling between pages can leave partial state in the sink — only the pages written-and-flushed before the cancel are durable, and a sink hard-aborted mid-write (past the flush grace) may leave a half-written file, an open transaction, or a connection that closed before the server's response was read. Idempotent sinks (JSONL append, S3 put with a fixed key, BigQuery streaming insert with `insertId`, upsert-style writes) handle re-runs cleanly. Non-idempotent sinks (`HTTP POST` without dedupe headers, `INSERT` with auto-id) may double-write on retry. If you can't tolerate that, prefer `on_error: continue` and reconcile failed rows after the fact.

#### Adaptive batch sizing

The optional `adaptive_batch_size:` sub-block under `execution:` enables the
AIMD controller that auto-tunes the effective write batch size from observed sink
latency and error rate. Default `enabled: false` (opt-in).

```yaml
execution:
  adaptive_batch_size:
    enabled: true
    min: 500               # lower bound (rows)
    max: 10000             # upper bound; inert above the source page size
    increase_step: 500     # additive growth per clean, fast batch
    decrease_factor: 0.5   # multiplicative shrink on error or high latency
    cooldown_batches: 5    # batches to skip after a shrink before growing again
    target_latency_ms: 1000  # optional write-latency target (ms)
    error_threshold: 0.01  # per-batch error rate that triggers a shrink
```

**Caveats:**

- **Error-driven shrink requires a `dlq:` block.** The error signal comes from
  per-row outcomes reported via the DLQ path. Without a DLQ the controller sees
  no errors; only `target_latency_ms` can drive shrinks.
- **Effective ceiling = source page size (within-page only in v1).** The
  controller reslices pages it already received — it cannot buffer across pages.
  Raise the source `batch_size` to allow bigger write batches.

See the [Adaptive batching cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/adaptive-batching.html)
for the full field reference, AIMD trajectory example, and the four Prometheus
metrics (`faucet_pipeline_adaptive_batch_*`).

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

### `dlq:` (optional)

Sibling of `source`, `sink`, `transforms`, `state` under `pipeline:`.

| Field | Type | Default | Notes |
|---|---|---|---|
| `sink` | ConnectorSpec | required | Any sink — typically `jsonl`, `s3`, `kafka`, `http`. |
| `on_batch_error` | `propagate` \| `dlq_all` | `propagate` | What to do when the main sink fails wholesale (no per-row info). |
| `max_failures_per_page` | integer | unset (unlimited) | Abort if a single page produces more than this many DLQ records. |
| `max_failures_total` | integer | unset (unlimited) | Abort if the run-wide DLQ count exceeds this. |
| `include_original_payload` | bool | `true` | Reserved for a future headers-only mode. Always `true` in v1. |

Matrix rows can override the inherited `dlq:` wholesale, or disable
inherited DLQ for that row with `dlq: null`.

Example:

```yaml
pipeline:
  source: { type: rest, config: { base_url: "https://api.example.com", path: "/v1/users" } }
  sink:
    type: bigquery
    config:
      project_id: my-project
      dataset_id: prod
      table_id: users
  dlq:
    sink:
      type: jsonl
      config: { path: ./dlq/users.jsonl }
    on_batch_error: propagate
    max_failures_per_page: 100
    max_failures_total: 10000
```

### Transforms

Eleven built-in transforms are exposed as `type:` values: `flatten`,
`rename_keys`, `keys_case`, `spell_symbols`, `select`, `drop`, `set`,
`rename_field`, `cast`, `redact`, `value_case`. They run in declared
order. The
[record transforms cookbook page](../docs/book/src/cookbook/transforms.md)
has the full reference.

```yaml
transforms:
  - type: flatten
    config: { separator: "__" }
  - type: select
    config:
      fields: [id, name, email]
  - type: cast
    config:
      fields: { id: string }
      on_error: error
  - type: redact
    config:
      fields: [email]
      mask: "***"
  - type: set
    config:
      values:
        _source: my-api
```

### Compression

File-shaped connectors (JSONL/CSV/S3/GCS source and sink) accept a `compression` field. Default `auto` detects `.gz` and `.zst` from the file path or object key.

```yaml
version: 1
pipeline:
  source:
    kind: csv
    config:
      path: data.csv.gz
      compression: auto      # or 'gzip', 'zstd', 'none'
  sink:
    kind: jsonl
    config:
      path: out.jsonl.zst
      compression: auto
```

Build the CLI with the feature enabled:

```bash
cargo install --path cli --features compression
```

## Secrets-manager interpolation

Pull secret values directly from a secrets manager using `${scheme:reference}`
directives anywhere in your config. Resolution happens at config-load time:
values are fetched concurrently (up to 8 in parallel), de-duplicated, and
substituted in place. They are never written to disk.

```yaml
auth:
  type: bearer
  config:
    token: "${vault:secret/data/myapp/api#token}"
```

| Backend | Directive | Auth |
|---------|-----------|------|
| HashiCorp Vault KV v2 | `${vault:<path>[#field]}` | `VAULT_ADDR` + `VAULT_TOKEN` (+ optional `VAULT_NAMESPACE`) |
| AWS Secrets Manager | `${aws-sm:<name-or-ARN>[#field]}` | `aws-config` default chain |
| GCP Secret Manager | `${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}` | Application Default Credentials |
| Azure Key Vault | `${azure-kv:<vault>/<secret>[/<version>]}` | `AZURE_*` env / managed identity / `az login` |

The `#field` selector (Vault and AWS) parses the secret body as JSON and returns
one key. Omit it to receive the full secret body as a string.

**Build features** — none compiled in by default:

```bash
cargo install faucet-cli --features secrets          # all four backends
cargo install faucet-cli --features secrets-vault    # Vault only
cargo install faucet-cli --features secrets-aws-sm   # AWS only
cargo install faucet-cli --features secrets-gcp-sm   # GCP only
cargo install faucet-cli --features secrets-azure-kv # Azure only
```

**Validation flags:**

- `faucet validate pipeline.yaml` — resolves all secrets as a preflight; prints
  `secret: <scheme>:<reference> → resolved` per reference (never the value).
- `faucet validate --no-secrets pipeline.yaml` — grammar / structure only; no
  network or credentials required.
- `faucet schema secrets` — prints the grammar descriptor as JSON.

**Redaction:** faucet scrubs every resolved secret value from its own tracing,
log, and error output via a `RedactingWriter` on the tracing subscriber.
This boundary covers faucet's own output only — connector libraries that
debug-log deserialized config fields are outside it; never enable debug logging
on connectors that hold resolved secrets.

**Known limitation:** secret directives are resolved in connector configs,
transforms, state, dlq, and matrix rows. They are **not** resolved in the
top-level `auth:` catalog or `vars:` block. Put secrets in a connector's inline
`auth:` config instead of the shared catalog until this is lifted.

See the [docs-site secrets cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/secrets.html)
for full examples and details.

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
| `FAUCET_TRANSFORM_<N>` | Optional. Indexed transforms — `FAUCET_TRANSFORM_1=keys_case`, `FAUCET_TRANSFORM_2=flatten`. Indices must be contiguous starting at 1. |
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
- Named templates: [`templates_dry_rest`](examples/templates_dry_rest.yaml) — shared REST source template across multiple matrix rows; [`templates_users_posts`](examples/templates_users_posts.yaml) — templates with parent/child DAG fan-out

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

## Troubleshooting / FAQ

**"No config file found" / wrong file picked up.** With no path argument, `faucet` auto-discovers `faucet.yaml`, then `faucet.yml`, then `faucet.json` in the current directory. Pass the path explicitly (`faucet run path/to/pipeline.yaml`) when the file lives elsewhere or has a different name.

**Environment variables / `.env` not applied.** `faucet` loads a `.env` from the current directory by default; point at another with `--env-file path/.env`, or disable loading entirely with `--no-env-file`. `${env:VAR}` / `${file:PATH}` placeholders resolve at config-load time, so the var must be set (or the `.env` loaded) before the command runs.

**"unknown source/sink type" or a connector seems missing.** Connectors are feature-gated. A slim build (`--no-default-features --features …`) only includes the connectors you compiled in. Run `faucet list` to see exactly which sources, sinks, transforms, and state backends are present in your binary; reinstall with the needed `--features` (or the `full` feature) if one is absent.

**`faucet validate` fails.** Validation parses the config, expands the matrix, and checks every connector/transform spec (plus exactly-once and write-mode gates) without running. The error names the offending matrix row and field. Use `faucet validate --show-composed` to print the fully merged document (after `extends:` / `!include` / profiles) and `faucet schema source|sink|transform <name>` to confirm the expected field shape.

**Secrets not resolving (`${vault:…}` / `${aws-sm:…}` / `${gcp-sm:…}` / `${azure-kv:…}`).** Secrets resolution is feature-gated — install with the matching `secrets-*` feature (or the `secrets` aggregate). Run `faucet validate` (without `--no-secrets`) as a real preflight: it fetches each reference and prints `secret: <scheme>:<reference> → resolved`, surfacing missing credentials (e.g. `VAULT_ADDR`/`VAULT_TOKEN`, the AWS default chain, GCP ADC, Azure env/managed identity) before a run. Use `--no-secrets` to validate offline without contacting any secrets manager.

**Pipeline runs but I see no records / no logs.** Pipeline records and command output go to **stdout**; logs go to **stderr**. Raise verbosity with `--log-level debug` or `FAUCET_LOG=debug`. Never enable debug logging on a pipeline whose connector configs hold resolved secrets — third-party connector debug output is outside faucet's redaction boundary.

## See also

- [`faucet-stream`](https://crates.io/crates/faucet-stream) — the umbrella library this CLI is built on.
- [`faucet-core`](https://crates.io/crates/faucet-core) — shared traits, pipeline orchestration, and error types.
- [Documentation site](https://pawansikawat.github.io/faucet-stream/) — guides, the connector capability matrix, and the config-file grammar reference.
- [GitHub repository](https://github.com/PawanSikawat/faucet-stream) — source, examples (`cli/examples/`), and issue tracker.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
