# CLI commands

The `faucet` binary exposes these commands. Pass `--log-level <level>` (or set
`FAUCET_LOG`) to control logging.

| Command | What it does |
|---------|--------------|
| `faucet run [config]` | Run the pipeline(s) in a config file. |
| `faucet validate [config]` | Parse, expand, and validate a config without running it. |
| `faucet preview [config]` | Run only the source side and print records to stdout. |
| `faucet schema <target>` | Print the JSON Schema for the whole config (`config`), a connector, a transform, or any block. |
| `faucet list` | List every compiled-in source, sink, and transform with a one-line description. |
| `faucet init [name]` | Scaffold a commented config skeleton from connector schemas. |
| `faucet new connector <name> --kind <source\|sink>` | Scaffold a ready-to-build connector crate. |
| `faucet doctor [config]` | Probe every connector (auth/network/permissions) and print a checklist. |
| `faucet test <specs…>` | Run fixture-based offline pipeline tests from one or more spec files. |
| `faucet replicate [config]` | Bulk-snapshot a table, then hand off to CDC for a gap-free mirror. |
| `faucet schedule [config]` | Run a pipeline on a cron schedule (long-running foreground process). |
| `faucet serve` | Run a long-running HTTP control plane: submit / poll / cancel pipeline runs over REST. |

`[config]` is optional for `run` / `validate` / `preview` / `doctor` / `replicate` / `schedule`: if
omitted, faucet auto-discovers `faucet.yaml` → `.yml` → `.json` in the current directory.

## `run`

```bash
faucet run pipeline.yaml
faucet run                              # auto-discover faucet.yaml in cwd
faucet run --from-env                   # build the pipeline entirely from FAUCET_* env vars
faucet run pipeline.yaml --env-file prod.env
faucet run pipeline.yaml --no-env-file
faucet run pipeline.yaml --clock 2026-03-01          # backfill: set ${now.*} clock to midnight UTC
faucet run pipeline.yaml --clock 2026-03-01T02:00:00-08:00  # backfill: precise RFC 3339 timestamp
```

Flags:

| Flag | Purpose |
|------|---------|
| `--clock <value>` | Override the clock used by `${now.*}` tokens. Accepts an RFC 3339 timestamp (`2026-03-01T00:00:00Z`) or a bare date (`2026-03-01`, treated as midnight UTC). Default: process start time in UTC. Use this for backfills — run the same config with a different date without changing the file. |
| `--profile <name>` | Select a named overlay from the config's `profiles:` block (see [Config composition](config.md#config-composition)). Overrides `FAUCET_PROFILE`. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `validate` / `preview`. |
| `--from-env` | Build the pipeline entirely from `FAUCET_*` environment variables; mutually exclusive with a positional config path. |

## `validate`

Reports one line per expanded matrix row. Use it in CI to catch config errors
before deploying.

```bash
faucet validate pipeline.yaml
```

When the config contains secrets-manager directives (`${vault:…}`, `${aws-sm:…}`,
etc.), `faucet validate` resolves them as a real preflight and prints one
confirmation line per reference (never the value):

```
secret: vault:secret/data/faucet/api#token → resolved
ok: 'my-pipeline' rows=1 (roots=1, children=0) execution=(defaults)
  - default [root] source=rest sink=jsonl delivery=at-least-once
```

Each row line ends with the **derived end-to-end delivery guarantee** for that
row's source × sink × config — `at-least-once`,
`effectively-once (atomic watermark)`, or `effectively-once (keyed upsert)` —
computed regardless of the requested `delivery:` mode, so an upsert-keyed row
is reported as effectively-once even without `delivery: exactly_once`. See
[`delivery`](./config.md#delivery).

Pass `--no-secrets` to validate grammar and structure only, skipping all secret
fetches. This is useful in CI environments that lack credentials, or in local
development before vault access is available:

```bash
faucet validate --no-secrets pipeline.yaml
```

### Composition flags

When a config uses [composition](config.md#config-composition) (`extends:` /
`profiles:` / `!include`), `validate` resolves it like `run` does:

```bash
faucet validate app.yaml --profile prod        # select a named overlay
faucet validate app.yaml --show-composed       # print the fully merged config
```

- `--profile <name>` selects a named overlay from `profiles:` (also settable via
  `FAUCET_PROFILE`; the flag wins). An undeclared name is a clear load-time error.
- `--show-composed` prints the fully composed document — bases merged, the
  selected profile applied, `!include` fragments substituted, and the
  `extends:` / `profiles:` metadata stripped — *before* `${...}` interpolation.
  It's the fastest way to confirm a multi-file setup resolves to what you expect.

## `discover`

```bash
faucet discover conn.yaml                      # print a generated config to stdout
faucet discover conn.yaml -o pipeline.yaml     # write it to a file (--force to overwrite)
faucet discover conn.yaml --include 'public.*' --exclude '*.tmp_*'
faucet discover conn.yaml --source warehouse   # introspect a named pipeline.sources template
faucet discover conn.yaml --json               # machine-readable dataset list
```

Connects to the config's source, enumerates the datasets behind it (tables /
collections / indices / object-store prefixes), and emits a ready-to-run config
with **one matrix row per dataset** — the input document with its `matrix:`
block replaced, secrets echoed as raw `${…}` references. The generated config
passes `faucet validate`. Supported sources: `postgres`, `mysql`, `mssql`,
`sqlite`, `mongodb`, `elasticsearch`, `bigquery`, `snowflake`, `s3`, `gcs`.

| Flag | Purpose |
|------|---------|
| `--source <name>` | Which `pipeline.sources` template to introspect (default `default`, the singular `pipeline.source`). |
| `--include <glob>` / `--exclude <glob>` | Repeatable `*`-wildcard filters on dataset names (no includes = everything; excludes win). |
| `-o, --output <file>` / `--force` | Write the generated config to a file instead of stdout; `--force` overwrites. |
| `--json` | Emit the discovered `DatasetDescriptor` list as JSON instead of a config. |
| `--profile` / `--env-file` / `--no-env-file` | Same semantics as `run` / `validate`. |

See the [source discovery cookbook](../cookbook/discover.md).

## `preview`

Runs the first root row's source and prints records (via the stdout sink).
Children aren't previewed because they need parent records to resolve
`${parent.path}` tokens.

```bash
faucet preview pipeline.yaml --limit 10
faucet preview app.yaml --profile dev --limit 5   # preview with a named profile overlay
```

`--profile <name>` / `FAUCET_PROFILE` selects a named overlay from `profiles:` before
previewing. Same semantics as `run` and `validate`.

## `schema`

```bash
faucet schema config          # the WHOLE config document (top-level grammar)
faucet schema source rest
faucet schema sink bigquery
faucet schema transform keys_case
faucet schema dlq
faucet schema execution
faucet schema contract
faucet schema masking
faucet schema sla
faucet schema notifications
faucet schema secrets
faucet schema triggers
faucet schema catalog
```

`faucet schema config` prints a composed JSON Schema for the **entire**
`faucet.yaml` / `faucet.json` document — the top-level grammar (`version`,
`name`, `vars`, `auth`, `pipeline`, `matrix`, `execution`, and every optional
block such as `schedule` / `lineage` / `quality` / `dlq` / `resilience` that is
compiled into your binary) plus per-connector `type` discrimination: the
`source` / `sink` positions become a `oneOf` over the connector kinds your
binary knows, each branch embedding that connector's own config schema. Point an
editor at it for autocomplete and validation as you type — see
[Editor setup](./editor-setup.md).

`faucet schema transform <name>` prints the inline config schema for a
transform (e.g. `keys_case` lists the valid `mode:` values). Run
`faucet list` to see which transforms are compiled into your binary.

`faucet schema execution` prints the schema for the top-level `execution:`
block, including concurrency, error handling, and adaptive batch sizing.

`faucet schema masking` prints the JSON Schema for the `pipeline.masking:`
(PII detection + column-masking) block — see [masking](../cookbook/masking.md).

`faucet schema sla` prints the schema for the top-level `sla:`
(freshness/volume SLA) block — see [SLA monitoring](../cookbook/sla.md).

`faucet schema secrets` prints the directive grammar and auth requirements for
all four secrets-manager backends in machine-readable JSON — useful for tooling
that needs to understand the interpolation syntax without reading the docs.

`faucet schema triggers` prints the JSON Schema for the `--triggers` file format
(the `TriggersFile` / `TriggerSpec` / `TriggerKind` types). Requires the
`triggers` Cargo feature.

`faucet schema catalog` prints the JSON Schema for the top-level `catalog:`
(Data Movement Catalog store) block — see
[the catalog cookbook](../cookbook/catalog.md). Requires the `catalog` Cargo
feature.

## `init`

```bash
faucet init my_pipeline --source postgres --sink bigquery
```

Required fields are surfaced with a typed placeholder and a `# REQUIRED` marker;
optional fields are commented out so connector defaults apply. The interactive
mode (`--interactive`) is gated behind the `cli-interactive` feature.

**Singer discovery.** For the [Singer bridge](connectors.md) source, add
`--discover --executable <tap>` to run the tap's `--discover`, write the returned
catalog to `catalog.json`, and scaffold a config that inlines the catalog and
lists the discovered streams (with `stream:` left empty for you to choose):

```bash
faucet init --source singer --discover --executable tap-github -o pipeline.yaml
```

`faucet doctor` then verifies the tap resolves on `PATH` and that the selected
`stream` exists in the catalog.

## `new`

Scaffold a new **connector crate** (not a config) that follows every repo
convention — ready to `cargo build` and publish:

```bash
faucet new connector acme --kind source            # → faucet-source-acme/
faucet new connector acme --kind sink --common      # + a faucet-common-acme/ crate
faucet new connector acme --kind source -o crates/  # write into crates/
```

The generated crate has the standard module layout (`config.rs`, `stream.rs` or
`sink.rs`), a `JsonSchema`-deriving config, `config_schema()` / `connector_name()`
overrides, the `#![cfg_attr(docsrs, feature(doc_cfg))]` crate-root line, the
`[package.metadata.docs.rs]` block, system-name-first crates.io keywords, a
README, and a passing unit test — so `cargo test` is green out of the box with a
trivial passthrough. Fill in the `TODO`s, then publish. See
[Authoring a connector](../extending/authoring-connectors.md).

## `doctor`

```bash
faucet doctor pipeline.yaml                  # checklist; exit code = # of failed probes
faucet doctor pipeline.yaml --timeout-secs 5 # per-probe timeout (default 10)
faucet doctor pipeline.yaml --json           # machine-readable, for CI gating
faucet doctor app.yaml --profile prod        # probe with a named profile overlay applied
```

Runs a fast, **non-mutating** preflight against every connector in the config so
misconfiguration surfaces before a real run. For each root invocation it probes
the source, sink, and state store and prints a green/red checklist with elapsed
times; the **exit code equals the number of failed probes** (clamped to 255).

- **Sources** reuse the real read path — the probe pulls a single page and stops
  (never the full dataset). Sources whose first page would block or mutate use a
  targeted probe instead: `webhook` (port bindable), `websocket` (TCP connect),
  `postgres-cdc` (slot reachable), `kafka` (cluster metadata).
- **Sinks** run a read-only connect/auth/metadata call — `SELECT 1`, `HeadBucket`,
  `PING`, `tables.get`, cluster health, `fetch_metadata`, or a directory-writable
  check for file sinks. Never a real write.
- **State stores** do a sentinel `put`/`get`/`delete` that leaves no residue.
- **SLA** (when a top-level [`sla:` block](config.md#sla) is configured) reads the
  persisted run history and reports staleness of the last successful run vs
  `max_staleness_secs` and volume-baseline warm-up state — read-only.

Child invocations (parent/child matrix rows) are listed but not probed — their
configs depend on parent records that only exist at run time. Probe messages are
scrubbed for resolved secrets before printing.

`--profile <name>` / `FAUCET_PROFILE` selects a named overlay from `profiles:` before
probing (same semantics as `run` and `validate`).

See the [Troubleshooting](../cookbook/troubleshooting.md) cookbook page for
reading the output and common failures.

## `test`

```bash
faucet test tests/*.yaml                    # run every case; exit code = # of failed cases
faucet test tests/orders.yaml --filter null # only cases whose name contains "null"
faucet test tests/*.yaml --json             # machine-readable { total, passed, failed, tests }
faucet test tests/*.yaml --clock 2026-03-01 # default ${now.*} clock for cases without clock:
```

Runs fixture-based, **fully-offline** pipeline tests. Each case in a spec file
feeds sample records through the real transform → quality → contract path with
an in-memory source, sink, and DLQ — the configured source and sink are never
built or contacted — and asserts the output records, DLQ routing, counts, or an
expected failure. The **exit code equals the number of failed cases** (clamped
to 255), so CI gates on it directly.

Flags:

| Flag | Purpose |
|------|---------|
| `--filter <substring>` | Run only cases whose name contains the substring. |
| `--json` | Emit the JSON report instead of the human checklist. |
| `--clock <value>` | Default `${now.*}` clock for cases without `clock:` (RFC 3339 or `YYYY-MM-DD`). |
| `--profile <name>` | Profile overlay applied to referenced configs (same semantics as `run`). |
| `--resolve-secrets` | Resolve secrets-manager directives in referenced configs. Default: offline, directives stay unresolved. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

`faucet schema test` prints the spec file's JSON Schema. See the
[Testing pipelines](../cookbook/testing.md) cookbook page for the spec grammar,
matching semantics, and a CI recipe.

## `dlq`

Inspect, replay, and discard the dead-letter-queue envelopes a pipeline's `dlq:`
sink wrote. A DLQ *location* is a local `.jsonl` file, a directory of `*.jsonl`
files, or a glob.

```bash
faucet dlq inspect ./dlq/breaches.jsonl                          # breakdown + sample
faucet dlq replay pipeline.yaml --from ./dlq/breaches.jsonl --dry-run
faucet dlq replay pipeline.yaml --from ./dlq/breaches.jsonl      # re-feed through the pipeline
faucet dlq discard ./dlq/breaches.jsonl --reason contract --before 7d
```

**`faucet dlq inspect <location>`** — group envelopes by reason and error kind
with a sample.

| Flag | Effect |
|------|--------|
| `--reason <r>` | Only include envelopes with this reason (`partial` / `dlq_all` / `quality` / `schema_drift` / `contract`). |
| `--limit <n>` | Sample size. Default: 5. |
| `--json` | Emit a JSON summary. |

**`faucet dlq replay <config> --from <location>`** — re-feed the quarantined
payloads through the config's transforms → quality → contract → sink. Rows that
fail again go to a *fresh* DLQ, never back to the source.

| Flag | Effect |
|------|--------|
| `--from <location>` | DLQ location to replay from (required). |
| `--reason <r>` | Replay only envelopes with this reason. |
| `--failed-dlq <path>` | Where re-failed rows go. Default: a `replay-failed.jsonl` sibling of the source. |
| `--row <id>` | Which root of the config to replay through. Default: the first root. |
| `--dry-run` | Report what would be replayed without writing. |
| `--json` | Emit a JSON result. |
| `--env-file <path>` / `--no-env-file` / `--profile <name>` | Same config-load handling as `run`. |

**`faucet dlq discard <location>`** — remove processed envelopes.

| Flag | Effect |
|------|--------|
| `--reason <r>` | Only discard envelopes with this reason. |
| `--before <when>` | Only discard envelopes older than an RFC 3339 timestamp or a relative age (`7d` / `24h` / `30m`). |
| `--delete` | Permanently delete instead of archiving to a `<file>.archived.jsonl` sibling. |
| `--json` | Emit a JSON result. |

See the [Dead-letter queues](../cookbook/dlq.md) cookbook page for the envelope
shape and the inspect → fix → replay → discard workflow.

## `contract`

```bash
faucet contract pipeline.yaml                       # validate + human summary
faucet contract pipeline.yaml --export contract     # canonical contract JSON
faucet contract pipeline.yaml --export json-schema  # standalone JSON Schema
faucet contract pipeline.yaml --export openlineage  # OpenLineage schema facet
```

Validates the config's `pipeline.contract:` block (a malformed contract exits
non-zero with the compile error) and prints a summary of the promised fields,
constraints, and breach policy — or, with `--export`, a machine-readable
artifact for downstream consumers. Offline-safe: secrets are never fetched.
Requires the `contract` Cargo feature (in the default build). See the
[Data contracts](../cookbook/contracts.md) cookbook page.

## `masking`

```bash
faucet masking pipeline.yaml     # validate + per-destination rule breakdown
faucet masking                   # auto-discover faucet.yaml in cwd
```

Validates the config's `pipeline.masking:` block (a malformed policy exits
non-zero with the compile error) and prints, per destination sink, which rules
apply — the fast way to confirm `applies_to` scoping. Offline-safe: secrets are
never fetched. Requires the `masking` Cargo feature (in the default build). See
the [masking](../cookbook/masking.md) cookbook page.

## `catalog`

*(requires the `catalog` build feature — included in `full`)*

```bash
faucet catalog datasets --config pipeline.yaml                 # list catalogued datasets
faucet catalog datasets --config pipeline.yaml --kind csv --q users --json
faucet catalog show 3f2a9c1e0b7d4a55 --config pipeline.yaml    # detail (id prefix ok)
faucet catalog lineage --config pipeline.yaml --root 3f2a9c1e0b7d4a55 --depth 3
```

Browses the [Data Movement Catalog](../cookbook/catalog.md) named by the
config's `catalog:` block: the dataset list (newest activity first, `--kind` /
`--q` filters), one dataset's detail (schema timeline with diffs, recent
volume, upstream/downstream edges), and the lineage graph. All subcommands
accept `--json`; `--config` auto-discovers `faucet.yaml` in cwd when omitted.
Read-only — it never mutates the store.

## `notify`

*(requires the `notify` build feature)*

```bash
faucet notify test pipeline.yaml --event run_failure
faucet notify test --event circuit_open        # auto-discover faucet.yaml
```

Fires one **synthetic** event through the config's `notifications:` rules using
the real delivery path (no pipeline runs) — the fast way to confirm a Slack /
PagerDuty / webhook channel is wired correctly. `--event` accepts any event
kind (`run_failure`, `run_success`, `sla_breach`, `circuit_open`,
`contract_abort`, `dlq_threshold`, `scheduler_stuck`). See the
[Notifications](../cookbook/notifications.md) cookbook page.

## `replicate`

```bash
faucet replicate pipeline.yaml                 # bulk snapshot, then stream CDC; Ctrl-C to stop
faucet replicate                               # auto-discover faucet.yaml in cwd
faucet replicate pipeline.yaml --env-file prod.env
faucet replicate pipeline.yaml --no-env-file
faucet replicate app.yaml --profile prod       # apply a named profile overlay
```

Bulk-snapshots a database table and then hands off to **change-data-capture from
a position captured *before* the snapshot**, producing a true mirror (no gap, no
duplicate rows) when paired with `write_mode: upsert`. The config must contain a
top-level `replication:` block (see [config reference](config.md#replication));
`faucet run` ignores that block, exactly as it ignores `schedule:`.

It runs two phases in order:

1. **Bulk snapshot** — the `replication.snapshot.source` (a non-CDC query reader)
   back-fills the destination through the same sink and pipeline-level transforms.
2. **CDC handoff** — the `pipeline.source` CDC connector streams every change
   committed after the captured position over the snapshot baseline.

When `replication.continuous` is `true` (the default) the CDC phase is a
**long-running foreground process** — stop it with Ctrl-C or SIGTERM; the
in-flight page flushes at the next page boundary before the process exits. With
`continuous: false` it drains CDC once and exits. A [durable state
backend](../cookbook/state.md#state-stores) (`file` / `redis` / `postgres`, not
`memory`) is required so an interrupted run resumes correctly.

Flags:

| Flag | Purpose |
|------|---------|
| `--profile <name>` | Select a named overlay from `profiles:` (also settable via `FAUCET_PROFILE`; the flag wins). Same semantics as `run` / `validate`. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

See the [replication cookbook](../cookbook/replication.md) for the correctness
model, the resume behaviour, and the per-database retention caveats.

## `backfill`

```bash
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d --dry-run
faucet backfill pipeline.yaml --from 2026-06-01 --to 2026-07-01 --window 1d --resume
faucet backfill pipeline.yaml --from-bookmark '42' --to-bookmark '99' --bookmark-field seq
```

Replays a bounded historical window: chunks `[from, to)` into contiguous
half-open window units, runs each through the normal pipeline path with its
`${backfill.*}` tokens substituted and the `${now.*}` clock set to the window
start, and records durable, resumable progress in the config's `state:` store.
Unit state keys are namespaced (`{name}::backfill::{unit}`) so the forward-sync
bookmark is never touched; delivery is forced to at-least-once (pair with
`write_mode: upsert`). Exits non-zero with the failed-unit count.

| Flag | Purpose |
|------|---------|
| `--from` / `--to` | Wall-clock range: RFC3339 or `YYYY-MM-DD` (midnight in `--timezone`). Half-open. |
| `--window <dur>` | Chunk size (`45s`, `30m`, `6h`, `1d`, `1w`). Default: the config's `backfill.window`; omitted = one unit. |
| `--from-bookmark <v>` | Bookmark mode: seed the scoped state key with this value (JSON or bare string) and run one unit. Requires a `state:` block. |
| `--to-bookmark <v>` / `--bookmark-field <f>` | Upper bookmark bound: drop records whose field orders after the bound. |
| `--concurrency <n>` | Max window units in flight. Default: `backfill.concurrency`, else 1. |
| `--timezone <IANA>` | Date-boundary / `${now.*}` timezone. Default: `backfill.timezone`, else UTC. |
| `--row <id>` | Which root row to backfill (required when the config has several). |
| `--into <sink>` | Redirect writes to a named `pipeline.sinks` template (staging-first). |
| `--dry-run` | Print the planned units without executing. |
| `--resume` / `--restart` | Continue a prior backfill of the same range / discard its marker and start over. |
| `--json` | Machine-readable plan/report. |
| `--profile` / `--env-file` / `--no-env-file` | Same semantics as `run` / `validate`. |

See the [backfill cookbook](../cookbook/backfill.md) and the
[`backfill:` config block](config.md#backfill).

## `schedule`

```bash
faucet schedule pipeline.yaml                  # run on cron schedule, foreground; Ctrl-C to stop
faucet schedule pipeline.yaml --once           # run exactly once now, then exit
faucet schedule pipeline.yaml --env-file prod.env
faucet schedule pipeline.yaml --no-env-file
faucet schedule app.yaml --profile prod        # schedule with a named profile overlay applied
```

Runs a pipeline on a recurring cron schedule in a **long-running foreground process**. The config
must contain a top-level `schedule:` block (without one, faucet errors and suggests `faucet run`).
Requires the `schedule` Cargo feature (included in `full`).

- Stop with Ctrl-C or SIGTERM; the in-flight run drains for up to `shutdown_grace_secs` (default 30)
  before the process exits.
- `--once` ignores cron timing and runs the pipeline exactly once immediately — handy for testing
  a scheduled config or for one-shot container invocations.
- Missed ticks are skipped, not backfilled. A run that starts late emits
  `faucet_schedule_run_lateness_seconds` for monitoring.

Flags:

| Flag | Purpose |
|------|---------|
| `--once` | Run exactly once now, then exit. Ignores cron timing. |
| `--profile <name>` | Select a named overlay from `profiles:` (also settable via `FAUCET_PROFILE`; the flag wins). Same semantics as `run` / `validate`. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run` / `validate`. |

See the [scheduling cookbook](../cookbook/scheduling.md) for worked examples, the overlap-policy
decision tree, the resilience/supervisor model, and the full metric set to scrape.

## `serve`

```bash
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 0.0.0.0:8080
faucet serve --no-auth                             # explicit opt-in; required if no token
faucet serve --history sqlite:/var/lib/faucet/runs.db --default-config defaults.yaml
```

Runs a **long-running HTTP control plane** that accepts pipeline configs over REST, executes them
under bounded concurrency (reusing the same executor as `faucet run`), and exposes status / cancel /
list / SSE-logs endpoints plus `/healthz`, `/readyz`, and `/metrics`. Requires the `serve` Cargo
feature (included in `full`).

Unlike the other commands, `serve` takes **no config file** — configs arrive per request. Auth is
mandatory: pass `--auth-token`/`FAUCET_SERVE_AUTH_TOKEN`, or `--no-auth` to explicitly disable it
(absent both, startup fails).

Selected flags (`faucet serve --help` for the full list):

| Flag | Purpose |
|------|---------|
| `--listen <addr>` | Bind address (default `127.0.0.1:8080`; env `FAUCET_SERVE_LISTEN`). |
| `--auth-token <t>` / `--no-auth` | Bearer token (prefer the env var) or explicit no-auth opt-in. |
| `--auth-config <path>` | RBAC principals file (`{ name, token, role }`; roles `viewer`/`operator`/`admin`) — enables role enforcement + the `GET /v1/audit` log. Mutually exclusive with `--auth-token`/`--no-auth`. |
| `--max-concurrent-runs <n>` / `--max-queued-runs <n>` | Concurrency + queue caps (429 past the queue). |
| `--history <url>` | `postgres://…` / `sqlite:…` for durable run history (feature-gated; default in-memory). |
| `--default-config <path>` | Workspace defaults merged under every submitted run. |
| `--cors-origin <origin>` | Allow-list a browser origin (repeatable; CORS off by default). |
| `--lease-ttl-secs <n>` | Run-ownership lease TTL (default 30) for multi-instance orphan fencing on a shared persistent backend — set above worst-case stalls. See the [serve cookbook](../cookbook/serve.md#multi-instance-orphan-recovery-run-ownership-leases). |
| `--cluster` | Enable cluster mode: instances pull-balance `pending` runs from the shared `--history` DB and provide crash-failover. Requires a persistent `--history` backend (postgres or sqlite). See [Running a cluster](../cookbook/cluster.md). |
| `--cluster-poll-secs <n>` | Claim-loop poll interval in seconds (default `2`). Also the maximum lag before a cross-instance cancel is propagated to the executing instance. |
| `--cluster-max-attempts <n>` | Maximum total attempts (including crash-failovers) before a run is poisoned and marked `failed` (default `3`). |
| `--body-limit-bytes` / `--shutdown-grace-secs` / `--retain-terminal-runs-secs` / `--idempotency-retention-secs` | Tuning knobs. |
| `--no-ui` | Disable the embedded web console at runtime even when the binary was built with `serve-ui`. |
| `--triggers <path>` | Path to a YAML triggers file that defines event-driven watchers (object-arrival / webhook / queue-depth). Requires the `triggers` Cargo feature. See [Triggers reference](./triggers.md). |

### Optional embedded web console (`serve-ui`)

When built with the `serve-ui` Cargo feature, `faucet serve` also serves a
browser-based web console at `/` (and static assets at `/assets/*`):

```bash
cargo install faucet-cli --features serve-ui
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 127.0.0.1:8080
# Open http://127.0.0.1:8080/ in a browser.
```

The static shell is public; all `/v1` data is bearer-gated as usual. The
browser is prompted for the token on first load; it is stored in `localStorage`
and sent on every `/v1` call. Pass `--no-ui` to disable the console at runtime
without rebuilding.

`serve-ui` implies `serve` and is included in the `full` aggregate. It ships
three additional bearer-gated endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/schemas` | Catalog of compiled sources, sinks, transforms, and state-store kinds. |
| `GET` | `/v1/schemas/{kind}/{name}` | JSON Schema for one connector or transform (`kind` ∈ `source`/`sink`/`transform`). 404 for unknown. |
| `POST` | `/v1/doctor` | Validate + probe a submitted config without running it. 200 (pass) / 422 (fail). Body: `{ "config": "…", "config_format": "yaml" }`. |

These endpoints require `serve` and are available regardless of `--no-ui`. See
the [web console guide](../cookbook/web-console.md) for the full walkthrough and
the [HTTP API reference](./http-api.md) for the complete endpoint/schema
reference.

> ⚠️ `serve` executes arbitrary client-supplied configs with the server's identity (secrets, files,
> network egress). Run single-tenant, authenticated, behind egress controls. See the
> [serve cookbook](../cookbook/serve.md) for the security model and the
> [HTTP API reference](./http-api.md) for endpoints.

## Environment-only mode

`faucet run --from-env` assembles a pipeline from a `FAUCET_*` snapshot
(`FAUCET_SOURCE_*`, `FAUCET_SINK_*`, `FAUCET_STATE_*`, `FAUCET_TRANSFORM_<N>_*`),
which is handy for containerized deployments where everything comes from the
environment. Nested/tagged-enum fields use a `*_JSON` suffix.

> The complete config grammar (matrix, templates, vars, execution) lives in
> [`cli/README.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/README.md).
