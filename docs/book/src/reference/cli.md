# CLI commands

The `faucet` binary exposes these commands. Pass `--log-level <level>` (or set
`FAUCET_LOG`) to control logging.

| Command | What it does |
|---------|--------------|
| `faucet run [config]` | Run the pipeline(s) in a config file. |
| `faucet validate [config]` | Parse, expand, and validate a config without running it. |
| `faucet preview [config]` | Run only the source side and print records to stdout. |
| `faucet schema <target>` | Print the JSON Schema for a connector, transform, or the DLQ. |
| `faucet list` | List every compiled-in source, sink, and transform with a one-line description. |
| `faucet init [name]` | Scaffold a commented config skeleton from connector schemas. |
| `faucet doctor [config]` | Probe every connector (auth/network/permissions) and print a checklist. |
| `faucet schedule [config]` | Run a pipeline on a cron schedule (long-running foreground process). |
| `faucet serve` | Run a long-running HTTP control plane: submit / poll / cancel pipeline runs over REST. |

`[config]` is optional for `run` / `validate` / `preview` / `doctor` / `schedule`: if omitted,
faucet auto-discovers `faucet.yaml` → `.yml` → `.json` in the current directory.

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
  - default [root] source=rest sink=jsonl
```

Pass `--no-secrets` to validate grammar and structure only, skipping all secret
fetches. This is useful in CI environments that lack credentials, or in local
development before vault access is available:

```bash
faucet validate --no-secrets pipeline.yaml
```

## `preview`

Runs the first root row's source and prints records (via the stdout sink).
Children aren't previewed because they need parent records to resolve
`${parent.path}` tokens.

```bash
faucet preview pipeline.yaml --limit 10
```

## `schema`

```bash
faucet schema source rest
faucet schema sink bigquery
faucet schema transform keys_case
faucet schema dlq
faucet schema secrets
```

`faucet schema transform <name>` prints the inline config schema for a
transform (e.g. `keys_case` lists the valid `mode:` values). Run
`faucet list` to see which transforms are compiled into your binary.

`faucet schema secrets` prints the directive grammar and auth requirements for
all four secrets-manager backends in machine-readable JSON — useful for tooling
that needs to understand the interpolation syntax without reading the docs.

## `init`

```bash
faucet init my_pipeline --source postgres --sink bigquery
```

Required fields are surfaced with a typed placeholder and a `# REQUIRED` marker;
optional fields are commented out so connector defaults apply. The interactive
mode (`--interactive`) is gated behind the `cli-interactive` feature.

## `doctor`

```bash
faucet doctor pipeline.yaml                  # checklist; exit code = # of failed probes
faucet doctor pipeline.yaml --timeout-secs 5 # per-probe timeout (default 10)
faucet doctor pipeline.yaml --json           # machine-readable, for CI gating
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

Child invocations (parent/child matrix rows) are listed but not probed — their
configs depend on parent records that only exist at run time. Probe messages are
scrubbed for resolved secrets before printing.

See the [Troubleshooting](../cookbook/troubleshooting.md) cookbook page for
reading the output and common failures.

## `schedule`

```bash
faucet schedule pipeline.yaml                  # run on cron schedule, foreground; Ctrl-C to stop
faucet schedule pipeline.yaml --once           # run exactly once now, then exit
faucet schedule pipeline.yaml --env-file prod.env
faucet schedule pipeline.yaml --no-env-file
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
