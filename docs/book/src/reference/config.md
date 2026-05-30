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

## Interpolation

Three stages resolve placeholders:

- **Load time:** `${env:VAR}`, `${file:PATH}`, `${secret:VAR}` are resolved when
  the file is read. `${vars.X}` resolves against the top-level `vars:` block;
  `${sources.NAME.PATH}` / `${sinks.NAME.PATH}` resolve against named templates.
  Secret-manager directives (see below) run as the final load-time stage.
- **Runtime:** `${row_id.dotted.path}` tokens are resolved per parent record in
  DAG runs.

Reference cycles surface as a clear `InterpolationCycle` error.

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

## `execution`

- `max_concurrent` — one shared concurrency budget across roots and child
  fan-outs.
- `on_error` — `continue` (siblings finish; failed subtree skipped) or `stop`
  (abort pending and in-flight work on first failure).

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

## Discovery & env files

`run` / `validate` / `preview` / `schedule` auto-discover `faucet.yaml` → `.yml` → `.json` in
the current directory, and load a sibling `.env` unless `--no-env-file` is given
(`--env-file PATH` points elsewhere).

> The authoritative, exhaustive grammar — including every matrix and template
> edge case — is in
> [`cli/README.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/README.md).
