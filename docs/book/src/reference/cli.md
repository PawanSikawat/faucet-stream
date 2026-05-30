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

`[config]` is optional for `run` / `validate` / `preview` / `doctor`: if omitted,
faucet auto-discovers `faucet.yaml` → `.yml` → `.json` in the current directory.

## `run`

```bash
faucet run pipeline.yaml
faucet run                       # auto-discover faucet.yaml in cwd
faucet run --from-env            # build the pipeline entirely from FAUCET_* env vars
faucet run pipeline.yaml --env-file prod.env
faucet run pipeline.yaml --no-env-file
```

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

## Environment-only mode

`faucet run --from-env` assembles a pipeline from a `FAUCET_*` snapshot
(`FAUCET_SOURCE_*`, `FAUCET_SINK_*`, `FAUCET_STATE_*`, `FAUCET_TRANSFORM_<N>_*`),
which is handy for containerized deployments where everything comes from the
environment. Nested/tagged-enum fields use a `*_JSON` suffix.

> The complete config grammar (matrix, templates, vars, execution) lives in
> [`cli/README.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/README.md).
