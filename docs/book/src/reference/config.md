# Configuration file format

A faucet config is a YAML or JSON document with this top-level shape:

```yaml
version: 1                 # required, must be 1
name: my_pipeline          # optional; used in state keys and metrics
vars: {}                   # optional; reusable values referenced as ${vars.X}
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

## Interpolation

Three stages resolve placeholders:

- **Load time:** `${env:VAR}`, `${file:PATH}`, `${secret:VAR}` are resolved when
  the file is read. `${vars.X}` resolves against the top-level `vars:` block;
  `${sources.NAME.PATH}` / `${sinks.NAME.PATH}` resolve against named templates.
- **Runtime:** `${row_id.dotted.path}` tokens are resolved per parent record in
  DAG runs.

Reference cycles surface as a clear `InterpolationCycle` error.

## `matrix`

Each row is deep-merged onto `pipeline` (scalars replace, objects merge, arrays
replace). A row with `parent:` runs once per parent record. See the
[matrix DAG tutorial](../tutorials/matrix-dag.md). For DRY configs with many
rows, define named templates under `pipeline.sources` / `pipeline.sinks` and
select them per row with `ref:`.

## `execution`

- `max_concurrent` — one shared concurrency budget across roots and child
  fan-outs.
- `on_error` — `continue` (siblings finish; failed subtree skipped) or `stop`
  (abort pending and in-flight work on first failure).

## Discovery & env files

`run` / `validate` / `preview` auto-discover `faucet.yaml` → `.yml` → `.json` in
the current directory, and load a sibling `.env` unless `--no-env-file` is given
(`--env-file PATH` points elsewhere).

> The authoritative, exhaustive grammar — including every matrix and template
> edge case — is in
> [`cli/README.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/README.md).
