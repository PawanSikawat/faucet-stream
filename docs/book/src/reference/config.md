# Configuration file format

A faucet config is a YAML or JSON document with this top-level shape:

```yaml
version: 1                 # required, must be 1
name: my_pipeline          # optional; used in state keys and metrics
vars: {}                   # optional; reusable values referenced as ${vars.X}
auth: {}                   # optional; named shared auth providers (see below)
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

## Discovery & env files

`run` / `validate` / `preview` auto-discover `faucet.yaml` → `.yml` → `.json` in
the current directory, and load a sibling `.env` unless `--no-env-file` is given
(`--env-file PATH` points elsewhere).

> The authoritative, exhaustive grammar — including every matrix and template
> edge case — is in
> [`cli/README.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/README.md).
