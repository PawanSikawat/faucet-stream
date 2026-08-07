# Parameters & pipeline templates

Most fleets run the *same* pipeline shape over and over with a handful of values
that change: a tenant id, a date window, a target table, a source URL. Copying the
config once per tenant means N files to keep in sync; re-sending the whole config
on every trigger means re-validating it every time and hoping the caller got the
shape right.

faucet splits that into two pieces:

- **`params:`** — a config declares its typed, trigger-time surface. Available
  everywhere, no build feature needed.
- **the template registry** — register a parameterized config **once**, then
  trigger runs by `{id, params}`. Needs the `templates` build feature.

## Declaring parameters

```yaml
version: 1
name: tenant-sync

params:
  tenant_id:
    type: string
    required: true
    description: Tenant whose events to sync
  since:
    type: string
    default: "1970-01-01"
  page_size:
    type: int
    default: 500
  api_token:
    type: string
    required: true
    secret: true

pipeline:
  source:
    type: rest
    config:
      url: "https://api.example.com/tenants/${param.tenant_id}/events?since=${param.since}"
      auth: { type: bearer, config: { token: "${param.api_token}" } }
      pagination:
        type: page_number
        config: { page_param: page, size_param: per_page, size: "${param.page_size}" }
  sink:
    type: jsonl
    config:
      path: "./out/${param.tenant_id}/events.jsonl"
```

Fields per entry:

| Field | Meaning |
|---|---|
| `type` | `string` (default) · `int` · `float` · `bool` |
| `required` | The caller must supply a value. Mutually exclusive with `default`. |
| `default` | Value when the caller supplies none. An ordinary config scalar, so `default: "${env:SINCE}"` works. |
| `secret` | Registered for redaction the instant it is bound — never reaches a log, an error message, an API response, the audit log, or the registry. |
| `description` | Shown by `faucet template list` / `show`, `GET /v1/templates`, and the MCP `get_template` tool. |

Reference a param anywhere in the config as `${param.NAME}`. Write `$${param.x}`
for a literal `${param.x}`.

### Types are real

When `${param.NAME}` is a scalar's **entire** text, the declared type survives:
`page_size` above arrives at the connector as the JSON number `500`, not `"500"`.
Embedded in a longer string (`.../events?since=${param.since}`) it is stringified,
like every other interpolation namespace.

Values are accepted in either wire shape, so a CLI `--param page_size=500` and an
HTTP `{"page_size": 500}` behave identically — and `--param page_size=abc` is
rejected up front, naming the param.

## Running a parameterized config directly

`faucet run` and `faucet validate` take `--param`:

```bash
faucet run tenant-sync.yaml --param tenant_id=acme --param since=2026-01-01 \
  --param api_token="$TOKEN"

# Validate in CI without inventing values: required params bind to type-shaped
# placeholders, so the config's structure is still fully checked.
faucet validate tenant-sync.yaml

# Or check one concrete invocation end to end (strict binding).
faucet validate tenant-sync.yaml --param tenant_id=acme --param api_token=x
```

`--param-env NAME=VALUE` overrides an environment variable for that run's
`${env:VAR}` resolution only; bare `--param-env TOKEN` takes the value from your
own environment, so a secret never appears in the process arguments. The process
environment itself is never modified — which is what makes this safe inside a
concurrent server.

```bash
faucet run tenant-sync.yaml --param tenant_id=acme --param-env API_HOST=eu.example.com
```

## Registering a template

```bash
faucet template register tenant-sync.yaml --store sqlite:./faucet-templates.db
# registered template 'tenant-sync' version 1
#
# params:
#   api_token            string  required  [secret]
#   page_size            int     default 500
#   since                string  default "1970-01-01"
#   tenant_id            string  required  — Tenant whose events to sync

faucet template list  --store sqlite:./faucet-templates.db
faucet template show  tenant-sync --store sqlite:./faucet-templates.db
faucet template run   tenant-sync --store sqlite:./faucet-templates.db \
  --param tenant_id=acme --param api_token="$TOKEN"
faucet template delete tenant-sync --store sqlite:./faucet-templates.db --version 1
```

`--store` accepts `sqlite:<path>`, a `postgres://…` URL, or `memory`
(process-lifetime only, for a smoke test) — the same grammar as `catalog.url` and
`faucet serve --history`, and it can be set once via `FAUCET_TEMPLATE_STORE`. SQL
stores need the matching `serve-history-sqlite` / `serve-history-postgres` build
feature.

`faucet template run` materializes the template and then runs it through the
*identical* path as `faucet run` — observability, lineage, notifications, the
catalog, SLA evaluation and row selection all behave the same.

### Versions and named channels

Every `register` appends a **new numeric version**, auto-incrementing from 1 —
iterating a template never overwrites what is already running. On top of the
numbers sits a **closed set of named channels**: movable pointers you promote
between versions, exactly like container image tags.

| Channel | Meaning |
|---|---|
| `latest` | **Derived** — always the newest registration. Never assignable. |
| `dev` | Day-to-day development |
| `test` | QA / integration testing |
| `staging` | Staging |
| `pre-prod` | Pre-production / release-candidate soak |
| `canary` | Partial-traffic canary ahead of `prod` |
| `stable` | The version blessed as known-good |
| `prod` | Production |
| `previous` | The previously-blessed version, for a one-step rollback |

The set is **closed on purpose**: an open-ended tag namespace becomes a second,
unreviewable naming system in which a typo (`prd`) silently creates a channel
nobody watches. An unknown name is rejected with the valid list. Names are
forgiving about spelling — `pre-prod`, `pre_prod`, `PreProd`, and `preprod` are
one channel — and if you need a free-form label, put it in the run's `labels`,
not in the registry.

A version is selected in one of three ways:

| Selector | Resolves to |
|---|---|
| *(omitted)* | the newest version |
| a channel name (`latest`, `prod`, `dev`, …) | whatever that channel points at |
| a number (`2`) | exactly that version |

```bash
faucet template run tenant-sync --param tenant_id=acme            # latest
faucet template run tenant-sync --version latest --param …        # same thing
faucet template run tenant-sync --version prod   --param …        # whatever prod names
faucet template run tenant-sync --version 2      --param …        # pinned
```

`latest` is derived, so it can never be assigned or moved; the others start unset
and stay wherever you last promoted them. Asking for an unset channel is an error
naming the channels that *are* set — silently falling back to `latest` would run
the wrong code.

#### Promoting

Tag on register, or promote afterwards:

```bash
# Register v4 and point `dev` at it in one step.
faucet template register tenant-sync.yaml --tag dev

# Promote by copying another channel's current target — the usual pipeline.
faucet template promote tenant-sync --tag test     --version dev
faucet template promote tenant-sync --tag pre-prod --version test
faucet template promote tenant-sync --tag prod     --version pre-prod

# Or point at an exact version (a rollback).
faucet template promote tenant-sync --tag prod --version 3
```

```bash
curl -sX POST localhost:8080/v1/templates/tenant-sync/tags \
  -H "Authorization: Bearer $TOKEN" -d '{"tag":"prod","version":"stable"}'
# → 200 {"id":"tenant-sync","tag":"prod","version":7}
```

Promoting *from* `latest` resolves to the concrete newest version at that moment,
so the pointer does not silently follow future registrations.

#### Inspecting

`GET /v1/templates/{id}` (and `faucet template show`) reports the whole picture,
so a client can pin, promote, or roll back without a second call:

```jsonc
{
  "id": "tenant-sync", "version": 2,      // the version returned
  "versions": [3, 2, 1],                  // everything stored, newest first
  "latest_version": 3,                    // what `latest` resolves to now
  "is_latest": false,                     // ⇒ this response is a pinned older one
  "tags": { "dev": 3, "prod": 1 },        // channel pointers (never `latest`)
  "body": "version: 1\nname: tenant-sync\n…"
}
```

`faucet template list` shows one row per id — always its **latest** version —
and `faucet template show` marks the returned version `[latest]` when it is and
prints the channel map.

#### Wire shapes and cleanup

`version` accepts a channel name (`"prod"`), a numeric string (`"2"`), or a bare
number (`2`), so a query string, a JSON body, and an MCP tool argument all mean
the same thing. `0` and unknown names are rejected rather than silently falling
back.

Deleting: `--version <N|channel>` removes one version;
`faucet template delete <id>` with no `--version` removes the template entirely.
A channel pointing at a deleted version is dropped with it, so a pointer never
dangles. Runs already produced are untouched.

The 20 most recent versions of each id are kept, so a template re-registered on
every deploy keeps a useful rollback window without growing without bound.

**Practical pattern.** Let deploys `register --tag dev` freely; walk a version up
the channels (`dev` → `test` → `pre-prod` → `prod`) as it earns trust; point
scheduled jobs at a channel (`--version prod`) rather than `latest`, so a deploy
never silently changes what production runs.

## Triggering over HTTP

Point `faucet serve --history` at the same store and the same templates become
triggerable over HTTP and MCP — one registry, not three:

```bash
faucet serve --history sqlite:./faucet-templates.db --auth-token "$TOKEN"
```

```bash
# Register (operator+ / TemplateWrite)
curl -sX POST localhost:8080/v1/templates \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"id":"tenant-sync","config":"'"$(sed 's/"/\\"/g;:a;N;$!ba;s/\n/\\n/g' tenant-sync.yaml)"'"}'

# Browse (viewer+ / TemplateRead)
curl -s localhost:8080/v1/templates            -H "Authorization: Bearer $TOKEN"
curl -s localhost:8080/v1/templates/tenant-sync -H "Authorization: Bearer $TOKEN"

# Trigger (operator+ / RunWrite)
curl -sX POST localhost:8080/v1/templates/tenant-sync/runs \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"params":{"tenant_id":"acme","api_token":"'"$TOKEN"'"},"env":{"API_HOST":"eu.example.com"}}'
# → 202 {"run_id":"…","status":"queued","template_id":"tenant-sync",
#        "template_version":1,"params":{"tenant_id":"acme","api_token":"***",…}}
```

A trigger is submitted through the same path as `POST /v1/runs`, so idempotency
keys, `doctor_first`, queue limits, cluster dispatch, metrics, and the audit log
all behave identically. The run is labelled `template` and `template_version`, so
`GET /v1/runs?…` and your dashboards can group by provenance.

See the [HTTP API reference](../reference/http-api.md) for the full endpoint list.

## Agent tools (MCP)

With `--mcp`, an agent gets `list_templates` / `get_template` read-only, plus
`register_template` / `run_template` behind `--mcp-allow-mutations` and the
caller's `RunWrite` scope:

```bash
faucet serve --history sqlite:./faucet-templates.db --mcp --mcp-allow-mutations
# or, over stdio:
faucet mcp --template-store sqlite:./faucet-templates.db --allow-mutations
```

Without a store the template tools are not advertised at all, so an agent never
sees a tool it cannot use.

## What is and isn't stored

The config body is stored **verbatim**. `${env:…}` / `${vault:…}` / `${secret:…}`
stay unresolved tokens and are resolved *at trigger time*, on the instance that
runs the pipeline — the same privilege surface as any normally-submitted config.
That is the recommended way to get a credential into a template: reference it from
the body, don't pass it as a param.

A caller-supplied `secret: true` param value is never persisted. It lives only for
the duration of one trigger: bound into the materialized config, registered for
redaction, and echoed back as `"***"`.

One consequence is worth stating plainly: a **clustered** server persists the
materialized config so a peer can execute the run, which would put a secret param
value in the shared history database. A clustered trigger of a template declaring
`secret: true` params is therefore refused with a `422` explaining the two safe
alternatives (reference the secret from the body, or trigger on a non-clustered
server). Non-clustered servers store no config body and are unaffected.

## Safety properties

- **Structure safety.** Params are substituted per JSON/YAML scalar, before the
  typed parse — a value containing `:`, a newline, or `-` stays the single scalar
  it replaced and can never inject a key or an array element. SQL-bound and
  JSON-safe substitution paths downstream are untouched, so the existing
  SQL/JSON-injection guarantees hold for param-derived text too.
- **No re-interpolation of caller input.** Env/file/secret directives resolve
  *before* params bind, so a supplied value is never itself scanned for
  directives. A supplied value containing `${` is rejected outright: params are
  data, not directives.
- **Typos fail loudly.** An undeclared `--param`, an undeclared `${param.x}`
  reference, a missing `required` param, or a type mismatch is an error naming the
  param — never a silent no-op.
- **Nothing leaks by accident.** `${param.*}` binding happens pre-parse on every
  load path, and a token that somehow survived to matrix expansion is rejected
  there as a backstop rather than reaching a connector as literal text.

## Build features

| Feature | Enables |
|---|---|
| *(none)* | The `params:` block, `${param.*}`, `--param` / `--param-env`, `faucet schema params` |
| `templates` | `faucet template …`, `/v1/templates*`, the MCP template tools (implies `serve`) |
| `serve-history-sqlite` / `serve-history-postgres` | A registry that survives a restart |

`templates` is in `--features full`, not in `default`.

## See also

- Runnable example: [`cli/examples/rest_to_jsonl_templated.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_jsonl_templated.yaml)
- [`params:` reference](../reference/config.md#params) · [CLI reference](../reference/cli.md) · [HTTP API](../reference/http-api.md)
- [Config composition](./composition.md) — `extends` / `profiles` / `!include`, for
  variation that is *static* rather than per-run
- [Event-driven triggers](./triggers.md) — firing runs on object arrival, a
  webhook, or queue depth
