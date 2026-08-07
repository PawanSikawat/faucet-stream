# HTTP API reference (`faucet serve`)

`faucet serve` exposes a JSON REST control plane for submitting, polling,
listing, cancelling, and streaming the logs of pipeline runs, plus
unauthenticated health and Prometheus endpoints. A machine-readable
[`docs/openapi.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/docs/openapi.yaml)
spec ships alongside this page and is kept in sync with the router by a CI test.

See the [serve cookbook](../cookbook/serve.md) for a guided quickstart, the
security model, and operational guidance. This page is the endpoint reference.

## Authentication

All `/v1/*` endpoints require `Authorization: Bearer <token>` unless the server
was started with `--no-auth`. The token is compared in constant time; the
`Authorization` header is the only accepted credential (no query-string auth).
`/healthz`, `/readyz`, and `/metrics` are always unauthenticated (probes /
scrapers). `OPTIONS` preflight bypasses auth so browsers behind a CORS policy
work.

### RBAC & the audit log (`--auth-config`)

A single `--auth-token` is one implicit **admin** principal. For a team
deployment, `--auth-config <file>` promotes the server to **role-based access
control**: a YAML/JSON file of principals, each a `{ name, token, role }`. Three
built-in roles form a ladder:

| Role | Permitted |
|------|-----------|
| `viewer` | read-only: `GET /v1/runs*`, `GET /v1/schemas*`, `GET /v1/catalog/*`, `GET /v1/templates*` |
| `operator` | everything a viewer can do **plus** submit / cancel / delete runs, `POST /v1/doctor`, firing triggers, and registering / deleting / triggering pipeline templates |
| `admin` | everything, including `GET /v1/audit` |

```yaml
# auth.yaml
principals:
  - { name: alice, token: "${env:ALICE_TOKEN}", role: admin }
  - { name: ci,    token: "${env:CI_TOKEN}",    role: operator }
  - { name: dash,  token: "${env:DASH_TOKEN}",  role: viewer }
```

```bash
faucet serve --auth-config auth.yaml
```

A request whose role lacks the route's required permission gets `403 forbidden`
(and a `denied` audit record). `--auth-config` is mutually exclusive with
`--auth-token` / `--no-auth`. Every token is registered for log redaction at
startup.

**Audit log.** Every mutating action (`run.submit` / `run.cancel` / `run.delete` /
`template.register` / `template.delete` / `template.run` / `template.promote`)
and every denied attempt is recorded with principal, role, action, run id,
config fingerprint (submit), source IP, timestamp, and result. Admins read it via
`GET /v1/audit`. Records persist in the run-history backend (`faucet_serve_audit`
for the SQL backends; an in-memory ring otherwise) and expire with the
`--retain-terminal-runs-secs` window.

## Endpoints

| Method | Path | Success | Notes |
|--------|------|---------|-------|
| `POST` | `/v1/runs` | `202` | Submit a run; config validated synchronously |
| `GET` | `/v1/runs` | `200` | List runs (filters below) |
| `GET` | `/v1/runs/{id}` | `200` | Get one run record |
| `DELETE` | `/v1/runs/{id}` | `204` | Remove a terminal run from history |
| `POST` | `/v1/runs/{id}/cancel` | `202` / `200` | Request cancel (202) or no-op if terminal (200) |
| `GET` | `/v1/runs/{id}/logs` | `200` | Stream the run's logs as `text/event-stream` |
| `POST` | `/v1/backfill` | `202` | Submit a windowed backfill: one tracked run per window unit (operator) |
| `GET` | `/v1/audit` | `200` | Read the audit log — **admin only** (RBAC). Filters: `principal`, `action`, `since`, `until`, `limit` |
| `POST` | `/v1/reload` | `200` / `422` | Hot-reload the `--default-config` merge base — **admin only** (RBAC). No-op (`reloaded:false`) if no default-config; `422` (old config kept) if the new one is invalid |
| `GET` | `/v1/catalog/datasets` | `200` | List catalogued datasets (`kind`, `q`, `limit`, `cursor`) — requires the `catalog` build feature |
| `GET` | `/v1/catalog/datasets/{id}` | `200` | One dataset's detail: schema timeline, volume, edges |
| `GET` | `/v1/catalog/lineage` | `200` | The lineage edge graph (`root`, `depth`) |
| `POST` | `/v1/templates` | `201` | Register a pipeline template (operator / `TemplateWrite`) — requires the `templates` build feature |
| `GET` | `/v1/templates` | `200` | List templates, latest version each (viewer / `TemplateRead`) |
| `GET` | `/v1/templates/{id}` | `200` | One template + its version list. `?version=latest` (default) or `?version=N` |
| `DELETE` | `/v1/templates/{id}` | `204` | Delete one version (`?version=latest\|N`) or all (operator / `TemplateWrite`) |
| `POST` | `/v1/templates/{id}/runs` | `202` | Trigger a run from a template with `params` / `env` (operator / `RunWrite`) |
| `POST` | `/v1/templates/{id}/tags` | `200` | Point a named channel (`prod`, `dev`, …) at a version (operator / `TemplateWrite`) |
| `GET` | `/healthz` | `200` | Liveness (unauthenticated) |
| `GET` | `/readyz` | `200`/`503` | Readiness (unauthenticated) |
| `GET` | `/metrics` | `200` | Prometheus exposition (unauthenticated) |

### `POST /v1/runs`

Request body:

```json
{
  "config": "version: 1\npipeline:\n  source: {...}\n  sink: {...}\n",
  "config_format": "yaml",
  "name": "nightly-rollup",
  "labels": {"requester": "airflow"},
  "timeout_secs": 3600,
  "doctor_first": true,
  "idempotency_key": "airflow-task-123-attempt-2",
  "clock": "2026-05-29T00:00:00Z"
}
```

- **`config`** (required) — the YAML or JSON pipeline body.
- **`config_format`** — `yaml` (default) or `json`.
- **`name`** — metadata; also drives the **state-key and metric identity** (see
  the cookbook's cardinality note). Two submissions sharing a `name` share
  replication bookmarks.
- **`labels`** — arbitrary string metadata, stored on the run record only.
- **`timeout_secs`** — wall-clock cap; on expiry the run is marked failed.
- **`doctor_first`** — run preflight probes before executing; on any failure the
  submit returns `422` with the doctor report in `error.details`.
- **`idempotency_key`** — replay protection (see cookbook).
- **`clock`** — overrides the `${now.*}` clock for backfills (default: submit time).

Response (`202`):

```json
{ "run_id": "0192…", "status": "queued", "submitted_at": "2026-05-29T12:00:00Z" }
```

A `--default-config` (if the server was started with one) is merged **under** the
submitted config (submitted values win).

### `GET /v1/runs`

Query parameters: `status`, `name`, `since`, `until` (RFC3339), `limit` (default
50, max 500), `cursor`. Ordering is `(submitted_at DESC, run_id DESC)`; `cursor`
is the last `run_id` from the previous page.

```json
{ "runs": [ { "run_id": "…", "status": "completed", … } ], "next_cursor": "0192…" }
```

### `GET /v1/runs/{id}` → `RunRecord`

```json
{
  "run_id": "0192…",
  "name": "nightly-rollup",
  "labels": {"requester": "airflow"},
  "status": "completed",
  "submitted_at": "…", "started_at": "…", "finished_at": "…",
  "elapsed_secs": 12.4,
  "records_written": 4096,
  "invocations": [
    {"row_id": "default", "parent_record_key": null, "records_written": 4096, "error": null}
  ],
  "error": null,
  "idempotency_key": "airflow-task-123-attempt-2",
  "doctor_report": null
}
```

`status` is one of `queued`, `running`, `completed`, `failed`, `cancelled`.
`elapsed_secs` is filled live for running runs.

> **Bookmarks:** run records carry record counts + per-row outcomes, not
> replication bookmarks. Bookmark state is per-row/per-state-key and lives in the
> configured [state backend](../cookbook/state.md), not in the run record.

### `GET /v1/runs/{id}/logs` (SSE)

`text/event-stream`. The server replays the run's bounded ring buffer, then
streams the live tail. Event types:

- `event: log` — one captured log line (subject to the server's `FAUCET_LOG`
  level; secrets are redacted).
- `event: truncated` — the reader fell behind and lines were dropped; rely on
  the centralized log sink for the full history.
- `event: end` — the run reached a terminal state; the stream closes.

Log buffers are **ephemeral**: they survive a short drain window after the run
finishes (independent of run-record retention), then are dropped. A known run
whose buffer has expired yields a single `end`.

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8080/v1/runs/0192…/logs
```

### `GET /v1/catalog/*` (Data Movement Catalog)

Read-only browsing of the [Data Movement Catalog](../cookbook/catalog.md)
accumulated in the server's `--history` backend (every serve run records into
it automatically). Viewer-readable under RBAC; requires a build with the
`catalog` feature.

- `GET /v1/catalog/datasets?kind=&q=&limit=&cursor=` — paginated dataset list,
  ordered `(last_seen DESC, id DESC)`; `q` is a case-insensitive URI substring.
- `GET /v1/catalog/datasets/{id}` — the dataset plus its deduplicated schema
  timeline (each version with a `diff` vs the previous), recent per-run volume
  points, and upstream/downstream lineage edges. `404` for an unknown id.
- `GET /v1/catalog/lineage?root=&depth=` — the source→sink edge graph; with
  `root` (a dataset id), a BFS slice bounded by `depth` hops.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/catalog/datasets?kind=postgres&limit=20"
```

### `/v1/templates*` (pipeline template registry)

Register a config declaring [`params:`](config.md#params) once, then trigger runs
by `{id, params}` instead of re-sending the whole config. Storage rides the
server's `--history` backend, so `faucet template …` and the MCP template tools
see the same registry. Requires a build with the `templates` feature; see the
[cookbook page](../cookbook/templates.md).

```bash
# Register (the body is stored verbatim — ${env:…} / ${vault:…} stay unresolved).
curl -sX POST http://127.0.0.1:8080/v1/templates \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"id":"tenant-sync","config":"version: 1\nname: tenant-sync\n…","config_format":"yaml"}'
# → 201 {"id":"tenant-sync","version":1,"params":{…},"created_at":"…","created_by":"…"}

# Trigger.
curl -sX POST http://127.0.0.1:8080/v1/templates/tenant-sync/runs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"params":{"tenant_id":"acme"},"env":{"API_HOST":"eu.example.com"},"version":2}'
# → 202 {"run_id":"…","status":"queued","submitted_at":"…",
#        "template_id":"tenant-sync","template_version":2,
#        "params":{"tenant_id":"acme","api_token":"***"}}
```

**Version selection.** Versions are numeric and auto-incrementing; on top of them
sits a closed set of named channels — `latest` (derived: the newest registration,
and what an omitted selector resolves to) plus the movable `dev`, `test`,
`staging`, `pre-prod`, `canary`, `stable`, `prod`, `previous`. `version` accepts a
channel name (`"prod"`), a numeric string (`"2"`), or a bare number (`2`), so a
query string and a JSON body agree. `0` and unknown channel names are rejected
rather than silently falling back, and asking for an *unset* channel is a `422`
naming the channels that are set.

`POST /v1/templates/{id}/tags` moves a channel: `{"tag":"prod","version":"stable"}`
copies whatever `stable` names today; `{"tag":"prod","version":3}` pins one.
`latest` cannot be assigned (`422`). `GET /v1/templates/{id}` returns `versions`
(newest first), `latest_version`, `is_latest`, and the `tags` pointer map, so a
client can pin, promote, or roll back without a second request.

The trigger body's `params` / `env` / `version` are template-specific; every other
field (`name`, `labels`, `timeout_secs`, `doctor_first`, `idempotency_key`,
`clock`) behaves exactly as in `POST /v1/runs`, because the run is submitted
through the same path. The run is labelled `template` and `template_version`.

Status codes: `404` for an unknown id or pinned version; `422` for a missing
`required` param or a type mismatch, naming the param; `429` when the queue is
full. On a **clustered** server a template declaring `secret: true` params is
refused with `422` — the materialized config is persisted for peer execution, and
the shared history database is not a secret store. Reference the secret from the
template body (`${env:…}` / `${vault:…}`, resolved on the executing instance)
instead.

### `POST /v1/backfill`

Plans a `[from, to)` range into window units (chunked by `window`) and submits
**one tracked run per unit** — see the [backfill
cookbook](../cookbook/backfill.md) for the model.

```json
{
  "config": "version: 1\nname: orders\npipeline: {...}\n",
  "config_format": "yaml",
  "from": "2026-06-01",
  "to": "2026-07-01",
  "window": "1d",
  "timezone": "UTC",
  "name": "orders",
  "labels": {"requester": "airflow"},
  "timeout_secs": 3600
}
```

- **`config`** (required) — every root source must reference a `${backfill.*}`
  or `${now.*}` scoping token (400 otherwise). Bookmark-range backfills are
  CLI-only.
- **`from`** / **`to`** (required) — RFC3339 or `YYYY-MM-DD` (midnight in
  `timezone`), half-open.
- **`window`** / **`timezone`** — default to the config's `backfill:` block.
- **`name`** — base run name; unit runs are `{name}-backfill-{unit}` (the
  pipeline `name` is rewritten per unit so state keys never touch the live
  bookmark). `delivery` is forced to `at_least_once`; `timeout_secs` applies
  per unit.

`202` response: `{backfill, descriptor, planned, submitted, units: [{unit,
start, end, status, run_id?, error?}]}` where `backfill` is the stable range
hash carried as the `backfill` label on every unit run (plus a `backfill_unit`
label). Each unit is submitted with the deterministic idempotency key
`backfill:{hash}:{unit}`, so **re-POSTing the same body is replay-safe** —
already-submitted units replay their existing run, the rest submit (a full
queue marks the remainder `not_submitted`; re-POST to continue). A config
carrying `shard: {count}` makes each unit a sharded run tracked via shard
progress. Requires `RunWrite` (operator); audited as `backfill.submit`.

## Error envelope

Every error is a JSON `ApiError`:

```json
{ "error": { "code": "unprocessable", "message": "…", "details": { } } }
```

| Status | When |
|--------|------|
| `400` | Malformed body / parse / interpolation failure; a `schedule:` block in the config |
| `401` | Missing/invalid bearer token |
| `403` | Authenticated, but the principal's role lacks the required permission (RBAC) |
| `404` | Unknown `run_id` |
| `409` | `DELETE` on a running run; idempotency key reused with a different payload |
| `413` | Body exceeds `--body-limit-bytes` |
| `422` | Expand/validation failure; `doctor_first` failed (report in `details`) |
| `429` | Run queue full (carries `Retry-After`) |
| `500` | Internal error |

## Metrics

`/metrics` serves the standard `faucet_*` pipeline metrics plus serve-specific
series: `faucet_serve_requests_total{method,path,status}`,
`faucet_serve_request_duration_seconds{method,path}`, `faucet_serve_runs_queued`,
`faucet_serve_runs_in_flight`, `faucet_serve_runs_total{status,reason}`,
`faucet_serve_idempotency_hits_total`, and `faucet_serve_history_degraded`. See
[Observability](../operations/observability.md).
