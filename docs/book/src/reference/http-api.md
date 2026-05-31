# HTTP API reference (`faucet serve`)

`faucet serve` exposes a JSON REST control plane for submitting, polling,
listing, cancelling, and streaming the logs of pipeline runs, plus
unauthenticated health and Prometheus endpoints. A machine-readable
[`docs/openapi.yaml`](https://github.com/PawanSikawat/faucet-stream/blob/main/docs/openapi.yaml)
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

## Endpoints

| Method | Path | Success | Notes |
|--------|------|---------|-------|
| `POST` | `/v1/runs` | `202` | Submit a run; config validated synchronously |
| `GET` | `/v1/runs` | `200` | List runs (filters below) |
| `GET` | `/v1/runs/{id}` | `200` | Get one run record |
| `DELETE` | `/v1/runs/{id}` | `204` | Remove a terminal run from history |
| `POST` | `/v1/runs/{id}/cancel` | `202` / `200` | Request cancel (202) or no-op if terminal (200) |
| `GET` | `/v1/runs/{id}/logs` | `200` | Stream the run's logs as `text/event-stream` |
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

## Error envelope

Every error is a JSON `ApiError`:

```json
{ "error": { "code": "unprocessable", "message": "…", "details": { } } }
```

| Status | When |
|--------|------|
| `400` | Malformed body / parse / interpolation failure; a `schedule:` block in the config |
| `401` | Missing/invalid bearer token |
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
