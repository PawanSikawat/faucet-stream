# Running faucet as a service (`faucet serve`)

`faucet serve` turns faucet from a one-shot CLI into a long-running HTTP control
plane: an orchestrator (Airflow, Temporal, Dagster, Argo) submits pipeline
configs over HTTP, polls status, cancels runs, and streams logs — while faucet
amortizes startup (TLS handshakes, connection pools, schema introspection)
across many runs in one process. It is the second supported runtime mode
alongside one-shot [`faucet run`](../reference/cli.md) and the cron
[`faucet schedule`](./scheduling.md).

The full endpoint/schema reference is in [HTTP API reference](../reference/http-api.md);
this page is the guided tour. `serve` requires the `serve` Cargo feature
(`cargo install faucet-cli --features serve`, or `--features full`).

## Quickstart

```bash
# Start the server (loopback by default). Auth is mandatory — see below.
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 127.0.0.1:8080
```

```bash
# Submit a run.
curl -XPOST http://127.0.0.1:8080/v1/runs \
  -H "Authorization: Bearer s3cret" -H 'content-type: application/json' \
  -d '{"config":"version: 1\npipeline:\n  source: {type: csv, config: {path: in.csv}}\n  sink: {type: jsonl, config: {path: out.jsonl}}\n","name":"adhoc"}'
# → {"run_id":"0192…","status":"queued","submitted_at":"…"}

# Poll it to completion.
curl -H "Authorization: Bearer s3cret" http://127.0.0.1:8080/v1/runs/0192…

# Tail its logs (SSE).
curl -N -H "Authorization: Bearer s3cret" http://127.0.0.1:8080/v1/runs/0192…/logs
```

## ⚠️ Security model — read before exposing

`serve` executes **arbitrary client-supplied pipeline configs with the server's
identity**. That is a real privilege surface:

- **Full interpolation:** submitted configs resolve `${env:…}`, `${file:…}`,
  `${secret:…}`, and `${vault:…}`/`${aws-sm:…}`/… **against the server's**
  environment, filesystem, and credentials — exactly like `faucet run`. An
  authenticated caller can read any secret the server can reach.
- **SSRF / egress:** a submitted REST/HTTP source can be pointed at
  `169.254.169.254` or internal services and will be fetched with the server's
  network identity.

Mitigations are deployment-level and **mandatory**:

- Never run with `--no-auth` on a non-loopback bind. The no-auth gate is
  explicit: without `--auth-token`/`FAUCET_SERVE_AUTH_TOKEN` **and** without
  `--no-auth`, startup fails.
- Run single-tenant, behind authentication, behind egress controls / network
  policy. The default loopback bind (`127.0.0.1`) is deliberate — exposing
  externally is an explicit choice.
- Terminate TLS at a proxy/ingress (serve speaks plain HTTP).
- Prefer `FAUCET_SERVE_AUTH_TOKEN` over `--auth-token` (the latter leaks through
  `ps`/`/proc`).
- Never run a serve process at `FAUCET_LOG=debug` when submitted configs hold
  resolved secrets — only faucet's own log output is redacted, not third-party
  connector debug logging.

## Bounded concurrency & backpressure

`--max-concurrent-runs` (default `min(16, cpu_count())`) bounds how many runs
execute at once; `--max-queued-runs` (default 8×) bounds the queue. A submit
past the queue cap returns `429` with `Retry-After`. Note that total concurrent
pipeline work ≈ `max-concurrent-runs × each config's execution.max_concurrent`.

## Idempotency

Supply `idempotency_key` to make retries safe (Stripe-style):

- First submit with a key → runs normally.
- Re-submit the **same key + same request** within `--idempotency-retention-secs`
  (default 24h) → returns the **original** `run_id` (replayed, no new run).
- Same key + a **different** request → `409 Conflict`.
- After the retention window, the key is re-usable for a fresh run.
- Deleting a run also frees its idempotency key immediately — a later submit
  with that key starts a fresh run rather than 404-ing on the deleted record.

The "request" identity covers the merged config **and** the run-affecting
request fields — `clock`, `timeout_secs`, and `labels`. In particular, a retry
that reuses the key but changes the backfill `clock` is a `409`, not a replay of
the original window (so you can't silently get the original clock's results).

The claim is atomic, so concurrent retries can't both start a run.

> **Degraded mode:** while the persistent history backend is degraded (see
> [Run history](#run-history--persistence)), the in-memory fallback can't see
> claims the database made before the outage. Rather than risk a duplicate run,
> submissions **carrying an idempotency key are rejected with `503`** until the
> backend recovers — retry then, or resubmit without a key if at-least-once is
> acceptable. Submissions without a key are unaffected.

## Cancellation

`POST /v1/runs/{id}/cancel` cooperatively cancels an in-flight run (202); on an
already-terminal run it's a 200 no-op. Cancellation drops the pipeline future at
its next `.await` — the same partial-sink-state trade-off as `on_error: stop`.

## `--default-config` (workspace defaults)

Pass `--default-config <file>` to merge shared settings **under** every submitted
run (submitted values win; objects merge, scalars/arrays replace). Pin `state:`,
`execution:`, and the `auth:` catalog once instead of repeating them per request.
See [`cli/examples/serve_minimal.yaml`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/examples/serve_minimal.yaml).

> **Cardinality:** a config's `name:` field drives the metric `pipeline` label and
> the state-key prefix. Use a **stable** `name:` per logical pipeline — never an
> ad-hoc per-run string — or Prometheus cardinality blows up. The request-level
> `name`/`labels` are run-record metadata only, never metric labels.

## Run history & persistence

By default run records live in memory and are lost on restart. For durable
history across restarts, point `--history` at a database (requires the matching
build feature):

```bash
# Postgres (feature: serve-history-postgres)
faucet serve --history 'postgres://user:pw@db/faucet'
# SQLite (feature: serve-history-sqlite)
faucet serve --history 'sqlite:/var/lib/faucet/runs.db'
```

Both create their schema on first connect. On startup, any run left
non-terminal by a previous process is marked `failed` (no cross-process resume —
re-submit to retry). If the backend is unreachable at startup, or fails at
runtime, serve **degrades to the in-memory store** so it stays up: it logs once,
sets the `faucet_serve_history_degraded` gauge, and `/readyz` returns `503`.
Persisted records are not migrated into the fallback — degraded mode is a
stay-alive, not a replica. Terminal records are retained for
`--retain-terminal-runs-secs` (default 7 days).

## Graceful shutdown

`SIGTERM`/`SIGINT` stops accepting new connections, drains in-flight runs up to
`--shutdown-grace-secs` (default 60), then cancels the remainder (marked failed).

## Health & observability

- `/healthz` — liveness (always 200 while serving).
- `/readyz` — 503 when history is degraded or the queue is full.
- `/metrics` — Prometheus, including `faucet_serve_*` series. `/metrics` is
  unauthenticated; restrict it at the network layer if its labels are sensitive.
