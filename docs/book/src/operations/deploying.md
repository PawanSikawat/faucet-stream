# Deploying faucet

faucet runs pipelines **to completion** — it's not a long-running daemon. That
makes deployment simple: schedule the binary, point it at a config, and let it
exit. Durable state (bookmarks) lets the next run pick up where the last left off.

## Patterns

### Cron / scheduled jobs

The most common deployment. Run on an interval; incremental replication + a
durable state store mean each run only fetches what's new.

```bash
# crontab: every 15 minutes
*/15 * * * * faucet run /etc/faucet/events.yaml >> /var/log/faucet.log 2>&1
```

### Containers

Build a slim image with only the connectors you need, and supply config via a
mounted file or entirely from the environment:

```dockerfile
FROM rust:slim AS build
RUN cargo install faucet-cli --no-default-features \
    --features "source-rest,sink-bigquery,state-postgres,observability"

FROM debian:stable-slim
COPY --from=build /usr/local/cargo/bin/faucet /usr/local/bin/faucet
ENTRYPOINT ["faucet", "run"]
```

With `faucet run --from-env` you can drive the whole pipeline from `FAUCET_*`
environment variables — no config file in the image. See the
[CLI reference](../reference/cli.md#environment-only-mode).

### Kubernetes CronJob

Wrap the container above in a `CronJob`. Use the `postgres` or `redis` state
backend so bookmarks survive pod restarts, and scrape the metrics endpoint (see
[Observability](./observability.md)).

## Secrets

Never commit secrets. Use `${env:VAR}` / `${file:PATH}` in the config and inject
real values through your platform's secret mechanism (Kubernetes secrets, Docker
secrets, a mounted `.env`, etc.).

## Exit codes & retries

`faucet run` exits non-zero when a pipeline fails (subject to the
`execution.on_error` policy and any DLQ). Let your scheduler's retry/alert
mechanism react to a non-zero exit; because bookmarks only advance after the sink
confirms, a retried run resumes safely.
