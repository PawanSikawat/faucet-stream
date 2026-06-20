# Observability

Every source, sink, transform, and state-store operation is automatically wrapped
to emit `tracing` spans and `metrics` counters/histograms. Connector authors
write no observability code — they only override `connector_name()` for a
friendly label.

## Enabling the Prometheus endpoint

The CLI's `observability` feature (on by default in the full build) installs a
Prometheus exporter. Configure it from the pipeline config or environment; once
running, scrape the listen address with Prometheus.

## Common labels

`pipeline`, `row` (matrix row id; empty for non-matrix runs), and `connector`
(from `connector_name()`). `run_id` is a span attribute only — it's high
cardinality and never a Prometheus label.

## Key metrics

- **Source:** `faucet_source_records_total`, `faucet_source_errors_total{kind}`,
  `faucet_source_page_duration_seconds`, `faucet_source_in_flight`.
- **Sink:** `faucet_sink_records_total`, `faucet_sink_writes_total`,
  `faucet_sink_errors_total`, `faucet_sink_write_duration_seconds`,
  `faucet_sink_flush_duration_seconds`, `faucet_sink_in_flight`.
- **Transform:** `faucet_transform_records_in_total`,
  `faucet_transform_records_out_total` (use the `out/in` ratio for
  filter drop rate or explode fan-out), `faucet_transform_errors_total{kind}`,
  `faucet_transform_duration_seconds`.
- **State:** `faucet_state_{get,put,delete}_total` (get carries
  `outcome=hit|miss`), `faucet_state_errors_total{op,kind}`, plus duration
  histograms.
- **Pipeline:** `faucet_pipeline_runs_total{status=ok|err,kind}`,
  `faucet_pipeline_run_duration_seconds`, `faucet_pipeline_in_flight`,
  `faucet_pipeline_seconds_since_last_bookmark`,
  `faucet_pipeline_last_bookmark_unix_seconds`.
- **Build:** `faucet_build_info{version}` is set to `1` — `group_left` it onto
  other metrics to annotate dashboards with the running version.

## Reliability properties

- **Drop-guard timers** sample durations even when a task is cancelled.
- **Panic isolation** — a panicking connector surfaces as a `Panic` error kind
  rather than crashing the process.
- **Idempotent install** — installing the recorder/subscriber twice warns rather
  than panics.

## Cardinality rules

Never use high-cardinality values (record ids, URLs, query strings) as metric
labels. `parent_record_key` in a DAG is a span attribute only. Connector authors
must return a non-empty `&'static str` from `connector_name()`.

## Tracing

Spans carry `run_id`, `pipeline`, `row`, and per-operation timing. Point a
`tracing` subscriber at your logging/trace backend; control verbosity with
`--log-level` or `FAUCET_LOG`.

> Full design: `docs/superpowers/specs/2026-05-23-observability-otel-prometheus-design.md`.

## OTLP / OpenTelemetry export

The `otel` feature pushes traces **and** metrics to any OTLP-compatible
collector (Jaeger, Grafana Tempo, Honeycomb, Datadog, the OpenTelemetry
Collector, etc.) alongside — not instead of — the Prometheus endpoint. Build
the CLI with `cargo install faucet-cli --features otel`; the feature is
included in the `full` aggregate. Enable it in your pipeline config with an
`otel:` sub-block under the existing `observability:` key:

```yaml
observability:
  prometheus:
    listen: "0.0.0.0:9090"
  otel:
    endpoint: "https://api.honeycomb.io"
    protocol: grpc                        # grpc (default) | http
    headers:
      x-honeycomb-team: "${env:HONEYCOMB_KEY}"
    sample_ratio: 0.1                     # head-based; 1.0 = keep all traces
    export: [traces, metrics]             # which signals to push
    service_name: faucet                  # OTel resource service.name
    timeout_secs: 10
    metric_interval_secs: 60
```

The `observability.prometheus:` and `observability.otel:` blocks coexist
independently — both can be active in the same run and metrics are fanned out
to both exporters.

**Protocol notes:**

- `grpc` uses `tonic` (the default). The `faucet` CLI always runs inside a
  tokio runtime, so gRPC works without any extra setup.
- `http` uses HTTP/Protobuf. When `endpoint` does not already end in a
  per-signal path (`/v1/traces`, `/v1/metrics`), faucet appends it
  automatically — point `endpoint` at the base URL of the collector (e.g.
  `http://localhost:4318`) and the right path is added per signal.

**Reliability:** export is best-effort. An unreachable or slow collector
**never** fails or delays a pipeline run. Export failures increment
`faucet_otel_export_failures_total{signal}` so you can alert on a broken
pipeline to your observability backend.

See `examples/infra/otel-collector.yaml` for a minimal local collector config
you can run with `otelcol --config examples/infra/otel-collector.yaml`.

### OTLP metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `faucet_otel_export_failures_total` | `signal` (`traces`/`metrics`/`export`) | OTLP export attempts that failed. Failures are non-fatal; the pipeline continues. |
