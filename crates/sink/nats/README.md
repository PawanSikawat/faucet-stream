# faucet-sink-nats

A [NATS](https://nats.io) sink for [`faucet-stream`](https://crates.io/crates/faucet-stream).

Publishes each record as a JSON message to a subject — fixed, or per-record via
a configurable `subject_field`. The client is flushed after each batch, so
nothing is left buffered when a write returns.

Append-only: it does not override idempotency, upsert, or schema-evolution (the
trait defaults hold).

## Configuration

The shared connection surface (`servers` / `auth` / `tls` / `name` — see
[`faucet-common-nats`](https://crates.io/crates/faucet-common-nats)) is flattened
in alongside these fields:

| field           | type             | default | description                                                        |
|-----------------|------------------|---------|--------------------------------------------------------------------|
| `subject`       | `String`         | —       | default subject every record is published to. Required.            |
| `subject_field` | `Option<String>` | —       | top-level record field whose string value overrides `subject` per record. |
| `batch_size`    | `usize`          | `1000`  | records per publish batch (client flushed after each batch).       |

## Example

```yaml
version: 1
pipeline:
  source:
    kind: jsonl
    config:
      path: events.jsonl
  sink:
    kind: nats
    config:
      servers: ["nats://127.0.0.1:4222"]
      subject: "events.ingested"
      batch_size: 1000
```

### Subject per record

```yaml
  sink:
    kind: nats
    config:
      servers: ["nats://127.0.0.1:4222"]
      subject: "events.default"    # fallback / default
      subject_field: "topic"        # each record's `topic` string is the subject
```

## License

Licensed under either of Apache-2.0 or MIT at your option.
