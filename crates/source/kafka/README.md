# faucet-source-kafka

Apache Kafka consumer source for `faucet-stream`. Subscribes to one or more topics, drains messages until a `max_messages` count or `idle_timeout` is reached, and emits each Kafka message as a structured JSON record. Built on [`rdkafka`](https://crates.io/crates/rdkafka) (librdkafka bindings). Supports durable offset tracking via any `faucet-core` `StateStore` so pipelines can resume exactly where they left off.

---

## Quick start

```yaml
# pipeline.yaml
version: 1

source:
  kind: kafka
  config:
    brokers: "broker1:9092,broker2:9092"
    topics:
      - orders
    group_id: faucet-orders-consumer
    value_format:
      type: json
    auto_offset_reset: earliest
    idle_timeout: 30      # stop after 30 s of no new messages
    max_messages: 10000   # or stop after 10 000 messages, whichever comes first

sink:
  kind: jsonl
  config:
    path: orders.jsonl
```

Run it:

```bash
faucet run pipeline.yaml
```

To resume from where the last run stopped, add a state store:

```yaml
state:
  kind: file
  config:
    directory: .faucet-state
```

---

## Full config reference

All fields are top-level keys under `source.config` in the pipeline YAML.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | `string` | **required** | Comma-separated list of bootstrap broker addresses, e.g. `"broker1:9092,broker2:9092"`. |
| `topics` | `string[]` | **required** | One or more topic names to subscribe to. |
| `group_id` | `string` | **required** | Kafka consumer group ID. Used for partition assignment and is part of the state-store key. |
| `auth` | `KafkaAuth` | `{type: none}` | Authentication mode. See [Auth](#auth) below. Full details in the `faucet-kafka-common` README. |
| `value_format` | `KafkaValueFormat` | `{type: json}` | How message value bytes are decoded. See [Value formats](#value-formats). |
| `key_format` | `KafkaValueFormat \| null` | `null` | How message key bytes are decoded. When absent or `null`, key bytes are decoded as UTF-8 (or `null` if no key was set on the message). |
| `auto_offset_reset` | `"earliest" \| "latest"` | `"latest"` | Where to start consuming when no committed offset exists for the group. |
| `max_messages` | `integer \| null` | `null` | Stop after this many messages have been consumed. At least one of `max_messages` and `idle_timeout` must be set. |
| `idle_timeout` | `integer \| null` | `null` | Stop after this many **seconds** with no new messages. At least one of `max_messages` and `idle_timeout` must be set. |
| `poll_timeout` | `integer` | `1` | Maximum seconds to wait on a single `consumer.recv()` call before checking termination conditions. Rarely needs tuning. |
| `session_timeout` | `integer` | `30` | Kafka `session.timeout.ms` in **seconds**. Increase for slow brokers or long GC pauses. |
| `on_decode_error` | `"fail" \| "skip"` | `"fail"` | What to do when a single message fails to decode. `fail` aborts the batch; `skip` drops the message and logs a warning. |
| `extra_client_config` | `object` | `{}` | Raw librdkafka client properties passed directly to the consumer. These can override anything set by `auth` or the typed fields above — use with care. |

---

## Record shape

Each Kafka message becomes one JSON object in the output stream:

```json
{
  "key": "order-42",
  "value": { "id": 42, "status": "shipped", "amount": 99.95 },
  "topic": "orders",
  "partition": 2,
  "offset": 10483,
  "timestamp": 1747483200000,
  "headers": {
    "content-type": "application/json",
    "trace-id": "abc123"
  }
}
```

**Field notes:**

- `key` — the message key decoded as UTF-8, or decoded according to `key_format` if set. `null` when the Kafka message carried no key.
- `value` — the decoded message payload. Shape depends on `value_format` (see below).
- `topic` — the topic name the message was consumed from.
- `partition` — the partition number (integer).
- `offset` — the offset of this message within its partition.
- `timestamp` — milliseconds since the Unix epoch, from the Kafka message timestamp. `0` when no timestamp is present in the message.
- `headers` — a flat object of string key → string value. Values that are not valid UTF-8 are base64-encoded. Empty object `{}` when no headers were set on the message.

---

## Value formats

Configured via `value_format` (and optionally `key_format`). All formats use a `type` discriminator field.

| Format | `type` | Description |
|--------|--------|-------------|
| JSON | `json` | Parse value bytes as a JSON document. Default. |
| Raw string | `raw_string` | Treat value bytes as a UTF-8 string. The string becomes the `value` field. |
| Bytes | `bytes` | Pass bytes through as a **base64-encoded string** inside `value`. No parsing is attempted. |
| Confluent Avro | `confluent_avro` | Confluent wire-format Avro: `[0x00][schema_id 4B][Avro binary]`. Requires `schema-registry` feature. |
| Confluent Protobuf | `confluent_protobuf` | Confluent wire-format Protobuf. Requires `schema-registry` feature. v1 returns an error — full descriptor support tracked in issue #44. |
| Confluent JSON Schema | `confluent_json_schema` | Confluent wire-format JSON: `[0x00][schema_id 4B][JSON bytes]`. Optional schema validation. Requires `schema-registry` feature. |

The three Confluent formats require building with the `schema-registry` feature flag:

```toml
[dependencies]
faucet-source-kafka = { version = "...", features = ["schema-registry"] }
```

Each Confluent format takes a `schema_registry` block — see the `faucet-kafka-common` README for the full `SchemaRegistryConfig` options (URL, basic auth, cache capacity, request timeout).

---

## Auth

Authentication is configured via the `auth` field using `KafkaAuth` from `faucet-kafka-common`. The full auth reference — SASL/PLAIN, SASL/SCRAM, SSL client certificates, and SASL+SSL — is in the [`faucet-kafka-common` README](../kafka-common/README.md#auth-modes).

Quick example for SASL/PLAIN (Confluent Cloud, MSK with SASL):

```yaml
auth:
  type: sasl_plain
  username: "${env:KAFKA_USERNAME}"
  password: "${env:KAFKA_PASSWORD}"
```

Use `${env:VAR}` interpolation so credentials never land in the YAML file.

---

## Resume and state store

When a `StateStore` is wired into the pipeline (via `state:` in the YAML, or `Pipeline::with_state_store` in Rust), the source participates in durable offset tracking:

1. **Before each run**, the pipeline reads the stored bookmark from the `StateStore` using the source's state key and calls `apply_start_bookmark`. The bookmark is buffered in memory; no seeking happens yet.

2. **After the first Kafka message arrives** (which triggers partition assignment), the source calls `seek` on each `(topic, partition)` pair stored in the bookmark, positioning the consumer at the next unread offset.

3. **After the sink confirms the batch**, the pipeline persists the new bookmark — a list of `{topic, partition, offset}` entries recording one past the highest offset written. The bookmark is only written on successful sink completion, so a crashed run never marks data as consumed.

**State key format:**

```
kafka:{group_id}:{topic1}.{topic2}...
```

Topics are sorted alphabetically before joining, so the key is stable regardless of the order you list topics in the config. For example, `group_id = "my-group"` and `topics = ["beta", "alpha"]` produces:

```
kafka:my-group:alpha.beta
```

**Important — at-least-once delivery on restart:**

Because `seek` is called after the first poll (a librdkafka constraint — partition assignment is not final until the first message is received), exactly one message per assigned partition may be delivered twice across a restart boundary. This is at-least-once delivery. Sinks should be idempotent or otherwise accept duplicate records.

A future enhancement will install a custom `ConsumerContext` with a `pre_rebalance` callback to eliminate the restart duplicate — tracked in the follow-up issue filed alongside this README.

---

## Termination

The consume loop exits when any of the following is true:

- **`max_messages`** — the configured number of messages has been collected.
- **`idle_timeout`** — no new message has arrived within the configured number of seconds.
- **`Ctrl+C` (SIGINT)** — the source catches the signal and exits the loop cleanly. The bookmark for everything consumed up to that point is returned to the pipeline, which persists it before exiting.

At least one of `max_messages` and `idle_timeout` must be set; the config is rejected at construction time otherwise.

Both conditions can be combined — the loop stops as soon as either threshold is hit:

```yaml
max_messages: 50000
idle_timeout: 60
```

---

## Throughput notes

librdkafka is one of the fastest Kafka client implementations available, benchmarked at millions of messages per second on well-provisioned hardware. For most pipelines the bottleneck is the downstream sink (BigQuery inserts, Postgres writes, S3 uploads), not the consume loop itself.

The v1 consume loop is single-threaded — one goroutine polls one `StreamConsumer`. For higher throughput, partition your topic and run multiple `faucet` pipeline instances with the same `group_id`. Kafka assigns disjoint partition sets to each instance, and they scale linearly.

Tuning tips:

- Increase `max_messages` for larger batch sizes — fewer pipeline runs per unit of data.
- Reduce `idle_timeout` for latency-sensitive pipelines where you want frequent small batches.
- Use `extra_client_config` to pass `fetch.max.bytes`, `max.partition.fetch.bytes`, or `queued.max.messages.kbytes` if you need to push beyond librdkafka's defaults.

---

## CLI integration

Run with the `faucet` binary:

```bash
faucet run pipeline.yaml
faucet validate pipeline.yaml
faucet preview pipeline.yaml --limit 5   # consume 5 messages and print to stdout
```

Inspect the full JSON Schema for every config field:

```bash
faucet schema source kafka
```

A complete working example is in [`cli/examples/kafka_to_jsonl.yaml`](../../cli/examples/kafka_to_jsonl.yaml).

---

## See also

- [`faucet-sink-kafka`](../../crates/sink/kafka/README.md) — produce records to Kafka topics.
- [`faucet-kafka-common`](../kafka-common/README.md) — shared auth modes, value formats, schema registry client, and policy enums used by both connectors.

---

## License

Dual-licensed under MIT and Apache-2.0, matching the workspace `license` field.
