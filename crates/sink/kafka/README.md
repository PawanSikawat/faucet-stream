# faucet-sink-kafka

Apache Kafka producer sink for `faucet-stream`. Publishes records to one or more Kafka topics. Built on [`rdkafka`](https://crates.io/crates/rdkafka) (`FutureProducer`). Supports idempotent producer, fixed or per-record topic routing, JSONPath-driven key/partition/headers extraction, configurable compression, and `QueueFull` retry.

---

## Quick start

```yaml
# pipeline.yaml
version: 1

source:
  kind: rest
  config:
    base_url: "https://api.example.com"
    path: "/orders"
    records_path: "$.orders"

sink:
  kind: kafka
  config:
    brokers: "broker1:9092,broker2:9092"
    topic:
      type: fixed
      name: orders
    value_format:
      type: json
    compression: lz4
    idempotent: true
    acks: all
```

Run it:

```bash
faucet run pipeline.yaml
```

---

## Full config reference

All fields are top-level keys under `sink.config` in the pipeline YAML.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | `string` | **required** | Comma-separated bootstrap broker addresses, e.g. `"broker1:9092,broker2:9092"`. |
| `topic` | `KafkaSinkTopic` | **required** | Topic routing strategy. See [Topic resolution](#topic-resolution). |
| `auth` | `KafkaAuth` | `{type: none}` | Authentication mode. See [SASL/SSL setup](#saslssl-setup). |
| `value_format` | `KafkaValueFormat` | `{type: json}` | How each record is encoded as message bytes. See [Value + key encoding](#value--key-encoding). |
| `key_format` | `KafkaValueFormat \| null` | `null` | Encoding applied to the extracted key value. When absent, key bytes are serialized as UTF-8. |
| `key_path` | `string \| null` | `null` | JSONPath into each record to extract the message key. |
| `partition_path` | `string \| null` | `null` | JSONPath into each record to extract the target partition number (integer). |
| `headers_path` | `string \| null` | `null` | JSONPath into each record to extract a flat object of header key → string value pairs. |
| `on_key_error` | `"fail" \| "skip" \| "round_robin"` | `"fail"` | What to do when key or partition extraction fails. See [OnKeyError policy](#onkeyerror-policy). |
| `compression` | `"none" \| "gzip" \| "snappy" \| "lz4" \| "zstd"` | `"none"` | Producer-side compression codec. See [Compression](#compression). |
| `acks` | `"none" \| "leader" \| "all"` | `"all"` | Broker acknowledgment level. Must be `"all"` when `idempotent: true`. |
| `idempotent` | `bool` | `true` | Enable the idempotent producer (`enable.idempotence = true`). Requires `acks: all`. |
| `linger` | `integer` (seconds) | `0` | Time the producer waits before flushing a partial batch. Use `extra_client_config: {linger.ms: "5"}` for millisecond precision. |
| `batch_size` | `integer` | `1000` (`DEFAULT_BATCH_SIZE`) | Maximum number of in-flight `FuturesUnordered` send futures per `write_batch` call. Also seeds librdkafka's `queue.buffering.max.messages` so the broker-side buffer matches the send window. `0` disables the explicit cap (bounded only by `max_in_flight`) and leaves `queue.buffering.max.messages` at its librdkafka default. See [Streaming and batching](#streaming-and-batching). |
| `message_timeout` | `integer` (seconds) | `30` | Delivery timeout per message. |
| `max_in_flight` | `integer` | `100` | Maximum concurrent produce requests. Must be ≥ 1. |
| `queue_full_backoff` | `integer` (seconds) | `0` | Pause between retries on `QueueFull` error. |
| `queue_full_max_retries` | `integer` | `3` | Maximum `QueueFull` retries before the error surfaces. |
| `extra_client_config` | `object` | `{}` | Raw librdkafka client properties. Overrides anything set by `auth` or the typed fields above. |

---

## Topic resolution

Two strategies are available, selected by the `type` discriminator.

### `fixed` — all records go to the same topic

```yaml
topic:
  type: fixed
  name: orders
```

### `from_path` — topic extracted per record via JSONPath

```yaml
topic:
  type: from_path
  path: "$.dest"   # JSONPath extracted from each record
```

The path must resolve to a non-empty string for every record. A missing or non-string value is always fatal — there is no `on_key_error` equivalent for topic routing.

---

## Value + key encoding

Configured via `value_format` (and optionally `key_format`). All formats use a `type` discriminator.

| Format | `type` | Description |
|--------|--------|-------------|
| JSON | `json` | Serialize the record as a JSON document. Default. |
| Raw string | `raw_string` | Write the string representation as UTF-8 bytes. |
| Bytes | `bytes` | Expect a base64-encoded string; decode and write raw bytes. |
| Confluent Avro | `confluent_avro` | Confluent wire-format Avro. Subject is `{topic}-value`. Requires `schema-registry` feature. |
| Confluent Protobuf | `confluent_protobuf` | v1 returns an error — tracked in issue #44. Requires `schema-registry` feature. |
| Confluent JSON Schema | `confluent_json_schema` | Confluent wire-format JSON. Requires `schema-registry` feature. |

The three Confluent formats require the `schema-registry` feature flag. Each takes a `schema_registry` block — see the [`faucet-common-kafka` README](../../common/kafka/README.md#value-formats) for `SchemaRegistryConfig` options.

When a Confluent format is used on the **sink** side you must also supply the schema text to register and encode against: set `value_schema` (for `value_format`) and/or `key_schema` (for `key_format`) to the Avro `.avsc` JSON, `.proto`, or JSON Schema document. They are registered under the `{topic}-value` / `{topic}-key` subjects on first use. The config is rejected at load time if a Confluent format is selected without its schema.

```yaml
sink:
  type: kafka
  config:
    brokers: localhost:9092
    topic: { type: fixed, name: events }
    value_format:
      type: confluent_avro
      schema_registry: { url: http://localhost:8081 }
    value_schema: '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}'
```

---

## Key / partition / headers

`key_path`, `partition_path`, and `headers_path` are JSONPath expressions evaluated against each record.

```yaml
key_path: "$.order_id"          # string → Kafka message key
partition_path: "$.shard"       # integer → target partition
headers_path: "$.meta"          # flat object → Kafka headers
```

When any path is absent, the corresponding attribute is not set and librdkafka applies its default (round-robin for partition, no key, no headers).

---

## OnKeyError policy

Controls what happens when `key_path` or `partition_path` extraction fails (path absent, type mismatch, or invalid partition).

| Value | Behaviour |
|-------|-----------|
| `fail` (default) | Abort the batch with `FaucetError::Sink`. |
| `skip` | Drop the record, log `WARN`, and continue. |
| `round_robin` | Send the record with no key; librdkafka assigns the partition. Record is not dropped. |

`on_key_error` does not apply to `from_path` topic failures — those are always fatal.

---

## Idempotence + acks

`idempotent: true` (the default) sets `enable.idempotence = true` in librdkafka. This prevents duplicate messages from retried produce calls within a single producer session. Combined with `acks: all`, this is the no-duplicate, no-loss guarantee for individual produce calls.

The config is rejected at construction time if `idempotent: true` and `acks != all`:

```
kafka sink: idempotent=true requires acks=all
```

To trade durability for throughput:

```yaml
idempotent: false
acks: leader   # or "none" for fire-and-forget
```

---

## Compression

| Value | Description |
|-------|-------------|
| `none` (default) | No compression. |
| `gzip` | Good ratio; higher CPU cost. |
| `snappy` | Balanced speed and ratio. |
| `lz4` | Fast encode/decode; recommended for throughput-sensitive pipelines. |
| `zstd` | Best ratio at moderate CPU cost. Requires Kafka broker ≥ 2.1. |

---

## Throughput knobs

| Field | Default | Effect |
|-------|---------|--------|
| `linger` | `0` s | Time the producer waits to accumulate records. Increase for larger batches. Use `linger.ms` in `extra_client_config` for sub-second precision. |
| `batch_size` | `1000` | In-flight `FuturesUnordered` send-window cap and seed for librdkafka's `queue.buffering.max.messages`. See [Streaming and batching](#streaming-and-batching). |
| `max_in_flight` | `100` | Concurrent produce requests. Set to 1 with `idempotent: true` for strict ordering. |
| `message_timeout` | `30` s | Delivery timeout. Increase for high-latency brokers. |
| `queue_full_backoff` | `0` s | Backoff on `QueueFull`. Set to e.g. `1` to avoid tight-looping. |
| `queue_full_max_retries` | `3` | Maximum `QueueFull` retries before the error surfaces. |

Tune the librdkafka byte-size batch knob — historically exposed as `batch_size` (bytes) — via `extra_client_config: {batch.size: "16384"}`. The default (16 KiB) is rarely worth changing unless you have very small or very large records.

---

## Streaming and batching

The sink is driven from the streaming pipeline via `Sink::write_batch`. Within a single `write_batch` call, records are produced to the broker through a `FuturesUnordered` of `send_result` futures so multiple sends can fly concurrently. The `batch_size` field bounds how many of those futures are in flight at any moment:

- **Effective in-flight cap = `min(max_in_flight, batch_size)`** when `batch_size > 0`.
- **Effective in-flight cap = `max_in_flight`** when `batch_size = 0` (the "no batching" sentinel — pre-streaming behaviour).

When `batch_size > 0`, `KafkaSink::new` also sets librdkafka's `queue.buffering.max.messages` to `batch_size` so the producer's broker-side message buffer can hold one full send window. This avoids the asymmetry where the FuturesUnordered cap permits N concurrent sends but the producer queue rejects them with `QueueFull` immediately. `extra_client_config` takes precedence — pass a smaller `queue.buffering.max.messages` there if you want to force backpressure (e.g. to exercise the `QueueFull` retry path in tests).

`QueueFull` retry semantics are unaffected by `batch_size`. Whenever librdkafka rejects an enqueue with `QueueFull`, the existing loop (`queue_full_backoff` / `queue_full_max_retries`) still applies; the in-flight cap simply makes those rejections less likely in the common case.

**When to tune away from the default:**

- **Throughput-bound, large records.** If `DEFAULT_BATCH_SIZE = 1000` is leaving the producer thread starved, raise `batch_size` (and consequently `max_in_flight`) to push more concurrent requests in flight. The librdkafka `queue.buffering.max.messages` rises with it automatically.
- **Strict ordering.** Combine `batch_size = 1` with `max_in_flight = 1` and `idempotent: true` so at most one send is on the wire at a time. Lower throughput, but per-partition order is preserved.
- **One-shot drain.** Use `batch_size = 0` for a small lookup-table source that emits a single page per run, so the entire write_batch fires in parallel up to `max_in_flight` without the extra cap.

---

## SASL/SSL setup

Authentication is configured via the `auth` field using `KafkaAuth` from `faucet-common-kafka`. The full auth reference is in the [`faucet-common-kafka` README](../../common/kafka/README.md#auth-modes).

```yaml
# SASL/PLAIN (Confluent Cloud, Amazon MSK)
auth:
  type: sasl_plain
  username: "${env:KAFKA_USERNAME}"
  password: "${env:KAFKA_PASSWORD}"
```

```yaml
# SASL/SCRAM-SHA-512 over TLS
auth:
  type: sasl_ssl
  sasl:
    type: sasl_scram
    mechanism: sha512
    username: "${env:KAFKA_USERNAME}"
    password: "${env:KAFKA_PASSWORD}"
  ssl:
    type: ssl
    ca_path: /etc/kafka/certs/ca.pem
    cert_path: /etc/kafka/certs/client.pem
    key_path: /etc/kafka/certs/client.key
```

Use `${env:VAR}` interpolation so credentials never land in the YAML file.

---

## CLI integration

```bash
faucet run cli/examples/rest_to_kafka.yaml
faucet validate cli/examples/rest_to_kafka.yaml
faucet preview cli/examples/rest_to_kafka.yaml --limit 5
faucet schema sink kafka
```

A complete working example is in [`cli/examples/rest_to_kafka.yaml`](../../cli/examples/rest_to_kafka.yaml).

---

## See also

- [`faucet-source-kafka`](../../crates/source/kafka/README.md) — consume records from Kafka topics.
- [`faucet-common-kafka`](../../common/kafka/README.md) — shared auth modes, value formats, schema registry client, and policy enums.

---

## License

Dual-licensed under MIT and Apache-2.0, matching the workspace `license` field.
