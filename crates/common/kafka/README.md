# faucet-common-kafka

Shared configuration types for `faucet-source-kafka` and `faucet-sink-kafka`.

Most users don't depend on this crate directly — they import from
`faucet-source-kafka` or `faucet-sink-kafka`, which re-export everything. Third-party
connector authors building their own Kafka source/sink for the faucet ecosystem should
depend on this crate to stay interchangeable with the first-party connectors.

---

## Features

### `default`

No features enabled. Provides:

- `KafkaAuth` — all authentication modes
- `KafkaValueFormat::{Json, RawString, Bytes}` — the three wire-independent formats
- `CompressionType` — producer-side compression enum
- `OnDecodeError` — per-message decode failure policy
- `OnKeyError` — per-record key/partition failure policy

### `schema-registry`

Enables Confluent Schema Registry integration:

- `KafkaValueFormat::{ConfluentAvro, ConfluentProtobuf, ConfluentJsonSchema}` variants
- `SchemaRegistryConfig` — connection settings for the registry client
- `schema_registry::client::SchemaRegistryClient` — HTTP client with LRU schema cache
- Per-format codec modules: `schema_registry::avro`, `schema_registry::protobuf`,
  `schema_registry::json_schema`, `schema_registry::envelope`

Additional dependencies pulled in: `reqwest`, `lru`, `bytes`, `apache-avro`,
`prost-reflect`, `prost`, `jsonschema`, `tokio`, `url`, `urlencoding`.

---

## Auth modes

Authentication is configured via `KafkaAuth` (`crates/common/kafka/src/auth.rs`).
The `type` discriminator uses `snake_case` matching the enum variant names.

### `none` (default)

```yaml
auth:
  type: none
```

Plaintext brokers only. Sets `security.protocol = PLAINTEXT`.

### `sasl_plain`

```yaml
auth:
  type: sasl_plain
  username: my-user
  password: my-secret
```

Sets `security.protocol = SASL_PLAINTEXT`, `sasl.mechanism = PLAIN`. Both
`username` and `password` must be non-empty or the config is rejected.

### `sasl_scram`

```yaml
auth:
  type: sasl_scram
  mechanism: sha256   # or sha512
  username: my-user
  password: my-secret
```

`mechanism` is a `ScramMechanism` enum: `sha256` maps to `SCRAM-SHA-256`,
`sha512` maps to `SCRAM-SHA-512`. Sets `security.protocol = SASL_PLAINTEXT`.

### `ssl`

```yaml
auth:
  type: ssl
  ca_path: /etc/kafka/certs/ca.pem
  cert_path: /etc/kafka/certs/client.pem
  key_path: /etc/kafka/certs/client.key
  key_password: optional-passphrase   # omit if key is unencrypted
```

All three path fields are validated to exist on the filesystem at config time.
`key_password` is optional and omitted from serialization when absent.
Sets `security.protocol = SSL`.

### `sasl_ssl`

```yaml
auth:
  type: sasl_ssl
  sasl:
    type: sasl_plain
    username: my-user
    password: my-secret
  ssl:
    type: ssl
    ca_path: /etc/kafka/certs/ca.pem
    cert_path: /etc/kafka/certs/client.pem
    key_path: /etc/kafka/certs/client.key
```

Combines a SASL mechanism with TLS transport. The `sasl` field must be either
`sasl_plain` or `sasl_scram`; the `ssl` field must be `ssl`. The inner configs
are applied in order (`ssl` first, then `sasl`), then `security.protocol` is
overridden to `SASL_SSL`.

---

## Value formats

Configured via `KafkaValueFormat` (`crates/common/kafka/src/format.rs`).
The `type` discriminator uses `snake_case`.

### `json` (default)

```yaml
value_format:
  type: json
```

Parses message bytes as a JSON document. Invalid JSON fails per `on_decode_error`.

### `raw_string`

```yaml
value_format:
  type: raw_string
```

Treats message bytes as a UTF-8 string. The string becomes the `value` field
in the JSON record. Invalid UTF-8 fails per `on_decode_error`.

### `bytes`

```yaml
value_format:
  type: bytes
```

Passes message bytes through as a base64-encoded string in the JSON record.
On the sink side, expects a base64 string in the source record.

### `confluent_avro` (feature: `schema-registry`)

```yaml
value_format:
  type: confluent_avro
  schema_registry:
    url: http://localhost:8081
    auth:                         # optional
      username: sr-user
      password: sr-secret
    cache_capacity: 1024          # default 1024
    request_timeout: 10           # seconds, default 10
```

Decodes messages using the Confluent wire envelope: `[0x00][be u32 schema_id][Avro binary]`.
The writer schema is fetched from the registry by `schema_id` and cached. On the sink side,
the subject name is `{topic}-value` (Confluent TopicNameStrategy).

### `confluent_protobuf` (feature: `schema-registry`)

```yaml
value_format:
  type: confluent_protobuf
  schema_registry:
    url: http://localhost:8081
```

**Note:** `ConfluentProtobuf` is present in the type system for API symmetry with Avro
and JSON Schema, but v1 returns a `FaucetError::Config` on both encode and decode.
Full descriptor support (pre-built `FileDescriptorSet` or `protoc`-based compilation)
is tracked in issue #44. The wire format expected is:
`[0x00][be u32 schema_id][message_indexes][protobuf bytes]`, where v1 only supports
the single-message case (`message_indexes = [0x00]`).

### `confluent_json_schema` (feature: `schema-registry`)

```yaml
value_format:
  type: confluent_json_schema
  schema_registry:
    url: http://localhost:8081
    auth:
      username: sr-user
      password: sr-secret
  validate: false   # default false; set true to validate decoded JSON against the schema
```

Decodes using `[0x00][be u32 schema_id][JSON bytes]`. When `validate: true`, the
decoded JSON object is validated against the registered JSON Schema; validation
errors fail per `on_decode_error`.

---

## Schema Registry

Configuration struct: `SchemaRegistryConfig` (`crates/common/kafka/src/schema_registry/mod.rs`).

**Wire envelope** (`crates/common/kafka/src/schema_registry/envelope.rs`):

```
[0x00] [schema_id: 4 bytes big-endian] [payload bytes...]
```

The magic byte `0x00` identifies the Confluent wire format. `envelope::decode` strips
the header and returns `(schema_id, payload_bytes)`. `envelope::encode` prepends the
header to a serialized payload.

**Client** (`crates/common/kafka/src/schema_registry/client.rs`):

`SchemaRegistryClient` is `Arc`-cloneable and safe to share across tasks. Schema
fetches are cached in an LRU bounded by `cache_capacity` (default 1024), keyed by
the integer schema ID; misses hit `GET /schemas/ids/{id}`. Schema *registrations*
are cached in a second LRU of the same capacity, keyed by `(subject, schema_type,
schema_text)` — so encoding a stream of records that share one schema issues a
single `POST /subjects/{subject}/versions` instead of one per record. Registry auth
is `BasicAuth` (optional); `request_timeout` applies per HTTP call (default 10 s).

**Subject naming on the sink side**: `{topic}-value` (Confluent TopicNameStrategy).
The sink registers or looks up the schema under this subject before the first produce call.

**Decoder flow**: read `schema_id` from wire envelope → cache lookup or HTTP fetch →
parse writer schema → decode payload bytes into `serde_json::Value`.

---

## Policy enums

All three enums are in `crates/common/kafka/src/format.rs` and derive `Serialize`,
`Deserialize`, `JsonSchema`.

### `OnDecodeError`

What the source does when a single message fails to decode. Serializes as `snake_case`.

| Value | Behaviour | Default |
|-------|-----------|---------|
| `fail` | Surface `FaucetError::Source` and abort the batch | yes |
| `skip` | Drop the message and continue (logs a `WARN`) | |

### `OnKeyError`

What the sink does when key or partition extraction fails for a record. Serializes as
`snake_case`.

| Value | Behaviour | Default |
|-------|-----------|---------|
| `fail` | Surface `FaucetError::Sink` and abort the batch | yes |
| `skip` | Drop the record and continue (logs a `WARN`) | |
| `round_robin` | Send the record with no key; librdkafka chooses the partition | |

### `CompressionType`

Producer-side compression applied to outbound batches. Serializes as `lowercase`.

| Value | librdkafka string | Default |
|-------|-------------------|---------|
| `none` | `none` | yes |
| `gzip` | `gzip` | |
| `snappy` | `snappy` | |
| `lz4` | `lz4` | |
| `zstd` | `zstd` | |

---

## License

Dual-licensed under MIT and Apache-2.0, per the workspace `license` field.
