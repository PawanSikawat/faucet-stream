# faucet-common-nats

Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
NATS **source** ([`faucet-source-nats`](https://crates.io/crates/faucet-source-nats))
and **sink** ([`faucet-sink-nats`](https://crates.io/crates/faucet-sink-nats)).

Both connectors depend on this crate and re-export its types, so end-user
imports do not change.

## Types

- **`NatsAuth`** — authentication mode, serialized as the standard faucet
  `{ type, config }` adjacent tag:
  - `none` (default) — anonymous
  - `token` — `{ token }`
  - `user_password` — `{ username, password }`
  - `nkey` — `{ nkey }` (Ed25519 seed)
  - `creds_file` — `{ path }` (an `nsc`-produced `.creds` bundle)

  Its `Debug` implementation is secret-safe: `token`, `password`, and `nkey`
  are never printed.

- **`NatsConnectionConfig`** — the connection surface both connectors
  `#[serde(flatten)]` into their config:

  | field     | type            | default                      | description                              |
  |-----------|-----------------|------------------------------|------------------------------------------|
  | `servers` | `Vec<String>`   | `["nats://127.0.0.1:4222"]`  | server URLs (first reachable wins)       |
  | `auth`    | `NatsAuth`      | `none`                       | authentication mode                      |
  | `tls`     | `bool`          | `false`                      | require a TLS connection                 |
  | `name`    | `Option<String>`| —                            | client connection name (server monitor)  |

- **`connect(&NatsConnectionConfig) -> Result<async_nats::Client, FaucetError>`**
  — the single client builder both connectors use. Retry-on-initial-connect is
  left off, so an unreachable server fails immediately with a typed error.

## Example (embedded in a connector config)

```yaml
servers: ["nats://127.0.0.1:4222"]
auth:
  type: token
  config:
    token: ${env:NATS_TOKEN}
tls: false
name: faucet
```

## License

Licensed under either of Apache-2.0 or MIT at your option.
