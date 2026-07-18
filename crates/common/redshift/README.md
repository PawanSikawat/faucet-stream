# faucet-common-redshift

Shared connection, credentials, and connection-pool types for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) Amazon Redshift
**source** (`faucet-source-redshift`) and **sink** (`faucet-sink-redshift`)
connectors.

Redshift speaks the PostgreSQL wire protocol, so both connectors connect through
`sqlx`'s Postgres driver. This crate centralises that so TLS, auth, and pooling
behave identically on both sides.

## Types

- **`RedshiftCredentials`** — adjacently-tagged `{ type, config }` enum:
  - `password` — username/password auth (the user comes from the connection
    block). **The only mechanism implemented in v1.**
  - `iam` / `redshift_data_api` — reserved for a future release; building a
    client with either currently returns a typed
    `FaucetError::Config`.
- **`RedshiftConnection`** — `host`, `port` (default `5439`), `database`,
  `user`, `credentials`, and a `tls` toggle (default `true`). Flattened into
  both end configs.

## Helpers

- `build_connect_options(&RedshiftConnection)` — pure `PgConnectOptions` builder.
  `tls: true` → `sslmode=require`; `tls: false` → `sslmode=prefer`.
- `build_pool_lazy(conn, max)` — lazily-connected pool (no I/O at construction).
- `build_pool(conn, max)` — eagerly validated pool (fails fast on bad creds).
- `resolve_password(&RedshiftCredentials)` — extracts the password.

## Example

```yaml
host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
port: 5439
database: dev
user: admin
credentials:
  type: password
  config:
    password: ${env:REDSHIFT_PASSWORD}
tls: true
```

License: MIT OR Apache-2.0
