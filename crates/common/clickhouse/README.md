# faucet-common-clickhouse

Shared configuration and HTTP-protocol helpers for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ClickHouse **source**
(`faucet-source-clickhouse`) and **sink** (`faucet-sink-clickhouse`) connectors.

Both connectors talk to ClickHouse over its
[HTTP interface](https://clickhouse.com/docs/en/interfaces/http) using
[`reqwest`](https://crates.io/crates/reqwest), so this crate holds the surface
they share:

- **`ClickHouseConnection`** — endpoint (`url` **or** `host` + `http_port` +
  `tls`), target `database`, and optional `user` / `password`. Flattened into
  both end configs so the wire shape is identical on the source and the sink.
  Its `Debug` impl masks the password as `***`.
- **`base_url()`** — resolves the `scheme://host:port` base URL (no trailing
  slash) the HTTP interface is reached at.
- **`build_client`** — the single place a reqwest `Client` is constructed.
- **`query_params`** — builds the `?database=…&<setting>=…` query string
  (settings such as `async_insert`, `default_format`).
- **`apply_auth`** — attaches the `X-ClickHouse-User` / `X-ClickHouse-Key`
  authentication headers (never URL query parameters, so credentials do not leak
  into request logs).
- **`parse_json_each_row`** / **`build_json_each_row`** — decode / encode the
  newline-delimited `JSONEachRow` format used for both reads and writes.
- **`sql_literal`** — injection-safe SQL literal encoding for a JSON scalar,
  used to push an incremental bookmark down into a `WHERE` clause.

Authentication is username + password (ClickHouse native HTTP auth) only in v1.

You normally depend on `faucet-source-clickhouse` / `faucet-sink-clickhouse`,
which re-export `ClickHouseConnection` — not on this crate directly.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
