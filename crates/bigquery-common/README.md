# faucet-bigquery-common

Shared credential configuration and client construction for the BigQuery source and sink connectors in the [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

## Contents

- `BigQueryCredentials` — credential source: `ServiceAccountKeyPath(String)`, `ServiceAccountKey(String)` (inline JSON), or `ApplicationDefault`. Derives `Serialize`, `Deserialize`, `JsonSchema`. Its `Debug` impl masks inline JSON as `"***"`.
- `build_client(&BigQueryCredentials)` — async helper that turns a `BigQueryCredentials` into a ready-to-use `gcp_bigquery_client::Client`.

## Usage

Most users do not depend on this crate directly. It is re-exported by:

- [`faucet-source-bigquery`](https://crates.io/crates/faucet-source-bigquery)
- [`faucet-sink-bigquery`](https://crates.io/crates/faucet-sink-bigquery)

Depend on this crate directly only if you are building a third-party connector that needs to accept the same credentials shape in its config.

## License

MIT OR Apache-2.0
