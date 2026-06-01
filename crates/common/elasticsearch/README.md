# faucet-common-elasticsearch

Shared configuration types for the Elasticsearch source and sink connectors in the [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

## Types

- `ElasticsearchAuth` — authentication mode: `None`, `Basic`, `Bearer`, or `ApiKey`. Derives `Serialize`, `Deserialize`, `JsonSchema`. Its `Debug` impl masks credentials as `"***"`.

## Usage

Most users do not depend on this crate directly. It is re-exported by:

- [`faucet-source-elasticsearch`](https://crates.io/crates/faucet-source-elasticsearch)
- [`faucet-sink-elasticsearch`](https://crates.io/crates/faucet-sink-elasticsearch)

Depend on this crate directly only if you are building a third-party connector that needs to accept the same auth shape in its config.

## License

MIT OR Apache-2.0
