# faucet-snowflake-common

Shared configuration types and helpers for the Snowflake source and sink connectors in the [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

## Contents

- `SnowflakeAuth` — JWT key-pair (`KeyPair { user, private_key_pem }`) or OAuth bearer (`OAuth { token }`). Derives `Serialize`, `Deserialize`, `JsonSchema`. Its `Debug` impl masks credentials as `"***"`.
- `authorization_header(&SnowflakeAuth, account)` — produces the `Authorization` header value for a Snowflake SQL REST API request (`Bearer {jwt}` for `KeyPair`, `Snowflake Token="{token}"` for `OAuth`).
- `snowflake_token_type(&SnowflakeAuth)` — matching `X-Snowflake-Authorization-Token-Type` header value (`KEYPAIR_JWT` / `OAUTH`).

## Usage

Most users do not depend on this crate directly. It is re-exported by:

- [`faucet-source-snowflake`](https://crates.io/crates/faucet-source-snowflake)
- [`faucet-sink-snowflake`](https://crates.io/crates/faucet-sink-snowflake)

Depend on this crate directly only if you are building a third-party connector that needs to accept the same auth shape in its config.

## License

MIT OR Apache-2.0
