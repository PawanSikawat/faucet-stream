# faucet-common-kinesis

Shared configuration types for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
AWS Kinesis Data Streams connectors — [`faucet-source-kinesis`](https://crates.io/crates/faucet-source-kinesis)
and [`faucet-sink-kinesis`](https://crates.io/crates/faucet-sink-kinesis).

Both connectors re-export these types, so you normally depend on the source or
sink crate rather than this one directly.

## Contents

- **`KinesisCredentials`** — the auth enum, serialized in faucet's consistent
  `{ type: <method>, config: { … } }` wire shape:

  | `type` | Fields | Meaning |
  |--------|--------|---------|
  | `default` | — | AWS SDK default provider chain (env vars, shared config, container/instance credentials, web identity) with automatic refresh |
  | `profile` | `name` | A named profile from the shared AWS config/credentials files |
  | `access_key` | `access_key_id`, `secret_access_key`, `session_token?` | Static keys — prefer `${env:…}` / secrets-manager interpolation over literals |
  | `assume_role` | `role_arn`, `session_name?`, `external_id?` | STS AssumeRole on top of the default chain |
  | `web_identity` | — | Web-identity federation (EKS IRSA); equivalent to `default`, kept explicit for config clarity |

- **`build_client`** — assembles an `aws_sdk_kinesis::Client` from
  region / `endpoint_url` (LocalStack, VPC endpoints) / credentials.
  Credential resolution stays inside `aws-config`, so rotating credentials
  refresh automatically.

## License

MIT OR Apache-2.0
