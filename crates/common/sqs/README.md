# faucet-common-sqs

Shared configuration types for the faucet-stream AWS SQS **source**
(`faucet-source-sqs`) and **sink** (`faucet-sink-sqs`) connectors.

It exposes:

- `SqsCredentials` — the AWS auth enum, serialized with the consistent
  `{ type, config }` wire shape (`default` / `profile` / `access_key` /
  `assume_role` / `web_identity`). Its `Debug` impl never prints key material.
- `build_client` — assembles an `aws_sdk_sqs::Client` from an optional region,
  an optional endpoint URL (LocalStack / VPC endpoints), and a `SqsCredentials`.

Both connector crates re-export these, so end users import from
`faucet-source-sqs` / `faucet-sink-sqs` directly and never depend on this crate
by name.

## Credentials

```yaml
credentials: { type: default }
credentials: { type: profile, config: { name: prod } }
credentials: { type: access_key, config: { access_key_id: AKIA…, secret_access_key: ${env:AWS_SECRET} } }
credentials: { type: assume_role, config: { role_arn: arn:aws:iam::…:role/x, external_id: … } }
credentials: { type: web_identity }
```

## License

MIT OR Apache-2.0
