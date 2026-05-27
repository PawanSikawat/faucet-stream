# Authentication

The REST source supports several auth strategies via the `auth:` block. Always
pull secrets from the environment with `${env:VAR}` (or `${file:PATH}` /
`${secret:VAR}`) rather than hard-coding them in the config.

## API key / header

```yaml
auth:
  type: ApiKey
  header: Authorization
  value: "Bearer ${env:API_TOKEN}"
```

## Bearer token

```yaml
auth:
  type: Bearer
  token: ${env:API_TOKEN}
```

## Basic auth

```yaml
auth:
  type: Basic
  username: ${env:API_USER}
  password: ${env:API_PASS}
```

## OAuth2 client credentials

The source fetches and refreshes the token automatically, refreshing before
expiry:

```yaml
auth:
  type: OAuth2
  token_url: https://auth.example.com/oauth/token
  client_id: ${env:CLIENT_ID}
  client_secret: ${env:CLIENT_SECRET}
  scopes: ["read:events"]
```

## Custom token endpoint

For non-standard token endpoints, `TokenEndpoint` lets you describe the request
and point at the access-token and expiry fields in the response. See
`faucet schema source rest` for the full field list.

## Connector-specific auth

Other connectors carry their own auth types, shared through a common crate where
a source/sink pair exists:

- **BigQuery** — `BigQueryCredentials`: service-account key path, inline JSON, or
  application-default credentials.
- **Snowflake** — `SnowflakeAuth`: JWT (key-pair) or OAuth.
- **Kafka** — `KafkaAuth`: SASL mechanisms + TLS via `KafkaTlsConfig`.
- **Elasticsearch** — `ElasticsearchAuth`: basic, API key, bearer, or none.

Inspect any connector's auth shape with `faucet schema source <name>` /
`faucet schema sink <name>`.

## Secret interpolation

`${env:VAR}` and `${file:PATH}` are resolved at config-load time, so secrets
never need to appear in the file. A sibling `.env` is loaded automatically (use
`--no-env-file` to disable, or `--env-file PATH` to point elsewhere).
