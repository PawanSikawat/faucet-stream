# Authentication

Every connector's `auth:` block uses one consistent shape — a `type:`
discriminator plus a nested `config:` map:

```yaml
auth:
  type: <method>
  config:
    <method-specific fields>
```

Always pull secrets from the environment with `${env:VAR}` (or `${file:PATH}` /
`${secret:VAR}`) rather than hard-coding them.

## API key / header

```yaml
auth:
  type: api_key
  config:
    header: Authorization
    value: "Bearer ${env:API_TOKEN}"
```

## Bearer token

```yaml
auth:
  type: bearer
  config:
    token: ${env:API_TOKEN}
```

## Basic auth

```yaml
auth:
  type: basic
  config:
    username: ${env:API_USER}
    password: ${env:API_PASS}
```

## OAuth2 client credentials

The source fetches and refreshes the token automatically (before expiry):

```yaml
auth:
  type: oauth2
  config:
    token_url: https://auth.example.com/oauth/token
    client_id: ${env:CLIENT_ID}
    client_secret: ${env:CLIENT_SECRET}
    scopes: ["read:events"]
```

## Custom token endpoint

For non-standard token endpoints, `token_endpoint` lets you describe the request
and point at the access-token and expiry fields in the response. See
`faucet schema source rest` for the full field list.

Two knobs cover the less-standard flows:

- **`encoding: form`** sends the token request as
  `application/x-www-form-urlencoded` (OAuth endpoints that expect a `resource=`
  param need this; the default is `json`).
- **`apply_as: { header, template }`** puts the fetched token in an arbitrary
  header instead of `Authorization: Bearer` — e.g. a session cookie. `template`
  is the header value with `{token}` substituted.

```yaml
auth:
  sap:                          # SAP B1: SessionId carried as a cookie
    type: token_endpoint
    config:
      url: "https://host:50000/b1s/v1/Login"
      body: { CompanyDB: "${param.company_db}", UserName: "${param.user}", Password: "${secret:sap_pw}" }
      token_path: "$.SessionId"
      apply_as: { header: "Cookie", template: "B1SESSION={token}; CompanyDB=${param.company_db}" }
```

## Persisting a rotating refresh token

Some providers (Microsoft Graph, Rippling) **rotate the `refresh_token` on every
refresh** — the old one is invalidated. In-memory rotation works for a single
run, but the *next* scheduled run would present the now-stale seed and get a 401.
Set `persist.path` on an `oauth2_refresh` provider to durably store the rotated
token (a file-backed state store) so later runs pick up where the last one left
off:

```yaml
auth:
  graph:
    type: oauth2_refresh
    config:
      token_url: "https://login.microsoftonline.com/${param.tenant}/oauth2/v2.0/token"
      client_id: "${secret:graph_client_id}"
      client_secret: "${secret:graph_client_secret}"
      refresh_token: "${secret:graph_seed_refresh_token}"   # seed; used only until the first rotation
      persist:
        path: "./state/auth"      # rotated refresh_token survives across runs
```

## Shared auth providers (`auth: { ref }`)

When several connectors authenticate against the **same** system — e.g. four
matrix rows reading four endpoints of one API, or four Snowflake tables — define
the credential **once** in the top-level `auth:` catalog and reference it with
`auth: { ref: <name> }`. faucet builds a single provider and shares it across
every row, so there is **one** token fetch and **one** refresh cycle
(single-flight) instead of each row racing to refresh a single-active / rotating
token:

```yaml
auth:
  api:
    type: oauth2_refresh        # rotating refresh token captured centrally
    config:
      token_url: ${env:API_TOKEN_URL}
      client_id: ${secret:API_CLIENT_ID}
      client_secret: ${secret:API_CLIENT_SECRET}
      refresh_token: ${secret:API_REFRESH_TOKEN}
pipeline:
  sources:
    ep:
      type: rest
      config:
        base_url: ${env:API_BASE_URL}
        auth: { ref: api }      # every row sharing this template shares ONE token
  sink: { type: stdout, config: {} }
matrix:
  - { id: customers, source: { ref: ep, config: { path: /customers } } }
  - { id: orders,    source: { ref: ep, config: { path: /orders } } }
```

Provider `type:` values (catalog only): `static`, `oauth2` (client-credentials),
`oauth2_refresh` (with rotation), `token_endpoint`. A connector's `auth:` is
**either** an inline definition **or** a `{ ref }` — never both. See
`cli/examples/shared_auth_rest.yaml` for a full four-row example.

Shared providers are supported by the bearer/header-based connectors (rest,
graphql, xml, grpc, websocket, http sink, elasticsearch, snowflake-OAuth).

**Library use:** build one `faucet_auth` provider, wrap it in an `Arc`, and pass
it to each source/sink with `.with_auth_provider(provider.clone())`.

## Connector-specific inline auth

Each connector also has its own inline auth methods, all under the `auth:` key
and all in `{ type, config }` form:

- **BigQuery** — `service_account_key_path`, `service_account_key`
  (inline JSON), or `application_default`.
- **Snowflake** — `key_pair` (JWT) or `oauth`.
- **Kafka** — `sasl_plain` / `sasl_scram` / `ssl` / `sasl_ssl`.
- **Elasticsearch** — `basic`, `api_key`, `bearer`, or `none`.
- **GCS** — `service_account_json_file`, `service_account_json_inline`,
  `application_default`, or `anonymous`.

Inspect any connector's auth shape with `faucet schema source <name>` /
`faucet schema sink <name>`.

## Secret interpolation

`${env:VAR}` and `${file:PATH}` are resolved at config-load time, so secrets
never need to appear in the file. A sibling `.env` is loaded automatically (use
`--no-env-file` to disable, or `--env-file PATH` to point elsewhere).
