# faucet-auth

Shared, single-flight authentication providers for
[`faucet-stream`](https://github.com/PawanSikawat/faucet-stream).

These implement [`faucet_core::AuthProvider`] — a live entity that owns a token
cache and refresh lifecycle. One instance, wrapped in an `Arc`, is shared across
every connector that references it, so **N connectors hitting one identity
provider share a single token with single-flight refresh** instead of each
racing to refresh a single-active / rotating token.

## Providers

| `type`           | Provider                          | Notes |
|------------------|-----------------------------------|-------|
| `static`         | `StaticProvider`                  | Fixed pre-minted credential (bearer / header / basic). |
| `oauth2`         | `OAuth2ClientCredentialsProvider` | OAuth2 `client_credentials` grant. |
| `oauth2_refresh` | `OAuth2RefreshProvider`           | OAuth2 `refresh_token` grant with refresh-token **rotation capture**. |
| `token_endpoint` | `TokenEndpointProvider`           | Fetch a token from any HTTP endpoint, extract via JSONPath. |

## CLI usage

Define a provider once in the top-level `auth:` catalog and reference it from any
connector via `auth: { ref: <name> }`:

```yaml
auth:
  sf:
    type: oauth2_refresh
    config:
      token_url: https://<acct>.snowflakecomputing.com/oauth/token-request
      client_id: ${secret:SF_CLIENT_ID}
      client_secret: ${secret:SF_CLIENT_SECRET}
      refresh_token: ${secret:SF_REFRESH_TOKEN}
pipeline:
  sources:
    sf_table:
      kind: snowflake
      config:
        account: ${vars.account}
        auth: { ref: sf }     # every row sharing this template shares ONE token
```

## Library usage

```rust
use std::sync::Arc;
use faucet_auth::OAuth2RefreshProvider;
use faucet_core::SharedAuthProvider;

let provider: SharedAuthProvider = Arc::new(OAuth2RefreshProvider::from_config(&cfg)?);
// Clone the Arc into every connector that should share the token:
let source_a = MySource::new(cfg_a, Some(provider.clone()));
let source_b = MySource::new(cfg_b, Some(provider.clone()));
```

## License

MIT OR Apache-2.0
