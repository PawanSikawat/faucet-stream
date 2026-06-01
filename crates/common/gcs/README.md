# faucet-common-gcs

Shared credential and client-construction types for `faucet-source-gcs` and
`faucet-sink-gcs`. Both crates re-export `GcsCredentials`, so end-users
typically don't depend on this crate directly.

Built on the official [`google-cloud-storage`](https://crates.io/crates/google-cloud-storage)
SDK (1.12) and [`google-cloud-auth`](https://crates.io/crates/google-cloud-auth) (1.10).

## `GcsCredentials`

Tagged enum (`#[serde(tag = "type")]`) so YAML/JSON configs read naturally.
The discriminator is `snake_case`.

| Variant | YAML example |
|---|---|
| `application_default` *(default)* | `{ type: application_default }` |
| `service_account_json_file` | `{ type: service_account_json_file, config: { path: /run/secrets/sa.json } }` |
| `service_account_json_inline` | `{ type: service_account_json_inline, config: { json: "${env:GCP_SA_JSON}" } }` |
| `anonymous` | `{ type: anonymous }` |

`application_default` (ADC) honours `GOOGLE_APPLICATION_CREDENTIALS`,
`gcloud auth application-default login` creds, and the GCE/GKE metadata
server in that order.

`anonymous` is for emulators (e.g. `fake-gcs-server`) that don't validate
bearer tokens. Production deployments should never use it.

## Client builders

```rust
pub async fn build_credentials(
    creds: &GcsCredentials,
) -> Result<google_cloud_auth::credentials::Credentials, FaucetError>;

pub async fn build_storage(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<google_cloud_storage::client::Storage, FaucetError>;

pub async fn build_storage_control(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<google_cloud_storage::client::StorageControl, FaucetError>;
```

- `build_storage` returns the data-plane client used by source reads and
  sink writes (`read_object`, `write_object`).
- `build_storage_control` returns the control-plane client used by source
  listings (`list_objects`).
- `storage_host` is an integration-test escape hatch — leave it `None` in
  production. Tests target `fake-gcs-server` via
  `Some("http://127.0.0.1:4443")`.

All failures map to `FaucetError::Auth` with a message that includes the
underlying SDK / I/O error.

## License

Dual-licensed under MIT and Apache-2.0, per the workspace `license` field.
