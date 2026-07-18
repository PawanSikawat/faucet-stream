# faucet-common-pubsub

Shared Google Cloud Pub/Sub configuration for the
[faucet-stream](https://github.com/PawanSikawat/faucet-stream) Pub/Sub
connectors — `faucet-source-pubsub` and `faucet-sink-pubsub`. Both connector
crates depend on this crate and re-export its types, so end-user imports do not
change.

## Contents

- **`PubsubCredentials`** — the auth enum, serialized as the project-wide
  `{ type, config }` shape:
  - `{ type: application_default }` — Application Default Credentials.
  - `{ type: service_account_json_file, config: { path } }` — key file on disk.
  - `{ type: service_account_json_inline, config: { json } }` — inline key
    (pair with `${secret:…}` / `${env:…}` interpolation).
  - `{ type: anonymous }` — no credentials (for the emulator).
- **`PubsubConnection`** — flattened connection block: `project_id`,
  `endpoint`, `emulator_host`, `credentials`.
- **`build_client`** — assembles a `gcloud_pubsub::client::Client`, honouring
  `emulator_host` / `PUBSUB_EMULATOR_HOST` (auth skipped) or resolving
  credentials for real Pub/Sub.
- **`PubsubMessage`** — re-exported from `gcloud-googleapis` so connector code
  round-trips messages without adding that crate directly.

## Delivery semantics

Pub/Sub is **at-least-once**. Neither connector advertises exactly-once
delivery — pair the source with an upsert sink keyed on `message_id` when
replays must converge.

License: MIT OR Apache-2.0.
