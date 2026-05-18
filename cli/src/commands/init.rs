//! `faucet init` — scaffold a starter `pipeline.yaml` in the new matrix-aware
//! shape.

use crate::cli::InitArgs;
use crate::error::{CliError, CliResult};

const TEMPLATE: &str = r#"version: 1
name: {NAME}

# The base pipeline. Every matrix row (below) is deep-merged into this.
# Even with no matrix block, this section runs once on its own.
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /things
      method: GET
      auth:
        type: ApiKey
        header: Authorization
        value: Bearer ${env:API_TOKEN}
      query_params: {}
      pagination:
        type: None
      max_retries: 3
      retry_backoff: 1
      tolerated_http_errors: []
      replication_method:
        type: FullTable
      primary_keys: []
      partitions: []
      schema_sample_size: 100

  transforms:
    - type: snake_case

  sink:
    type: jsonl
    config:
      path: ./out.jsonl

  state:
    type: file
    config:
      path: ./.faucet-state

# Optional. Each row is deep-merged into `pipeline:` above. Use `parent:` to
# fan one row out per record produced by another row, and `${row_id.field}`
# in any string to interpolate parent fields at runtime.
#
# matrix:
#   - id: users
#     source: { config: { path: /v1/users } }
#   - id: posts
#     parent: users
#     source: { config: { path: "/v1/users/${users.id}/posts" } }

# Optional execution controls.
# execution:
#   max_concurrent: 4
#   on_error: continue   # or `stop`
"#;

/// Execute the `init` subcommand.
pub async fn run(args: InitArgs) -> CliResult<()> {
    if args.output.exists() && !args.force {
        return Err(CliError::ScaffoldExists {
            path: args.output.clone(),
        });
    }
    let body = TEMPLATE.replace("{NAME}", &args.name);
    std::fs::write(&args.output, body)?;
    println!("wrote {}", args.output.display());
    Ok(())
}
