//! `faucet init` — scaffold a starter `pipeline.yaml`.

use crate::cli::InitArgs;
use crate::error::{CliError, CliResult};

const TEMPLATE: &str = r#"version: 1
name: {NAME}

# Pull JSON records from a REST API. Replace base_url/path with your own
# endpoint, and configure auth + pagination as needed. See:
#   faucet schema source rest
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /things
    method: GET
    # Auth — see `faucet schema source rest` for every supported variant.
    # ApiKey sends the value in a request header; swap to {type: Basic,
    # username, password} for HTTP basic, or {type: None} for no auth.
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

# Optional transforms applied to every record, in declaration order.
transforms:
  - type: snake_case

# Where the records go. Replace with bigquery/postgres/s3/etc as needed:
#   faucet list
sink:
  type: jsonl
  config:
    path: ./out.jsonl

# Optional. Tracks incremental-replication bookmarks across runs so the
# pipeline resumes from where it left off.
state:
  type: file
  config:
    path: ./.faucet-state
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
