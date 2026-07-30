//! `faucet mcp` — an MCP server over stdio (issue #420).
//!
//! Reads newline-delimited JSON-RPC 2.0 messages from stdin and writes each
//! response to stdout (one JSON object per line), dispatching through the
//! shared [`crate::mcp`] handler. stdio is local-trust: there is no bearer /
//! RBAC layer, so mutating tools require the explicit `--allow-mutations` flag.
//!
//! Logs go to **stderr** (installed in `run_main`) so the stdout JSON-RPC
//! stream stays clean.

use crate::cli::McpArgs;
use crate::error::CliResult;
use crate::mcp::{McpContext, serve_stdio};
use tokio::io::BufReader;

pub async fn run(args: McpArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    // stdio mode has no shared `auth:` catalog; inline connector auth still
    // works for configs passed to preview/validate/run_pipeline.
    let auth = crate::auth_catalog::build_auth_catalog(None)?;
    let ctx = McpContext::new(auth, args.allow_mutations);

    tracing::info!(
        allow_mutations = args.allow_mutations,
        "faucet mcp stdio server started"
    );

    let mut stdout = tokio::io::stdout();
    serve_stdio(&ctx, BufReader::new(tokio::io::stdin()), &mut stdout).await?;

    tracing::info!("faucet mcp stdio server stopped (stdin closed)");
    Ok(())
}
