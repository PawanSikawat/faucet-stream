//! Generic range partitioning (#479) — split one matrix row into N independent
//! invocations, each scoped to a chunk of a range via `${partition.*}` tokens.
//!
//! This is source-agnostic by construction: substitution walks the string leaves
//! of a connector config, so a REST URL, a SQL `WHERE`, an object prefix and a
//! Mongo filter all work with no connector code. See the [`mod@plan`] module for
//! the mechanism and [`spec`] for why the kinds are a tagged enum.

pub mod plan;
pub mod probe;
pub mod spec;

pub use plan::{PartitionChunk, plan, references_partition, substitute};
pub use probe::{needs_probe, resolve_bounds};

/// Resolve every discoverable partition bound in `cfg` in place (#479).
///
/// Runs before `expand`, because planning needs concrete bounds and `expand` is
/// synchronous with no registry access. A config with no probes does no I/O.
pub async fn resolve_config_bounds(
    cfg: &mut crate::config::PipelineConfig,
    auth: &crate::auth_catalog::AuthCatalog,
) -> crate::error::CliResult<()> {
    if let Some(spec) = cfg.partition.clone() {
        cfg.partition = Some(resolve_bounds(&spec, auth).await?);
    }
    for row in &mut cfg.matrix {
        if let Some(spec) = row.partition.clone() {
            row.partition = Some(resolve_bounds(&spec, auth).await?);
        }
    }
    Ok(())
}
pub use spec::{BoundProbe, CountBound, IntBound, PartitionSpec};
