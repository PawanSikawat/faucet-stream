//! Generic range partitioning (#479) — split one matrix row into N independent
//! invocations, each scoped to a chunk of a range via `${partition.*}` tokens.
//!
//! This is source-agnostic by construction: substitution walks the string leaves
//! of a connector config, so a REST URL, a SQL `WHERE`, an object prefix and a
//! Mongo filter all work with no connector code. See the [`mod@plan`] module for
//! the mechanism and [`spec`] for why the kinds are a tagged enum.

pub mod plan;
pub mod spec;

pub use plan::{PartitionChunk, plan, references_partition, substitute};
pub use spec::PartitionSpec;
