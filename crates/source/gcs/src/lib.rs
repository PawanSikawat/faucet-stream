//! Google Cloud Storage source connector.
//!
//! See the crate-level README for usage and config-field reference.

mod config;
mod stream;

pub use config::{GcsFileFormat, GcsSourceConfig};
pub use faucet_gcs_common::GcsCredentials;
pub use stream::GcsSource;
