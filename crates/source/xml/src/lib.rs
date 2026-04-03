//! # faucet-source-xml
//!
//! A config-driven XML/SOAP API source with automatic XML-to-JSON conversion,
//! element-path record extraction, and pluggable authentication.

pub mod config;
pub mod convert;
pub mod serde_helpers;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{XmlAuth, XmlPagination, XmlStreamConfig};
pub use stream::XmlStream;
