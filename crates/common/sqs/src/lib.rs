#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-sqs
//!
//! Shared configuration types for the faucet-stream AWS SQS source
//! (`faucet-source-sqs`) and sink (`faucet-sink-sqs`) connectors: the
//! [`SqsCredentials`] auth enum and the [`build_client`] helper that assembles
//! an `aws_sdk_sqs::Client` from region / endpoint / credential settings. Both
//! connector crates re-export these so end-user imports do not change.

mod auth;

pub use auth::{SqsCredentials, build_client};
