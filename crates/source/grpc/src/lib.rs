//! # faucet-source-grpc
//!
//! A config-driven gRPC source that uses protobuf reflection to call
//! any gRPC service dynamically and return records as JSON.
//!
//! Requires a compiled `FileDescriptorSet` (produced by
//! `protoc --descriptor_set_out=descriptor.bin`).

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{GrpcAuth, GrpcStreamConfig};
pub use stream::GrpcStream;
