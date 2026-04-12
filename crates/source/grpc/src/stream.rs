//! gRPC stream executor using dynamic protobuf messages.

use crate::config::{GrpcAuth, GrpcStreamConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use prost::Message;
use prost::bytes::Bytes;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use serde_json::Value;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::Channel;

/// A configured gRPC source that uses protobuf reflection to call
/// any gRPC method and return JSON records.
pub struct GrpcStream {
    config: GrpcStreamConfig,
    pool: DescriptorPool,
}

impl GrpcStream {
    /// Create a new gRPC stream. Loads the `FileDescriptorSet` from disk.
    pub fn new(config: GrpcStreamConfig) -> Result<Self, FaucetError> {
        let descriptor_bytes = std::fs::read(&config.descriptor_set_path).map_err(|e| {
            FaucetError::Config(format!(
                "failed to read descriptor set at {}: {e}",
                config.descriptor_set_path.display()
            ))
        })?;

        let pool = DescriptorPool::decode(Bytes::from(descriptor_bytes))
            .map_err(|e| FaucetError::Config(format!("failed to parse FileDescriptorSet: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Fetch all records by calling the configured gRPC method.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.fetch_resolved(
            &self.config.endpoint,
            &self.config.service_name,
            &self.config.method_name,
            &self.config.request,
        )
        .await
    }

    /// Internal fetch with resolved (context-substituted) parameters.
    async fn fetch_resolved(
        &self,
        endpoint: &str,
        service_name: &str,
        method_name: &str,
        request: &Value,
    ) -> Result<Vec<Value>, FaucetError> {
        let full_method = format!("/{service_name}/{method_name}");

        // Look up the method descriptor.
        let service = self.pool.get_service_by_name(service_name).ok_or_else(|| {
            FaucetError::Config(format!(
                "service '{service_name}' not found in descriptor set",
            ))
        })?;

        let method = service
            .methods()
            .find(|m| m.name() == method_name)
            .ok_or_else(|| {
                FaucetError::Config(format!(
                    "method '{method_name}' not found in service '{service_name}'",
                ))
            })?;

        // Build the request message from JSON.
        let input_desc = method.input();
        let request_msg = DynamicMessage::deserialize(input_desc, request)
            .map_err(|e| FaucetError::Config(format!("failed to build request message: {e}")))?;

        // Connect to the gRPC endpoint.
        let use_tls = self
            .config
            .tls
            .unwrap_or_else(|| endpoint.starts_with("https"));

        let channel_endpoint = Channel::from_shared(endpoint.to_string())
            .map_err(|e| FaucetError::Url(format!("invalid gRPC endpoint: {e}")))?;

        let channel: Channel = if use_tls {
            channel_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .map_err(|e| FaucetError::Config(format!("TLS config failed: {e}")))?
                .connect()
                .await
                .map_err(|e| FaucetError::Config(format!("gRPC connect failed: {e}")))?
        } else {
            channel_endpoint
                .connect()
                .await
                .map_err(|e| FaucetError::Config(format!("gRPC connect failed: {e}")))?
        };

        let output_desc = method.output();

        let mut grpc_client = tonic::client::Grpc::new(channel);
        grpc_client
            .ready()
            .await
            .map_err(|e| FaucetError::Config(format!("gRPC channel not ready: {e}")))?;

        let codec = DynamicCodec::new(output_desc);
        let path = tonic::codegen::http::uri::PathAndQuery::from_maybe_shared(full_method)
            .map_err(|e| FaucetError::Url(format!("invalid method path: {e}")))?;

        let mut request = tonic::Request::new(request_msg.encode_to_vec());

        // Apply auth metadata.
        match &self.config.auth {
            GrpcAuth::None => {}
            GrpcAuth::Bearer(token) => {
                let val: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                    format!("Bearer {token}")
                        .parse()
                        .map_err(|e| FaucetError::Auth(format!("invalid bearer token: {e}")))?;
                request.metadata_mut().insert("authorization", val);
            }
            GrpcAuth::Metadata(pairs) => {
                for (key, value) in pairs {
                    let val: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = value
                        .parse()
                        .map_err(|e| FaucetError::Auth(format!("invalid metadata value: {e}")))?;
                    let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> = key
                        .parse()
                        .map_err(|e| FaucetError::Auth(format!("invalid metadata key: {e}")))?;
                    request.metadata_mut().insert(key, val);
                }
            }
        }

        let response: tonic::Response<DynamicMessage> = grpc_client
            .unary(request, path, codec)
            .await
            .map_err(|e| FaucetError::Sink(format!("gRPC call failed: {e}")))?;

        let resp_msg = response.into_inner();

        // Convert response to JSON.
        let serialize_opts = SerializeOptions::new().stringify_64_bit_integers(false);
        let json_value = resp_msg
            .serialize_with_options(serde_json::value::Serializer, &serialize_opts)
            .map_err(|e| {
                FaucetError::Transform(format!("failed to serialize gRPC response to JSON: {e}"))
            })?;

        // Extract records using JSONPath if configured.
        let records =
            faucet_core::util::extract_records(&json_value, self.config.records_path.as_deref())?;

        tracing::info!(records = records.len(), "gRPC fetch complete");
        Ok(records)
    }
}

#[async_trait]
impl faucet_core::Source for GrpcStream {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            return GrpcStream::fetch_all(self).await;
        }

        let endpoint = faucet_core::util::substitute_context(&self.config.endpoint, context);
        let service_name =
            faucet_core::util::substitute_context(&self.config.service_name, context);
        let method_name = faucet_core::util::substitute_context(&self.config.method_name, context);

        let request = {
            let s = serde_json::to_string(&self.config.request)
                .map_err(|e| FaucetError::Config(format!("failed to serialize request: {e}")))?;
            let s = faucet_core::util::substitute_context(&s, context);
            serde_json::from_str(&s).map_err(|e| {
                FaucetError::Config(format!("failed to parse substituted request: {e}"))
            })?
        };

        self.fetch_resolved(&endpoint, &service_name, &method_name, &request)
            .await
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(GrpcStreamConfig))
            .expect("schema serialization")
    }
}

// ── Dynamic Codec ───────────────────────────────────────────────────────────

/// A tonic codec that encodes raw bytes and decodes into `DynamicMessage`.
struct DynamicCodec {
    output_desc: prost_reflect::MessageDescriptor,
}

impl DynamicCodec {
    fn new(output_desc: prost_reflect::MessageDescriptor) -> Self {
        Self { output_desc }
    }
}

impl Codec for DynamicCodec {
    type Encode = Vec<u8>;
    type Decode = DynamicMessage;
    type Encoder = RawEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            desc: self.output_desc.clone(),
        }
    }
}

struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        use prost::bytes::BufMut;
        buf.put_slice(&item);
        Ok(())
    }
}

struct DynamicDecoder {
    desc: prost_reflect::MessageDescriptor,
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = tonic::Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        use prost::bytes::Buf;
        if !buf.has_remaining() {
            return Ok(None);
        }
        let bytes = buf.copy_to_bytes(buf.remaining());
        let msg = DynamicMessage::decode(self.desc.clone(), bytes)
            .map_err(|e| tonic::Status::internal(format!("protobuf decode error: {e}")))?;
        Ok(Some(msg))
    }
}
