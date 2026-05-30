//! AWS Secrets Manager resolver (`${aws-sm:<name-or-ARN>[#field]}`).
//!
//! Auth: the standard `aws-config` default credential provider chain (env,
//! profile, instance profile, web identity). The SDK client is built lazily
//! on first resolve so a config that never references aws-sm pays nothing.

use super::{extract_field, split_field, SecretResolver};
use crate::error::{CliError, CliResult};
use async_trait::async_trait;
use tokio::sync::OnceCell;

pub struct AwsSmResolver {
    client: OnceCell<aws_sdk_secretsmanager::Client>,
}

impl Default for AwsSmResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsSmResolver {
    pub fn new() -> Self {
        Self {
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> &aws_sdk_secretsmanager::Client {
        self.client
            .get_or_init(|| async {
                let conf =
                    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                aws_sdk_secretsmanager::Client::new(&conf)
            })
            .await
    }
}

#[async_trait]
impl SecretResolver for AwsSmResolver {
    fn scheme(&self) -> &'static str {
        "aws-sm"
    }

    async fn resolve(&self, reference: &str) -> CliResult<String> {
        let (secret_id, field) = split_field(reference);
        let out = self
            .client()
            .await
            .get_secret_value()
            .secret_id(secret_id)
            .send()
            .await
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "aws-sm".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        let body = out.secret_string().ok_or_else(|| CliError::SecretNotFound {
            scheme: "aws-sm".into(),
            reference: reference.into(),
        })?;
        match field {
            Some(f) => extract_field("aws-sm", reference, body, f),
            None => Ok(body.to_owned()),
        }
    }
}
