// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod stub;

mod builder;
mod connector;
mod object_descriptor;
mod pending_range;
mod range_reader;
mod redirect;
mod resume_redirect;
mod retry_redirect;
mod transport;
mod worker;

pub use crate::request_options::RequestOptions;
use crate::storage::client::ClientBuilder;
pub use builder::OpenObject;
#[allow(unused_imports)]
pub use object_descriptor::ObjectDescriptor;
pub use range_reader::RangeReader;
use transport::ObjectDescriptorTransport;

#[derive(Clone, Debug)]
pub struct Bidi {
    client: gaxi::grpc::Client,
    options: RequestOptions,
}

impl Bidi {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) async fn new(
        builder: super::client::ClientBuilder,
    ) -> gax::client_builder::Result<Self> {
        let (client_config, options) = builder.into_client_config();
        let client = gaxi::grpc::Client::new(client_config, super::DEFAULT_HOST).await?;
        Ok(Self { client, options })
    }

    pub fn open_object<B, O>(&self, bucket: B, object: O) -> OpenObject
    where
        B: Into<String>,
        O: Into<String>,
    {
        OpenObject::new(
            bucket.into(),
            object.into(),
            self.client.clone(),
            self.options.clone(),
        )
    }
}

impl super::client::ClientBuilder {
    pub async fn build_bidi(self) -> gax::client_builder::Result<Bidi> {
        Bidi::new(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::google::storage::v2::{BidiReadHandle, BidiReadObjectRedirectedError};
    use crate::request_options::RequestOptions;
    use auth::credentials::anonymous::Builder as Anonymous;
    use gax::error::rpc::{Code, Status};
    use prost::Message as _;
    use std::sync::Arc;

    #[tokio::test]
    async fn create_new_client() -> anyhow::Result<()> {
        let _client = Bidi::builder()
            .with_credentials(Anonymous::new().build())
            .build_bidi()
            .await?;
        Ok(())
    }

    pub(super) fn redirect_handle() -> BidiReadHandle {
        BidiReadHandle {
            handle: bytes::Bytes::from_static(b"test-handle-redirect"),
        }
    }

    pub(super) fn redirect_status(routing: &str) -> tonic::Status {
        use crate::google::rpc::Status as RpcStatus;
        let redirect = BidiReadObjectRedirectedError {
            routing_token: Some(routing.to_string()),
            read_handle: Some(redirect_handle()),
        };
        let redirect = prost_types::Any::from_msg(&redirect).unwrap();
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "redirect".to_string(),
            details: vec![redirect],
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        tonic::Status::with_details(tonic::Code::Aborted, "redirect", details)
    }

    pub(super) fn redirect_error(routing: &str) -> Error {
        gaxi::grpc::from_status::to_gax_error(redirect_status(routing))
    }

    pub(super) fn permanent_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("uh-oh"),
        )
    }

    pub(super) fn transient_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try-again"),
        )
    }

    pub(super) fn test_options() -> RequestOptions {
        let mut options = RequestOptions::new();
        options.backoff_policy = Arc::new(test_backoff());
        options
    }

    fn test_backoff() -> impl gax::backoff_policy::BackoffPolicy {
        use std::time::Duration;
        gax::exponential_backoff::ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }
}
