// Copyright 2026 Google LLC
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

//! Internal traits and types for Appendable Object Write (Bidi Write).

mod redirect;
mod retry_redirect;
pub(crate) mod stub;

use crate::google::storage::v2::{BidiWriteObjectRequest, BidiWriteObjectResponse};
use crate::request_options::RequestOptions;
use gaxi::grpc::tonic::{Extensions, Response as TonicResponse, Result as TonicResult};
use std::future::Future;
use tokio::sync::mpsc::Receiver;

#[cfg(google_cloud_unstable_grpc_rust)]
pub(crate) type GrpcClient = gaxi::grpc::GrpcRustClient;
#[cfg(google_cloud_unstable_grpc_rust)]
pub(crate) type GrpcStream = gaxi::grpc::GrpcRustStreaming<BidiWriteObjectResponse>;

#[cfg(not(google_cloud_unstable_grpc_rust))]
pub(crate) type GrpcClient = gaxi::grpc::Client;
#[cfg(not(google_cloud_unstable_grpc_rust))]
pub(crate) type GrpcStream = gaxi::grpc::tonic::Streaming<BidiWriteObjectResponse>;

/// A trait to mock `Streaming<T>` in the unit tests.
///
/// This is not a public trait, we only need this for our own testing.
#[allow(dead_code)]
pub(crate) trait TonicStreaming: std::fmt::Debug + Send + 'static {
    fn next_message(
        &mut self,
    ) -> impl Future<Output = TonicResult<Option<BidiWriteObjectResponse>>> + Send;
}

/// Implement [TonicStreaming] for the one `Streaming<T>`` we use.
impl TonicStreaming for GrpcStream {
    async fn next_message(&mut self) -> TonicResult<Option<BidiWriteObjectResponse>> {
        self.message().await
    }
}

/// A trait to mock `gaxi::grpc::Client` in the unit tests.
///
/// This is not a public trait, we only need this for our own testing.
#[allow(dead_code)]
pub(crate) trait Client: std::fmt::Debug + Send + 'static {
    type Stream: Sized;
    fn start(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiWriteObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> impl Future<Output = crate::Result<TonicResult<TonicResponse<Self::Stream>>>> + Send;
}

impl Client for GrpcClient {
    type Stream = GrpcStream;
    async fn start(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiWriteObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResult<TonicResponse<Self::Stream>>> {
        let request = tokio_stream::wrappers::ReceiverStream::new(rx);
        self.bidi_stream_with_status(
            extensions,
            path,
            request,
            options.gax(),
            api_client_header,
            request_params,
        )
        .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::Error;
    use crate::google::storage::v2::{BidiWriteHandle, BidiWriteObjectRedirectedError};
    use crate::request_options::RequestOptions;
    use gaxi::grpc::tonic::{Code as TonicCode, Status as TonicStatus};
    use google_cloud_gax::error::rpc::{Code, Status};
    use prost::Message as _;
    use std::sync::Arc;

    pub(crate) fn redirect_handle() -> BidiWriteHandle {
        BidiWriteHandle {
            handle: bytes::Bytes::from_static(b"test-handle-redirect"),
        }
    }

    pub(crate) fn redirect_status(routing: &str) -> TonicStatus {
        use crate::google::rpc::Status as RpcStatus;
        let redirect = BidiWriteObjectRedirectedError {
            routing_token: Some(routing.to_string()),
            write_handle: Some(redirect_handle()),
            generation: Some(42),
        };
        let redirect = prost_types::Any::from_msg(&redirect).unwrap();
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "redirect".to_string(),
            details: vec![redirect],
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        TonicStatus::with_details(TonicCode::Aborted, "redirect", details)
    }

    pub(crate) fn redirect_error(routing: &str) -> Error {
        gaxi::grpc::from_status::to_gax_error(redirect_status(routing))
    }

    pub(crate) fn permanent_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("uh-oh"),
        )
    }

    pub(crate) fn transient_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try-again"),
        )
    }

    #[allow(dead_code)]
    pub(crate) fn test_options() -> RequestOptions {
        let mut options = RequestOptions::new();
        options.backoff_policy = Arc::new(test_backoff());
        options
    }

    #[allow(dead_code)]
    fn test_backoff() -> impl google_cloud_gax::backoff_policy::BackoffPolicy {
        use std::time::Duration;
        google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }
}
