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

mod active_read;
mod builder;
mod connector;
mod normalized_range;
mod range_reader;
mod redirect;
mod remaining_range;
mod requested_range;
mod resume_redirect;
mod retry_redirect;
pub(crate) mod stub;
mod transport;
mod worker;

use crate::google::storage::v2::{BidiReadObjectRequest, BidiReadObjectResponse};
use crate::request_options::RequestOptions;
use tokio::sync::mpsc::Receiver;

pub use builder::OpenObject;

/// A trait to mock `tonic::Streaming<T>` in the unit tests.
///
/// This is not a public trait, we only need this for our own testing.
pub trait TonicStreaming: std::fmt::Debug + Send + 'static {
    fn next_message(
        &mut self,
    ) -> impl Future<Output = tonic::Result<Option<BidiReadObjectResponse>>> + Send;
}

/// Implement [TonicStreaming] for the one `tonic::Streaming<T>` we use.
impl TonicStreaming for tonic::Streaming<BidiReadObjectResponse> {
    async fn next_message(&mut self) -> tonic::Result<Option<BidiReadObjectResponse>> {
        self.message().await
    }
}

/// A trait to mock `gaxi::grpc::Client` in the unit tests.
///
/// This is not a public trait, we only need this for our own testing.
pub trait Client: std::fmt::Debug + Send + 'static {
    type Stream: Sized;
    fn start(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiReadObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> impl Future<Output = crate::Result<tonic::Result<tonic::Response<Self::Stream>>>> + Send;
}

impl Client for gaxi::grpc::Client {
    type Stream = tonic::codec::Streaming<BidiReadObjectResponse>;
    async fn start(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiReadObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<tonic::Result<tonic::Response<Self::Stream>>> {
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
mod mocks;

#[cfg(test)]
mod tests {
    use crate::Error;
    use crate::google::storage::v2::{
        BidiReadHandle, BidiReadObjectRedirectedError, ReadRange as ProtoRange,
    };
    use crate::request_options::RequestOptions;
    use gax::error::rpc::{Code, Status};
    use prost::Message as _;
    use std::sync::Arc;

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

    pub(super) fn proto_range(offset: i64, length: i64) -> ProtoRange {
        ProtoRange {
            read_offset: offset,
            read_length: length,
            ..ProtoRange::default()
        }
    }

    pub(super) fn proto_range_id(offset: i64, length: i64, id: i64) -> ProtoRange {
        let mut range = proto_range(offset, length);
        range.read_id = id;
        range
    }
}
