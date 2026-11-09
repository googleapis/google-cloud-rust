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

//! Internal traits and types for parallel uploads.

use crate::google::storage::v2::{
    AbortObjectWriteSessionRequest, BidiWriteSegmentRequest, BidiWriteSegmentResponse,
    CreateObjectWriteSessionRequest, FinalizeObjectWriteSessionRequest, Object, ObjectWriteSession,
};
use gaxi::grpc::tonic::{Extensions, Response as TonicResponse, Result as TonicResult, Streaming};
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use std::future::Future;
use tokio::sync::mpsc::Receiver;

pub(crate) type GrpcClient = gaxi::grpc::Client;
pub(crate) type GrpcStream = Streaming<BidiWriteSegmentResponse>;

/// A trait to mock `Streaming<BidiWriteSegmentResponse>` in unit tests.
pub(crate) trait TonicStreaming: std::fmt::Debug + Send + 'static {
    fn next_message(
        &mut self,
    ) -> impl Future<Output = TonicResult<Option<BidiWriteSegmentResponse>>> + Send;
}

impl TonicStreaming for GrpcStream {
    async fn next_message(&mut self) -> TonicResult<Option<BidiWriteSegmentResponse>> {
        self.message().await
    }
}

/// A trait to mock `gaxi::grpc::Client` for parallel upload session RPCs in unit tests.
pub(crate) trait Client: Clone + std::fmt::Debug + Send + Sync + 'static {
    type Stream: TonicStreaming;

    fn create_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: CreateObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> impl Future<Output = crate::Result<TonicResponse<ObjectWriteSession>>> + Send;

    fn bidi_write_segment(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiWriteSegmentRequest>,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> impl Future<Output = crate::Result<TonicResult<TonicResponse<Self::Stream>>>> + Send;

    fn finalize_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: FinalizeObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> impl Future<Output = crate::Result<TonicResponse<Object>>> + Send;

    fn abort_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: AbortObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> impl Future<Output = crate::Result<TonicResponse<()>>> + Send;
}

impl Client for GrpcClient {
    type Stream = GrpcStream;

    async fn create_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: CreateObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResponse<ObjectWriteSession>> {
        self.execute(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
        .await
    }

    async fn bidi_write_segment(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiWriteSegmentRequest>,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResult<TonicResponse<Self::Stream>>> {
        let request = tokio_stream::wrappers::ReceiverStream::new(rx);
        self.bidi_stream_with_status(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
        .await
    }

    async fn finalize_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: FinalizeObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResponse<Object>> {
        self.execute(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
        .await
    }

    async fn abort_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: AbortObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResponse<()>> {
        self.execute(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
        .await
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod mocks;

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod tests {
    use super::mocks::SharedMockClient;
    use super::*;
    use crate::Error;
    use crate::request_options::RequestOptions;
    use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
    use gaxi::grpc::tonic::{GrpcMethod, Status as TonicStatus};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::retry_policy::NeverRetry;
    use http::uri::PathAndQuery;
    use static_assertions::assert_impl_all;
    use std::sync::Arc;
    use std::time::Duration;

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

    pub(crate) fn transient_status() -> TonicStatus {
        TonicStatus::unavailable("try-again")
    }

    pub(crate) fn test_options() -> RequestOptions {
        let mut options = RequestOptions::new();
        options.backoff_policy = Arc::new(test_backoff());
        options
    }

    fn test_backoff() -> impl google_cloud_gax::backoff_policy::BackoffPolicy {
        google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }

    fn method_extensions(method: &'static str) -> Extensions {
        let mut extensions = Extensions::new();
        extensions.insert(GrpcMethod::new("google.storage.v2.Storage", method));
        extensions
    }

    #[test]
    fn assertions() {
        assert_impl_all!(GrpcClient: Client, Clone, std::fmt::Debug, Send, Sync);
        assert_impl_all!(SharedMockClient: Client, Clone, std::fmt::Debug, Send, Sync);
    }

    #[tokio::test]
    async fn grpc_client_bad_endpoint() -> anyhow::Result<()> {
        let mut config = gaxi::options::ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        let client = GrpcClient::new(config, "http://127.0.0.1:1").await?;

        let mut options = test_options();
        options.retry_policy = Arc::new(NeverRetry);

        let err = client
            .create_object_write_session(
                method_extensions("CreateObjectWriteSession"),
                PathAndQuery::from_static("/google.storage.v2.Storage/CreateObjectWriteSession"),
                CreateObjectWriteSessionRequest::default(),
                options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                "bucket=projects%2F_%2Fbuckets%2Fb",
            )
            .await
            .unwrap_err();
        assert!(err.is_connect(), "{err:?}");

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let err = client
            .bidi_write_segment(
                method_extensions("BidiWriteSegment"),
                PathAndQuery::from_static("/google.storage.v2.Storage/BidiWriteSegment"),
                rx,
                options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                "bucket=projects%2F_%2Fbuckets%2Fb",
            )
            .await?
            .map_err(gaxi::grpc::from_status::to_gax_error)
            .unwrap_err();
        assert!(err.is_connect(), "{err:?}");

        let err = client
            .finalize_object_write_session(
                method_extensions("FinalizeObjectWriteSession"),
                PathAndQuery::from_static("/google.storage.v2.Storage/FinalizeObjectWriteSession"),
                FinalizeObjectWriteSessionRequest::default(),
                options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                "bucket=projects%2F_%2Fbuckets%2Fb",
            )
            .await
            .unwrap_err();
        assert!(err.is_connect(), "{err:?}");

        let err = client
            .abort_object_write_session(
                method_extensions("AbortObjectWriteSession"),
                PathAndQuery::from_static("/google.storage.v2.Storage/AbortObjectWriteSession"),
                AbortObjectWriteSessionRequest::default(),
                options.gax(),
                &X_GOOG_API_CLIENT_HEADER,
                "bucket=projects%2F_%2Fbuckets%2Fb",
            )
            .await
            .unwrap_err();
        assert!(err.is_connect(), "{err:?}");

        Ok(())
    }
}
