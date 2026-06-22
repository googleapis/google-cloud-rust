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

//! Internal traits and types for Appendable Object Write (Bidi Write).

#[cfg(google_cloud_unstable_storage_bidi)]
pub(crate) mod stub;

use crate::google::storage::v2::{BidiWriteObjectRequest, BidiWriteObjectResponse};
use crate::request_options::RequestOptions;
use gaxi::grpc::tonic::{Extensions, Response as TonicResponse, Result as TonicResult, Streaming};
use std::future::Future;
use tokio::sync::mpsc::Receiver;

/// A trait to mock `Streaming<T>` in the unit tests.
///
/// This is not a public trait, we only need this for our own testing.
#[cfg(google_cloud_unstable_storage_bidi)]
#[allow(dead_code)]
pub(crate) trait TonicStreaming: std::fmt::Debug + Send + 'static {
    fn next_message(
        &mut self,
    ) -> impl Future<Output = TonicResult<Option<BidiWriteObjectResponse>>> + Send;
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl TonicStreaming for Streaming<BidiWriteObjectResponse> {
    async fn next_message(&mut self) -> TonicResult<Option<BidiWriteObjectResponse>> {
        self.message().await
    }
}

/// A trait to mock `gaxi::grpc::Client` in the unit tests.
///
/// This is not a public trait, we only need this for our own testing.
#[cfg(google_cloud_unstable_storage_bidi)]
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

#[cfg(google_cloud_unstable_storage_bidi)]
impl Client for gaxi::grpc::Client {
    type Stream = Streaming<BidiWriteObjectResponse>;
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
