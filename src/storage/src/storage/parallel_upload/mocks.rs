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

use super::{Client, GaxRequestOptions, Receiver, TonicStreaming};
use crate::google::storage::v2::{
    AbortObjectWriteSessionRequest, BidiWriteSegmentRequest, BidiWriteSegmentResponse,
    CreateObjectWriteSessionRequest, FinalizeObjectWriteSessionRequest, Object, ObjectWriteSession,
};
use gaxi::grpc::tonic::{Extensions, Response as TonicResponse, Result as TonicResult};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

// `mockall` mocks are not `Clone`, so wrap `MockTestClient` in `Arc` for concurrent callers.
#[derive(Clone, Debug)]
pub(crate) struct SharedMockClient(pub(crate) Arc<MockTestClient>);

impl SharedMockClient {
    pub(crate) fn new(mock: MockTestClient) -> Self {
        Self(Arc::new(mock))
    }
}

impl Client for SharedMockClient {
    type Stream = MockStream;

    async fn create_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: CreateObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResponse<ObjectWriteSession>> {
        self.0.create_object_write_session(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
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
        self.0.bidi_write_segment(
            extensions,
            path,
            rx,
            options,
            api_client_header,
            request_params,
        )
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
        self.0.finalize_object_write_session(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
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
        self.0.abort_object_write_session(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
    }
}

impl TonicStreaming for Receiver<TonicResult<BidiWriteSegmentResponse>> {
    async fn next_message(&mut self) -> TonicResult<Option<BidiWriteSegmentResponse>> {
        self.recv().await.transpose()
    }
}

#[mockall::automock]
pub(crate) trait TestClient: std::fmt::Debug {
    fn create_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: CreateObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResponse<ObjectWriteSession>>;

    fn bidi_write_segment(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiWriteSegmentRequest>,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResult<TonicResponse<MockStream>>>;

    fn finalize_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: FinalizeObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResponse<Object>>;

    fn abort_object_write_session(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        request: AbortObjectWriteSessionRequest,
        options: GaxRequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResponse<()>>;
}

pub(crate) type MockStream = Receiver<TonicResult<BidiWriteSegmentResponse>>;
pub(crate) type MockStreamSender = Sender<TonicResult<BidiWriteSegmentResponse>>;
