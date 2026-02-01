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

use super::connector::Connector;
use super::tests::test_options;
use super::{Client, Receiver, RequestOptions, TonicStreaming};
use crate::google::storage::v2::{
    BidiReadObjectRequest, BidiReadObjectResponse, BidiReadObjectSpec,
};
use gaxi::grpc::tonic::{Extensions, Response as TonicResponse, Result as TonicResult};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

// mockall mocks are not `Clone` and we need a thing that can be cloned.
// The solution is to wrap the mock in a think that implements the right
// trait.
#[derive(Clone, Debug)]
pub struct SharedMockClient(pub(crate) Arc<MockTestClient>);

impl SharedMockClient {
    pub fn new(mock: MockTestClient) -> Self {
        Self(Arc::new(mock))
    }
}

impl Client for SharedMockClient {
    type Stream = MockStream;

    async fn start(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiReadObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResult<TonicResponse<Self::Stream>>> {
        self.0.start(
            extensions,
            path,
            rx,
            options,
            api_client_header,
            request_params,
        )
    }
}

impl TonicStreaming for Receiver<TonicResult<BidiReadObjectResponse>> {
    async fn next_message(&mut self) -> TonicResult<Option<BidiReadObjectResponse>> {
        self.recv().await.transpose()
    }
}

#[mockall::automock]
pub trait TestClient: std::fmt::Debug {
    fn start(
        &self,
        extensions: Extensions,
        path: http::uri::PathAndQuery,
        rx: Receiver<BidiReadObjectRequest>,
        options: &RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> crate::Result<TonicResult<TonicResponse<MockStream>>>;
}

pub type MockStream = Receiver<TonicResult<BidiReadObjectResponse>>;
pub type MockStreamSender = Sender<TonicResult<BidiReadObjectResponse>>;

pub fn mock_connector(mock: MockTestClient) -> Connector<SharedMockClient> {
    let client = SharedMockClient::new(mock);

    let spec = BidiReadObjectSpec {
        bucket: "projects/_/buckets/test-bucket".into(),
        object: "test-object".into(),
        ..BidiReadObjectSpec::default()
    };

    Connector::new(spec, test_options(), client.clone())
}

pub fn mock_stream() -> (MockStreamSender, MockStream) {
    tokio::sync::mpsc::channel(10)
}
