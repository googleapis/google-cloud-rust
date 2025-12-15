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

use crate::Result;
use crate::google::pubsub::v1::{StreamingPullRequest, StreamingPullResponse};
use tokio::sync::mpsc::Receiver;

pub(crate) trait TonicStreaming: std::fmt::Debug + Send + 'static {
    fn next_message(
        &mut self,
    ) -> impl Future<Output = tonic::Result<Option<StreamingPullResponse>>> + Send;
}

/// An internal trait for mocking the transport layer.
#[async_trait::async_trait]
pub(crate) trait Stub: std::fmt::Debug + Send + Sync {
    type Stream: Sized;

    async fn streaming_pull(
        &self,
        request_rx: Receiver<StreamingPullRequest>,
        _options: gax::options::RequestOptions,
    ) -> Result<tonic::Response<Self::Stream>>;
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tokio::sync::mpsc::Receiver;

    type MockStream = Receiver<tonic::Result<StreamingPullResponse>>;

    // Allow us to mock a tonic stream in our unit tests, using an mpsc receiver
    impl TonicStreaming for MockStream {
        async fn next_message(&mut self) -> tonic::Result<Option<StreamingPullResponse>> {
            self.recv().await.transpose()
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub(crate) Stub {}
        #[async_trait::async_trait]
        impl Stub for Stub {
            type Stream = MockStream;
            async fn streaming_pull(
                &self,
                request_rx: Receiver<StreamingPullRequest>,
                _options: gax::options::RequestOptions,
            ) -> Result<tonic::Response<MockStream>>;
        }
    }
}
