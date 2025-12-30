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

use super::stub::Stub;
use crate::Result;
use crate::generated::gapic_dataplane::stub::dynamic::Subscriber as GapicStub;
pub(crate) use crate::generated::gapic_dataplane::transport::Subscriber as Transport;
use crate::google::pubsub::v1::{StreamingPullRequest, StreamingPullResponse};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;

mod info {
    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    lazy_static::lazy_static! {
        pub(crate) static ref X_GOOG_API_CLIENT_HEADER: String = {
            let ac = gaxi::api_header::XGoogApiClient{
                name:          NAME,
                version:       VERSION,
                library_type:  gaxi::api_header::GCCL,
            };
            ac.grpc_header_value()
        };
    }
}

#[async_trait::async_trait]
impl Stub for Transport {
    type Stream = tonic::codec::Streaming<StreamingPullResponse>;
    async fn streaming_pull(
        &self,
        request_rx: Receiver<StreamingPullRequest>,
        options: gax::options::RequestOptions,
    ) -> Result<tonic::Response<Self::Stream>> {
        let request = ReceiverStream::new(request_rx);
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.pubsub.v1.Subscriber",
                "StreamingPull",
            ));
            e
        };
        let path =
            http::uri::PathAndQuery::from_static("/google.pubsub.v1.Subscriber/StreamingPull");
        self.inner
            .bidi_stream(
                extensions,
                path,
                request,
                options,
                &info::X_GOOG_API_CLIENT_HEADER,
                "",
            )
            .await
    }

    async fn modify_ack_deadline(
        &self,
        req: crate::model::ModifyAckDeadlineRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<()>> {
        GapicStub::modify_ack_deadline(self, req, options).await
    }

    async fn acknowledge(
        &self,
        req: crate::model::AcknowledgeRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<()>> {
        GapicStub::acknowledge(self, req, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::pubsub::v1::ReceivedMessage;
    use auth::credentials::anonymous::Builder as Anonymous;
    use pubsub_grpc_mock::google::pubsub::v1;
    use pubsub_grpc_mock::{MockSubscriber, start};

    async fn test_transport(endpoint: String) -> anyhow::Result<Transport> {
        let mut config = gaxi::options::ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        config.endpoint = Some(endpoint);
        Ok(Transport::new(config).await?)
    }

    // Both crates have their own copies of the protos. We can just serialize
    // then deserialize to convert between the two, as performance is not a
    // concern for these unit tests.
    fn convert(pb: &StreamingPullResponse) -> v1::StreamingPullResponse {
        use prost::Message;
        let v = pb.encode_to_vec();
        v1::StreamingPullResponse::decode(v.as_slice()).expect("encoding is always valid.")
    }

    #[tokio::test]
    async fn streaming_pull() -> anyhow::Result<()> {
        let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);
        let expected = StreamingPullResponse {
            received_messages: vec![ReceivedMessage {
                ack_id: "test-ack-id".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        response_tx.send(Ok(convert(&expected))).await?;

        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);
        request_tx.send(StreamingPullRequest::default()).await?;

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let mut stream = Stub::streaming_pull(
            &transport,
            request_rx,
            gax::options::RequestOptions::default(),
        )
        .await?
        .into_inner();

        use futures::StreamExt;
        assert_eq!(stream.next().await.transpose()?, Some(expected));

        drop(response_tx);
        assert_eq!(stream.next().await.transpose()?, None);

        Ok(())
    }

    #[tokio::test]
    async fn modify_ack_deadline() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_modify_ack_deadline()
            .return_once(|_| Ok(tonic::Response::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let _ = Stub::modify_ack_deadline(
            &transport,
            crate::model::ModifyAckDeadlineRequest::new(),
            gax::options::RequestOptions::default(),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn acknowledge() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_acknowledge()
            .return_once(|_| Ok(tonic::Response::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let _ = Stub::acknowledge(
            &transport,
            crate::model::AcknowledgeRequest::new(),
            gax::options::RequestOptions::default(),
        )
        .await?;
        Ok(())
    }
}
