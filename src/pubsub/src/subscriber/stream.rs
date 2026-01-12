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

use super::keepalive;
use super::stub::Stub;
use crate::google::pubsub::v1::StreamingPullRequest;
use crate::{Error, Result};
use gax::options::RequestOptions;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Open a stream for the `StreamingPull` RPC.
///
/// This returns the stream and a Sender for feeding the stream writes.
pub(super) async fn open_stream<T>(
    inner: Arc<T>,
    initial_req: StreamingPullRequest,
    shutdown: CancellationToken,
) -> Result<<T as Stub>::Stream>
where
    T: Stub,
{
    // The only writes we perform are keepalives, which are sent so infrequently
    // that we don't fear any back pressure on this channel.
    let (request_tx, request_rx) = mpsc::channel(1);
    request_tx.send(initial_req).await.map_err(Error::io)?;

    // Start the keepalive task **before** we open the stream.
    //
    // The future returned by tonic does not yield until the first response has
    // been returned on the stream.[^1]
    //
    // If we do not set up keepalives first, Pub/Sub will close the stream for
    // being idle for ~90s, leading to unnecessary retries.
    //
    // [1]: https://github.com/hyperium/tonic/issues/515
    keepalive::spawn(request_tx, shutdown.clone());

    let stream = inner
        .streaming_pull(request_rx, RequestOptions::default())
        .await?
        .into_inner();

    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::super::keepalive::KEEPALIVE_PERIOD;
    use super::super::lease_state::tests::test_ids;
    use super::super::stub::TonicStreaming;
    use super::super::stub::tests::MockStub;
    use super::*;
    use crate::google::pubsub::v1::{ReceivedMessage, StreamingPullResponse};

    fn test_response(range: std::ops::Range<i32>) -> StreamingPullResponse {
        StreamingPullResponse {
            received_messages: test_ids(range)
                .into_iter()
                .map(|ack_id| ReceivedMessage {
                    ack_id,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn initial_request() -> StreamingPullRequest {
        StreamingPullRequest {
            subscription: "projects/my-project/subscriptions/my-subscription".to_string(),
            stream_ack_deadline_seconds: 10,
            ..Default::default()
        }
    }

    fn keepalive_request() -> StreamingPullRequest {
        StreamingPullRequest::default()
    }

    #[tokio::test(start_paused = true)]
    async fn success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);

        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .times(1)
            .return_once(move |_r, _o| Ok(tonic::Response::from(response_rx)));

        response_tx.send(Ok(test_response(1..10))).await?;
        response_tx.send(Ok(test_response(11..20))).await?;
        response_tx.send(Ok(test_response(21..30))).await?;
        drop(response_tx);

        let mut stream =
            open_stream(Arc::new(mock), initial_request(), CancellationToken::new()).await?;
        assert_eq!(stream.next_message().await?, Some(test_response(1..10)));
        assert_eq!(stream.next_message().await?, Some(test_response(11..20)));
        assert_eq!(stream.next_message().await?, Some(test_response(21..30)));
        assert_eq!(stream.next_message().await?, None);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn keepalives() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        // We use this channel to surface writes (requests) from outside our
        // mock stream.
        let (recover_writes_tx, mut recover_writes_rx) = mpsc::channel(1);

        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .times(1)
            .return_once(move |mut request_rx, _o| {
                tokio::spawn(async move {
                    // Note that this task stays alive as long as we hold
                    // `recover_writes_rx`.
                    while let Some(request) = request_rx.recv().await {
                        recover_writes_tx
                            .send(request)
                            .await
                            .expect("forwarding writes always succeeds");
                    }
                });
                Ok(tonic::Response::from(response_rx))
            });

        let shutdown = CancellationToken::new();
        let mut stream = open_stream(Arc::new(mock), initial_request(), shutdown.clone()).await?;

        // Verify the stream is seeded with the initial request.
        assert_eq!(recover_writes_rx.recv().await, Some(initial_request()));

        // Verify the stream performs keepalives, even if no messages have been yielded.
        tokio::time::advance(KEEPALIVE_PERIOD).await;
        assert_eq!(recover_writes_rx.recv().await, Some(keepalive_request()));

        // Verify the bidi nature of the stream.
        response_tx.send(Ok(test_response(1..10))).await?;
        assert_eq!(stream.next_message().await?, Some(test_response(1..10)));

        // Shutdown the keepalive task.
        shutdown.cancel();
        assert_eq!(recover_writes_rx.recv().await, None);

        Ok(())
    }

    #[tokio::test]
    async fn error() -> anyhow::Result<()> {
        let mut mock = MockStub::new();
        mock.expect_streaming_pull()
            .times(1)
            .return_once(|_, _| Err(Error::io("fail")));

        let err = open_stream(Arc::new(mock), initial_request(), CancellationToken::new())
            .await
            .expect_err("open_stream should fail");
        assert!(err.is_io(), "{err:?}");

        Ok(())
    }
}
