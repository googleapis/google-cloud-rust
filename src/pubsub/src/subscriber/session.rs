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

use super::builder::StreamingPull;
use super::handler::{AckResult, AtLeastOnce, Handler};
use super::keepalive;
use super::lease_loop::LeaseLoop;
use super::lease_state::LeaseOptions;
use super::leaser::DefaultLeaser;
use super::stream::open_stream;
use super::stub::{Stub, TonicStreaming};
use super::transport::Transport;
use crate::google::pubsub::v1::StreamingPullRequest;
use crate::model::PubsubMessage;
use crate::{Error, Result};
use gaxi::grpc::from_status::to_gax_error;
use gaxi::prost::FromProto as _;
use std::collections::VecDeque;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::{CancellationToken, DropGuard};

/// Represents an open subscribe session.
///
/// This is a stream-like struct for serving messages to an application.
///
/// # Example
/// ```no_rust
/// # use google_cloud_pubsub::client::Subscriber;
/// # async fn sample(client: Subscriber) -> anyhow::Result<()> {
/// let mut session = client
///     .streaming_pull("projects/my-project/subscriptions/my-subscription")
///     .start()?;
/// while let Some((m, h)) = session.next().await.transpose()? {
///     println!("Received message m={m}");
///     h.ack();
/// }
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct Session {
    /// The bidirectional stream.
    stream: <Transport as Stub>::Stream,

    /// Applications ask for messages one at a time. Individual stream responses
    /// can contain multiple messages. We use `pool` to hold the extra messages
    /// while we wait to serve them to applications.
    ///
    /// A FIFO queue is necessary to preserve ordering.
    pool: VecDeque<(PubsubMessage, Handler)>,

    /// A sender for sending new messages from the stream into the lease
    /// management task.
    message_tx: UnboundedSender<String>,

    /// A sender for forwarding acks/nacks from the application to the lease
    /// management task. Each `Handler` holds a clone of this.
    ack_tx: UnboundedSender<AckResult>,

    /// A guard which signals a shutdown to the task sending keepalive pings
    /// when it is dropped.
    _keepalive_guard: DropGuard,

    /// A handle on the lease loop task.
    ///
    /// We hold onto this handle so we can await pending lease operations. While awaiting pending
    /// lease operations is useful for setting expectations in our unit tests, it is not that
    /// helpful to applications in practice.
    _lease_loop: tokio::task::JoinHandle<()>,
}

impl Session {
    pub(super) async fn new(builder: StreamingPull) -> Result<Self> {
        let shutdown = CancellationToken::new();
        let inner = builder.inner;
        let subscription = builder.subscription;

        let leaser = DefaultLeaser::new(
            inner.clone(),
            subscription.clone(),
            builder.ack_deadline_seconds,
        );
        let LeaseLoop {
            handle: _lease_loop,
            message_tx,
            ack_tx,
        } = LeaseLoop::new(leaser, LeaseOptions::default());

        let initial_req = StreamingPullRequest {
            subscription,
            stream_ack_deadline_seconds: builder.ack_deadline_seconds,
            max_outstanding_messages: builder.max_outstanding_messages,
            max_outstanding_bytes: builder.max_outstanding_bytes,
            ..Default::default()
        };
        let (stream, request_tx) = open_stream(inner, initial_req).await?;
        keepalive::spawn(request_tx, shutdown.clone());

        Ok(Self {
            stream,
            pool: VecDeque::new(),
            message_tx,
            ack_tx,
            _keepalive_guard: shutdown.drop_guard(),
            _lease_loop,
        })
    }

    /// Returns the next message received on this subscription.
    ///
    /// The message data is returned along with a [Handler] for acknowledging
    /// (ack) or rejecting (nack) the message.
    ///
    /// If the underlying stream encounters a permanent error, an `Error` is
    /// returned instead.
    ///
    /// `None` represents the end of a stream, but in practice, the stream stays
    /// open until it is cancelled or encounters a permanent error.
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::subscriber::session::Session;
    /// # async fn sample(mut session: Session) -> anyhow::Result<()> {
    /// while let Some((m, h)) = session.next().await.transpose()? {
    ///     println!("Received message m={m}");
    ///     h.ack();
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn next(&mut self) -> Option<Result<(PubsubMessage, Handler)>> {
        loop {
            // Serve a message if we have one ready.
            if let Some(item) = self.pool.pop_front() {
                return Some(Ok(item));
            }
            // Otherwise, read more messages from the stream.
            if let Err(e) = self.stream_next().await? {
                return Some(Err(e));
            }
        }
    }

    async fn stream_next(&mut self) -> Option<Result<()>> {
        let resp = match self.stream.next_message().await.transpose()? {
            Ok(resp) => resp,
            Err(e) => return Some(Err(to_gax_error(e))),
        };
        for rm in resp.received_messages {
            let Some(message) = rm.message else {
                // The message field should always be present. If not, the proto
                // message was corrupted while in transit, or there is a bug in
                // the service.
                //
                // The client can just ignore an ack ID without an associated
                // message.
                continue;
            };
            let _ = self.message_tx.send(rm.ack_id.clone());
            let message = match message.cnv().map_err(Error::deser) {
                Ok(message) => message,
                Err(e) => return Some(Err(e)),
            };
            self.pool.push_back((
                message,
                Handler::AtLeastOnce(AtLeastOnce {
                    ack_id: rm.ack_id,
                    ack_tx: self.ack_tx.clone(),
                }),
            ));
        }
        Some(Ok(()))
    }

    #[cfg(test)]
    /// Close the session, awaiting all pending acks and nacks.
    ///
    /// This is a useful method for setting clean test expectations.
    async fn close(self) -> anyhow::Result<()> {
        // Signal a shutdown to the keepalive task.
        drop(self._keepalive_guard);

        // Signal a shutdown to the lease management background task.
        drop(self.message_tx);

        // Wait for the lease management task to complete.
        self._lease_loop.await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::keepalive::KEEPALIVE_PERIOD;
    use super::super::lease_state::tests::{test_id, test_ids};
    use super::super::transport::tests::test_transport;
    use super::*;
    use pubsub_grpc_mock::google::pubsub::v1;
    use pubsub_grpc_mock::{MockSubscriber, start};
    use std::sync::Arc;
    use tokio::sync::mpsc::{channel, unbounded_channel};
    use tokio::task::JoinHandle;
    use tokio::time::Duration;

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    fn test_data(v: i32) -> bytes::Bytes {
        bytes::Bytes::from(format!("data-{}", test_id(v)))
    }

    fn test_response(range: std::ops::Range<i32>) -> v1::StreamingPullResponse {
        v1::StreamingPullResponse {
            received_messages: range
                .into_iter()
                .map(|i| v1::ReceivedMessage {
                    ack_id: test_id(i),
                    message: Some(v1::PubsubMessage {
                        data: test_data(i).to_vec(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn error_starting_stream() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Err(tonic::Status::internal("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let err = Session::new(builder)
            .await
            .expect_err("Session should not be created.");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(status.code, gax::error::rpc::Code::Internal);
        assert_eq!(status.message, "fail");

        Ok(())
    }

    #[tokio::test]
    async fn initial_request() -> anyhow::Result<()> {
        const MIB: i64 = 1024 * 1024;

        // We use this channel to surface writes (requests) from outside our
        // mock expectation.
        let (recover_writes_tx, mut recover_writes_rx) = channel(1);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull().return_once(move |request| {
            tokio::spawn(async move {
                // Note that this task stays alive as long as we hold
                // `recover_writes_rx`.
                let mut request_rx = request.into_inner();
                while let Some(request) = request_rx.recv().await {
                    recover_writes_tx
                        .send(request)
                        .await
                        .expect("forwarding writes always succeeds");
                }
            });
            Err(tonic::Status::internal("fail"))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        )
        .set_ack_deadline_seconds(20)
        .set_max_outstanding_messages(2000)
        .set_max_outstanding_bytes(200 * MIB);
        let _ = Session::new(builder).await;

        let initial_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a request")?;
        assert_eq!(initial_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(initial_req.stream_ack_deadline_seconds, 20);
        assert_eq!(initial_req.max_outstanding_messages, 2000);
        assert_eq!(initial_req.max_outstanding_bytes, 200 * MIB);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        mock.expect_acknowledge().returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(tonic::Response::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let mut session = Session::new(builder).await?;

        response_tx.send(Ok(test_response(1..2))).await?;
        response_tx.send(Ok(test_response(2..4))).await?;
        response_tx.send(Ok(test_response(4..7))).await?;
        drop(response_tx);

        for i in 1..7 {
            let (m, Handler::AtLeastOnce(h)) =
                session.next().await.transpose()?.expect("message {i}/6");
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id, test_id(i));
            h.ack();
        }
        let end = session.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        // Wait for the session to join its background tasks.
        session.close().await?;

        // Verify the acks went through.
        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(1..7));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn basic_lease_management() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (nack_tx, mut nack_rx) = unbounded_channel();
        let (extend_tx, mut extend_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        mock.expect_acknowledge().returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(tonic::Response::from(()))
        });
        mock.expect_modify_ack_deadline().returning(move |r| {
            let r = r.into_inner();
            if r.ack_deadline_seconds == 0 {
                nack_tx.send(r).expect("sending on channel always succeeds");
            } else {
                extend_tx
                    .send(r)
                    .expect("sending on channel always succeeds");
            }
            Ok(tonic::Response::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let mut session = Session::new(builder).await?;

        response_tx.send(Ok(test_response(0..30))).await?;
        drop(response_tx);

        // Ack some messages
        for i in 0..10 {
            let Some((_, Handler::AtLeastOnce(h))) = session.next().await.transpose()? else {
                anyhow::bail!("expected message {i}")
            };
            h.ack();
        }
        // Nack some messages
        for i in 10..20 {
            let Some((_, Handler::AtLeastOnce(h))) = session.next().await.transpose()? else {
                anyhow::bail!("expected message {i}")
            };
            h.nack();
        }
        // Take a long time to process some messages
        let mut hold = Vec::new();
        for i in 20..30 {
            let Some((_, Handler::AtLeastOnce(h))) = session.next().await.transpose()? else {
                anyhow::bail!("expected message {i}")
            };
            hold.push(h);
        }

        // Advance the clock 10s, which is the default stream ack deadline. In
        // this time, we should attempt at least one lease extension RPC.
        tokio::time::advance(Duration::from_secs(10)).await;

        // Close the session, to make sure pending operations complete.
        session.close().await?;

        // Verify the acks went through.
        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(0..10));
        assert!(ack_rx.is_empty());

        // Verify the initial nacks went through.
        let nack_req = nack_rx.try_recv()?;
        assert_eq!(nack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(nack_req.ack_deadline_seconds, 0);
        assert_eq!(sorted(nack_req.ack_ids), test_ids(10..20));

        // Verify that we nack the leftover messages when the stream shuts down.
        let nack_req = nack_rx.try_recv()?;
        assert_eq!(nack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(nack_req.ack_deadline_seconds, 0);
        assert_eq!(sorted(nack_req.ack_ids), test_ids(20..30));
        assert!(nack_rx.is_empty());

        // Verify at least one lease extension attempt was made.
        let extend_req = extend_rx.try_recv()?;
        assert_eq!(extend_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(extend_req.ack_deadline_seconds, 10);
        assert_eq!(sorted(extend_req.ack_ids), test_ids(20..30));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_responses() -> anyhow::Result<()> {
        // In this test, we verify the case where an application asks for a
        // message, but a response is not immediately available on the stream.

        let (response_tx, response_rx) = channel(10);
        let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            response_tx.send(Ok(test_response(1..2))).await?;
            Ok(())
        });

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let mut session = Session::new(builder).await?;
        let (m, Handler::AtLeastOnce(h)) = session
            .next()
            .await
            .transpose()?
            .expect("stream should wait for a message");
        assert_eq!(m.data, test_data(1));
        assert_eq!(h.ack_id, test_id(1));

        handle.await??;

        Ok(())
    }

    #[tokio::test]
    async fn serves_messages_immediately() -> anyhow::Result<()> {
        // This test verifies we do not do something crazy like draining the
        // stream (which would never end) before serving messages to the
        // application.

        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let mut session = Session::new(builder).await?;

        for i in 1..7 {
            response_tx.send(Ok(test_response(i..i + 1))).await?;

            let (m, Handler::AtLeastOnce(h)) =
                session.next().await.transpose()?.expect("message {i}/6");
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id, test_id(i));
        }
        drop(response_tx);
        let end = session.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        Ok(())
    }

    #[tokio::test]
    async fn handles_empty_response() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let mut session = Session::new(builder).await?;

        response_tx.send(Ok(test_response(1..2))).await?;
        // See if we can handle an empty range
        response_tx.send(Ok(test_response(2..2))).await?;
        response_tx.send(Ok(test_response(2..3))).await?;
        drop(response_tx);

        for i in 1..3 {
            let (m, Handler::AtLeastOnce(h)) =
                session.next().await.transpose()?.expect("message {i}/2");
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id, test_id(i));
        }
        let end = session.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn handles_missing_message_field() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (extend_tx, mut extend_rx) = unbounded_channel();

        let bad = v1::StreamingPullResponse {
            received_messages: vec![v1::ReceivedMessage {
                ack_id: "ignored-ack-id".to_string(),
                message: None,
                ..Default::default()
            }],
            ..Default::default()
        };

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        mock.expect_acknowledge()
            .returning(|_| Ok(tonic::Response::from(())));
        mock.expect_modify_ack_deadline().returning(move |r| {
            extend_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(tonic::Response::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let mut session = Session::new(builder).await?;

        response_tx.send(Ok(test_response(1..4))).await?;
        // See if we can handle an empty range
        response_tx.send(Ok(bad)).await?;
        response_tx.send(Ok(test_response(4..7))).await?;
        drop(response_tx);

        for i in 1..7 {
            let (m, Handler::AtLeastOnce(h)) =
                session.next().await.transpose()?.expect("message {i}/6");
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id, test_id(i));
        }
        let end = session.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        // Advance the clock 10s, which is the default stream ack deadline. In
        // this time, we should attempt at least one lease extension RPC.
        tokio::time::advance(Duration::from_secs(10)).await;

        // Close the session, to make sure pending operations complete.
        session.close().await?;

        // Verify at least one lease extension attempt was made.
        let extend_req = extend_rx.try_recv()?;
        assert_eq!(extend_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(extend_req.ack_deadline_seconds, 10);
        // Note that we do not expect to see "ignored-ack-id".
        assert_eq!(sorted(extend_req.ack_ids), test_ids(1..7));

        Ok(())
    }

    #[tokio::test]
    async fn permanent_error_midstream() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(tonic::Response::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let mut session = Session::new(builder).await?;

        response_tx.send(Ok(test_response(1..4))).await?;
        response_tx
            .send(Err(tonic::Status::internal("fail")))
            .await?;
        drop(response_tx);

        for i in 1..4 {
            let (m, Handler::AtLeastOnce(h)) =
                session.next().await.transpose()?.expect("message {i}/3");
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id, test_id(i));
        }
        let err = session
            .next()
            .await
            .transpose()
            .expect_err("expected an error from stream");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(status.code, gax::error::rpc::Code::Internal);
        assert_eq!(status.message, "fail");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn keepalives() -> anyhow::Result<()> {
        // We use this channel to surface writes (requests) from outside our
        // mock expectation.
        let (recover_writes_tx, mut recover_writes_rx) = channel(1);
        let (_response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull().return_once(move |request| {
            tokio::spawn(async move {
                // Note that this task stays alive as long as we hold
                // `recover_writes_rx`.
                let mut request_rx = request.into_inner();
                while let Some(request) = request_rx.recv().await {
                    recover_writes_tx
                        .send(request)
                        .await
                        .expect("forwarding writes always succeeds");
                }
            });
            Ok(tonic::Response::from(response_rx))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let builder = StreamingPull::new(
            Arc::new(transport),
            "projects/p/subscriptions/s".to_string(),
        );
        let session = Session::new(builder).await;

        let initial_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive an initial request")?;
        assert_eq!(initial_req.subscription, "projects/p/subscriptions/s");

        // Verify that we receive at least one keepalive request on the stream.
        tokio::time::advance(KEEPALIVE_PERIOD).await;
        let keepalive_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a keepalive request")?;
        assert_eq!(keepalive_req, v1::StreamingPullRequest::default());

        // Drop the session, which should signal a shutdown of the keepalive
        // task.
        drop(session);

        // Advance the time far enough to expect a keepalive ping, if the
        // keepalive task was still running.
        tokio::time::advance(4 * KEEPALIVE_PERIOD).await;
        assert!(recover_writes_rx.is_empty());

        Ok(())
    }
}
