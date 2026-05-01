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

use super::builder::Subscribe;
use super::handler::{AckResult, Action, AtLeastOnce, ExactlyOnce, Handler};
use super::lease_loop::LeaseLoop;
use super::lease_state::{AtLeastOnceInfo, ExactlyOnceInfo, LeaseInfo, LeaseOptions, NewMessage};
use super::leaser::DefaultLeaser;
use super::retry_policy::StreamRetryPolicy;
use super::shutdown_token::ShutdownToken;
use super::stream::Stream;
use super::stub::TonicStreaming as _;
use super::transport::Transport;
use crate::google::pubsub::v1::{StreamingPullRequest, StreamingPullResponse};
use crate::model::Message;
use crate::{Error, Result};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use gaxi::grpc::from_status::to_gax_error;
use gaxi::prost::FromProto as _;
use google_cloud_gax::retry_result::RetryResult;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedSender, WeakUnboundedSender, unbounded_channel};
use tokio::sync::oneshot::Receiver;
use tokio::time::Duration;
use tokio_util::sync::{CancellationToken, DropGuard};

/// Represents an open subscribe stream.
///
/// This is a stream-like struct for serving messages to an application.
///
/// # Example
/// ```
/// # use google_cloud_pubsub::client::Subscriber;
/// # async fn sample(client: Subscriber) -> anyhow::Result<()> {
/// let mut stream = client
///     .subscribe("projects/my-project/subscriptions/my-subscription")
///     .build();
/// while let Some((m, h)) = stream.next().await.transpose()? {
///     println!("Received message m={m:?}");
///     h.ack();
/// }
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct MessageStream {
    /// Implementation of the `MessageStream`.
    ///
    /// To avoid atomic increments in the critical path, we separate the
    /// shutdown token from the rest of the struct. This way we can hold a
    /// mutable reference to `self.inner`, and a reference to `self.shutdown` at
    /// the same time.
    inner: MessageStreamImpl,

    /// This future is ready when the lease loop shutdown completes.
    lease_loop: Shared<BoxFuture<'static, ()>>,

    /// A token that can detect a shutdown from the application.
    shutdown: CancellationToken,

    /// Signal a shutdown if the application drops this struct.
    ///
    /// This field is intentionally unused; it exists solely to trigger a
    /// shutdown signal via its `Drop` implementation.
    _shutdown_guard: DropGuard,
}

#[derive(Debug)]
pub struct MessageStreamImpl {
    /// The stub implementing this struct.
    stub: Arc<Transport>,

    /// The initial request used to start a stream.
    initial_req: StreamingPullRequest,

    /// The bidirectional stream.
    ///
    /// We choose to lazy-initialize the stream when the application asks for a
    /// message because tonic will not yield the stream to us until the first
    /// response is available.[^1]
    ///
    /// The usability of the `MessageStream` API would suffer if creating an instance
    /// of `MessageStream` is blocked on the first message being available.
    ///
    /// [^1]: <https://github.com/hyperium/tonic/issues/515>
    stream: Option<StreamState>,

    /// Applications ask for messages one at a time. Individual stream responses
    /// can contain multiple messages. We use `pool` to hold the extra messages
    /// while we wait to serve them to applications.
    ///
    /// A FIFO queue is necessary to preserve ordering.
    pool: VecDeque<(Message, HandlerInfo)>,

    /// A sender for sending new messages from the stream into the lease
    /// management task.
    message_tx: WeakUnboundedSender<NewMessage>,

    /// A sender for forwarding acks/nacks from the application to the lease
    /// management task. Each `Handler` holds a clone of this.
    ack_tx: WeakUnboundedSender<Action>,

    /// A token that can initiate shutdown after a stream error.
    shutdown: CancellationToken,
}

// We would rather always allocate enough space to hold the stream on the stack
// than add a layer of indirection by `Box`ing it.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum StreamState {
    /// The stream was cancelled or failed with a permanent error. It should not
    /// be re-opened.
    Closed,
    /// The stream is active.
    Active(Stream<Transport>),
}

impl MessageStream {
    pub(super) fn new(builder: Subscribe) -> Self {
        let stub = builder.inner;
        let subscription = builder.subscription;

        let (confirmed_tx, confirmed_rx) = unbounded_channel();
        let (eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let leaser = DefaultLeaser::new(
            stub.clone(),
            confirmed_tx,
            eo_extend_tx,
            subscription.clone(),
            builder.ack_deadline_seconds,
            builder.grpc_subchannel_count,
        );
        let options = LeaseOptions {
            max_lease: builder.max_lease,
            max_lease_extension: Duration::from_secs(builder.ack_deadline_seconds as u64),
            shutdown_behavior: builder.shutdown_behavior,
            ..Default::default()
        };
        let LeaseLoop {
            handle,
            message_tx,
            ack_tx,
            cancel: shutdown,
        } = LeaseLoop::new(leaser, confirmed_rx, eo_extend_rx, options);
        let lease_loop = handle.map(|_| ()).boxed().shared();
        let _shutdown_guard = shutdown.clone().drop_guard();

        let initial_req = StreamingPullRequest {
            subscription,
            stream_ack_deadline_seconds: builder.ack_deadline_seconds,
            max_outstanding_messages: builder.max_outstanding_messages,
            max_outstanding_bytes: builder.max_outstanding_bytes,
            client_id: builder.client_id,
            // `protocol_version == 1` means we support receiving heartbeats
            // (empty `StreamingPullResponse`s) from the server.
            protocol_version: 1,
            ..Default::default()
        };

        let inner = MessageStreamImpl {
            stub,
            initial_req,
            stream: None,
            pool: VecDeque::new(),
            message_tx,
            ack_tx,
            shutdown: shutdown.clone(),
        };
        Self {
            inner,
            lease_loop,
            shutdown,
            _shutdown_guard,
        }
    }

    /// Returns the next message received on this subscription.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::subscriber::MessageStream;
    /// # async fn sample(mut stream: MessageStream) -> anyhow::Result<()> {
    /// while let Some((m, h)) = stream.next().await.transpose()? {
    ///     println!("Received message m={m:?}");
    ///     h.ack();
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// Returns the message data along with a [Handler] to acknowledge (ack) the
    /// message.
    ///
    /// If the underlying stream encounters a permanent error, an `Error` is
    /// returned instead.
    ///
    /// `None` represents the end of a stream, but in practice, the stream stays
    /// open until it is cancelled or encounters a permanent error.
    pub async fn next(&mut self) -> Option<Result<(Message, Handler)>> {
        let next = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => {
                self.inner.close();
                None
            },
            n = self.inner.next() => n,
        };
        next
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Converts the `MessageStream` to a [`futures::Stream`].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::subscriber::MessageStream;
    /// # async fn sample(stream: MessageStream) -> anyhow::Result<()> {
    /// use futures::TryStreamExt;
    /// let mut stream = stream.into_stream();
    /// while let Some((m, h)) = stream.try_next().await? { /* ... */ }
    /// # Ok(()) }
    /// ```
    pub fn into_stream(self) -> impl futures::Stream<Item = Result<(Message, Handler)>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(self, |mut stream| async move {
            stream.next().await.map(|item| (item, stream))
        }))
    }

    /// Returns a shutdown token for the stream.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::subscriber::MessageStream;
    /// # async fn sample(mut stream: MessageStream) {
    /// // Get a shutdown token for the stream.
    /// let shutdown_token = stream.shutdown_token();
    ///
    /// // Signal and await a shutdown of the stream.
    /// shutdown_token.shutdown().await;
    ///
    /// // The stream stops yielding messages after a cancel.
    /// assert!(stream.next().await.is_none());
    /// # }
    /// ```
    ///
    /// Use this token to signal and/or await shutdown of the stream.
    ///
    /// Awaiting a stream shutdown gives the subscriber time to flush its
    /// pending acknowledgements, and schedule other messages for redelivery to
    /// another client as soon as possible.
    pub fn shutdown_token(&self) -> ShutdownToken {
        ShutdownToken {
            inner: self.shutdown.clone(),
            // This future is ready when the lease loop shutdown completes.
            fut: self.lease_loop.clone(),
        }
    }
}

impl MessageStreamImpl {
    async fn next(&mut self) -> Option<Result<(Message, Handler)>> {
        loop {
            // Serve a message if we have one ready.
            if let Some((m, hi)) = self.pool.pop_front() {
                return Some(Ok((m, hi.into_handler(self.ack_tx.upgrade()?))));
            }

            // Otherwise, read the next response from the stream, which will
            // likely populate the message pool.
            //
            // Note that a successful read does not necessarily mean there is a
            // message in the pool. The server occasionally sends heartbeats
            // (responses with an empty message list). Hence the loop.
            if let Err(e) = self.populate_pool().await? {
                // Handle errors opening or reading from the stream.
                match StreamRetryPolicy::on_midstream_error(e) {
                    RetryResult::Continue(_) => {
                        // The stream failed with a transient error. Reset the stream.
                        self.stream = None;
                        continue;
                    }
                    RetryResult::Permanent(e) | RetryResult::Exhausted(e) => {
                        // The stream failed with a permanent error. Return the error.
                        self.close();
                        return Some(Err(e));
                    }
                }
            }
        }
    }

    /// Make a new attempt to open the underlying gRPC stream.
    async fn open_stream(&mut self) -> Result<()> {
        let stream = Stream::<Transport>::new(self.stub.clone(), self.initial_req.clone()).await?;
        self.stream = Some(StreamState::Active(stream));
        Ok(())
    }

    /// Reads the next response from the stream.
    ///
    /// If necessary, this method will open a new stream.
    ///
    /// If we receive an error either opening or reading from the stream, we
    /// return it.
    async fn next_response(&mut self) -> Option<Result<StreamingPullResponse>> {
        if self.stream.is_none() {
            // Open the stream, if necessary.
            if let Err(e) = self.open_stream().await {
                return Some(Err(e));
            }
        }

        let stream = match self.stream.as_mut()? {
            StreamState::Closed => return None,
            StreamState::Active(s) => s,
        };
        stream
            .next_message()
            .await
            .map_err(to_gax_error)
            .transpose()
    }

    /// Populate the message pool by reading from the stream.
    ///
    /// Read the next response from the stream. If necessary, this method will
    /// open a new stream.
    ///
    /// If we receive a response, we store the messages in `self.pool` and
    /// forward the ack IDs to the lease management task.
    ///
    /// If we receive an error reading from the stream, we return it.
    async fn populate_pool(&mut self) -> Option<Result<()>> {
        // Read the next response from the stream.
        let resp = match self.next_response().await? {
            Ok(resp) => resp,
            Err(e) => return Some(Err(e)),
        };

        let exactly_once = resp
            .subscription_properties
            .is_some_and(|m| m.exactly_once_delivery_enabled);

        // Process the received messages in the response.
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

            let delivery_attempt = (rm.delivery_attempt > 0).then_some(rm.delivery_attempt);

            let (lease_info, handler_info) = if exactly_once {
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                (
                    LeaseInfo::ExactlyOnce(ExactlyOnceInfo::new(result_tx)),
                    HandlerInfo::ExactlyOnce {
                        ack_id: rm.ack_id.clone(),
                        result_rx,
                        delivery_attempt,
                    },
                )
            } else {
                (
                    LeaseInfo::AtLeastOnce(AtLeastOnceInfo::new()),
                    HandlerInfo::AtLeastOnce {
                        ack_id: rm.ack_id.clone(),
                        delivery_attempt,
                    },
                )
            };

            let _ = self.message_tx.upgrade()?.send(NewMessage {
                ack_id: rm.ack_id,
                lease_info,
            });
            let message = match message.cnv().map_err(Error::deser) {
                Ok(message) => message,
                Err(e) => return Some(Err(e)),
            };
            self.pool.push_back((message, handler_info));
        }
        Some(Ok(()))
    }

    // Permanently close the stream.
    fn close(&mut self) {
        self.stream = Some(StreamState::Closed);
        self.pool.clear();
        self.shutdown.cancel();
    }
}

/// A `Handler` without its action `Sender`.
///
/// We only want to create strong `Sender`s for `Handler`s that we yield to the
/// application.
///
/// Note that the application should be able to signal a shutdown without
/// dropping the `MessageStream` or calling `MessageStream::next()`.
///
/// In these cases, the items in the `MessageStream::pool` are not cleared. So,
/// if we hold a strong `Sender` in the `pool`, we would never initiate a
/// shutdown when configured to `WaitForProcessing`.
#[derive(Debug)]
enum HandlerInfo {
    AtLeastOnce {
        ack_id: String,
        delivery_attempt: Option<i32>,
    },
    ExactlyOnce {
        ack_id: String,
        result_rx: Receiver<AckResult>,
        delivery_attempt: Option<i32>,
    },
}

impl HandlerInfo {
    /// Convert this type to a `Handler`, by adding its action `Sender` before
    /// serving it to the application.
    fn into_handler(self, ack_tx: UnboundedSender<Action>) -> Handler {
        match self {
            HandlerInfo::AtLeastOnce {
                ack_id,
                delivery_attempt,
            } => Handler::AtLeastOnce(AtLeastOnce::new(ack_id, ack_tx, delivery_attempt)),
            HandlerInfo::ExactlyOnce {
                ack_id,
                result_rx,
                delivery_attempt,
            } => Handler::ExactlyOnce(ExactlyOnce::new(
                ack_id,
                ack_tx,
                result_rx,
                delivery_attempt,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ShutdownBehavior;
    use super::super::client::Subscriber;
    use super::super::keepalive::KEEPALIVE_PERIOD;
    use super::super::lease_state::tests::{test_id, test_ids};
    use super::super::stream::{INITIAL_DELAY, MAXIMUM_DELAY};
    use super::*;
    use gaxi::grpc::tonic::{Response as TonicResponse, Status as TonicStatus};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_test_macros::tokio_test_no_panics;
    use pubsub_grpc_mock::google::pubsub::v1;
    use pubsub_grpc_mock::{MockSubscriber, start};
    use test_case::test_case;
    use tokio::sync::mpsc::{channel, unbounded_channel};
    use tokio::task::{JoinHandle, JoinSet};
    use tokio::time::{Duration, Instant};

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

    fn test_exactly_once_response(range: std::ops::Range<i32>) -> v1::StreamingPullResponse {
        v1::StreamingPullResponse {
            subscription_properties: Some(v1::streaming_pull_response::SubscriptionProperties {
                exactly_once_delivery_enabled: true,
                ..Default::default()
            }),
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

    async fn test_client(endpoint: String) -> anyhow::Result<Subscriber> {
        Ok(Subscriber::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?)
    }

    #[tokio_test_no_panics]
    async fn error_starting_stream() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();
        let err = stream
            .next()
            .await
            .expect("stream should not be empty")
            .expect_err("the first streamed item should be an error");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(
            status.code,
            google_cloud_gax::error::rpc::Code::FailedPrecondition
        );
        assert_eq!(status.message, "fail");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn permanent_error_ends_stream() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .returning(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();
        let next = stream.next().await;
        assert!(
            matches!(next, Some(Err(_))),
            "expected permanent error, got {next:?}"
        );

        let next = stream.next().await;
        assert!(next.is_none(), "expected end of stream, got {next:?}");

        Ok(())
    }

    #[tokio_test_no_panics]
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
            Err(TonicStatus::failed_precondition("fail"))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let _ = client
            .subscribe("projects/p/subscriptions/s")
            .set_max_lease_extension(Duration::from_secs(20))
            .set_max_outstanding_messages(2000)
            .set_max_outstanding_bytes(200 * MIB)
            .build()
            .next()
            .await;

        let initial_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a request")?;
        assert_eq!(initial_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(initial_req.stream_ack_deadline_seconds, 20);
        assert_eq!(initial_req.max_outstanding_messages, 2000);
        assert_eq!(initial_req.max_outstanding_bytes, 200 * MIB);
        assert!(
            !initial_req.client_id.is_empty(),
            "initial request has empty client id: {initial_req:?}"
        );
        assert!(
            initial_req.protocol_version >= 1,
            "protocol_version={}",
            initial_req.protocol_version
        );

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        response_tx.send(Ok(test_response(1..2))).await?;
        response_tx.send(Ok(test_response(2..4))).await?;
        response_tx.send(Ok(test_response(4..7))).await?;
        drop(response_tx);

        for i in 1..7 {
            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/6")
            };
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id(), test_id(i));
            h.ack();
        }
        let end = stream.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        // Wait for the stream to join its background tasks.
        stream.shutdown_token().shutdown().await;

        // Verify the acks went through.
        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(1..7));

        Ok(())
    }

    #[test_case(0, None, false; "at_least_once_zero_maps_to_none")]
    #[test_case(5, Some(5), false; "at_least_once_positive_maps_to_some")]
    #[test_case(-1, None, false; "at_least_once_negative_maps_to_none")]
    #[test_case(1, Some(1), false; "at_least_once_one_maps_to_some")]
    #[test_case(i32::MAX, Some(i32::MAX), false; "at_least_once_max_maps_to_some")]
    #[test_case(0, None, true; "exactly_once_zero_maps_to_none")]
    #[test_case(5, Some(5), true; "exactly_once_positive_maps_to_some")]
    #[test_case(-1, None, true; "exactly_once_negative_maps_to_none")]
    #[test_case(1, Some(1), true; "exactly_once_one_maps_to_some")]
    #[test_case(i32::MAX, Some(i32::MAX), true; "exactly_once_max_maps_to_some")]
    #[tokio_test_no_panics(start_paused = true)]
    async fn delivery_attempt_mapping(
        input: i32,
        expected: Option<i32>,
        exactly_once: bool,
    ) -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        let resp = v1::StreamingPullResponse {
            subscription_properties: Some(v1::streaming_pull_response::SubscriptionProperties {
                exactly_once_delivery_enabled: exactly_once,
                ..Default::default()
            }),
            received_messages: vec![v1::ReceivedMessage {
                ack_id: test_id(0),
                message: Some(v1::PubsubMessage {
                    data: test_data(0).to_vec(),
                    ..Default::default()
                }),
                delivery_attempt: input,
            }],
            ..Default::default()
        };

        response_tx.send(Ok(resp)).await?;
        drop(response_tx);

        let Some((_, h)) = stream.next().await.transpose()? else {
            anyhow::bail!("expected message")
        };
        assert_eq!(h.delivery_attempt(), expected);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn basic_success_exactly_once() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        mock.expect_modify_ack_deadline()
            .returning(|_| Ok(TonicResponse::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_shutdown_behavior(ShutdownBehavior::WaitForProcessing)
            .build();

        response_tx
            .send(Ok(test_exactly_once_response(1..2)))
            .await?;
        response_tx
            .send(Ok(test_exactly_once_response(2..4)))
            .await?;
        response_tx
            .send(Ok(test_exactly_once_response(4..7)))
            .await?;
        drop(response_tx);

        let mut acks = JoinSet::new();
        for i in 1..7 {
            let Some((m, Handler::ExactlyOnce(h))) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/6")
            };
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id(), test_id(i));
            acks.spawn(h.confirmed_ack());
        }
        let end = stream.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        // Wait for the stream to join its background tasks.
        stream.shutdown_token().shutdown().await;

        // Verify the acks went through.
        while let Some(r) = acks.join_next().await {
            r??;
        }
        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(1..7));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn basic_lease_management() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (nack_tx, mut nack_rx) = unbounded_channel();
        let (extend_tx, mut extend_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
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
            Ok(TonicResponse::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_max_lease_extension(Duration::from_secs(10))
            .set_shutdown_behavior(ShutdownBehavior::NackImmediately)
            .build();

        response_tx.send(Ok(test_response(0..30))).await?;
        drop(response_tx);

        // Ack some messages
        for i in 0..10 {
            let Some((_, Handler::AtLeastOnce(h))) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}")
            };
            h.ack();
        }
        // Nack some messages
        for i in 10..20 {
            let Some((_, Handler::AtLeastOnce(h))) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}")
            };
            h.nack();
        }
        // Take a long time to process some messages
        let mut hold = Vec::new();
        for i in 20..30 {
            let Some((_, Handler::AtLeastOnce(h))) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}")
            };
            hold.push(h);
        }

        // Advance the clock 10s, which is the stream ack deadline. In this
        // time, we should attempt at least one lease extension RPC.
        tokio::time::advance(Duration::from_secs(10)).await;

        // Close the stream, to make sure pending operations complete.
        stream.shutdown_token().shutdown().await;

        // Verify the acks went through.
        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(0..10));
        assert!(ack_rx.is_empty(), "{ack_rx:?}");

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
        assert!(nack_rx.is_empty(), "{nack_rx:?}");

        // Verify at least one lease extension attempt was made.
        let extend_req = extend_rx.try_recv()?;
        assert_eq!(extend_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(extend_req.ack_deadline_seconds, 10);
        assert_eq!(sorted(extend_req.ack_ids), test_ids(20..30));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
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
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_modify_ack_deadline()
            .returning(|_| Ok(TonicResponse::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();
        let (m, h) = stream
            .next()
            .await
            .transpose()?
            .expect("stream should wait for a message");
        assert_eq!(m.data, test_data(1));
        assert_eq!(h.ack_id(), test_id(1));

        handle.await??;

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn serves_messages_immediately() -> anyhow::Result<()> {
        // This test verifies we do not do something crazy like draining the
        // stream (which would never end) before serving messages to the
        // application.

        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_modify_ack_deadline()
            .returning(|_| Ok(TonicResponse::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        for i in 1..7 {
            response_tx.send(Ok(test_response(i..i + 1))).await?;

            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/6")
            };
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id(), test_id(i));
        }
        drop(response_tx);
        let end = stream.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn handles_empty_response() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_modify_ack_deadline()
            .returning(|_| Ok(TonicResponse::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        response_tx.send(Ok(test_response(1..2))).await?;
        // See if we can handle an empty range
        response_tx.send(Ok(test_response(2..2))).await?;
        response_tx.send(Ok(test_response(2..3))).await?;
        drop(response_tx);

        for i in 1..3 {
            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/2")
            };
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id(), test_id(i));
        }
        let end = stream.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
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
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_modify_ack_deadline().returning(move |r| {
            let r = r.into_inner();
            if r.ack_deadline_seconds != 0 {
                extend_tx
                    .send(r)
                    .expect("sending on channel always succeeds");
            }
            Ok(TonicResponse::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_max_lease_extension(Duration::from_secs(10))
            .set_shutdown_behavior(ShutdownBehavior::NackImmediately)
            .build();

        response_tx.send(Ok(test_response(1..4))).await?;
        // See if we can handle an empty range
        response_tx.send(Ok(bad)).await?;
        response_tx.send(Ok(test_response(4..7))).await?;
        drop(response_tx);

        let mut handlers = Vec::new();
        for i in 1..7 {
            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/6")
            };
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id(), test_id(i));
            handlers.push(h);
        }

        // Advance the clock 10s, which is the stream ack deadline. In this
        // time, we should attempt at least one lease extension RPC.
        tokio::time::advance(Duration::from_secs(10)).await;

        // Close the stream, to make sure pending operations complete.
        stream.shutdown_token().shutdown().await;

        // Verify at least one lease extension attempt was made.
        let extend_req = extend_rx.try_recv()?;
        assert_eq!(extend_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(extend_req.ack_deadline_seconds, 10);
        // Note that we do not expect to see "ignored-ack-id".
        assert_eq!(sorted(extend_req.ack_ids), test_ids(1..7));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn permanent_error_midstream() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        response_tx.send(Ok(test_response(1..4))).await?;
        response_tx
            .send(Err(TonicStatus::failed_precondition("fail")))
            .await?;
        drop(response_tx);

        for i in 1..4 {
            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/3")
            };
            assert_eq!(m.data, test_data(i));
            assert_eq!(h.ack_id(), test_id(i));
        }
        let err = stream
            .next()
            .await
            .transpose()
            .expect_err("expected an error from stream");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(
            status.code,
            google_cloud_gax::error::rpc::Code::FailedPrecondition
        );
        assert_eq!(status.message, "fail");

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn keepalives() -> anyhow::Result<()> {
        // We use this channel to surface writes (requests) from outside our
        // mock expectation.
        let (recover_writes_tx, mut recover_writes_rx) = channel(1);
        let (response_tx, response_rx) = channel(10);

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
            Ok(TonicResponse::from(response_rx))
        });
        mock.expect_modify_ack_deadline()
            .returning(|_| Ok(TonicResponse::from(())));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();
        response_tx.send(Ok(test_response(1..4))).await?;
        let _ = stream.next().await;

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

        // Drop the stream, which should signal a shutdown of the keepalive
        // task.
        drop(stream);

        // Advance the time far enough to expect a keepalive ping, if the
        // keepalive task was still running.
        tokio::time::advance(4 * KEEPALIVE_PERIOD).await;
        assert!(recover_writes_rx.is_empty(), "{recover_writes_rx:?}");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn client_id() -> anyhow::Result<()> {
        // We use this channel to surface writes (requests) from outside our
        // mock expectation.
        let (recover_writes_tx, mut recover_writes_rx) = channel(10);
        let recover_writes_tx = std::sync::Arc::new(tokio::sync::Mutex::new(recover_writes_tx));

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .times(3)
            .returning(move |request| {
                let tx = recover_writes_tx.clone();
                tokio::spawn(async move {
                    // Note that this task stays alive as long as we hold
                    // `recover_writes_rx`.
                    let mut request_rx = request.into_inner();
                    while let Some(request) = request_rx.recv().await {
                        tx.lock()
                            .await
                            .send(request)
                            .await
                            .expect("forwarding writes always succeeds");
                    }
                });
                Err(TonicStatus::failed_precondition("fail"))
            });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        // Make two requests with the same client. The requests should have the
        // same client ID.
        let c1 = test_client(endpoint.clone()).await?;
        let _ = c1
            .subscribe("projects/p/subscriptions/s")
            .build()
            .next()
            .await;
        let req1 = recover_writes_rx
            .recv()
            .await
            .expect("should receive a request")?;
        let _ = c1
            .subscribe("projects/p/subscriptions/s")
            .build()
            .next()
            .await;
        let req2 = recover_writes_rx
            .recv()
            .await
            .expect("should receive a request")?;
        assert_eq!(req1.client_id, req2.client_id);

        // Make a third request with a different client. This request should
        // have a different client ID.
        let c2 = test_client(endpoint).await?;
        let _ = c2
            .subscribe("projects/p/subscriptions/s")
            .build()
            .next()
            .await;
        let req3 = recover_writes_rx
            .recv()
            .await
            .expect("should receive a request")?;
        assert_ne!(req1.client_id, req3.client_id);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn no_immediate_message() -> anyhow::Result<()> {
        const TEST_TIMEOUT: Duration = Duration::from_secs(42);

        let (_response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        let _ = tokio::time::timeout(TEST_TIMEOUT, stream.next())
            .await
            .expect_err("next() should never yield.");

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn retry_transient_when_starting_stream() -> anyhow::Result<()> {
        // The policy should retry forever. Our default retry policies have an
        // attempt limit of 10. So we arbitrarily pick a number greater than 10
        // for this test.
        const NUM_RETRIES: u32 = 20;

        let start_time = Instant::now();
        let mut seq = mockall::Sequence::new();
        let mut mock = MockSubscriber::new();

        // Simulate N transient errors
        mock.expect_streaming_pull()
            .times(NUM_RETRIES as usize)
            .in_sequence(&mut seq)
            .returning(|_| Err(TonicStatus::unavailable("try again")));
        // Simulate a permanent error. Otherwise, we would retry forever.
        mock.expect_streaming_pull()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();
        let err = stream
            .next()
            .await
            .expect("stream should not be empty")
            .expect_err("the first streamed item should be an error");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(
            status.code,
            google_cloud_gax::error::rpc::Code::FailedPrecondition
        );
        assert_eq!(status.message, "fail");

        let elapsed = start_time.elapsed();
        assert!(
            elapsed <= MAXIMUM_DELAY * NUM_RETRIES,
            "elapsed={elapsed:?}"
        );
        assert!(
            elapsed >= INITIAL_DELAY * NUM_RETRIES,
            "elapsed={elapsed:?}"
        );

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn resume_midstream_success() -> anyhow::Result<()> {
        let (response_tx_1, response_rx_1) = channel(10);
        let (response_tx_2, response_rx_2) = channel(10);
        let (response_tx_3, response_rx_3) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let mut seq = mockall::Sequence::new();
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|_| Ok(TonicResponse::from(response_rx_1)));
        mock.expect_streaming_pull()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_| Ok(TonicResponse::from(response_rx_2)));
        mock.expect_streaming_pull()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|_| Ok(TonicResponse::from(response_rx_3)));
        mock.expect_acknowledge().times(1..).returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        response_tx_1.send(Ok(test_response(0..10))).await?;
        response_tx_1.send(Ok(test_response(10..20))).await?;
        response_tx_1
            .send(Err(TonicStatus::unavailable("GFE disconnect. try again")))
            .await?;
        drop(response_tx_1);
        response_tx_2.send(Ok(test_response(20..30))).await?;
        response_tx_2.send(Ok(test_response(30..40))).await?;
        response_tx_2
            .send(Err(TonicStatus::unavailable("GFE disconnect. try again")))
            .await?;
        drop(response_tx_2);
        response_tx_3.send(Ok(test_response(40..50))).await?;
        drop(response_tx_3);

        for i in 0..50 {
            let (m, h) = stream
                .next()
                .await
                .unwrap_or_else(|| panic!("expected message {}/50", i + 1))?;
            assert_eq!(m.data, test_data(i));
            h.ack();
        }
        let end = stream.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        // Wait for the stream to join its background tasks.
        stream.shutdown_token().shutdown().await;

        // Verify the acks went through.
        let mut got = Vec::new();
        while let Ok(ack_req) = ack_rx.try_recv() {
            assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
            got.extend(ack_req.ack_ids);
        }
        assert_eq!(sorted(got), test_ids(0..50));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn resume_midstream_hits_permanent_error() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let mut seq = mockall::Sequence::new();
        let mut mock = MockSubscriber::new();
        // Start a successful stream, which will eventually disconnect.
        mock.expect_streaming_pull()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        // Simulate transient errors attempting to resume the stream.
        mock.expect_streaming_pull()
            .times(3)
            .in_sequence(&mut seq)
            .returning(|_| Err(TonicStatus::unavailable("try again")));
        // Simulate a permanent error attempting to resume the stream.
        mock.expect_streaming_pull()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        mock.expect_acknowledge().times(1..).returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();

        response_tx.send(Ok(test_response(0..10))).await?;
        response_tx.send(Ok(test_response(10..20))).await?;
        response_tx
            .send(Err(TonicStatus::unavailable("GFE disconnect. try again")))
            .await?;
        drop(response_tx);

        for i in 0..20 {
            let (m, h) = stream
                .next()
                .await
                .unwrap_or_else(|| panic!("expected message {}/20", i + 1))?;
            assert_eq!(m.data, test_data(i));
            h.ack();
        }
        let err = stream
            .next()
            .await
            .transpose()
            .expect_err("expected an error from stream");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(
            status.code,
            google_cloud_gax::error::rpc::Code::FailedPrecondition
        );
        assert_eq!(status.message, "fail");

        // Wait for the stream to join its background tasks.
        stream.shutdown_token().shutdown().await;

        // Verify the acks went through.
        let mut got = Vec::new();
        while let Ok(ack_req) = ack_rx.try_recv() {
            assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
            got.extend(ack_req.ack_ids);
        }
        assert_eq!(sorted(got), test_ids(0..20));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn routing_header() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();

        mock.expect_streaming_pull().return_once(move |request| {
            let metadata = request.metadata();
            assert_eq!(
                metadata
                    .get("x-goog-request-params")
                    .expect("routing header missing"),
                "subscription=projects/p/subscriptions/s"
            );
            Err(TonicStatus::failed_precondition("ignored"))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;

        let _ = client
            .subscribe("projects/p/subscriptions/s")
            .build()
            .next()
            .await;

        Ok(())
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio_test_no_panics(start_paused = true)]
    async fn into_stream() -> anyhow::Result<()> {
        use futures::TryStreamExt;
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;

        let stream = client
            .subscribe("projects/p/subscriptions/s")
            .build()
            .into_stream();

        response_tx.send(Ok(test_response(1..3))).await?;
        drop(response_tx);

        let got: Vec<_> = stream
            .map_ok(|(m, h)| {
                h.ack();
                m.data
            })
            .try_collect()
            .await?;
        assert_eq!(got, vec![test_data(1), test_data(2)]);

        let ack_req = ack_rx
            .recv()
            .await
            .expect("should receive acknowledgements");
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(1..3));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn basic_lease_expiration() -> anyhow::Result<()> {
        const MAX_LEASE_EXTENSION: Duration = Duration::from_secs(10);
        const MAX_LEASE: Duration = Duration::from_secs(30);
        // We configure a max lease for this test (30s) that differs from the
        // default (600s) to verify that an application's configuration
        // overrides the default.

        let (response_tx, response_rx) = channel(10);
        let (extend_tx, mut extend_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_modify_ack_deadline().returning(move |r| {
            extend_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_max_lease(MAX_LEASE)
            .set_max_lease_extension(MAX_LEASE_EXTENSION)
            .set_shutdown_behavior(ShutdownBehavior::NackImmediately)
            .build();

        response_tx.send(Ok(test_response(0..1))).await?;
        drop(response_tx);

        let (_m, _h) = stream
            .next()
            .await
            .expect("stream should yield a message")?;

        // Advance the clock well past the expected message expiration,
        // recording the time at which we sent the last lease extension.
        let start_time = Instant::now();
        let mut latest = None;
        for _ in 0..MAX_LEASE.as_secs() * 2 {
            while let Ok(r) = extend_rx.try_recv() {
                assert_ne!(r.ack_deadline_seconds, 0, "unexpectedly received a nack");
                latest = Some(start_time.elapsed());
            }
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        // Verify when we stop sending lease extensions.
        let expected_range = (MAX_LEASE - MAX_LEASE_EXTENSION)..=MAX_LEASE;
        assert!(
            latest.is_some_and(|t| expected_range.contains(&t)),
            "{latest:?}"
        );

        // Close the stream, to make sure pending operations complete.
        stream.shutdown_token().shutdown().await;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn shutdown_wait_for_processing() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge()
            .times(1)
            .returning(|_| Ok(TonicResponse::from(())));
        mock.expect_modify_ack_deadline()
            .returning(|_| Ok(TonicResponse::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_shutdown_behavior(ShutdownBehavior::WaitForProcessing)
            .build();

        response_tx.send(Ok(test_response(0..1))).await?;
        drop(response_tx);

        let (_m, h) = stream
            .next()
            .await
            .expect("stream should yield a message")?;

        tokio::spawn(async move {
            // Delay the ack until after the shutdown is signaled. It should
            // still go through.
            tokio::time::sleep(Duration::from_secs(5)).await;
            h.ack();
        });

        // Close the stream, to make sure pending operations complete.
        stream.shutdown_token().shutdown().await;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn at_least_once_and_exactly_once() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_modify_ack_deadline()
            .returning(|_| Ok(TonicResponse::from(())));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_shutdown_behavior(ShutdownBehavior::NackImmediately)
            .build();

        response_tx.send(Ok(test_response(0..1))).await?;
        response_tx
            .send(Ok(test_exactly_once_response(1..2)))
            .await?;
        response_tx.send(Ok(test_response(2..3))).await?;
        response_tx
            .send(Ok(test_exactly_once_response(3..4)))
            .await?;
        drop(response_tx);

        let (m, h) = stream.next().await.expect("should yield a message")?;
        assert_eq!(m.data, test_data(0));
        assert_eq!(h.ack_id(), test_id(0));
        assert!(matches!(h, Handler::AtLeastOnce(_)), "{h:?}");

        let (m, h) = stream.next().await.expect("should yield a message")?;
        assert_eq!(m.data, test_data(1));
        assert_eq!(h.ack_id(), test_id(1));
        assert!(matches!(h, Handler::ExactlyOnce(_)), "{h:?}");

        let (m, h) = stream.next().await.expect("should yield a message")?;
        assert_eq!(m.data, test_data(2));
        assert_eq!(h.ack_id(), test_id(2));
        assert!(matches!(h, Handler::AtLeastOnce(_)), "{h:?}");

        let (m, h) = stream.next().await.expect("should yield a message")?;
        assert_eq!(m.data, test_data(3));
        assert_eq!(h.ack_id(), test_id(3));
        assert!(matches!(h, Handler::ExactlyOnce(_)), "{h:?}");

        let end = stream.next().await.transpose()?;
        assert!(end.is_none(), "Received extra message: {end:?}");

        // Wait for the stream to join its background tasks.
        stream.shutdown_token().shutdown().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_before_open() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .returning(|_| Err(TonicStatus::unavailable("try again")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client.subscribe("projects/p/subscriptions/s").build();
        let shutdown_token = stream.shutdown_token();

        let next = tokio::spawn(async move { stream.next().await });
        shutdown_token.shutdown().await;

        let end = next.await?;
        assert!(end.is_none(), "Shutdown should end the stream, got {end:?}");

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn cancel_midstream() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (nack_tx, mut nack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().times(1).returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        mock.expect_modify_ack_deadline()
            .times(1)
            .returning(move |r| {
                nack_tx
                    .send(r.into_inner())
                    .expect("sending on channel always succeeds");
                Ok(TonicResponse::from(()))
            });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_shutdown_behavior(ShutdownBehavior::WaitForProcessing)
            .build();
        let shutdown_token = stream.shutdown_token();

        response_tx.send(Ok(test_response(1..10))).await?;
        for i in 1..6 {
            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/5")
            };
            assert_eq!(m.data, test_data(i));
            h.ack();
        }
        let shutdown = tokio::spawn(async move {
            shutdown_token.shutdown().await;
        });
        tokio::task::yield_now().await;
        let end = stream.next().await.transpose()?;
        assert!(end.is_none(), "Shutdown should end the stream, got {end:?}");

        // Verify that we drop the messages and handles in the pool that we have
        // not returned to the application yet.
        shutdown.await?;

        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(1..6));

        let nack_req = nack_rx.try_recv()?;
        assert_eq!(nack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(nack_req.ack_deadline_seconds, 0);
        assert_eq!(sorted(nack_req.ack_ids), test_ids(6..10));

        Ok(())
    }

    #[test_case(ShutdownBehavior::NackImmediately)]
    #[test_case(ShutdownBehavior::WaitForProcessing)]
    #[tokio_test_no_panics(start_paused = true)]
    async fn shutdown_without_next(shutdown_behavior: ShutdownBehavior) -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (nack_tx, mut nack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().times(1).returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        mock.expect_modify_ack_deadline()
            .times(1)
            .returning(move |r| {
                nack_tx
                    .send(r.into_inner())
                    .expect("sending on channel always succeeds");
                Ok(TonicResponse::from(()))
            });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_shutdown_behavior(shutdown_behavior)
            .build();
        let shutdown_token = stream.shutdown_token();

        response_tx.send(Ok(test_response(1..10))).await?;
        for i in 1..6 {
            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/5")
            };
            assert_eq!(m.data, test_data(i));
            h.ack();
        }
        // Note that the application does not have to call `stream.next()`, or
        // `drop(stream)` to begin the shutdown procedure after a cancel.
        shutdown_token.shutdown().await;

        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(1..6));

        let nack_req = nack_rx.try_recv()?;
        assert_eq!(nack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(nack_req.ack_deadline_seconds, 0);
        assert_eq!(sorted(nack_req.ack_ids), test_ids(6..10));

        Ok(())
    }

    #[test_case(ShutdownBehavior::NackImmediately)]
    #[test_case(ShutdownBehavior::WaitForProcessing)]
    #[tokio_test_no_panics(start_paused = true)]
    async fn stream_error_initiates_shutdown(
        shutdown_behavior: ShutdownBehavior,
    ) -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().times(1).returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_shutdown_behavior(shutdown_behavior)
            .build();
        let shutdown_token = stream.shutdown_token();

        response_tx.send(Ok(test_response(0..1))).await?;
        response_tx
            .send(Err(TonicStatus::failed_precondition("fail")))
            .await?;
        drop(response_tx);

        let (m, h) = stream.next().await.expect("should yield a message")?;
        assert_eq!(m.data, test_data(0));
        h.ack();

        let err = stream.next().await.expect("should yield an error");
        assert!(err.is_err(), "{err:?}");

        // Note that the application does not have to initiate a shutdown after
        // a permanent error.
        shutdown_token.wait_for_shutdown().await;

        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(ack_req.ack_ids, test_ids(0..1));

        Ok(())
    }

    #[test_case(ShutdownBehavior::NackImmediately)]
    #[test_case(ShutdownBehavior::WaitForProcessing)]
    #[tokio_test_no_panics(start_paused = true)]
    async fn drop_cancels(shutdown_behavior: ShutdownBehavior) -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(10);
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (nack_tx, mut nack_rx) = unbounded_channel();

        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        mock.expect_acknowledge().times(1).returning(move |r| {
            ack_tx
                .send(r.into_inner())
                .expect("sending on channel always succeeds");
            Ok(TonicResponse::from(()))
        });
        mock.expect_modify_ack_deadline()
            .times(1)
            .returning(move |r| {
                nack_tx
                    .send(r.into_inner())
                    .expect("sending on channel always succeeds");
                Ok(TonicResponse::from(()))
            });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = test_client(endpoint).await?;
        let mut stream = client
            .subscribe("projects/p/subscriptions/s")
            .set_shutdown_behavior(shutdown_behavior)
            .build();
        let shutdown_token = stream.shutdown_token();

        response_tx.send(Ok(test_response(1..10))).await?;
        for i in 1..6 {
            let Some((m, h)) = stream.next().await.transpose()? else {
                anyhow::bail!("expected message {i}/5")
            };
            assert_eq!(m.data, test_data(i));
            h.ack();
        }
        drop(stream); // Equivalent to cancelling the `ShutdownToken`
        shutdown_token.wait_for_shutdown().await;

        let ack_req = ack_rx.try_recv()?;
        assert_eq!(ack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(sorted(ack_req.ack_ids), test_ids(1..6));

        let nack_req = nack_rx.try_recv()?;
        assert_eq!(nack_req.subscription, "projects/p/subscriptions/s");
        assert_eq!(nack_req.ack_deadline_seconds, 0);
        assert_eq!(sorted(nack_req.ack_ids), test_ids(6..10));

        Ok(())
    }
}
