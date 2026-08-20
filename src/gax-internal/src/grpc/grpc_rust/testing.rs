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

//! Common test helpers, fixtures, and mock streams for `grpc_rust` unit tests.

use bytes::Bytes;
use grpc::client::{
    CallOptions, Invoke, RecvStream, RequestHeaders, ResponseHeaders, ResponseStreamItem,
    SendOptions, SendStream, Trailers,
};
use grpc::core::{RecvMessage, SendMessage};
use prost::Message;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq, Message)]
pub struct TestMessage {
    /// The string payload value.
    #[prost(string, tag = "1")]
    pub value: String,
}

impl TestMessage {
    /// Creates a new [`TestMessage`] with the provided value.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

/// A mock [`SendStream`] that captures sent [`TestMessage`] items and [`SendOptions`].
#[derive(Clone, Default)]
pub struct MockSendStream {
    observed_messages: Arc<Mutex<Vec<TestMessage>>>,
    observed_send_options: Arc<Mutex<Option<SendOptions>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl MockSendStream {
    /// Creates a new [`MockSendStream`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the observed messages.
    pub fn observed_messages(&self) -> Vec<TestMessage> {
        self.observed_messages
            .lock()
            .expect("lock observed messages")
            .clone()
    }

    /// Returns the observed send options, if any.
    pub fn observed_send_options(&self) -> Option<SendOptions> {
        self.observed_send_options
            .lock()
            .expect("lock observed send options")
            .clone()
    }

    /// Returns the notification handle triggered when a message is sent.
    pub fn notify_handle(&self) -> Arc<tokio::sync::Notify> {
        self.notify.clone()
    }
}

impl SendStream for MockSendStream {
    async fn send(&mut self, message: &dyn SendMessage, options: SendOptions) -> Result<(), ()> {
        let mut encoded = message.encode().map_err(|_| ())?;
        let decoded = TestMessage::decode(&mut encoded).map_err(|_| ())?;
        self.observed_messages
            .lock()
            .expect("lock observed messages")
            .push(decoded);
        *self
            .observed_send_options
            .lock()
            .expect("lock observed send options") = Some(options);
        self.notify.notify_one();
        Ok(())
    }
}

/// A [`SendStream`] that immediately fails every send attempt.
#[derive(Clone, Debug)]
pub struct FailingSendStream;

impl SendStream for FailingSendStream {
    async fn send(&mut self, _message: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
        Err(())
    }
}

/// A [`SendStream`] that returns pending forever.
#[derive(Clone, Debug)]
pub struct PendingSendStream;

impl SendStream for PendingSendStream {
    async fn send(&mut self, _message: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
        std::future::pending().await
    }
}

/// A [`SendStream`] that panics on send with a given message.
#[derive(Clone, Debug)]
pub struct PanicSendStream {
    /// Panic message to emit.
    pub panic_msg: &'static str,
}

impl PanicSendStream {
    /// Creates a new [`PanicSendStream`].
    pub const fn new(panic_msg: &'static str) -> Self {
        Self { panic_msg }
    }
}

impl SendStream for PanicSendStream {
    async fn send(&mut self, _message: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
        panic!("{}", self.panic_msg);
    }
}

/// A [`SendStream`] that succeeds on its first message and then waits on `fail_gate` before failing.
pub struct FailAfterFirstSendStream {
    sent_count: usize,
    fail_gate: Arc<tokio::sync::Notify>,
    failed: Arc<tokio::sync::Notify>,
}

impl FailAfterFirstSendStream {
    /// Creates a gated stream returning `(stream, fail_gate, failed)`.
    pub fn gated() -> (Self, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let fail_gate = Arc::new(tokio::sync::Notify::new());
        let failed = Arc::new(tokio::sync::Notify::new());
        (
            Self {
                sent_count: 0,
                fail_gate: fail_gate.clone(),
                failed: failed.clone(),
            },
            fail_gate,
            failed,
        )
    }
}

impl SendStream for FailAfterFirstSendStream {
    async fn send(&mut self, _message: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
        self.sent_count += 1;
        if self.sent_count == 1 {
            return Ok(());
        }
        self.fail_gate.notified().await;
        self.failed.notify_one();
        Err(())
    }
}

/// Scripted actions yielded by [`MockRecvStream`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum MockRecvAction {
    /// Wait until the given [`tokio::sync::Notify`] is triggered.
    Wait(Arc<tokio::sync::Notify>),
    /// Yield response headers.
    Headers(ResponseHeaders),
    /// Yield a response message.
    Message(TestMessage),
    /// Yield response trailers.
    Trailers(Trailers),
}

/// A mock [`RecvStream`] that executes a queue of [`MockRecvAction`]s.
#[derive(Clone, Debug)]
pub struct MockRecvStream {
    actions: VecDeque<MockRecvAction>,
}

impl MockRecvStream {
    /// Creates a new [`MockRecvStream`] from an iterator of actions.
    pub fn new(actions: impl IntoIterator<Item = MockRecvAction>) -> Self {
        Self {
            actions: actions.into_iter().collect(),
        }
    }

    /// Creates a stream that yields only immediate trailers.
    pub fn with_immediate_trailers(trailers: Trailers) -> Self {
        Self::new([MockRecvAction::Trailers(trailers)])
    }

    /// Creates a stream that yields headers followed by trailers.
    pub fn with_headers_and_trailers(headers: ResponseHeaders, trailers: Trailers) -> Self {
        Self::new([
            MockRecvAction::Headers(headers),
            MockRecvAction::Trailers(trailers),
        ])
    }
}

impl RecvStream for MockRecvStream {
    async fn recv(&mut self, message: &mut dyn RecvMessage) -> ResponseStreamItem {
        while let Some(action) = self.actions.pop_front() {
            match action {
                MockRecvAction::Wait(notify) => notify.notified().await,
                MockRecvAction::Headers(headers) => {
                    return ResponseStreamItem::Headers(headers);
                }
                MockRecvAction::Message(response) => {
                    let mut encoded = Bytes::from(response.encode_to_vec());
                    message
                        .decode(&mut encoded)
                        .expect("decode response message");
                    return ResponseStreamItem::Message;
                }
                MockRecvAction::Trailers(trailers) => {
                    return ResponseStreamItem::Trailers(trailers);
                }
            }
        }
        ResponseStreamItem::StreamClosed
    }
}

/// A [`RecvStream`] that is immediately closed.
#[derive(Clone, Debug)]
pub struct ClosedRecvStream;

impl RecvStream for ClosedRecvStream {
    async fn recv(&mut self, _message: &mut dyn RecvMessage) -> ResponseStreamItem {
        ResponseStreamItem::StreamClosed
    }
}

/// A [`RecvStream`] that stays pending forever.
#[derive(Clone, Debug)]
pub struct PendingRecvStream;

impl RecvStream for PendingRecvStream {
    async fn recv(&mut self, _message: &mut dyn RecvMessage) -> ResponseStreamItem {
        std::future::pending().await
    }
}

/// A [`RecvStream`] that panics when polled.
#[derive(Clone, Debug)]
pub struct PanicRecvStream {
    /// Panic message to emit.
    pub panic_msg: &'static str,
}

impl PanicRecvStream {
    /// Creates a new [`PanicRecvStream`].
    pub const fn new(panic_msg: &'static str) -> Self {
        Self { panic_msg }
    }
}

impl RecvStream for PanicRecvStream {
    async fn recv(&mut self, _message: &mut dyn RecvMessage) -> ResponseStreamItem {
        panic!("{}", self.panic_msg);
    }
}

/// A mock implementation of [`Invoke`] that dispenses preconfigured send and receive streams.
pub struct MockInvoker<S = MockSendStream, R = MockRecvStream> {
    send_stream: Mutex<Option<S>>,
    recv_stream: Mutex<Option<R>>,
    observed_headers: Arc<Mutex<Option<RequestHeaders>>>,
}

impl<S, R> MockInvoker<S, R> {
    /// Creates a new [`MockInvoker`] with the provided send and receive streams.
    pub fn new(send_stream: S, recv_stream: R) -> Self {
        Self {
            send_stream: Mutex::new(Some(send_stream)),
            recv_stream: Mutex::new(Some(recv_stream)),
            observed_headers: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the observed request headers, if any.
    pub fn observed_headers(&self) -> Option<RequestHeaders> {
        self.observed_headers
            .lock()
            .expect("lock observed headers")
            .clone()
    }
}

impl<S, R> Invoke for MockInvoker<S, R>
where
    S: SendStream + Send + 'static,
    R: RecvStream + Send + 'static,
{
    type SendStream = S;
    type RecvStream = R;

    async fn invoke(
        &self,
        headers: RequestHeaders,
        _options: CallOptions,
    ) -> (Self::SendStream, Self::RecvStream) {
        *self.observed_headers.lock().expect("lock observed headers") = Some(headers);
        (
            self.send_stream
                .lock()
                .expect("lock send stream")
                .take()
                .expect("send stream should only be invoked once"),
            self.recv_stream
                .lock()
                .expect("lock recv stream")
                .take()
                .expect("recv stream should only be invoked once"),
        )
    }
}
