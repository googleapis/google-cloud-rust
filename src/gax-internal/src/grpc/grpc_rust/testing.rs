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
    RecvStream, ResponseHeaders, ResponseStreamItem, SendOptions, SendStream, Trailers,
};
use grpc::core::{RecvMessage, SendMessage};
use prost::Message;
use std::collections::VecDeque;

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

/// A [`SendStream`] that immediately fails every send attempt.
pub struct FailingSendStream;

impl SendStream for FailingSendStream {
    async fn send(&mut self, _message: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
        Err(())
    }
}

/// A [`SendStream`] that returns pending forever.
pub struct PendingSendStream;

impl SendStream for PendingSendStream {
    async fn send(&mut self, _message: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
        std::future::pending().await
    }
}

/// A [`SendStream`] that panics on send with a given message.
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

/// Scripted actions yielded by [`MockRecvStream`].
#[non_exhaustive]
pub enum MockRecvAction {
    /// Yield response headers.
    Headers(ResponseHeaders),
    /// Yield a response message.
    Message(TestMessage),
    /// Yield response trailers.
    Trailers(Trailers),
}

/// A mock [`RecvStream`] that executes a queue of [`MockRecvAction`]s.
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
}

impl RecvStream for MockRecvStream {
    async fn recv(&mut self, message: &mut dyn RecvMessage) -> ResponseStreamItem {
        if let Some(action) = self.actions.pop_front() {
            match action {
                MockRecvAction::Headers(headers) => ResponseStreamItem::Headers(headers),
                MockRecvAction::Message(response) => {
                    let mut encoded = Bytes::from(response.encode_to_vec());
                    message
                        .decode(&mut encoded)
                        .expect("decode response message");
                    ResponseStreamItem::Message
                }
                MockRecvAction::Trailers(trailers) => ResponseStreamItem::Trailers(trailers),
            }
        } else {
            ResponseStreamItem::StreamClosed
        }
    }
}

/// A [`RecvStream`] that is immediately closed.
pub struct ClosedRecvStream;

impl RecvStream for ClosedRecvStream {
    async fn recv(&mut self, _message: &mut dyn RecvMessage) -> ResponseStreamItem {
        ResponseStreamItem::StreamClosed
    }
}

/// A [`RecvStream`] that stays pending forever.
pub struct PendingRecvStream;

impl RecvStream for PendingRecvStream {
    async fn recv(&mut self, _message: &mut dyn RecvMessage) -> ResponseStreamItem {
        std::future::pending().await
    }
}

/// A [`RecvStream`] that panics when polled.
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
