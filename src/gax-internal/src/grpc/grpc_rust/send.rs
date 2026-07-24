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

use bytes::{Buf, Bytes};
use grpc::client::{SendOptions, SendStream};
use grpc::core::SendMessage;
use prost::Message;

/// A [`SendMessage`] adapter that encodes Prost protobuf messages for `grpc-rust`.
#[derive(Debug)]
pub struct GrpcRustSend<T>(pub T);

/// Implementation of [`SendMessage`] so that [`GrpcRustSend`] knows how to encode a [`prost::Message`] into raw protobuf bytes.
impl<T> SendMessage for GrpcRustSend<T>
where
    T: prost::Message,
{
    fn encode(&self) -> std::result::Result<Box<dyn Buf + Send + Sync>, String> {
        Ok(Box::new(Bytes::from(self.0.encode_to_vec())))
    }
}

/// A handle for the background task pumping outbound requests to a `grpc-rust` [`SendStream`].
///
/// This background pump is required because [`SendStream::send`](grpc::client::SendStream::send) is not cancellation-safe.
/// If an outer caller cancels an async operation (e.g., via a timeout, `tokio::select!`, or dropping a stream early) that directly
/// uses `SendStream::send`, the stream could become corrupted. This might corrupt the entire HTTP/2 connection,
/// causing all other concurrent RPCs multiplexed over that same connection to fail or disconnect.
///
/// Isolating `SendStream` in a dedicated background task ensures `send` calls run to completion, while aborting the task
/// on drop cleanly releases the owned [`SendStream`]. Because `SendStream` is owned by the background loop,
/// `send()` is never interrupted mid-call by outer task drops.
pub(super) struct SendTask {
    // The handle is wrapped in an `Option` to ensure it is not aborted twice if the
    // [`SendTask`] is [`join()`](Self::join)ed before it is dropped.
    handle: Option<tokio::task::JoinHandle<tonic::Result<()>>>,
}

impl SendTask {
    const ERROR_MESSAGE_STREAM_CLOSED: &'static str = "grpc-rust request stream closed";

    /// Creates a new [`SendTask`] wrapping a background request task handle.
    pub(super) const fn new(handle: tokio::task::JoinHandle<tonic::Result<()>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Starts a background task that pumps requests from `requests` stream into `send`.
    ///
    /// Returns a [`SendTask`] for managing background task cancellation and monitoring.
    pub(super) fn start<Request, S, R>(send: S, requests: R) -> Self
    where
        Request: Message + 'static,
        S: SendStream + 'static,
        R: tokio_stream::Stream<Item = Request> + Send + 'static,
    {
        let handle = tokio::spawn(send_requests(send, requests));
        Self::new(handle)
    }

    /// `true` if the background request task can be [`join()`](Self::join)ed.
    pub(super) fn is_joinable(&self) -> bool {
        self.handle.is_some()
    }

    /// Awaits the background request task and checks for send failures.
    pub(super) async fn join(&mut self) -> tonic::Result<()> {
        // Await the background task to finish before taking it so that if `join()` is cancelled
        // mid-await, `self.handle` remains `Some(...)` and `SendTask::drop()` can still abort the
        // task.
        //
        // If we were to `take()` the handle before awaiting it, a cancellation during `join()`
        // would leave `self.handle` as `None`, preventing `drop()` from aborting the handle and
        // causing the background task to become an orphaned "ghost" task.
        let Some(handle) = self.handle.as_mut() else {
            return Ok(());
        };
        let result = handle.await;
        self.handle.take();
        match result {
            // Task ran to completion without encountering send errors.
            Ok(Ok(())) => Ok(()),
            // Task completed, but send loop returned a status error (e.g., stream closed).
            Ok(Err(status)) => Err(status),
            // Task panicked or failed unexpectedly.
            Err(error) => Err(tonic::Status::internal(format!(
                "grpc-rust request task failed: {error}"
            ))),
        }
    }
}

impl Drop for SendTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            // The task owns the SendStream, so aborting drops that too.
            handle.abort();
        }
    }
}

/// Drives the outbound request pump loop.
pub(super) async fn send_requests<Request>(
    mut send: impl SendStream + 'static,
    requests: impl tokio_stream::Stream<Item = Request> + Send + 'static,
) -> tonic::Result<()>
where
    Request: Message + 'static,
{
    use tokio_stream::StreamExt;

    tokio::pin!(requests);
    while let Some(request) = requests.next().await {
        send.send(&GrpcRustSend(request), SendOptions::default())
            .await
            .map_err(|_| tonic::Status::internal(SendTask::ERROR_MESSAGE_STREAM_CLOSED))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    struct TestPendingSendStream;

    impl SendStream for TestPendingSendStream {
        async fn send(
            &mut self,
            _message: &dyn SendMessage,
            _options: SendOptions,
        ) -> Result<(), ()> {
            std::future::pending().await
        }
    }

    struct TestFailingSendStream;

    impl SendStream for TestFailingSendStream {
        async fn send(
            &mut self,
            _message: &dyn SendMessage,
            _options: SendOptions,
        ) -> Result<(), ()> {
            Err(())
        }
    }

    #[test]
    fn grpc_rust_send_encodes_correctly() -> anyhow::Result<()> {
        // Arrange
        let want = TestMessage {
            value: "hello".to_string(),
        };

        // Act
        let mut encoded = GrpcRustSend(want.clone())
            .encode()
            .map_err(anyhow::Error::msg)?;
        let got = TestMessage::decode(encoded.copy_to_bytes(encoded.remaining()))?;

        // Assert
        assert_eq!(got, want);
        Ok(())
    }

    #[tokio::test]
    async fn send_task_join_returns_status_when_send_fails() -> anyhow::Result<()> {
        // Arrange
        let stream = tokio_stream::iter(vec![TestMessage {
            value: "hello".to_string(),
        }]);
        let mut task = SendTask::start(TestFailingSendStream, stream);

        // Act
        let status = task
            .join()
            .await
            .expect_err("expected status error when send fails");

        // Assert
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), SendTask::ERROR_MESSAGE_STREAM_CLOSED);
        Ok(())
    }

    #[tokio::test]
    async fn send_task_join_returns_internal_status_when_task_panics() -> anyhow::Result<()> {
        // Arrange
        const SIMULATED_PANIC_MSG: &str = "simulated panic in send stream";

        struct TestPanicSendStream;

        impl SendStream for TestPanicSendStream {
            async fn send(
                &mut self,
                _message: &dyn SendMessage,
                _options: SendOptions,
            ) -> Result<(), ()> {
                panic!("{SIMULATED_PANIC_MSG}");
            }
        }

        let stream = tokio_stream::iter(vec![TestMessage {
            value: "hello".to_string(),
        }]);
        let mut task = SendTask::start(TestPanicSendStream, stream);

        // Act
        let status = task
            .join()
            .await
            .expect_err("expected status error on task panic");

        // Assert
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains(SIMULATED_PANIC_MSG));
        Ok(())
    }

    #[tokio::test]
    async fn send_task_join_clears_handle_allowing_safe_drop() -> anyhow::Result<()> {
        // Arrange
        let stream = tokio_stream::empty::<TestMessage>();
        let mut task = SendTask::start(TestPendingSendStream, stream);

        // Act
        let result = task.join().await;

        // Assert
        assert!(result.is_ok(), "task join should return Ok");
        assert!(
            !task.is_joinable(),
            "task should no longer be joinable after join"
        );
        assert!(task.handle.is_none(), "handle should be cleared after join");
        drop(task); // Shouldn't panic
        Ok(())
    }
}
