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

//! This module provides [`ReceiveTask`] to safely pump gRPC responses from a
//! `grpc-rust` [`RecvStream`](grpc::client::RecvStream) in a background task,
//! as well as [`GrpcRustRecv`] to decode protobuf messages using Prost.

use bytes::Buf;
use grpc::StatusCodeError;
use grpc::client::{RecvStream, ResponseStreamItem};
use grpc::core::{RecvMessage, Trailers};
use prost::Message;

/// A [`RecvMessage`] adapter that decodes raw byte payloads into Prost protobuf messages.
pub(super) struct GrpcRustRecv<T>(Option<T>);

impl<T> Default for GrpcRustRecv<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrpcRustRecv<T> {
    /// Creates a new unpopulated [`GrpcRustRecv`] receiver.
    pub(super) const fn new() -> Self {
        Self(None)
    }

    /// Takes the decoded message.
    pub(super) fn take(&mut self) -> tonic::Result<T> {
        self.0.take().ok_or_else(|| {
            tonic::Status::internal("grpc-rust response message missing or already taken")
        })
    }
}

/// Implementation of [`RecvMessage`] so that [`GrpcRustRecv`] knows how to decode raw protobuf bytes into a [`prost::Message`].
impl<T> RecvMessage for GrpcRustRecv<T>
where
    T: prost::Message + Default,
{
    fn decode(&mut self, data: &mut dyn Buf) -> std::result::Result<(), String> {
        self.0 = Some(T::decode(data).map_err(|e| e.to_string())?);
        Ok(())
    }
}

/// A handle for the background task pulling responses from a `grpc-rust` [`RecvStream`].
///
/// This background pump is required because [`RecvStream::recv`](grpc::client::RecvStream::recv) is not cancellation-safe.
/// If an outer caller cancels an async operation (e.g., via a timeout, `tokio::select!`, or dropping a stream early) that directly
/// uses `RecvStream::recv`, the stream could become corrupted.
///
/// Isolating `RecvStream` in a dedicated background task ensures `recv` calls run to completion, while aborting the task
/// on drop cleanly releases the owned [`RecvStream`]. Because `RecvStream` is owned by the background loop,
/// `recv()` is never interrupted mid-call by outer task drops. Downstream application code only reads from a
/// [`tokio::sync::mpsc::Receiver`], which is safe to drop or cancel at any time.
#[derive(Debug)]
pub struct ReceiveTask {
    // The handle is wrapped in an `Option` to ensure the handle is not aborted twice if the
    // [`ReceiveTask`] is [`join()`](Self::join)ed before it is dropped.
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ReceiveTask {
    const ERROR_MESSAGE_TASK_EXITED: &'static str =
        "grpc-rust response task exited without a terminal response";
    const ERROR_MESSAGE_TASK_CANCELLED: &'static str = "grpc-rust response task was cancelled";
    const ERROR_MESSAGE_TASK_ALREADY_JOINED: &'static str =
        "grpc-rust response task was already joined";

    /// Starts a background task that pumps responses from `recv` into a bounded channel.
    ///
    /// Returns a channel receiver for responses and a [`ReceiveTask`] handle for managing task cancellation.
    pub fn start<Response, R>(
        recv: R,
    ) -> (
        tokio::sync::mpsc::Receiver<tonic::Result<Option<Response>>>,
        Self,
    )
    where
        Response: Message + Default + Send + 'static,
        R: RecvStream + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let handle = tokio::spawn(receive_responses(recv, tx));
        (
            rx,
            Self {
                handle: Some(handle),
            },
        )
    }

    /// Awaits the background response pump task and retrieves its termination error status.
    ///
    /// Called when the response channel closes unexpectedly (e.g. `recv()` returns `None`).
    /// It joins the background task and maps its result into a [`tonic::Status`] error.
    pub(super) async fn join(&mut self) -> tonic::Status {
        let Some(handle) = self.handle.as_mut() else {
            return tonic::Status::internal(Self::ERROR_MESSAGE_TASK_ALREADY_JOINED);
        };
        // Await the background task to finish before taking it so that if `join()` is cancelled
        // mid-await, `self.handle` remains `Some(...)` and `ReceiveTask::drop()` can still abort the
        // task.
        //
        // If we were to `take()` the handle before awaiting it, a cancellation during `join()`
        // would leave `self.handle` as `None`, preventing `drop()` from aborting the handle and
        // causing the background task to become an orphaned "ghost" task.
        let result = handle.await;
        self.handle.take();
        match result {
            // Task exited cleanly.
            Ok(()) => tonic::Status::internal(Self::ERROR_MESSAGE_TASK_EXITED),
            // Task was explicitly cancelled.
            Err(error) if error.is_cancelled() => {
                tonic::Status::cancelled(Self::ERROR_MESSAGE_TASK_CANCELLED)
            }
            // Task panicked or failed unexpectedly.
            Err(error) => {
                tonic::Status::internal(format!("grpc-rust response task failed: {error}"))
            }
        }
    }
}

impl Drop for ReceiveTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            // The task owns the RecvStream, so aborting drops that too.
            handle.abort();
        }
    }
}

/// Drives the response pump loop
async fn receive_responses<Response, R>(
    mut recv: R,
    tx: tokio::sync::mpsc::Sender<tonic::Result<Option<Response>>>,
) where
    Response: Message + Default + Send + 'static,
    R: RecvStream + 'static,
{
    // A container into which incoming message payloads are decoded.
    let mut slot = GrpcRustRecv::<Response>::default();
    loop {
        // Reserve before receiving to apply backpressure.
        let Ok(permit) = tx.reserve().await else {
            return;
        };
        let (response, is_terminal) = match recv.recv(&mut slot).await {
            // TODO(#5991): Headers will be processed separately in a later PR.
            ResponseStreamItem::Headers(_) => continue,
            ResponseStreamItem::Message => match slot.take() {
                Ok(message) => (Ok(Some(message)), false),
                // A missing/undecodable message breaks the stream sequence
                // and renders it unrecoverable.
                Err(status) => (Err(status), true),
            },
            ResponseStreamItem::Trailers(trailers) => (
                trailers_to_tonic_status(trailers).map_or(Ok(None), Err),
                true,
            ),
            ResponseStreamItem::StreamClosed => (
                Err(tonic::Status::internal(
                    "grpc-rust response stream closed without trailers",
                )),
                true,
            ),
        };
        permit.send(response);
        if is_terminal {
            return;
        }
    }
}

/// Converts gRPC response [`Trailers`] into a [`tonic::Status`], preserving status code, message, metadata, and `grpc-status-details-bin`.
pub(super) fn trailers_to_tonic_status(trailers: Trailers) -> Option<tonic::Status> {
    let status = trailers.status().as_ref().err()?;
    let metadata: tonic::metadata::MetadataMap = trailers.metadata().clone().into();
    let details = metadata
        .get_bin("grpc-status-details-bin")
        .and_then(|value| value.to_bytes().ok())
        .unwrap_or_default();
    Some(tonic::Status::with_details_and_metadata(
        grpc_rust_error_to_tonic_code(status.code()),
        status.message().to_string(),
        details,
        metadata,
    ))
}

/// Maps a `grpc-rust` [`StatusCodeError`] to the corresponding [`tonic::Code`].
// TODO(#5991): Consider skipping conversion to `tonic::Code` by mapping
// directly to `google_cloud_gax` error types
// (https://github.com/googleapis/google-cloud-rust/pull/6082#discussion_r3603786027).
fn grpc_rust_error_to_tonic_code(code: StatusCodeError) -> tonic::Code {
    match code {
        StatusCodeError::Cancelled => tonic::Code::Cancelled,
        StatusCodeError::Unknown => tonic::Code::Unknown,
        StatusCodeError::InvalidArgument => tonic::Code::InvalidArgument,
        StatusCodeError::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        StatusCodeError::NotFound => tonic::Code::NotFound,
        StatusCodeError::AlreadyExists => tonic::Code::AlreadyExists,
        StatusCodeError::PermissionDenied => tonic::Code::PermissionDenied,
        StatusCodeError::ResourceExhausted => tonic::Code::ResourceExhausted,
        StatusCodeError::FailedPrecondition => tonic::Code::FailedPrecondition,
        StatusCodeError::Aborted => tonic::Code::Aborted,
        StatusCodeError::OutOfRange => tonic::Code::OutOfRange,
        StatusCodeError::Unimplemented => tonic::Code::Unimplemented,
        StatusCodeError::Internal => tonic::Code::Internal,
        StatusCodeError::Unavailable => tonic::Code::Unavailable,
        StatusCodeError::DataLoss => tonic::Code::DataLoss,
        StatusCodeError::Unauthenticated => tonic::Code::Unauthenticated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grpc::StatusError;
    use grpc::metadata::MetadataValue;
    use pretty_assertions::assert_eq;
    use test_case::test_case;

    #[test]
    fn trailers_to_tonic_status_preserves_details_and_metadata() {
        // Arrange
        const TEST_HEADER: &str = "x-test-header";
        const TEST_VALUE: &str = "test-value";
        const GRPC_STATUS_DETAILS_BIN: &str = "grpc-status-details-bin";
        const ERROR_MESSAGE_DENIED: &str = "denied";

        let details = b"status-details";
        let mut metadata = grpc::metadata::MetadataMap::new();
        metadata.insert_bin(GRPC_STATUS_DETAILS_BIN, MetadataValue::from_bytes(details));
        metadata.insert(TEST_HEADER, MetadataValue::from_static(TEST_VALUE));
        let trailers = Trailers::new(Err(StatusError::new(
            StatusCodeError::PermissionDenied,
            ERROR_MESSAGE_DENIED,
        )))
        .with_metadata(metadata);

        // Act
        let got = trailers_to_tonic_status(trailers).expect("non-OK trailers should map to status");

        // Assert
        assert_eq!(got.code(), tonic::Code::PermissionDenied);
        assert_eq!(got.message(), ERROR_MESSAGE_DENIED);
        assert_eq!(got.details(), details);
        assert_eq!(
            got.metadata()
                .get(TEST_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some(TEST_VALUE)
        );
    }

    #[test_case(StatusCodeError::Cancelled, tonic::Code::Cancelled)]
    #[test_case(StatusCodeError::Unknown, tonic::Code::Unknown)]
    #[test_case(StatusCodeError::InvalidArgument, tonic::Code::InvalidArgument)]
    #[test_case(StatusCodeError::DeadlineExceeded, tonic::Code::DeadlineExceeded)]
    #[test_case(StatusCodeError::NotFound, tonic::Code::NotFound)]
    #[test_case(StatusCodeError::AlreadyExists, tonic::Code::AlreadyExists)]
    #[test_case(StatusCodeError::PermissionDenied, tonic::Code::PermissionDenied)]
    #[test_case(StatusCodeError::ResourceExhausted, tonic::Code::ResourceExhausted)]
    #[test_case(StatusCodeError::FailedPrecondition, tonic::Code::FailedPrecondition)]
    #[test_case(StatusCodeError::Aborted, tonic::Code::Aborted)]
    #[test_case(StatusCodeError::OutOfRange, tonic::Code::OutOfRange)]
    #[test_case(StatusCodeError::Unimplemented, tonic::Code::Unimplemented)]
    #[test_case(StatusCodeError::Internal, tonic::Code::Internal)]
    #[test_case(StatusCodeError::Unavailable, tonic::Code::Unavailable)]
    #[test_case(StatusCodeError::DataLoss, tonic::Code::DataLoss)]
    #[test_case(StatusCodeError::Unauthenticated, tonic::Code::Unauthenticated)]
    fn grpc_rust_error_to_tonic_code_maps_correctly(input: StatusCodeError, want: tonic::Code) {
        // Act & Assert
        assert_eq!(grpc_rust_error_to_tonic_code(input), want);
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    #[test]
    fn grpc_rust_recv_decodes_correctly() -> anyhow::Result<()> {
        // Arrange
        let want = TestMessage {
            value: "hello".to_string(),
        };
        let mut encoded = bytes::Bytes::from(want.encode_to_vec());
        let mut recv = GrpcRustRecv::<TestMessage>::new();

        // Act
        RecvMessage::decode(&mut recv, &mut encoded).map_err(anyhow::Error::msg)?;
        let got = recv.take()?;

        // Assert
        assert_eq!(got, want);
        Ok(())
    }

    struct TestClosedStream;

    impl RecvStream for TestClosedStream {
        async fn recv(&mut self, _buf: &mut dyn RecvMessage) -> ResponseStreamItem {
            ResponseStreamItem::StreamClosed
        }
    }

    struct TestPendingStream;

    impl RecvStream for TestPendingStream {
        async fn recv(&mut self, _buf: &mut dyn RecvMessage) -> ResponseStreamItem {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn receive_task_join_returns_internal_status_when_stream_closed_without_trailers()
    -> anyhow::Result<()> {
        // Arrange
        let (mut rx, mut task) = ReceiveTask::start::<TestMessage, _>(TestClosedStream);

        // Act
        let item = rx
            .recv()
            .await
            .expect("channel should yield error status item")
            .expect_err("expected internal error on stream closed without trailers");

        // Assert
        assert_eq!(item.code(), tonic::Code::Internal);

        // Act
        let status = task.join().await;

        // Assert
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), ReceiveTask::ERROR_MESSAGE_TASK_EXITED);
        Ok(())
    }

    #[tokio::test]
    async fn receive_task_join_returns_internal_status_when_receiver_dropped_early()
    -> anyhow::Result<()> {
        // Arrange
        let (rx, mut task) = ReceiveTask::start::<TestMessage, _>(TestPendingStream);
        drop(rx);

        // Act
        let status = task.join().await;

        // Assert
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), ReceiveTask::ERROR_MESSAGE_TASK_EXITED);
        Ok(())
    }

    #[tokio::test]
    async fn receive_task_join_returns_internal_status_when_task_panics() -> anyhow::Result<()> {
        // Arrange
        const SIMULATED_PANIC_MSG: &str = "simulated panic in recv stream";

        struct TestPanicStream;

        impl RecvStream for TestPanicStream {
            async fn recv(&mut self, _buf: &mut dyn RecvMessage) -> ResponseStreamItem {
                panic!("{SIMULATED_PANIC_MSG}");
            }
        }

        let (_rx, mut task) = ReceiveTask::start::<TestMessage, _>(TestPanicStream);

        // Act
        let status = task.join().await;

        // Assert
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains(SIMULATED_PANIC_MSG));
        Ok(())
    }

    #[tokio::test]
    async fn receive_task_join_returns_cancelled_status_when_task_aborted() -> anyhow::Result<()> {
        // Arrange
        let (_rx, mut task) = ReceiveTask::start::<TestMessage, _>(TestPendingStream);
        let handle = task
            .handle
            .as_ref()
            .expect("ReceiveTask should contain task handle after start");
        handle.abort();

        // Act
        let status = task.join().await;

        // Assert
        assert_eq!(status.code(), tonic::Code::Cancelled);
        assert_eq!(status.message(), ReceiveTask::ERROR_MESSAGE_TASK_CANCELLED);
        Ok(())
    }

    #[tokio::test]
    async fn receive_task_join_clears_handle_allowing_safe_drop() -> anyhow::Result<()> {
        // Arrange
        let (_rx, mut task) = ReceiveTask::start::<TestMessage, _>(TestClosedStream);

        // Act
        let status = task.join().await;

        // Assert
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(task.handle.is_none(), "handle should be cleared after join");
        drop(task); // Shouldn't panic
        Ok(())
    }

    #[tokio::test]
    async fn receive_task_join_called_multiple_times_returns_error() -> anyhow::Result<()> {
        // Arrange
        let (_rx, mut task) = ReceiveTask::start::<TestMessage, _>(TestPendingStream);
        let handle = task
            .handle
            .as_ref()
            .expect("ReceiveTask should contain task handle after start");
        handle.abort();

        // Act
        let first_status = task.join().await;
        let second_status = task.join().await;

        // Assert
        // 1st join() retrieves the actual background task termination status
        assert_eq!(first_status.code(), tonic::Code::Cancelled);
        assert_eq!(
            first_status.message(),
            ReceiveTask::ERROR_MESSAGE_TASK_CANCELLED
        );

        // 2nd join() safely handles `self.handle == None`
        assert_eq!(second_status.code(), tonic::Code::Internal);
        assert_eq!(
            second_status.message(),
            ReceiveTask::ERROR_MESSAGE_TASK_ALREADY_JOINED
        );
        assert!(
            task.handle.is_none(),
            "handle should remain cleared after second join"
        );
        Ok(())
    }
}
