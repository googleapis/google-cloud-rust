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

pub use super::receive::ReceiveTask;
pub use super::receive::RecvItem;
pub use super::send::GrpcRustSend;
use super::send::SendTask;
use prost::Message;

/// A `tonic` adapter for `grpc-rust` RPCs.
///
/// Incoming server response messages are pulled via [`GrpcRustStreaming::message`].
///
/// Internally, this stream orchestrates:
/// - A background [`ReceiveTask`](super::receive::ReceiveTask) that decodes inbound protobuf messages into `Response`s.
/// - A background [`SendTask`](super::send::SendTask) that pumps outbound request messages.
///
/// `grpc-rust` stream operations ([`RecvStream::recv`](grpc::client::RecvStream::recv)
/// and [`SendStream::send`](grpc::client::SendStream::send)) are not cancellation-safe. Isolating them in
/// dedicated background tasks ensures that cancelling [`GrpcRustStreaming::message`] or dropping
/// [`GrpcRustStreaming`] is safe and prevents stream corruption.
pub struct GrpcRustStreaming<Response> {
    responses: Option<tokio::sync::mpsc::Receiver<tonic::Result<Option<RecvItem<Response>>>>>,
    receive_task: Option<ReceiveTask>,
    send_task: SendTask,
    /// Holds a pre-decoded initial response message (e.g. from stream setup or handshake)
    /// to yield on the first call to [`GrpcRustStreaming::message`] before polling `responses`.
    pending_response: Option<Response>,
}

impl<Response> GrpcRustStreaming<Response>
where
    Response: Message + Default + Send + 'static,
{
    /// Creates a new [`GrpcRustStreaming`] instance with an active response channel and [`ReceiveTask`].
    pub(super) fn new(
        responses: tokio::sync::mpsc::Receiver<tonic::Result<Option<RecvItem<Response>>>>,
        receive_task: ReceiveTask,
        send_task: SendTask,
    ) -> Self {
        Self {
            responses: Some(responses),
            receive_task: Some(receive_task),
            send_task,
            pending_response: None,
        }
    }

    /// Creates a new [`GrpcRustStreaming`] instance with a pre-decoded initial response message.
    ///
    /// The first call to [`GrpcRustStreaming::message`] will yield `pending` before returning
    /// subsequent items from the response channel.
    pub(super) fn new_with_pending(
        responses: tokio::sync::mpsc::Receiver<tonic::Result<Option<RecvItem<Response>>>>,
        receive_task: ReceiveTask,
        send_task: SendTask,
        pending: Response,
    ) -> Self {
        let mut stream = Self::new(responses, receive_task, send_task);
        stream.pending_response = Some(pending);
        stream
    }

    /// Creates a terminal (already closed) [`GrpcRustStreaming`] instance holding the outbound task.
    ///
    /// Subsequent calls to [`GrpcRustStreaming::message`] will return `Ok(None)`.
    pub(super) fn new_terminal(send_task: SendTask) -> Self {
        Self {
            responses: None,
            receive_task: None,
            send_task,
            pending_response: None,
        }
    }

    /// Yields the next response message from the stream, or `Ok(None)` if the stream has ended.
    pub async fn message(&mut self) -> tonic::Result<Option<Response>> {
        if let Some(message) = self.pending_response.take() {
            return Ok(Some(message));
        }
        // If the response stream is absent, signal end-of-stream.
        let Some(responses) = self.responses.as_mut() else {
            return Ok(None);
        };

        // Check for inbound responses or the status of the outbound send.
        loop {
            tokio::select! {
                // Prioritize the response arm: a send failure may result
                // from the server closing after sending its final response,
                // and we want to inspect that response to know what the
                // error is.
                biased;
                // Get the next response from the background receive task.
                response = responses.recv() => match response {
                    // Successfully received a response message.
                    Some(Ok(Some(RecvItem::Message(message)))) => return Ok(Some(message)),
                    // TODO(#5991): Consider logging (e.g. tracing::debug!) for unexpected mid-stream headers.
                    // Ignore additional headers received mid-stream.
                    Some(Ok(Some(RecvItem::Headers(_)))) => continue,
                    // Stream reached terminal state. Could be clean termination (`Ok(None)`) or an error (`Err(status)`).
                    Some(terminal) => {
                        let res = match terminal {
                            Ok(None) => Ok(None),
                            Err(status) => Err(status),
                            Ok(Some(_)) => unreachable!(),
                        };
                        self.terminate();
                        return res;
                    }
                    // Response channel closed unexpectedly without sending a terminal item.
                    // Join the background receive task to extract the final exit status/error.
                    None => {
                        self.responses.take();
                        self.send_task.abort();
                        let status = self
                            .receive_task
                            .as_mut()
                            .expect("active streams must have a receive task")
                            .join()
                            .await;
                        return Err(status);
                    }
                },
                // Monitor the background send task. If sending failed, fail early and terminate the stream.
                status = self.send_task.join(), if self.send_task.is_joinable() => {
                    if let Err(status) = status {
                        // TODO(#5991): Consider waiting until the receive
                        // side terminates, so server responses that haven't
                        // arrived are not lost.
                        self.terminate();
                        return Err(status);
                    }
                }
            }
        }
    }

    /// Terminates the stream, cleaning up background tasks and response channels.
    fn terminate(&mut self) {
        self.responses.take();
        self.receive_task.take();
        self.send_task.abort();
    }
}

impl<Response> std::fmt::Debug for GrpcRustStreaming<Response> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRustStreaming")
            .field("pending_response", &self.pending_response.is_some())
            .field("is_terminal", &self.responses.is_none())
            .finish_non_exhaustive()
    }
}

// TODO(#5991): Add tests for GrpcRustStreaming in an upcoming PR.
