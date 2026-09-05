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

// TODO(#5716): Lift to shared bidi module

use super::connector::{Connection, Connector};
use super::replay_buffer::{ReplayBuffer, ReplayChunk};
use super::{Client, TonicStreaming};
use crate::Error;
use crate::error::WriteError;
use crate::google::storage::v2::{
    BidiWriteObjectRequest, BidiWriteObjectResponse, bidi_write_object_request::Data,
    bidi_write_object_response::WriteStatus,
};
use std::collections::VecDeque;
use std::sync::Arc;

use gaxi::grpc::tonic::Result as TonicResult;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

type WriteResult<T> = std::result::Result<T, WriteError>;
type LoopResult<T> = std::result::Result<T, Error>;

/// The intent sent from the foreground task to the background worker.
pub enum UploadIntent {
    Append(BidiWriteObjectRequest),
    Flush(
        BidiWriteObjectRequest,
        oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
    ),
    Finalize(
        BidiWriteObjectRequest,
        oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
    ),
}

/// Tracks an in-flight flush or finalize request awaiting server confirmation.
#[derive(Debug)]
enum PendingRequest {
    Flush {
        target_offset: i64,
        request: BidiWriteObjectRequest,
        sender: oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
    },
    Finalize {
        target_offset: i64,
        request: BidiWriteObjectRequest,
        sender: oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
    },
}

impl PendingRequest {
    fn request(&self) -> BidiWriteObjectRequest {
        match self {
            PendingRequest::Flush { request, .. } => request.clone(),
            PendingRequest::Finalize { request, .. } => request.clone(),
        }
    }

    fn is_satisfied(&self, response: &BidiWriteObjectResponse, persisted_size: i64) -> bool {
        match self {
            PendingRequest::Flush { target_offset, .. } => persisted_size >= *target_offset,
            PendingRequest::Finalize { target_offset, .. } => {
                matches!(response.write_status, Some(WriteStatus::Resource(_)))
                    && persisted_size >= *target_offset
            }
        }
    }

    fn complete(self, response: crate::Result<BidiWriteObjectResponse>) {
        match self {
            PendingRequest::Flush { sender, .. } => {
                let _ = sender.send(response);
            }
            PendingRequest::Finalize { sender, .. } => {
                let _ = sender.send(response);
            }
        }
    }
}

/// The background worker that manages the live gRPC stream, unacknowledged chunk replay,
/// and automatic reconnection.
pub struct Worker<C> {
    connector: Connector<C>,
    replay_buffer: ReplayBuffer,
    pending_requests: VecDeque<PendingRequest>,
    /// Tracks if the client intends to complete the upload by sending a Finalize intent.
    finalized: bool,
}

impl<C> Worker<C> {
    pub fn new(connector: Connector<C>) -> Self {
        Self {
            connector,
            replay_buffer: ReplayBuffer::new(),
            pending_requests: VecDeque::new(),
            finalized: false,
        }
    }
}

impl<C> Worker<C>
where
    C: Client + Clone + 'static,
    <C as Client>::Stream: TonicStreaming,
{
    pub async fn run(
        mut self,
        connection: Connection<C::Stream>,
        mut requests: Receiver<UploadIntent>,
    ) -> LoopResult<()> {
        let (mut rx, mut tx) = (connection.rx, connection.tx);

        let error = loop {
            tokio::select! {
                m = rx.next_message() => {
                    match self.handle_response(m).await {
                        // Successful end of stream, return without error.
                        None => break None,
                        // An unrecoverable error in the stream or its data, return
                        // the error.
                        Some(Err(e)) => break Some(e),
                        // New message on the stream handled successfully,
                        // continue.
                        Some(Ok(None)) => {},
                        // The stream reconnected successfully, update the local
                        // variables and continue.
                        Some(Ok(Some(connection))) => {
                            (rx, tx) = (connection.rx, connection.tx);
                        }
                    }
                },
                intent = requests.recv(), if !self.replay_buffer.is_full() => {
                    match intent {
                        Some(intent) => {
                            let request = self.process_intent(intent);
                            if let Err(e) = tx.send(request).await {
                                match self.reconnect(Error::io(e)).await {
                                    Some(Ok(Some(connection))) => {
                                        (rx, tx) = (connection.rx, connection.tx);
                                    }
                                    Some(Err(e)) => break Some(e),
                                    _ => {}
                                }
                            }
                        }
                        None => {
                            drop(tx);
                            break self.wait_for_server_completion(rx).await;
                        }
                    }
                }
            }
        };

        if let Some(e) = error {
            let shared_error = Arc::new(e);
            self.drain_intents_on_error(requests, Arc::clone(&shared_error))
                .await;
            return Err(Error::ser(shared_error));
        }

        Ok(())
    }

    fn process_intent(&mut self, intent: UploadIntent) -> BidiWriteObjectRequest {
        match intent {
            UploadIntent::Append(req) => {
                if let Some(Data::ChecksummedData(ref cd)) = req.data {
                    let crc32c = cd.crc32c.unwrap_or_else(|| crc32c::crc32c(&cd.content));
                    self.replay_buffer.push(ReplayChunk::new(
                        req.write_offset,
                        cd.content.clone(),
                        crc32c,
                    ));
                }
                req
            }
            UploadIntent::Flush(req, sender) => {
                assert!(
                    req.state_lookup,
                    "state_lookup must be true for Flush intents"
                );
                assert!(req.flush, "flush must be true for Flush intents");
                self.pending_requests.push_back(PendingRequest::Flush {
                    target_offset: req.write_offset,
                    request: req.clone(),
                    sender,
                });
                req
            }
            UploadIntent::Finalize(req, sender) => {
                assert!(req.flush, "flush must be true for Finalize intents");
                assert!(
                    req.finish_write,
                    "finish_write must be true for Finalize intents"
                );
                self.finalized = true;
                self.pending_requests.push_back(PendingRequest::Finalize {
                    target_offset: req.write_offset,
                    request: req.clone(),
                    sender,
                });
                req
            }
        }
    }

    /// Handles an incoming response message or stream completion from the server.
    ///
    /// Returns `None` when the stream has terminated cleanly, `Some(Err(e))` if an
    /// unrecoverable error occurred or reconnection failed, `Some(Ok(None))` if the
    /// response message was processed successfully on the existing connection, or
    /// `Some(Ok(Some(connection)))` if the connection was reconnected and replayed.
    pub async fn handle_response(
        &mut self,
        message: TonicResult<Option<BidiWriteObjectResponse>>,
    ) -> Option<LoopResult<Option<Connection<C::Stream>>>> {
        let response = match message {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                // If the stream is unexpectedly closed by the server before the client
                // intends to finalize the upload, treat it as an error to trigger reconnect
                // or prevent silent failures on subsequent client writes.
                if !self.pending_requests.is_empty() || !self.finalized {
                    return self
                        .reconnect(Error::io("stream closed unexpectedly"))
                        .await;
                }
                return None;
            }
            Err(e) => return self.reconnect(Error::io(e)).await,
        };
        self.handle_response_success(response);
        Some(Ok(None))
    }

    /// Processes a successful [`BidiWriteObjectResponse`] from the server.
    ///
    /// Updates acknowledged offsets in the replay buffer and completes any matching
    /// in-flight flush or finalize requests.
    pub fn handle_response_success(&mut self, response: BidiWriteObjectResponse) {
        let persisted_size = match response.write_status.as_ref() {
            Some(WriteStatus::PersistedSize(s)) => *s,
            Some(WriteStatus::Resource(r)) => r.size,
            None => 0,
        };

        self.replay_buffer.acknowledge(persisted_size);

        let mut matched = false;
        while let Some(front) = self.pending_requests.front() {
            if front.is_satisfied(&response, persisted_size) {
                let req = self.pending_requests.pop_front().unwrap();
                req.complete(Ok(response.clone()));
                matched = true;
            } else {
                break;
            }
        }

        if !matched {
            tracing::debug!(
                "Received unprompted BidiWriteObjectResponse from server: {:?}",
                response
            );
        }
    }

    async fn reconnect(
        &mut self,
        last_error: Error,
    ) -> Option<LoopResult<Option<Connection<C::Stream>>>> {
        let (initial_response, connection) = match self.connector.reconnect(last_error).await {
            Ok(res) => res,
            Err(e) => return Some(Err(e)),
        };

        // Process initial response from reconnected stream
        let initial_persisted_size = match initial_response.write_status.as_ref() {
            Some(WriteStatus::PersistedSize(s)) => *s,
            Some(WriteStatus::Resource(r)) => r.size,
            None => 0,
        };
        self.replay_buffer.acknowledge(initial_persisted_size);

        // Replay all unpersisted chunks
        for chunk in self.replay_buffer.chunks_to_replay() {
            if let Err(e) = connection.tx.send(chunk.to_request()).await {
                return Some(Err(Error::io(e.to_string())));
            }
        }

        // Re-send pending flush / finalize requests
        for pending in &self.pending_requests {
            if let Err(e) = connection.tx.send(pending.request()).await {
                return Some(Err(Error::io(e.to_string())));
            }
        }

        Some(Ok(Some(connection)))
    }

    async fn wait_for_server_completion(&mut self, mut rx: C::Stream) -> Option<Error> {
        loop {
            match rx.next_message().await {
                Ok(Some(msg)) => {
                    self.handle_response_success(msg);
                }
                Ok(None) => break None,
                Err(e) => break Some(Error::io(e)),
            }
        }
    }

    async fn drain_intents_on_error(
        &mut self,
        mut requests: Receiver<UploadIntent>,
        shared_error: Arc<Error>,
    ) {
        for pending in self.pending_requests.drain(..) {
            pending.complete(Err(Error::ser(Arc::clone(&shared_error))));
        }
        // Drain remaining requests to notify pending flush/finalize intents if the stream failed.
        requests.close();
        while let Some(intent) = requests.recv().await {
            match intent {
                UploadIntent::Flush(_, sender) | UploadIntent::Finalize(_, sender) => {
                    let _ = sender.send(Err(Error::ser(Arc::clone(&shared_error))));
                }
                UploadIntent::Append(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::mocks::{MockTestClient, mock_connector};
    use super::super::tests::permanent_error;
    use super::*;
    use crate::google::storage::v2::{
        BidiWriteObjectRequest, BidiWriteObjectResponse, bidi_write_object_response::WriteStatus,
    };
    use gaxi::grpc::tonic::Response as TonicResponse;
    use gaxi::grpc::tonic::Result as TonicResult;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    type TestWorkerContext = (
        tokio::task::JoinHandle<LoopResult<()>>,
        mpsc::Sender<UploadIntent>,
        mpsc::Receiver<BidiWriteObjectRequest>,
        mpsc::Sender<TonicResult<BidiWriteObjectResponse>>,
    );

    fn spawn_test_worker() -> TestWorkerContext {
        let (request_tx, request_rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);
        let (tx, rx) = mpsc::channel(10);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start().never();

        let connector = mock_connector(mock);
        let worker = Worker::new(connector);
        let handle = tokio::spawn(worker.run(connection, rx));

        (handle, tx, request_rx, response_tx)
    }

    #[tokio::test]
    async fn run_append() -> anyhow::Result<()> {
        let (handle, tx, mut request_rx, _response_tx) = spawn_test_worker();

        let append_request = BidiWriteObjectRequest {
            write_offset: 10,
            ..Default::default()
        };
        tx.send(UploadIntent::Append(append_request)).await?;

        let stream_req = request_rx.recv().await.unwrap();
        assert_eq!(stream_req.write_offset, 10);

        drop(tx);
        tokio::task::yield_now().await;
        drop(_response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_flush() -> anyhow::Result<()> {
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        let (flush_tx, flush_rx) = oneshot::channel();
        let flush_request = BidiWriteObjectRequest {
            flush: true,
            state_lookup: true,
            write_offset: 100,
            ..Default::default()
        };
        tx.send(UploadIntent::Flush(flush_request.clone(), flush_tx))
            .await?;

        let stream_req = request_rx.recv().await.unwrap();
        assert!(stream_req.flush);
        assert!(stream_req.state_lookup);

        let server_resp = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(100)),
            ..Default::default()
        };
        response_tx.send(Ok(server_resp.clone())).await?;

        let received_resp = flush_rx.await??;
        assert_eq!(received_resp.write_status, server_resp.write_status);

        drop(tx);
        tokio::task::yield_now().await;
        drop(response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_finalize() -> anyhow::Result<()> {
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        let (finalize_tx, finalize_rx) = oneshot::channel();
        let finalize_request = BidiWriteObjectRequest {
            flush: true,
            finish_write: true,
            write_offset: 100,
            ..Default::default()
        };
        tx.send(UploadIntent::Finalize(
            finalize_request.clone(),
            finalize_tx,
        ))
        .await?;

        let stream_req = request_rx.recv().await.unwrap();
        assert!(stream_req.finish_write);

        let object = crate::google::storage::v2::Object {
            name: "test-obj".into(),
            size: 100,
            ..Default::default()
        };
        let server_resp = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::Resource(object)),
            ..Default::default()
        };
        response_tx.send(Ok(server_resp.clone())).await?;

        let received_resp = finalize_rx.await??;
        assert_eq!(received_resp.write_status, server_resp.write_status);

        drop(response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_reconnect_and_replay_unpersisted_chunks() -> anyhow::Result<()> {
        // Arrange.
        let (stream1_tx, mut stream1_rx) = mpsc::channel(10);
        let (stream1_resp_tx, stream1_resp_rx) = mpsc::channel(10);
        let conn1 = Connection::new(stream1_tx, stream1_resp_rx);

        let (captured_stream2_req_tx, mut captured_stream2_req_rx) =
            mpsc::channel::<mpsc::Receiver<BidiWriteObjectRequest>>(1);
        let (stream2_resp_tx, stream2_resp_rx) = mpsc::channel(10);
        let stream2 = TonicResponse::from(stream2_resp_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, req_rx, _, _, _| {
                let _ = captured_stream2_req_tx.try_send(req_rx);
                Ok(Ok(stream2))
            });

        let mut connector = mock_connector(mock);
        let initial_spec = crate::google::storage::v2::AppendObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 0,
            routing_token: None,
            write_handle: None,
            ..Default::default()
        };
        connector.set_spec_state(super::super::state::AppendObjectSpecState::Append {
            spec: initial_spec,
            initial_chunk: None,
        });

        let worker = Worker::new(connector);

        let (intent_tx, intent_rx) = mpsc::channel(10);
        let handle = tokio::spawn(worker.run(conn1, intent_rx));

        // Append two 10-byte chunks
        let chunk1 = bytes::Bytes::from_static(b"0123456789");
        let req1 = BidiWriteObjectRequest {
            write_offset: 0,
            data: Some(Data::ChecksummedData(
                crate::google::storage::v2::ChecksummedData {
                    content: chunk1.clone(),
                    crc32c: Some(crc32c::crc32c(&chunk1)),
                },
            )),
            ..Default::default()
        };
        let chunk2 = bytes::Bytes::from_static(b"abcdefghij");
        let req2 = BidiWriteObjectRequest {
            write_offset: 10,
            data: Some(Data::ChecksummedData(
                crate::google::storage::v2::ChecksummedData {
                    content: chunk2.clone(),
                    crc32c: Some(crc32c::crc32c(&chunk2)),
                },
            )),
            ..Default::default()
        };

        // Act.
        intent_tx.send(UploadIntent::Append(req1)).await?;
        intent_tx.send(UploadIntent::Append(req2)).await?;

        // Assert.
        // Ensure both chunks were dispatched on stream 1 and buffered for replay
        let s1_req1 = stream1_rx.recv().await.unwrap();
        assert_eq!(s1_req1.write_offset, 0);
        let s1_req2 = stream1_rx.recv().await.unwrap();
        assert_eq!(s1_req2.write_offset, 10);

        // Act.
        // Simulate stream 1 failure by dropping response stream
        drop(stream1_resp_tx);

        // Connector reconnects to stream 2; server initial message reports
        // persisted_size = 10 (chunk 1 persisted)
        let reconnect_initial = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(10)),
            ..Default::default()
        };
        stream2_resp_tx.send(Ok(reconnect_initial)).await?;

        // Assert.
        // Verify that stream 2 received the reconnect opening handshake request
        let mut stream2_req_rx = captured_stream2_req_rx.recv().await.unwrap();
        let initial_req = stream2_req_rx.recv().await.unwrap();
        assert!(initial_req.first_message.is_some());

        // Verify that chunk 2 (unpersisted) is replayed over stream 2!
        let replayed_req = stream2_req_rx.recv().await.unwrap();
        assert_eq!(replayed_req.write_offset, 10);
        if let Some(Data::ChecksummedData(cd)) = replayed_req.data {
            assert_eq!(cd.content, chunk2);
            assert_eq!(cd.crc32c, Some(crc32c::crc32c(&chunk2)));
        } else {
            panic!("expected ChecksummedData");
        }

        drop(intent_tx);
        tokio::task::yield_now().await;
        drop(stream2_resp_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_stop_on_closed_requests() -> anyhow::Result<()> {
        let (handle, tx, _request_rx, _response_tx) = spawn_test_worker();
        drop(tx);
        tokio::task::yield_now().await;
        drop(_response_tx);
        handle.await??;
        Ok(())
    }

    fn setup_mock_worker_with_reconnect_error(err: Error) -> TestWorkerContext {
        let (request_tx, request_rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);
        let (tx, rx) = mpsc::channel(10);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .return_once(move |_, _, _, _, _, _| Err(err));

        let mut connector = mock_connector(mock);
        let initial_spec = crate::google::storage::v2::AppendObjectSpec {
            bucket: "projects/_/buckets/test-bucket".into(),
            object: "test-object".into(),
            generation: 0,
            routing_token: None,
            write_handle: None,
            ..Default::default()
        };
        connector.set_spec_state(super::super::state::AppendObjectSpecState::Append {
            spec: initial_spec,
            initial_chunk: None,
        });

        let worker = Worker::new(connector);
        let handle = tokio::spawn(worker.run(connection, rx));

        (handle, tx, request_rx, response_tx)
    }

    #[tokio::test]
    async fn run_server_closes_unexpectedly() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, _request_rx, response_tx) =
            setup_mock_worker_with_reconnect_error(permanent_error());

        // Act.
        // Close the stream from the server side unexpectedly while upload is not finalized.
        drop(response_tx);

        // Assert.
        let result = handle.await?;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot serialize the request"));
        assert!(err.contains("PERMISSION_DENIED"));

        drop(tx);
        Ok(())
    }

    #[tokio::test]
    async fn run_stream_error_during_flush() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) =
            setup_mock_worker_with_reconnect_error(permanent_error());

        let (flush_tx, flush_rx) = oneshot::channel();
        let flush_request = BidiWriteObjectRequest {
            flush: true,
            state_lookup: true,
            write_offset: 100,
            ..Default::default()
        };

        // Act.
        tx.send(UploadIntent::Flush(flush_request.clone(), flush_tx))
            .await?;

        let stream_req = request_rx.recv().await.unwrap();
        assert!(stream_req.flush);

        // Drop response stream and simulate failed reconnect
        drop(response_tx);

        // Assert.
        let received_resp = flush_rx.await?;
        assert!(received_resp.is_err());
        let err = received_resp.unwrap_err().to_string();
        assert!(err.contains("cannot serialize the request"));
        assert!(err.contains("PERMISSION_DENIED"));

        let result = handle.await?;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot serialize the request"));
        assert!(err.contains("PERMISSION_DENIED"));
        Ok(())
    }

    #[tokio::test]
    async fn run_stream_error_then_queue_requests() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, _request_rx, response_tx) =
            setup_mock_worker_with_reconnect_error(permanent_error());

        let (flush_tx1, flush_rx1) = oneshot::channel();
        let (flush_tx2, flush_rx2) = oneshot::channel();

        // Act.
        // Drop the server response stream to simulate the remote network crash.
        // The worker will wake up and attempt reconnect, which fails and triggers draining.
        drop(response_tx);

        // Put requests into the channel immediately. Because it has capacity 10
        // and we haven't yielded, these are queued in the requests buffer synchronously.
        let valid_flush = || BidiWriteObjectRequest {
            flush: true,
            state_lookup: true,
            write_offset: 100,
            ..Default::default()
        };
        tx.send(UploadIntent::Flush(valid_flush(), flush_tx1))
            .await?;
        tx.send(UploadIntent::Flush(valid_flush(), flush_tx2))
            .await?;

        // Assert.
        let payload1 = flush_rx1.await.unwrap();
        assert!(payload1.is_err());
        let err1 = payload1.unwrap_err().to_string();
        assert!(err1.contains("cannot serialize the request"));
        assert!(err1.contains("PERMISSION_DENIED"));

        let payload2 = flush_rx2.await.unwrap();
        assert!(payload2.is_err());
        let err2 = payload2.unwrap_err().to_string();
        assert!(err2.contains("cannot serialize the request"));
        assert!(err2.contains("PERMISSION_DENIED"));

        let result = handle.await?;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot serialize the request"));
        assert!(err.contains("PERMISSION_DENIED"));

        Ok(())
    }

    #[tokio::test]
    async fn run_panic_on_flush_missing_state_lookup() {
        let (handle, tx, _request_rx, _response_tx) = spawn_test_worker();
        let (flush_tx, _flush_rx) = oneshot::channel();
        let flush_request = BidiWriteObjectRequest {
            flush: true,
            state_lookup: false, // Invalid
            ..Default::default()
        };
        let _ = tx.send(UploadIntent::Flush(flush_request, flush_tx)).await;
        assert!(handle.await.unwrap_err().is_panic());
    }

    #[tokio::test]
    async fn run_panic_on_flush_missing_flush() {
        let (handle, tx, _request_rx, _response_tx) = spawn_test_worker();
        let (flush_tx, _flush_rx) = oneshot::channel();
        let flush_request = BidiWriteObjectRequest {
            flush: false, // Invalid
            state_lookup: true,
            ..Default::default()
        };
        let _ = tx.send(UploadIntent::Flush(flush_request, flush_tx)).await;
        assert!(handle.await.unwrap_err().is_panic());
    }

    #[tokio::test]
    async fn run_panic_on_finalize_missing_finish_write() {
        let (handle, tx, _request_rx, _response_tx) = spawn_test_worker();
        let (finalize_tx, _finalize_rx) = oneshot::channel();
        let finalize_request = BidiWriteObjectRequest {
            finish_write: false, // Invalid
            flush: true,
            ..Default::default()
        };
        let _ = tx
            .send(UploadIntent::Finalize(finalize_request, finalize_tx))
            .await;
        assert!(handle.await.unwrap_err().is_panic());
    }

    #[tokio::test]
    async fn run_panic_on_finalize_missing_flush() {
        let (handle, tx, _request_rx, _response_tx) = spawn_test_worker();
        let (finalize_tx, _finalize_rx) = oneshot::channel();
        let finalize_request = BidiWriteObjectRequest {
            finish_write: true,
            flush: false, // Invalid
            ..Default::default()
        };
        let _ = tx
            .send(UploadIntent::Finalize(finalize_request, finalize_tx))
            .await;
        assert!(handle.await.unwrap_err().is_panic());
    }
}
