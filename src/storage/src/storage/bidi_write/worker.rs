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

use super::connector::{Connection, Connector};
use super::replay_buffer::{ReplayBuffer, ReplayChunk};
use super::{Client, MAX_WRITE_CHUNK_SIZE, TonicStreaming, is_finalized, persisted_size};
use crate::Error;
use crate::error::WriteError;
use crate::google::storage::v2::{
    BidiWriteObjectRequest, BidiWriteObjectResponse, bidi_write_object_request::Data,
};
use std::collections::VecDeque;
use std::sync::Arc;

use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Result as TonicResult;
use tokio::sync::mpsc::{Receiver, Sender};
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

/// What [`Worker::run`] must do after handling one server message.
enum ResponseAction<S> {
    /// Keep using the current stream.
    Continue,
    /// The stream was replaced; adopt these halves.
    Reconnected(Connection<S>),
    /// The server closed the stream and the upload is complete.
    Finished,
}

/// Which kind of confirmation a [`PendingRequest`] is waiting for.
#[derive(Debug)]
enum PendingKind {
    Flush,
    Finalize,
}

/// Tracks an in-flight flush or finalize request awaiting server confirmation.
#[derive(Debug)]
struct PendingRequest {
    kind: PendingKind,
    target_offset: i64,
    request: BidiWriteObjectRequest,
    sender: oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
}

impl PendingRequest {
    /// Returns `true` when `response` confirms this request completed.
    ///
    /// A flush is satisfied as soon as the service acknowledges the target offset. A finalize
    /// additionally requires a *finalized* object resource: the first message of a reconnected
    /// create or handle-less takeover stream also carries a resource, and only `finalize_time`
    /// tells the two apart. Without that check a reconnect handshake could complete the caller's
    /// `finalize()` with an object that was never finalized.
    fn is_satisfied(&self, response: &BidiWriteObjectResponse, persisted_size: i64) -> bool {
        if persisted_size < self.target_offset {
            return false;
        }
        match self.kind {
            PendingKind::Flush => true,
            PendingKind::Finalize => is_finalized(response),
        }
    }

    fn complete(self, response: crate::Result<BidiWriteObjectResponse>) {
        let _ = self.sender.send(response);
    }
}

/// The background worker that manages the live gRPC stream, unacknowledged chunk replay, and
/// automatic reconnection.
pub struct Worker<C> {
    connector: Connector<C>,
    replay_buffer: ReplayBuffer,
    pending_requests: VecDeque<PendingRequest>,
    /// Tracks the highest byte offset acknowledged by the server.
    persisted_size: i64,
    /// Tracks if the client intends to complete the upload by sending a Finalize intent.
    finalized: bool,
    /// Tracks if a worker-initiated `state_lookup` request is awaiting a server response.
    ///
    /// The worker injects such a request when the replay buffer crosses its high watermark, so that
    /// an `ack()` is guaranteed to arrive before the buffer fills.
    self_flush_outstanding: bool,
}

impl<C> Worker<C> {
    pub fn new(connector: Connector<C>) -> Self {
        Self::with_replay_buffer(connector, ReplayBuffer::new())
    }

    /// Creates a [`Worker`] with a caller-provided [`ReplayBuffer`].
    ///
    /// The transport uses this to seed the buffer with the payload sent in the opening request, and
    /// tests use it to exercise the capacity limits without buffering
    /// [`DEFAULT_REPLAY_BUFFER_SIZE`][super::replay_buffer::DEFAULT_REPLAY_BUFFER_SIZE] bytes.
    pub fn with_replay_buffer(connector: Connector<C>, replay_buffer: ReplayBuffer) -> Self {
        Self {
            connector,
            replay_buffer,
            pending_requests: VecDeque::new(),
            persisted_size: 0,
            finalized: false,
            self_flush_outstanding: false,
        }
    }

    /// Sets the initial acknowledged byte offset from the opening or reopen response.
    pub fn with_persisted_size(mut self, persisted_size: i64) -> Self {
        self.persisted_size = persisted_size;
        self
    }

    /// Returns `true` if the replay buffer has crossed its high watermark and no worker-initiated
    /// `state_lookup` is outstanding.
    ///
    /// The replay buffer only shrinks when [`ReplayBuffer::ack`] is called, which requires a server
    /// response, and the server only responds when a request sets `state_lookup`. Without injecting
    /// one, a caller that appends [`ReplayBuffer::capacity`] bytes without an explicit flush would
    /// fill the buffer, disable the intent branch of the worker loop, and stall forever.
    ///
    /// The two-chunk headroom is not a guarantee that the injected request is answered before
    /// [`ReplayBuffer::is_full`] trips: the intent channel and the coalescing buffer can hold more
    /// than that in flight. It only ensures the request is *dispatched*, which is what makes the
    /// eventual `ack()` certain and keeps the full state transient.
    fn needs_watermark_flush(&self) -> bool {
        !self.self_flush_outstanding
            && self.replay_buffer.unpersisted_bytes()
                >= self
                    .replay_buffer
                    .capacity()
                    .saturating_sub(2 * MAX_WRITE_CHUNK_SIZE)
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
            // A full buffer disables the intent branch below, so the loop can only make progress if
            // the service still owes a response. Fail loudly instead of waiting forever if that
            // invariant is broken.
            if self.replay_buffer.is_full()
                && !self.self_flush_outstanding
                && self.pending_requests.is_empty()
            {
                break Some(Error::io(
                    "replay buffer is full and no server response is outstanding",
                ));
            }

            tokio::select! {
                m = rx.next_message() => {
                    match self.handle_response(m).await {
                        Err(e) => break Some(e),
                        // Successful end of stream, return without error.
                        Ok(ResponseAction::Finished) => break None,
                        // The stream was replaced, adopt the new halves.
                        Ok(ResponseAction::Reconnected(connection)) => {
                            (rx, tx) = (connection.rx, connection.tx);
                        }
                        // Message handled on the existing stream. Re-evaluate the watermark, since
                        // the ack may have left the buffer above it.
                        Ok(ResponseAction::Continue) => {
                            if let Err(e) = self.maybe_send_watermark_flush(&tx).await
                                && let Err(fatal) = self.reconnect_into(e, &mut rx, &mut tx).await {
                                break Some(fatal);
                            }
                        }
                    }
                },
                intent = requests.recv(), if !self.replay_buffer.is_full() => {
                    match intent {
                        Some(intent) => {
                            let request = self.process_intent(intent);
                            if let Err(e) = tx.send(request).await
                                && let Err(fatal) =
                                    self.reconnect_into(Error::io(e), &mut rx, &mut tx).await
                            {
                                break Some(fatal);
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
            UploadIntent::Append(mut req) => {
                if let Some(Data::ChecksummedData(ref cd)) = req.data {
                    let crc32c = cd.crc32c.unwrap_or_else(|| crc32c::crc32c(&cd.content));
                    self.replay_buffer.push(ReplayChunk::new(
                        req.write_offset,
                        cd.content.clone(),
                        crc32c,
                    ));
                }
                // Piggyback a `state_lookup` on this append once the replay buffer crosses its high
                // watermark. Only `state_lookup` elicits a response, and only a response drives
                // `ReplayBuffer::ack()`.
                if self.needs_watermark_flush() {
                    req.flush = true;
                    req.state_lookup = true;
                    self.self_flush_outstanding = true;
                }
                req
            }
            UploadIntent::Flush(req, sender) => {
                assert!(
                    req.state_lookup,
                    "state_lookup must be true for Flush intents"
                );
                assert!(req.flush, "flush must be true for Flush intents");
                self.pending_requests.push_back(PendingRequest {
                    kind: PendingKind::Flush,
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
                self.pending_requests.push_back(PendingRequest {
                    kind: PendingKind::Finalize,
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
    /// Returns the action [`Self::run`] must take next, or an error if the failure is unrecoverable
    /// or reconnection failed.
    async fn handle_response(
        &mut self,
        message: TonicResult<Option<BidiWriteObjectResponse>>,
    ) -> LoopResult<ResponseAction<C::Stream>> {
        let response = match message {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                // If the stream is unexpectedly closed by the server before the client intends to
                // finalize the upload, treat it as an error to trigger reconnect or prevent silent
                // failures on subsequent client writes.
                if !self.pending_requests.is_empty() || !self.finalized {
                    let connection = self
                        .reconnect(Error::io("stream closed unexpectedly"))
                        .await?;
                    return Ok(ResponseAction::Reconnected(connection));
                }
                return Ok(ResponseAction::Finished);
            }
            Err(e) => {
                let connection = self.reconnect(to_gax_error(e)).await?;
                return Ok(ResponseAction::Reconnected(connection));
            }
        };
        self.handle_response_success(response)?;
        Ok(ResponseAction::Continue)
    }

    /// Processes a successful [`BidiWriteObjectResponse`] from the server.
    ///
    /// Updates acknowledged offsets in the replay buffer and completes any matching in-flight flush
    /// or finalize requests. Returns an error if the server reports a `persisted_size` lower than
    /// previously acknowledged bytes.
    fn handle_response_success(&mut self, response: BidiWriteObjectResponse) -> LoopResult<()> {
        // Every response ends the wait for a worker-initiated `state_lookup`, including one that
        // carries no `write_status`. Clearing the flag only on the acknowledged-bytes path would
        // suppress all later watermark probes and stall the worker once the replay buffer fills.
        let self_flush_outstanding = std::mem::take(&mut self.self_flush_outstanding);

        let Some(persisted_size) = persisted_size(&response) else {
            // A response with no `write_status` carries no progress, for example one that only
            // refreshes the write handle.
            tracing::debug!("Received BidiWriteObjectResponse with no write_status: {response:?}");
            return Ok(());
        };

        // `front_offset()` can exceed `persisted_size` if an `Append` intent uses an offset ahead
        // of the last acknowledged byte; guard against both.
        let min_expected_offset = self
            .replay_buffer
            .front_offset()
            .map(|front| std::cmp::max(self.persisted_size, front))
            .unwrap_or(self.persisted_size);
        if persisted_size < min_expected_offset {
            return Err(Error::io(format!(
                "server persisted_size ({persisted_size}) regressed below \
                 acknowledged offset ({min_expected_offset})"
            )));
        }

        self.persisted_size = persisted_size;
        self.replay_buffer.ack(persisted_size);

        // Pending requests are answered in order, so stop at the first one this response does not
        // satisfy. That one is still waiting for its own response, which the service owes us, so
        // the loop stays live.
        let mut matched = false;
        while let Some(front) = self.pending_requests.front() {
            if !front.is_satisfied(&response, persisted_size) {
                break;
            }
            let request = self
                .pending_requests
                .pop_front()
                .expect("front() just returned a request");
            request.complete(Ok(response.clone()));
            matched = true;
        }

        // A worker-initiated `state_lookup` has no entry in `pending_requests`, so its response
        // legitimately matches nothing. Do not report it as unprompted.
        if !matched && !self_flush_outstanding {
            tracing::debug!(
                "Received unprompted BidiWriteObjectResponse from server: {:?}",
                response
            );
        }
        Ok(())
    }

    /// Sends a standalone `flush + state_lookup` probe when the replay buffer remains above its
    /// watermark and no user `Flush` or `Finalize` is pending.
    async fn maybe_send_watermark_flush(
        &mut self,
        tx: &Sender<BidiWriteObjectRequest>,
    ) -> LoopResult<()> {
        if self.pending_requests.is_empty() && self.needs_watermark_flush() {
            let write_offset = self
                .replay_buffer
                .end_offset()
                .unwrap_or(self.persisted_size);
            let request = BidiWriteObjectRequest {
                write_offset,
                flush: true,
                state_lookup: true,
                ..BidiWriteObjectRequest::default()
            };
            tx.send(request).await.map_err(Error::io)?;
            self.self_flush_outstanding = true;
        }
        Ok(())
    }

    /// Reconnects, then restores the session state on the new stream.
    ///
    /// Replays every unacknowledged chunk, re-sends the flush and finalize requests still waiting
    /// for a response, and re-arms the watermark probe.
    async fn reconnect(&mut self, last_error: Error) -> LoopResult<Connection<C::Stream>> {
        // Any response the worker-initiated `state_lookup` was waiting on will never arrive on the
        // dead stream.
        self.self_flush_outstanding = false;

        let (initial_response, connection) = self
            .connector
            .reconnect(last_error, self.persisted_size)
            .await?;

        self.handle_response_success(initial_response)?;

        // Replay all unpersisted chunks.
        for chunk in self.replay_buffer.chunks_to_replay() {
            connection
                .tx
                .send(chunk.to_request())
                .await
                .map_err(Error::io)?;
        }

        // Re-send pending flush / finalize requests.
        for pending in &self.pending_requests {
            connection
                .tx
                .send(pending.request.clone())
                .await
                .map_err(Error::io)?;
        }

        // Replayed chunks set neither `flush` nor `state_lookup`. If the buffer is still above the
        // watermark and no user request is pending, nothing would elicit a response on the new
        // stream, so restore the invariant explicitly.
        self.maybe_send_watermark_flush(&connection.tx).await?;

        Ok(connection)
    }

    /// Reconnects and swaps the new stream halves in place.
    async fn reconnect_into(
        &mut self,
        last_error: Error,
        rx: &mut C::Stream,
        tx: &mut Sender<BidiWriteObjectRequest>,
    ) -> LoopResult<()> {
        let connection = self.reconnect(last_error).await?;
        (*rx, *tx) = (connection.rx, connection.tx);
        Ok(())
    }

    async fn wait_for_server_completion(&mut self, mut rx: C::Stream) -> Option<Error> {
        loop {
            match rx.next_message().await {
                Ok(Some(msg)) => {
                    if let Err(e) = self.handle_response_success(msg) {
                        break Some(e);
                    }
                }
                Ok(None) => break None,
                Err(e) => break Some(to_gax_error(e)),
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
    use super::super::replay_buffer::MIN_REPLAY_BUFFER_SIZE;
    use super::super::state::AppendObjectSpecState;
    use super::super::tests::permanent_error;
    use super::*;
    use crate::google::storage::v2::{
        AppendObjectSpec, BidiWriteObjectRequest, BidiWriteObjectResponse, Object,
        bidi_write_object_response::WriteStatus,
    };
    use gaxi::grpc::tonic::Response as TonicResponse;
    use gaxi::grpc::tonic::Result as TonicResult;
    use gaxi::grpc::tonic::Status;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    type TestWorkerContext = (
        tokio::task::JoinHandle<LoopResult<()>>,
        mpsc::Sender<UploadIntent>,
        mpsc::Receiver<BidiWriteObjectRequest>,
        mpsc::Sender<TonicResult<BidiWriteObjectResponse>>,
    );

    /// Defines the payload size of the synthetic appends used in the watermark tests.
    const TEST_CHUNK_SIZE: usize = 512;

    /// Places the watermark (`capacity - 2 * MAX_WRITE_CHUNK_SIZE`) at exactly two test chunks, so
    /// the second append is the one that crosses it.
    const TEST_CAPACITY: usize = 2 * MAX_WRITE_CHUNK_SIZE + 2 * TEST_CHUNK_SIZE;

    fn spawn_test_worker() -> TestWorkerContext {
        spawn_test_worker_with_replay_capacity(
            super::super::replay_buffer::DEFAULT_REPLAY_BUFFER_SIZE,
        )
    }

    fn spawn_test_worker_with_replay_capacity(capacity: usize) -> TestWorkerContext {
        let (request_tx, request_rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);
        let (tx, rx) = mpsc::channel(10);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start().never();

        let connector = mock_connector(mock);
        let worker = Worker::with_replay_buffer(connector, ReplayBuffer::with_capacity(capacity));
        let handle = tokio::spawn(worker.run(connection, rx));

        (handle, tx, request_rx, response_tx)
    }

    fn append_intent(write_offset: i64, len: usize) -> UploadIntent {
        let content = bytes::Bytes::from(vec![b'x'; len]);
        let crc32c = crc32c::crc32c(&content);
        UploadIntent::Append(BidiWriteObjectRequest {
            write_offset,
            data: Some(Data::ChecksummedData(
                crate::google::storage::v2::ChecksummedData {
                    content,
                    crc32c: Some(crc32c),
                },
            )),
            ..Default::default()
        })
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

    /// Builds the response the service sends once it finalized the object.
    fn finalized_response(size: i64) -> BidiWriteObjectResponse {
        BidiWriteObjectResponse {
            write_status: Some(WriteStatus::Resource(Object {
                name: "test-obj".into(),
                size,
                finalize_time: Some(prost_types::Timestamp::default()),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn run_finalize() -> anyhow::Result<()> {
        // Arrange.
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

        // Act.
        let server_resp = finalized_response(100);
        response_tx.send(Ok(server_resp.clone())).await?;

        // Assert.
        let received_resp = finalize_rx.await??;
        assert_eq!(received_resp.write_status, server_resp.write_status);

        drop(response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_finalize_ignores_resource_without_finalize_time() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        let (finalize_tx, mut finalize_rx) = oneshot::channel();
        tx.send(UploadIntent::Finalize(
            BidiWriteObjectRequest {
                flush: true,
                finish_write: true,
                write_offset: 100,
                ..Default::default()
            },
            finalize_tx,
        ))
        .await?;
        let _ = request_rx.recv().await.unwrap();

        // Act.
        // A bare resource is what a create or handle-less takeover stream returns as its first
        // message. It reaches the target offset, but the object was not finalized.
        response_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(Object {
                    name: "test-obj".into(),
                    size: 100,
                    finalize_time: None,
                    ..Default::default()
                })),
                ..Default::default()
            }))
            .await?;
        tokio::task::yield_now().await;

        // Assert.
        // The caller is still waiting, rather than being told the upload finalized.
        assert!(matches!(
            finalize_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // A genuinely finalized resource completes it.
        response_tx.send(Ok(finalized_response(100))).await?;
        let got = finalize_rx.await??;
        assert!(is_finalized(&got), "{got:?}");

        drop(tx);
        tokio::task::yield_now().await;
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

        // Connector reconnects to stream 2; server initial message reports persisted_size = 10
        // (chunk 1 persisted)
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

    #[tokio::test]
    async fn run_handles_trailing_response_after_intents_close() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, _request_rx, response_tx) = spawn_test_worker();

        // Act.
        // Close the intent channel first so the worker drains the stream in
        // `wait_for_server_completion`, then deliver one late response.
        drop(tx);
        tokio::task::yield_now().await;
        response_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(10)),
                ..Default::default()
            }))
            .await?;
        drop(response_tx);

        // Assert.
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_fails_on_stream_error_after_intents_close() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, _request_rx, response_tx) = spawn_test_worker();

        // Act.
        // Once the intent channel is closed the worker no longer reconnects, so a stream error
        // while draining is terminal.
        drop(tx);
        tokio::task::yield_now().await;
        response_tx
            .send(Err(Status::unavailable("try again")))
            .await?;

        // Assert.
        let err = handle.await?.unwrap_err().to_string();
        assert!(err.contains("try again"), "{err}");
        Ok(())
    }

    fn setup_mock_worker_with_reconnect_error(err: Error) -> TestWorkerContext {
        setup_mock_worker_with_reconnect_error_and_capacity(
            err,
            super::super::replay_buffer::DEFAULT_REPLAY_BUFFER_SIZE,
        )
    }

    fn setup_mock_worker_with_reconnect_error_and_capacity(
        err: Error,
        capacity: usize,
    ) -> TestWorkerContext {
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

        let worker = Worker::with_replay_buffer(connector, ReplayBuffer::with_capacity(capacity));
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
    async fn run_error_drains_intents_queued_behind_full_buffer() -> anyhow::Result<()> {
        // Arrange.
        // A single append fills the buffer, which disables the intent branch of the `run()` select.
        // Intents sent afterwards stay in the channel and can only be completed by the
        // channel-drain loop in `drain_intents_on_error`.
        let capacity = MIN_REPLAY_BUFFER_SIZE;
        let (handle, tx, mut request_rx, response_tx) =
            setup_mock_worker_with_reconnect_error_and_capacity(permanent_error(), capacity);

        tx.send(append_intent(0, capacity)).await?;
        let stream_req = request_rx.recv().await.unwrap();
        // The append crossed the watermark, so the worker piggybacked a state lookup and is now
        // waiting for the response with a full buffer.
        assert!(stream_req.state_lookup);

        let (flush_tx, flush_rx) = oneshot::channel();
        tx.send(UploadIntent::Flush(
            BidiWriteObjectRequest {
                flush: true,
                state_lookup: true,
                write_offset: capacity as i64,
                ..Default::default()
            },
            flush_tx,
        ))
        .await?;
        let (finalize, finalize_rx) = finalize_intent(capacity as i64);
        tx.send(finalize).await?;

        // Act.
        // Fail the stream. Reconnecting fails permanently, so the worker drains.
        drop(response_tx);

        // Assert.
        for received in [flush_rx.await?, finalize_rx.await?] {
            let err = received.unwrap_err().to_string();
            assert!(err.contains("PERMISSION_DENIED"), "{err}");
        }
        let err = handle.await?.unwrap_err().to_string();
        assert!(err.contains("PERMISSION_DENIED"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn run_fails_when_buffer_is_full_with_no_outstanding_response() -> anyhow::Result<()> {
        // Arrange.
        // Hand the worker a buffer that is already full. Normal operation cannot reach this state,
        // but the guard must fail loudly rather than hang.
        let (request_tx, _request_rx) = mpsc::channel(10);
        let (_response_tx, response_rx) = mpsc::channel(10);
        let (_tx, rx) = mpsc::channel(10);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start().never();

        let mut replay_buffer = ReplayBuffer::with_capacity(MIN_REPLAY_BUFFER_SIZE);
        let content = bytes::Bytes::from(vec![b'x'; MIN_REPLAY_BUFFER_SIZE]);
        let crc32c = crc32c::crc32c(&content);
        replay_buffer.push(ReplayChunk::new(0, content, crc32c));
        let worker = Worker::with_replay_buffer(mock_connector(mock), replay_buffer);

        // Act.
        let result = worker.run(connection, rx).await;

        // Assert.
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("replay buffer is full and no server response is outstanding"),
            "{err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_full_buffer_recovers_and_resumes_after_server_ack() -> anyhow::Result<()> {
        // Arrange.
        // Fill the replay buffer past `MIN_REPLAY_BUFFER_SIZE` (4 MiB + 1 byte) with three 2 MiB
        // appends so `is_full()` becomes true and the `run()` `tokio::select!` guard disables the
        // `requests.recv()` branch. Queue a fourth append while the buffer is full, then send a
        // server ack that drains the buffer and verify the fourth append is picked up and sent.
        let (handle, tx, mut request_rx, response_tx) =
            spawn_test_worker_with_replay_capacity(MIN_REPLAY_BUFFER_SIZE);

        let chunk_len = MAX_WRITE_CHUNK_SIZE;
        for i in 0..3_i64 {
            tx.send(append_intent(i * chunk_len as i64, chunk_len))
                .await?;
            let dispatched = request_rx.recv().await.expect("chunk must be dispatched");
            assert_eq!(dispatched.write_offset, i * chunk_len as i64);
        }

        // Queue a fourth append while the replay buffer is full (6 MiB >= 4 MiB + 1). Because the
        // intent branch is disabled, the worker must not forward it yet.
        let fourth_offset = 3 * chunk_len as i64;
        tx.send(append_intent(fourth_offset, TEST_CHUNK_SIZE))
            .await?;
        tokio::task::yield_now().await;
        assert!(
            request_rx.try_recv().is_err(),
            "fourth append must remain gated in the intent channel while replay_buffer.is_full()"
        );

        // Act.
        // Acknowledge all 6 MiB so `replay_buffer.ack()` drains the buffer and re-enables the
        // intent branch in the next loop iteration.
        response_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(fourth_offset)),
                ..Default::default()
            }))
            .await?;

        // Assert.
        let fourth = request_rx
            .recv()
            .await
            .expect("fourth append must be dispatched once the buffer drains");
        assert_eq!(fourth.write_offset, fourth_offset);

        drop(tx);
        tokio::task::yield_now().await;
        drop(response_tx);
        handle.await??;
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

    #[tokio::test]
    async fn run_append_injects_watermark_flush() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) =
            spawn_test_worker_with_replay_capacity(TEST_CAPACITY);

        // Act.
        tx.send(append_intent(0, TEST_CHUNK_SIZE)).await?;
        tx.send(append_intent(TEST_CHUNK_SIZE as i64, TEST_CHUNK_SIZE))
            .await?;

        // Assert.
        // The first append stays below the watermark and is dispatched untouched.
        let first = request_rx.recv().await.unwrap();
        assert!(!first.flush, "{first:?}");
        assert!(!first.state_lookup, "{first:?}");

        // The second append crosses the watermark, so the worker piggybacks a flush and a
        // state_lookup on it. Only state_lookup elicits a server response.
        let second = request_rx.recv().await.unwrap();
        assert!(second.flush, "{second:?}");
        assert!(second.state_lookup, "{second:?}");

        drop(tx);
        tokio::task::yield_now().await;
        drop(response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_append_does_not_repeat_watermark_flush() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) =
            spawn_test_worker_with_replay_capacity(TEST_CAPACITY);

        // Act.
        // Three appends, all above the watermark from the second one onwards, with no server
        // response in between.
        for i in 0..3 {
            tx.send(append_intent((i * TEST_CHUNK_SIZE) as i64, TEST_CHUNK_SIZE))
                .await?;
        }

        // Assert.
        let _first = request_rx.recv().await.unwrap();
        let second = request_rx.recv().await.unwrap();
        assert!(second.state_lookup, "{second:?}");

        // A state_lookup is already outstanding, so the third append is not flagged.
        let third = request_rx.recv().await.unwrap();
        assert!(!third.flush, "{third:?}");
        assert!(!third.state_lookup, "{third:?}");

        drop(tx);
        tokio::task::yield_now().await;
        drop(response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_appends_past_watermark_without_explicit_flush() -> anyhow::Result<()> {
        // Arrange.
        // Use a capacity where `capacity - 2 * MAX_WRITE_CHUNK_SIZE` is non-zero (`2 *
        // TEST_CHUNK_SIZE`), so only appends at or above the watermark are flagged.
        const APPEND_COUNT: usize = 16;
        let (handle, tx, mut request_rx, response_tx) =
            spawn_test_worker_with_replay_capacity(TEST_CAPACITY);

        // A server that replies only when the client asks for it via state_lookup.
        let server = tokio::spawn(async move {
            let mut data_requests = 0_usize;
            let mut persisted_size = 0_i64;
            while let Some(request) = request_rx.recv().await {
                if let Some(Data::ChecksummedData(cd)) = request.data.as_ref() {
                    data_requests += 1;
                    persisted_size = request.write_offset + cd.content.len() as i64;
                }
                if request.state_lookup {
                    let response = BidiWriteObjectResponse {
                        write_status: Some(WriteStatus::PersistedSize(persisted_size)),
                        ..Default::default()
                    };
                    if response_tx.send(Ok(response)).await.is_err() {
                        break;
                    }
                }
            }
            data_requests
        });

        // Act.
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async move {
            for i in 0..APPEND_COUNT {
                tx.send(append_intent((i * TEST_CHUNK_SIZE) as i64, TEST_CHUNK_SIZE))
                    .await
                    .expect("the worker must keep accepting appends");
            }
            drop(tx);
            handle.await.expect("the worker task must not panic")
        })
        .await
        .expect("appends without an explicit flush must not deadlock");

        // Assert.
        result?;
        assert_eq!(server.await?, APPEND_COUNT);
        Ok(())
    }

    #[tokio::test]
    async fn run_reconnect_restores_watermark_state_lookup() -> anyhow::Result<()> {
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
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "projects/_/buckets/test-bucket".into(),
                object: "test-object".into(),
                ..Default::default()
            },
            initial_chunk: None,
        });

        let worker =
            Worker::with_replay_buffer(connector, ReplayBuffer::with_capacity(TEST_CAPACITY));
        let (intent_tx, intent_rx) = mpsc::channel(10);
        let handle = tokio::spawn(worker.run(conn1, intent_rx));

        // Fill the replay buffer up to the watermark on the first stream.
        intent_tx.send(append_intent(0, TEST_CHUNK_SIZE)).await?;
        intent_tx
            .send(append_intent(TEST_CHUNK_SIZE as i64, TEST_CHUNK_SIZE))
            .await?;
        let _ = stream1_rx.recv().await.unwrap();
        let _ = stream1_rx.recv().await.unwrap();

        // Act.
        // Break the first stream. The watermark state_lookup sent on it is lost.
        drop(stream1_resp_tx);

        // The reconnected stream reports nothing persisted, so both chunks are replayed.
        stream2_resp_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(0)),
                ..Default::default()
            }))
            .await?;

        // Assert.
        let mut stream2_req_rx = captured_stream2_req_rx.recv().await.unwrap();
        let initial_req = stream2_req_rx.recv().await.unwrap();
        assert!(initial_req.first_message.is_some());

        // Replayed chunks carry data but neither flush nor state_lookup.
        for i in 0..2 {
            let replayed = stream2_req_rx.recv().await.unwrap();
            assert_eq!(replayed.write_offset, (i * TEST_CHUNK_SIZE) as i64);
            assert!(!replayed.state_lookup, "{replayed:?}");
        }

        // The buffer is still at the watermark and no user request is pending, so the worker
        // re-establishes the invariant with a standalone state_lookup.
        let watermark_req = stream2_req_rx.recv().await.unwrap();
        assert!(watermark_req.flush, "{watermark_req:?}");
        assert!(watermark_req.state_lookup, "{watermark_req:?}");
        assert_eq!(watermark_req.write_offset, (2 * TEST_CHUNK_SIZE) as i64);

        drop(intent_tx);
        tokio::task::yield_now().await;
        drop(stream2_resp_tx);
        handle.await??;
        Ok(())
    }

    /// Spawns a worker whose first stream is `conn1` and whose reconnect attempt yields a second
    /// stream, returning the handles needed to drive both.
    #[allow(clippy::type_complexity)]
    fn spawn_reconnecting_worker() -> (
        tokio::task::JoinHandle<LoopResult<()>>,
        mpsc::Sender<UploadIntent>,
        mpsc::Receiver<BidiWriteObjectRequest>,
        mpsc::Sender<TonicResult<BidiWriteObjectResponse>>,
        mpsc::Receiver<mpsc::Receiver<BidiWriteObjectRequest>>,
        mpsc::Sender<TonicResult<BidiWriteObjectResponse>>,
    ) {
        let (stream1_tx, stream1_rx) = mpsc::channel(10);
        let (stream1_resp_tx, stream1_resp_rx) = mpsc::channel(10);
        let conn1 = Connection::new(stream1_tx, stream1_resp_rx);

        let (captured_stream2_req_tx, captured_stream2_req_rx) =
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
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "projects/_/buckets/test-bucket".into(),
                object: "test-object".into(),
                ..Default::default()
            },
            initial_chunk: None,
        });

        let worker = Worker::new(connector);
        let (intent_tx, intent_rx) = mpsc::channel(10);
        let handle = tokio::spawn(worker.run(conn1, intent_rx));

        (
            handle,
            intent_tx,
            stream1_rx,
            stream1_resp_tx,
            captured_stream2_req_rx,
            stream2_resp_tx,
        )
    }

    fn finalize_intent(
        write_offset: i64,
    ) -> (
        UploadIntent,
        oneshot::Receiver<crate::Result<BidiWriteObjectResponse>>,
    ) {
        let (finalize_tx, finalize_rx) = oneshot::channel();
        let intent = UploadIntent::Finalize(
            BidiWriteObjectRequest {
                write_offset,
                flush: true,
                finish_write: true,
                ..Default::default()
            },
            finalize_tx,
        );
        (intent, finalize_rx)
    }

    #[tokio::test]
    async fn run_reconnect_resends_pending_finalize() -> anyhow::Result<()> {
        // Arrange.
        let (
            handle,
            intent_tx,
            mut stream1_rx,
            stream1_resp_tx,
            mut captured_stream2_req_rx,
            stream2_resp_tx,
        ) = spawn_reconnecting_worker();

        let (intent, mut finalize_rx) = finalize_intent(50);
        intent_tx.send(intent).await?;
        let sent_finalize = stream1_rx.recv().await.unwrap();
        assert!(sent_finalize.finish_write);

        // Act.
        // Stream 1 breaks before delivering the finalize response.
        drop(stream1_resp_tx);

        // The reconnect handshake returns a resource without `finalize_time`, which is what a
        // takeover stream reports. It is not a finalization.
        stream2_resp_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(Object {
                    name: "test-object".into(),
                    size: 50,
                    finalize_time: None,
                    ..Default::default()
                })),
                ..Default::default()
            }))
            .await?;

        // Assert.
        // The caller is not told the upload finalized, ...
        tokio::task::yield_now().await;
        assert!(matches!(
            finalize_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // ... and the finalize is re-sent on the new stream after the handshake.
        let mut stream2_req_rx = captured_stream2_req_rx.recv().await.unwrap();
        let initial_req = stream2_req_rx.recv().await.unwrap();
        assert!(initial_req.first_message.is_some());
        let resent = stream2_req_rx.recv().await.unwrap();
        assert!(resent.finish_write, "{resent:?}");
        assert_eq!(resent.write_offset, 50);

        // The service then answers the re-sent finalize.
        stream2_resp_tx.send(Ok(finalized_response(50))).await?;
        let got = finalize_rx.await??;
        assert!(is_finalized(&got), "{got:?}");

        drop(intent_tx);
        tokio::task::yield_now().await;
        drop(stream2_resp_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_reconnect_completes_already_finalized_request_without_resending()
    -> anyhow::Result<()> {
        // Arrange.
        let (
            handle,
            intent_tx,
            mut stream1_rx,
            stream1_resp_tx,
            mut captured_stream2_req_rx,
            stream2_resp_tx,
        ) = spawn_reconnecting_worker();

        let (intent, finalize_rx) = finalize_intent(50);
        intent_tx.send(intent).await?;
        let sent_finalize = stream1_rx.recv().await.unwrap();
        assert!(sent_finalize.finish_write);

        // Act.
        // Stream 1 breaks after the service finalized the object but before it could deliver the
        // response.
        drop(stream1_resp_tx);
        let finalized = finalized_response(50);
        stream2_resp_tx.send(Ok(finalized.clone())).await?;

        // Assert.
        // The handshake response completes the pending finalize.
        let got_resp = finalize_rx.await??;
        assert_eq!(got_resp, finalized);

        // Stream 2 only received the initial AppendObjectSpec handshake, NOT a duplicate Finalize.
        let mut stream2_req_rx = captured_stream2_req_rx.recv().await.unwrap();
        let initial_req = stream2_req_rx.recv().await.unwrap();
        assert!(initial_req.first_message.is_some());
        assert!(stream2_req_rx.try_recv().is_err());

        drop(intent_tx);
        tokio::task::yield_now().await;
        drop(stream2_resp_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_send_error_reconnects_into_new_stream_and_replays_chunk() -> anyhow::Result<()> {
        // Arrange.
        let (
            handle,
            intent_tx,
            stream1_rx,
            _stream1_resp_tx,
            mut captured_stream2_req_rx,
            stream2_resp_tx,
        ) = spawn_reconnecting_worker();

        // Prepare Stream 2's initial handshake response (`PersistedSize(0)`).
        stream2_resp_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(0)),
                ..Default::default()
            }))
            .await?;

        // Act.
        // Drop Stream 1's request receiver while keeping its response sender alive so
        // `rx.next_message()` stays pending and the failure is discovered by
        // `tx.send(request).await` -> `Worker::reconnect_into`.
        drop(stream1_rx);
        intent_tx.send(append_intent(0, 16)).await?;

        // Assert.
        // `reconnect_into` replaces the stream halves in place and replays the chunk that
        // `process_intent` buffered just before `tx.send` failed.
        let mut stream2_req_rx = captured_stream2_req_rx.recv().await.unwrap();
        let initial_req = stream2_req_rx.recv().await.unwrap();
        assert!(initial_req.first_message.is_some());

        let replayed_req = stream2_req_rx.recv().await.unwrap();
        assert_eq!(replayed_req.write_offset, 0);
        let Some(Data::ChecksummedData(cd)) = replayed_req.data else {
            panic!("expected ChecksummedData on replayed append");
        };
        assert_eq!(cd.content.len(), 16);

        drop(intent_tx);
        tokio::task::yield_now().await;
        drop(stream2_resp_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_watermark_rearms_after_non_draining_ack() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) =
            spawn_test_worker_with_replay_capacity(TEST_CAPACITY);

        // Send two chunks to hit the watermark (`2 * TEST_CHUNK_SIZE`).
        tx.send(append_intent(0, TEST_CHUNK_SIZE)).await?;
        tx.send(append_intent(TEST_CHUNK_SIZE as i64, TEST_CHUNK_SIZE))
            .await?;
        let _first = request_rx.recv().await.unwrap();
        let second = request_rx.recv().await.unwrap();
        assert!(second.state_lookup);

        // Act.
        // Server responds with PersistedSize(0), clearing self_flush_outstanding without draining
        // the buffer.
        response_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(0)),
                ..Default::default()
            }))
            .await?;

        // Assert.
        // Worker immediately dispatches a standalone watermark state_lookup at offset 2 *
        // TEST_CHUNK_SIZE.
        let rearmed = request_rx.recv().await.unwrap();
        assert!(rearmed.flush, "{rearmed:?}");
        assert!(rearmed.state_lookup, "{rearmed:?}");
        assert_eq!(rearmed.write_offset, (2 * TEST_CHUNK_SIZE) as i64);

        drop(tx);
        tokio::task::yield_now().await;
        drop(response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_response_missing_write_status_does_not_trip_regression_or_flush()
    -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        tx.send(append_intent(0, 10)).await?;
        let _ = request_rx.recv().await.unwrap();

        // Acknowledge 10 bytes first so worker.persisted_size = 10.
        response_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(10)),
                ..Default::default()
            }))
            .await?;

        // Act.
        // Send a response with write_status: None (e.g. write_handle refresh only). It must not be
        // treated as persisted_size = 0 (which would fail the regression check).
        response_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: None,
                ..Default::default()
            }))
            .await?;

        // Assert.
        drop(tx);
        tokio::task::yield_now().await;
        drop(response_tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_reconnect_fails_on_persisted_size_regression() -> anyhow::Result<()> {
        // Arrange.
        let (stream1_tx, mut stream1_rx) = mpsc::channel(10);
        let (stream1_resp_tx, stream1_resp_rx) = mpsc::channel(10);
        let conn1 = Connection::new(stream1_tx, stream1_resp_rx);

        let (stream2_resp_tx, stream2_resp_rx) = mpsc::channel(10);
        let stream2 = TonicResponse::from(stream2_resp_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start()
            .times(1)
            .return_once(move |_, _, _, _, _, _| Ok(Ok(stream2)));

        let mut connector = mock_connector(mock);
        connector.set_spec_state(AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "projects/_/buckets/test-bucket".into(),
                object: "test-object".into(),
                ..Default::default()
            },
            initial_chunk: None,
        });

        let worker = Worker::new(connector);
        let (intent_tx, intent_rx) = mpsc::channel(10);
        let handle = tokio::spawn(worker.run(conn1, intent_rx));

        intent_tx.send(append_intent(0, 20)).await?;
        let _ = stream1_rx.recv().await.unwrap();

        // Server acknowledges 15 bytes, trimming [0..15) from the replay buffer.
        stream1_resp_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(15)),
                ..Default::default()
            }))
            .await?;
        tokio::task::yield_now().await;

        // Act.
        // Stream 1 breaks; Stream 2 claims only 10 bytes persisted (< 15 already trimmed).
        drop(stream1_resp_tx);
        stream2_resp_tx
            .send(Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::PersistedSize(10)),
                ..Default::default()
            }))
            .await?;

        // Assert.
        let err = handle.await?.unwrap_err();
        assert!(
            err.to_string()
                .contains("regressed below acknowledged offset"),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_stream_permanent_grpc_error_fails_without_reconnecting() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, _request_rx, response_tx) = spawn_test_worker();

        // Act.
        // Send a permanent gRPC error (INVALID_ARGUMENT) on the stream. Because spawn_test_worker
        // sets `mock.expect_start().never()`, any reconnect attempt would panic.
        response_tx
            .send(Err(Status::invalid_argument("bad write_offset")))
            .await?;

        // Assert.
        let err = handle.await?.unwrap_err();
        assert!(err.to_string().contains("bad write_offset"), "{err:?}");
        drop(tx);
        Ok(())
    }
}
