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
use super::{Client, TonicStreaming, is_finalized, persisted_size};
use crate::Error;
use crate::google::storage::v2::{BidiWriteObjectRequest, BidiWriteObjectResponse};
use std::collections::VecDeque;
use std::sync::Arc;

use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Result as TonicResult;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

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
    sender: oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
}

impl PendingRequest {
    fn new(
        kind: PendingKind,
        request: BidiWriteObjectRequest,
        sender: oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
    ) -> Self {
        Self {
            kind,
            target_offset: request.write_offset,
            sender,
        }
    }

    /// Returns `true` when `response` confirms this request completed.
    ///
    /// A flush is satisfied once the service acknowledges the target offset on an unfinalized
    /// object. A finalize additionally requires a *finalized* object resource: the first message of
    /// a reconnected create or handle-less takeover stream also carries a resource, and only
    /// `finalize_time` tells the two apart. Without that check a reconnect handshake could complete
    /// the caller's `finalize()` with an object that was never finalized.
    fn is_satisfied(&self, response: &BidiWriteObjectResponse, persisted_size: i64) -> bool {
        if persisted_size < self.target_offset {
            return false;
        }
        match self.kind {
            PendingKind::Flush => !is_finalized(response),
            PendingKind::Finalize => is_finalized(response),
        }
    }

    fn complete(self, response: crate::Result<BidiWriteObjectResponse>) {
        let _ = self.sender.send(response);
    }
}

/// The background worker that manages the live gRPC stream.
pub struct Worker<C> {
    _connector: Connector<C>,
    pending_requests: VecDeque<PendingRequest>,
    /// Tracks the highest byte offset acknowledged by the server.
    persisted_size: i64,
    /// Tracks if the client intends to complete the upload by sending a Finalize intent.
    finalized: bool,
}

impl<C> Worker<C> {
    pub fn new(connector: Connector<C>) -> Self {
        Self {
            _connector: connector,
            pending_requests: VecDeque::new(),
            persisted_size: 0,
            finalized: false,
        }
    }

    /// Sets the initial acknowledged byte offset from the opening or reopen response.
    pub fn with_persisted_size(mut self, persisted_size: i64) -> Self {
        self.persisted_size = persisted_size;
        self
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
                    match self.handle_response(m) {
                        // Successful end of stream, return without error.
                        None => break None,
                        // An unrecoverable error in the stream or its data, return
                        // the error.
                        Some(Err(e)) => break Some(e),
                        // New message on the stream handled successfully,
                        // continue.
                        Some(Ok(None)) => {},
                        // TODO(#5716): Update when implementing reconnect logic.
                        // The stream reconnected successfully, update the local
                        // variables and continue.
                        Some(Ok(Some(connection))) => {
                            (rx, tx) = (connection.rx, connection.tx);
                        }
                    }
                },
                intent = requests.recv() => {
                    match intent {
                        Some(intent) => {
                            let request = self.process_intent(intent);
                            if let Err(e) = tx.send(request).await {
                                break Some(Error::io(e));
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
            UploadIntent::Append(req) => req,
            UploadIntent::Flush(req, sender) => {
                assert!(
                    req.state_lookup,
                    "state_lookup must be true for Flush intents"
                );
                assert!(req.flush, "flush must be true for Flush intents");
                self.pending_requests.push_back(PendingRequest::new(
                    PendingKind::Flush,
                    req.clone(),
                    sender,
                ));
                req
            }
            UploadIntent::Finalize(req, sender) => {
                assert!(req.flush, "flush must be true for Finalize intents");
                assert!(
                    req.finish_write,
                    "finish_write must be true for Finalize intents"
                );
                self.finalized = true;
                self.pending_requests.push_back(PendingRequest::new(
                    PendingKind::Finalize,
                    req.clone(),
                    sender,
                ));
                req
            }
        }
    }

    pub fn handle_response(
        &mut self,
        message: TonicResult<Option<BidiWriteObjectResponse>>,
    ) -> Option<LoopResult<Option<Connection<C::Stream>>>> {
        let response = match message {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                // If the stream is unexpectedly closed by the server before the client
                // intends to finalize the upload, treat it as an error to prevent silent
                // failures on subsequent client writes.
                if !self.pending_requests.is_empty() || !self.finalized {
                    return Some(Err(Error::io("stream closed unexpectedly")));
                }
                return None;
            }
            Err(e) => return Some(Err(Error::io(e))),
        };
        if let Err(e) = self.handle_response_success(response) {
            return Some(Err(e));
        }

        // TODO(#5716): Implement reconnect logic.
        Some(Ok(None))
    }

    /// Processes a successful [`BidiWriteObjectResponse`] from the server.
    ///
    /// Completes any matching in-flight flush or finalize requests. Returns an error if the server
    /// reports a `persisted_size` lower than previously acknowledged bytes.
    fn handle_response_success(&mut self, response: BidiWriteObjectResponse) -> LoopResult<()> {
        let Some(persisted_size) = persisted_size(&response) else {
            // A response with no `write_status` carries no progress, for example one that only
            // refreshes the write handle.
            tracing::debug!("Received BidiWriteObjectResponse with no write_status: {response:?}");
            return Ok(());
        };

        if persisted_size < self.persisted_size {
            return Err(Error::io(format!(
                "server persisted_size ({persisted_size}) regressed below \
                 acknowledged offset ({})",
                self.persisted_size
            )));
        }

        self.persisted_size = persisted_size;

        // Pending requests are answered in order, so stop at the first one this response does not
        // satisfy. That one is still waiting for its own response, which the service owes us, so
        // the loop stays live.
        let mut matched = false;
        while let Some(front) = self.pending_requests.front()
            && front.is_satisfied(&response, persisted_size)
        {
            let request = self
                .pending_requests
                .pop_front()
                .expect("front() just returned a request");
            request.complete(Ok(response.clone()));
            matched = true;
        }

        // An object reported as finalized before the client requested finalization (or before its
        // target offset was reached) cannot accept further writes or replayed chunks.
        if is_finalized(&response) && (!self.finalized || !self.pending_requests.is_empty()) {
            return Err(Error::io("object is already finalized"));
        }

        if !matched {
            tracing::debug!(
                "Received unprompted BidiWriteObjectResponse from server: {:?}",
                response
            );
        }
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
    use super::*;
    use crate::google::storage::v2::{
        BidiWriteObjectRequest, BidiWriteObjectResponse, Object, bidi_write_object_request::Data,
        bidi_write_object_response::WriteStatus,
    };
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

    fn flush_intent(
        write_offset: i64,
    ) -> (
        UploadIntent,
        oneshot::Receiver<crate::Result<BidiWriteObjectResponse>>,
    ) {
        let (flush_tx, flush_rx) = oneshot::channel();
        let intent = UploadIntent::Flush(
            BidiWriteObjectRequest {
                write_offset,
                flush: true,
                state_lookup: true,
                ..Default::default()
            },
            flush_tx,
        );
        (intent, flush_rx)
    }

    fn persisted_size_response(size: i64) -> BidiWriteObjectResponse {
        BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(size)),
            ..Default::default()
        }
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
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        let (intent, mut flush_rx) = flush_intent(100);
        tx.send(intent).await?;

        let stream_req = request_rx.recv().await.unwrap();
        assert!(stream_req.flush);
        assert!(stream_req.state_lookup);

        // Act.
        // An ack below `target_offset` (50 < 100) leaves the flush pending.
        response_tx.send(Ok(persisted_size_response(50))).await?;
        tokio::task::yield_now().await;
        assert!(matches!(
            flush_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let server_resp = persisted_size_response(100);
        response_tx.send(Ok(server_resp.clone())).await?;

        // Assert.
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

        let (intent, finalize_rx) = finalize_intent(100);
        tx.send(intent).await?;

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

        let (intent, mut finalize_rx) = finalize_intent(100);
        tx.send(intent).await?;
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
        response_tx.send(Ok(persisted_size_response(10))).await?;
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

    #[tokio::test]
    async fn run_server_closes_unexpectedly() -> anyhow::Result<()> {
        let (handle, _tx, _request_rx, response_tx) = spawn_test_worker();

        // Close the stream from the server side unexpectedly.
        drop(response_tx);

        let result = handle.await?;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "cannot serialize the request the transport reports an error: stream closed unexpectedly"
        );

        Ok(())
    }

    #[tokio::test]
    async fn run_stream_error_during_flush() -> anyhow::Result<()> {
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        let (flush_tx, flush_rx) = oneshot::channel();
        let flush_request = BidiWriteObjectRequest {
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        tx.send(UploadIntent::Flush(flush_request.clone(), flush_tx))
            .await?;

        let stream_req = request_rx.recv().await.unwrap();
        assert!(stream_req.flush);

        // Before the server responds, the stream unexpectedly closes.
        drop(response_tx);

        let received_resp = flush_rx.await?;
        assert!(received_resp.is_err());
        assert_eq!(
            received_resp.unwrap_err().to_string(),
            "cannot serialize the request the transport reports an error: stream closed unexpectedly"
        );

        let result = handle.await?;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn run_stream_error_then_queue_requests() -> anyhow::Result<()> {
        let (request_tx, _request_rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);
        let (tx, rx) = mpsc::channel(10);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start().never();

        let connector = mock_connector(mock);
        let worker = Worker::new(connector);
        let handle = tokio::spawn(worker.run(connection, rx));

        let (flush_tx1, flush_rx1) = oneshot::channel();
        let (flush_tx2, flush_rx2) = oneshot::channel();

        // Drop the server response stream to simulate the remote network crash.
        // The worker will wake up and eventually process this, triggering the drain.
        drop(response_tx);

        // Put requests into the channel immediately. Because it has capacity 10
        // and we haven't yielded, these are queued in the requests buffer synchronously.
        let valid_flush = || BidiWriteObjectRequest {
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        tx.send(UploadIntent::Flush(valid_flush(), flush_tx1))
            .await?;
        tx.send(UploadIntent::Flush(valid_flush(), flush_tx2))
            .await?;

        let payload1 = flush_rx1.await.unwrap();
        assert!(payload1.is_err());
        assert!(
            payload1
                .unwrap_err()
                .to_string()
                .contains("stream closed unexpectedly")
        );

        let payload2 = flush_rx2.await.unwrap();
        assert!(payload2.is_err());
        assert!(
            payload2
                .unwrap_err()
                .to_string()
                .contains("stream closed unexpectedly")
        );

        let result = handle.await?;
        assert!(result.is_err());

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
    async fn run_response_missing_write_status_does_not_trip_regression_or_flush()
    -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        tx.send(append_intent(0, 10)).await?;
        let _ = request_rx.recv().await.unwrap();

        // Acknowledge 10 bytes first so worker.persisted_size = 10.
        response_tx.send(Ok(persisted_size_response(10))).await?;

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
    async fn run_fails_when_server_reports_unexpected_finalized_object() -> anyhow::Result<()> {
        // Arrange.
        let (handle, tx, mut request_rx, response_tx) = spawn_test_worker();

        tx.send(append_intent(0, 10)).await?;
        let _ = request_rx.recv().await.unwrap();
        drop(tx);
        tokio::task::yield_now().await;

        // Act.
        // Server returns a finalized Object resource while draining in
        // `wait_for_server_completion` even though no Finalize intent was sent.
        response_tx.send(Ok(finalized_response(10))).await?;

        // Assert.
        let err = handle.await?.unwrap_err();
        assert!(
            err.to_string().contains("object is already finalized"),
            "{err:?}"
        );
        Ok(())
    }
}
