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

use super::connector::Connection;
use super::{Client, TonicStreaming};
use crate::Error;
use crate::error::WriteError;
use crate::google::storage::v2::{BidiWriteObjectRequest, BidiWriteObjectResponse};
use gaxi::grpc::tonic::Result as TonicResult;
use std::sync::Arc;

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

/// The background worker that manages the live gRPC stream.
pub struct Worker<C> {
    _connector: super::connector::Connector<C>,
    pending_flushes:
        std::collections::VecDeque<oneshot::Sender<crate::Result<BidiWriteObjectResponse>>>,
    /// Tracks if the client intends to complete the upload, by sending a Finalize intent.
    finalized: bool,
}

impl<C> Worker<C> {
    pub fn new(connector: super::connector::Connector<C>) -> Self {
        Self {
            _connector: connector,
            pending_flushes: std::collections::VecDeque::new(),
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
                self.pending_flushes.push_back(sender);
                req
            }
            UploadIntent::Finalize(req, sender) => {
                assert!(req.flush, "flush must be true for Finalize intents");
                assert!(
                    req.finish_write,
                    "finish_write must be true for Finalize intents"
                );
                self.pending_flushes.push_back(sender);
                self.finalized = true;
                req
            }
        }
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
        for sender in self.pending_flushes.drain(..) {
            let _ = sender.send(Err(Error::ser(Arc::clone(&shared_error))));
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
                if !self.pending_flushes.is_empty() || !self.finalized {
                    return Some(Err(Error::io("stream closed unexpectedly")));
                }
                return None;
            }
            Err(e) => return Some(Err(Error::io(e))),
        };
        self.handle_response_success(response);

        // TODO(#5716): Implement reconnect logic.
        Some(Ok(None))
    }

    pub fn handle_response_success(&mut self, response: BidiWriteObjectResponse) {
        if let Some(sender) = self.pending_flushes.pop_front() {
            let _ = sender.send(Ok(response));
        } else {
            // Log unprompted server responses.
            tracing::debug!(
                "Received unprompted BidiWriteObjectResponse from server: {:?}",
                response
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::mocks::{MockTestClient, mock_connector};
    use super::*;
    use crate::google::storage::v2::{
        BidiWriteObjectRequest, BidiWriteObjectResponse, bidi_write_object_response::WriteStatus,
    };
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    type TestWorkerContext = (
        tokio::task::JoinHandle<LoopResult<()>>,
        mpsc::Sender<UploadIntent>,
        mpsc::Receiver<BidiWriteObjectRequest>,
        mpsc::Sender<TonicResult<BidiWriteObjectResponse>>,
    );

    fn spawn_test_worker() -> TestWorkerContext {
        let (request_tx, request_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(10);
        let (tx, rx) = mpsc::channel(1);
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
            ..Default::default()
        };
        tx.send(UploadIntent::Finalize(
            finalize_request.clone(),
            finalize_tx,
        ))
        .await?;

        let stream_req = request_rx.recv().await.unwrap();
        assert!(stream_req.finish_write);

        let server_resp = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(100)),
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
    async fn run_stop_on_closed_requests() -> anyhow::Result<()> {
        let (handle, tx, _request_rx, _response_tx) = spawn_test_worker();
        drop(tx);
        tokio::task::yield_now().await;
        drop(_response_tx);
        handle.await??;
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
}
