// Copyright 2025 Google LLC
#![allow(dead_code)]
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
use crate::error::WriteError;
use crate::google::storage::v2::{BidiWriteObjectRequest, BidiWriteObjectResponse};
use gaxi::grpc::tonic::Result as TonicResult;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

type WriteResult<T> = std::result::Result<T, WriteError>;
type LoopResult<T> = std::result::Result<T, Arc<crate::Error>>;

/// The intent sent from the user to the background worker.
pub enum UploadIntent {
    /// Append a chunk of data.
    Append(BidiWriteObjectRequest),
    /// Flush the data to the server.
    Flush(
        BidiWriteObjectRequest,
        oneshot::Sender<crate::Result<BidiWriteObjectResponse>>,
    ),
    /// Finalize the upload.
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
}

impl<C> Worker<C> {
    pub fn new(connector: super::connector::Connector<C>) -> Self {
        Self {
            _connector: connector,
            pending_flushes: std::collections::VecDeque::new(),
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
                        // An unrecoverable in the stream or its data, return
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
                intent = requests.recv() => {
                    match intent {
                        Some(UploadIntent::Append(request)) => {
                            if let Err(e) = tx.send(request).await {
                                break Some(Arc::new(crate::Error::io(e)));
                            }
                        }
                        Some(UploadIntent::Flush(request, sender)) => {
                            self.pending_flushes.push_back(sender);
                            if let Err(e) = tx.send(request).await {
                                break Some(Arc::new(crate::Error::io(e)));
                            }
                        }
                        Some(UploadIntent::Finalize(request, sender)) => {
                            self.pending_flushes.push_back(sender);
                            if let Err(e) = tx.send(request).await {
                                break Some(Arc::new(crate::Error::io(e)));
                            }
                        }
                        None => {
                            break None;
                        }
                    }
                }
            }
        };

        if let Some(e) = error.clone() {
            for sender in self.pending_flushes.drain(..) {
                let _ = sender.send(Err(crate::Error::io(e.to_string())));
            }
            return Err(e);
        }

        Ok(())
    }

    pub fn handle_response(
        &mut self,
        message: TonicResult<Option<BidiWriteObjectResponse>>,
    ) -> Option<LoopResult<Option<Connection<C::Stream>>>> {
        let response = match message {
            Ok(Some(msg)) => msg,
            Ok(None) => return None,
            Err(e) => return Some(Err(Arc::new(crate::Error::io(e)))),
        };
        self.handle_response_success(response);
        // TODO(#5716): Update when implementing reconnect logic.
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
    use super::super::mocks::{MockTestClient, mock_connector, mock_stream};
    use super::*;
    use crate::google::storage::v2::{
        BidiWriteObjectRequest, BidiWriteObjectResponse, bidi_write_object_response::WriteStatus,
    };
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn run_immediately_closed() -> anyhow::Result<()> {
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mock_stream();
        let (tx, rx) = mpsc::channel(1);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start().never();

        let connector = mock_connector(mock);
        let worker = Worker::new(connector);
        let handle = tokio::spawn(worker.run(connection, rx));

        drop(response_tx);
        drop(tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_flush_response() -> anyhow::Result<()> {
        let (request_tx, mut request_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mock_stream();
        let (tx, rx) = mpsc::channel(1);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start().never();

        let connector = mock_connector(mock);
        let worker = Worker::new(connector);
        let handle = tokio::spawn(worker.run(connection, rx));

        let (flush_tx, flush_rx) = oneshot::channel();
        let flush_request = BidiWriteObjectRequest {
            flush: true,
            ..Default::default()
        };
        tx.send(UploadIntent::Flush(flush_request.clone(), flush_tx))
            .await?;

        // The worker should send the request to the stream.
        let stream_req = request_rx.recv().await.unwrap();
        assert!(stream_req.flush);

        // The server responds
        let server_resp = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(100)),
            ..Default::default()
        };
        response_tx.send(Ok(server_resp.clone())).await?;

        // The flush sender should get the response
        let received_resp = flush_rx.await??;
        assert_eq!(received_resp.write_status, server_resp.write_status);

        drop(response_tx);
        drop(tx);
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn run_stop_on_closed_requests() -> anyhow::Result<()> {
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_response_tx, response_rx) = mock_stream();
        let (tx, rx) = mpsc::channel(1);
        let connection = Connection::new(request_tx, response_rx);

        let mut mock = MockTestClient::new();
        mock.expect_start().never();

        let connector = mock_connector(mock);
        let worker = Worker::new(connector);
        drop(tx);
        worker.run(connection, rx).await?;
        Ok(())
    }
}
