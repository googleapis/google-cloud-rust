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

use super::error::{AppendError, AppendResult};
use super::runner::WriteRequest;
use crate::google::cloud::bigquery::storage::v1::{AppendRowsRequest, AppendRowsResponse};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{mpsc, oneshot};

/// An entry in the stream pool serviced by a `Runner`.
#[derive(Clone, Debug)]
pub(crate) struct StreamEntry {
    /// Unique identifier for this stream connection.
    pub(crate) id: u64,

    /// Channel to send requests to the stream's background runner task.
    pub(crate) req_tx: mpsc::UnboundedSender<WriteRequest>,

    /// The number of outstanding requests on this stream.
    pub(crate) outstanding_requests: Arc<AtomicU64>,

    /// The total outstanding bytes on this stream.
    pub(crate) outstanding_bytes: Arc<AtomicU64>,
}

impl StreamEntry {
    /// Send a write to the stream task and process the result
    pub(crate) async fn send(&self, req: AppendRowsRequest) -> AppendResult<AppendRowsResponse> {
        // TODO(#6122) - track load on the stream entry

        let (resp_tx, resp_rx) = oneshot::channel();
        let write = WriteRequest { req, resp_tx };

        self.req_tx
            .send(write)
            .map_err(|_| AppendError::UnexpectedEndOfStream)?;

        resp_rx
            .await
            .map_err(|_| AppendError::UnexpectedEndOfStream)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::error::AppendError;
    use crate::write::test::*;

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let entry = StreamEntry {
            id: 0,
            req_tx,
            outstanding_requests: Arc::new(AtomicU64::new(0)),
            outstanding_bytes: Arc::new(AtomicU64::new(0)),
        };
        let handle = tokio::spawn(async move { entry.send(test_request(1)).await });

        // Receive and verify the request
        let write = req_rx.recv().await.expect("should receive request");
        assert_eq!(write.req.write_stream, write_stream());

        // Provide a successful response
        write
            .resp_tx
            .send(Ok(test_response(1)))
            .expect("sending on channel always succeeds");

        let resp = handle.await??;
        assert_eq!(resp, test_response(1));
        Ok(())
    }

    #[tokio::test]
    async fn stream_closed() -> anyhow::Result<()> {
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let entry = StreamEntry {
            id: 0,
            req_tx,
            outstanding_requests: Arc::new(AtomicU64::new(0)),
            outstanding_bytes: Arc::new(AtomicU64::new(0)),
        };
        let handle = tokio::spawn(async move { entry.send(test_request(1)).await });

        // Simulate a stream closure
        drop(req_rx);

        let err = handle.await?.expect_err("should return an error");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
        Ok(())
    }

    #[tokio::test]
    async fn rpc_error() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let entry = StreamEntry {
            id: 0,
            req_tx,
            outstanding_requests: Arc::new(AtomicU64::new(0)),
            outstanding_bytes: Arc::new(AtomicU64::new(0)),
        };
        let handle = tokio::spawn(async move { entry.send(test_request(1)).await });

        // Simulate a stream ending in a known error
        let write = req_rx.recv().await.expect("should receive request");
        let append_err: AppendError = Error::io("fail").into();
        write
            .resp_tx
            .send(Err(append_err))
            .expect("sending on channel always succeeds");

        let err = handle.await?.expect_err("should return an error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));
        Ok(())
    }
}
