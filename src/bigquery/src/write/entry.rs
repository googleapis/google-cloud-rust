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
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
        let req_len = req.encoded_len() as u64;
        let _guard = LoadGuard::new(self, req_len);

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

/// RAII guard that increments load metrics on entry and decrements on drop.
pub(crate) struct LoadGuard {
    outstanding_requests: Arc<AtomicU64>,
    outstanding_bytes: Arc<AtomicU64>,
    bytes: u64,
}

impl LoadGuard {
    pub(crate) fn new(entry: &StreamEntry, bytes: u64) -> Self {
        entry.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        entry.outstanding_bytes.fetch_add(bytes, Ordering::Relaxed);
        Self {
            outstanding_requests: Arc::clone(&entry.outstanding_requests),
            outstanding_bytes: Arc::clone(&entry.outstanding_bytes),
            bytes,
        }
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.outstanding_requests.fetch_sub(1, Ordering::Relaxed);
        self.outstanding_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::error::AppendError;
    use crate::write::test::*;
    use std::collections::VecDeque;

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
        assert_eq!(write.req, test_request(1));

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

    #[tokio::test]
    async fn load() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let entry = Arc::new(StreamEntry {
            id: 0,
            req_tx,
            outstanding_requests: Arc::new(AtomicU64::new(0)),
            outstanding_bytes: Arc::new(AtomicU64::new(0)),
        });
        let bytes_per_req = test_request(1).encoded_len() as u64;

        let mut writes = VecDeque::new();
        for i in 1_u64..5_u64 {
            let e = entry.clone();
            let handle = tokio::spawn(async move { e.send(test_request(1)).await });
            let write = req_rx.recv().await.expect("should receive request");
            writes.push_back((handle, write));

            // Verify the load grows as we queue up requests
            assert_eq!(entry.outstanding_requests.load(Ordering::Relaxed), i);
            assert_eq!(
                entry.outstanding_bytes.load(Ordering::Relaxed),
                i * bytes_per_req
            );
        }

        while let Some((handle, write)) = writes.pop_front() {
            let i = writes.len() as u64;
            write
                .resp_tx
                .send(Ok(test_response(1)))
                .expect("sending on channel always succeeds");
            let _ = handle.await??;

            // Verify the load shrinks as we receive responses
            assert_eq!(entry.outstanding_requests.load(Ordering::Relaxed), i);
            assert_eq!(
                entry.outstanding_bytes.load(Ordering::Relaxed),
                i * bytes_per_req
            );
        }

        Ok(())
    }
}
