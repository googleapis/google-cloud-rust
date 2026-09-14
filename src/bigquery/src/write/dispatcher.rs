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

use super::append_response::{AppendResponse, to_result};
use super::entry::StreamEntry;
use super::error::{AppendError, AppendResult};
use super::pool::StreamPool;
use crate::Error;
use crate::model::AppendRowsRequest;
use arc_swap::ArcSwap;
use gaxi::prost::{FromProto, ToProto};
use std::sync::Arc;

/// Efficiently dispatches writes to a stream in a stream pool.
///
/// This struct caches a `StreamEntry` and loads it atomically on each write.
///
/// On a transient error, the `Dispatcher` notifies the `StreamPool` of the
/// failed `StreamEntry` and receives a new `StreamEntry` to use for future
/// writes.
///
/// This struct is also responsible for retrying individual writes.
#[derive(Debug)]
pub(crate) struct Dispatcher {
    pub(crate) pool: Arc<StreamPool>,
    pub(crate) entry: ArcSwap<StreamEntry>,
}

impl Dispatcher {
    /// Creates a new `Dispatcher` for a given `StreamPool`.
    pub(crate) fn new(pool: Arc<StreamPool>) -> Self {
        let stream = pool.get();
        Self {
            pool,
            entry: ArcSwap::from_pointee(stream),
        }
    }

    /// Send the write and process the response.
    ///
    /// Evicts and updates its cached stream on transient errors.
    pub(crate) async fn send(&self, req: AppendRowsRequest) -> AppendResult<AppendResponse> {
        let req = req.to_proto().map_err(Error::ser)?;

        let stream = self.entry.load_full();
        let stream_id = stream.id;

        let resp = match stream.send(req).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                if is_transient_error(&err) {
                    // Atomically evicts failed_id and returns a new stream for use.
                    let new_stream = self.pool.evict_and_replace(stream_id);

                    // The application can `send()` multiple writes
                    // concurrently. Only one `send()` will update the cached
                    // stream on a transient error.
                    let _ = self.entry.compare_and_swap(&stream, Arc::new(new_stream));

                    // TODO(#6355): implement retries
                }
                Err(err)
            }
        }?;

        let resp = resp.cnv().map_err(Error::deser)?;
        to_result(resp)
    }
}

pub(crate) fn is_transient_error(err: &AppendError) -> bool {
    match err {
        AppendError::UnexpectedEndOfStream => true,
        // TODO(#6355): classify transient RPC errors
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::{Response as TonicResponse, Status as TonicStatus};
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinSet;

    fn test_req() -> AppendRowsRequest {
        AppendRowsRequest::new()
    }

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, 10));
        let dispatcher = Arc::new(Dispatcher::new(pool));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write1 = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };
        let write2 = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Respond to the writes
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        assert_eq!(write1.await??.offset, Some(1));

        response_tx.send(Ok(convert(&test_response(2)))).await?;
        assert_eq!(write2.await??.offset, Some(2));

        // Verify we are still on the same stream.
        assert_eq!(dispatcher.entry.load().id, 1);

        Ok(())
    }

    #[tokio::test]
    async fn stream_closed() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, 10));
        let dispatcher = Arc::new(Dispatcher::new(pool));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Simulate the stream closing before responding to the request.
        drop(response_tx);

        // TODO(#6355) - expect retries.
        let err = write.await?.expect_err("should return an error");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));

        // We ran into a transient error. We should now have a new stream.
        assert_eq!(dispatcher.entry.load().id, 2);

        Ok(())
    }

    #[tokio::test]
    async fn permanent_error() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, 10));
        let dispatcher = Arc::new(Dispatcher::new(pool));
        assert_eq!(dispatcher.entry.load().id, 1);

        let write = {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.send(test_req()).await })
        };

        // Simulate a permanent stream error
        response_tx
            .send(Err(TonicStatus::failed_precondition("fail")))
            .await?;

        let err = write.await?.expect_err("should return an error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));

        Ok(())
    }

    #[tokio::test]
    async fn transient_error_evict_contention() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, 10));
        let dispatcher = Arc::new(Dispatcher::new(pool.clone()));
        assert_eq!(dispatcher.entry.load().id, 1);

        let mut writes = JoinSet::new();
        for _ in 0..1000 {
            let d = dispatcher.clone();
            writes.spawn(async move { d.send(test_req()).await });
        }

        // Simulate the stream closing before responding to the requests.
        drop(response_tx);

        while let Some(write) = writes.join_next().await {
            let err = write?.expect_err("should return an error");
            assert!(matches!(err, AppendError::UnexpectedEndOfStream));
        }

        // We ran into a transient error. We should now have a new stream. Only
        // one of the callers should have evicted the failed stream.
        assert_eq!(dispatcher.entry.load().id, 2);
        assert_eq!(pool.stream_ids(), [2]);

        Ok(())
    }

    #[tokio::test]
    async fn writes_bypass_pool_lock() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(move |_| Ok(TonicResponse::from(response_rx)));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, 10));
        let dispatcher = Arc::new(Dispatcher::new(pool.clone()));

        // Acquire the stream pool's lock to simulate a pool scaling event. This
        // needs to run in a separate thread because we don't want to hold the
        // `std::sync::MutexGuard` across `await` points.
        let (lock_acquired_tx, lock_acquired_rx) = oneshot::channel();
        let (release_lock_tx, release_lock_rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let _guard = pool.lock();
            let _ = lock_acquired_tx.send(());
            let _ = release_lock_rx.recv();
        });

        // Wait until the lock is acquired to send a write.
        lock_acquired_rx.await?;
        let write = tokio::spawn(async move { dispatcher.send(test_req()).await });

        // Verify the write goes through, even with the pool's lock held.
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        assert_eq!(write.await??.offset, Some(1));

        // Release the lock
        drop(release_lock_tx);

        Ok(())
    }
}
