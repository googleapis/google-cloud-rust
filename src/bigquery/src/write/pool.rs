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

use super::runner::{Runner, WriteRequest};
use super::transport::Transport;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

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

/// A pool of open streams that supports multiplexing, load balancing.
#[derive(Debug)]
pub(crate) struct StreamPool {
    inner: Arc<Transport>,
    next_stream_id: AtomicU64,
    // We hold the streams in a `std::sync::Mutex` because we only want a single
    // caller to be able to scale up the pool, or remove a failed stream.
    //
    // Note that we do not acquire the lock on the hot path during writes. We
    // only acquire the lock when adding a new writer or recovering from a
    // stream error.
    streams: Mutex<Vec<StreamEntry>>,
    max_streams: usize,
    max_outstanding_requests: Option<u64>,
    max_outstanding_bytes: Option<u64>,
    load_threshold: f64,
}

impl StreamPool {
    /// Initializes a new [StreamPool].
    pub(crate) fn new(inner: Arc<Transport>, max_streams: usize) -> Self {
        Self {
            inner,
            next_stream_id: AtomicU64::new(1),
            streams: Mutex::new(Vec::new()),
            max_streams,
            max_outstanding_requests: Some(1000),
            max_outstanding_bytes: None,
            load_threshold: 0.2,
        }
    }

    /// Returns a stream.
    pub(crate) fn get(&self) -> StreamEntry {
        let mut streams = self.streams.lock().unwrap();
        self.get_impl(&mut streams)
    }

    /// Evicts a failed stream and replaces it in-place.
    ///
    /// If multiple callers report the same stream ID simultaneously,
    /// only the first caller provisions a replacement.
    pub(crate) fn evict_and_replace(&self, failed_id: u64) -> StreamEntry {
        let mut streams = self.streams.lock().unwrap();
        if let Some(pos) = streams.iter().position(|entry| entry.id == failed_id) {
            // If we have not yet replaced the failed stream, do so.
            let stream = self.new_stream_entry();
            streams[pos] = stream.clone();
            return stream;
        }
        self.get_impl(&mut streams)
    }

    /// Selects the stream connection with the least load.
    ///
    /// If necessary, the pool will dynamically scale up.
    ///
    /// This is only called when...
    /// - a new writer is added
    /// - a stream fails with a transient error
    fn get_impl(&self, streams: &mut Vec<StreamEntry>) -> StreamEntry {
        let least_loaded = streams.iter().min_by(|a, b| {
            let load_a = self.normalize_load(a);
            let load_b = self.normalize_load(b);
            load_a.total_cmp(&load_b)
        });
        let should_grow = least_loaded.is_none_or(|s| self.is_loaded(s));
        if streams.len() < self.max_streams && should_grow {
            // If we can and should scale up, do so.
            let stream = self.new_stream_entry();
            streams.push(stream.clone());
            return stream;
        }
        match least_loaded {
            Some(s) => s.clone(),
            None => unreachable!("this can only happen when `max_streams == 0`"),
        }
    }

    fn new_stream_entry(&self) -> StreamEntry {
        let id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let runner = Runner::new(self.inner.clone());

        StreamEntry {
            id,
            req_tx: runner.req_tx,
            outstanding_requests: Arc::new(AtomicU64::new(0)),
            outstanding_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Estimate the current load on the stream.
    ///
    /// Our best proxy is to use outstanding requests, outstanding bytes.
    fn normalize_load(&self, entry: &StreamEntry) -> f64 {
        let r = self
            .max_outstanding_requests
            .map(|m| entry.outstanding_requests.load(Ordering::Relaxed) as f64 / m as f64)
            .unwrap_or_default();
        let b = self
            .max_outstanding_bytes
            .map(|m| entry.outstanding_bytes.load(Ordering::Relaxed) as f64 / m as f64)
            .unwrap_or_default();
        f64::max(r, b)
    }

    /// Determine if the stream is approaching load
    fn is_loaded(&self, entry: &StreamEntry) -> bool {
        self.normalize_load(entry) > self.load_threshold
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::runner::tests::*;
    use super::super::transport::tests::*;
    use super::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use test_case::test_case;
    use tokio::sync::oneshot;
    use tokio::task::JoinSet;

    #[test_case(10, Some(100), 10_000, Some(100_000), 0.1, false)]
    #[test_case(90, Some(100), 10_000, Some(100_000), 0.9, true)]
    #[test_case(10, Some(100), 90_000, Some(100_000), 0.9, true)]
    #[test_case(90, Some(100), 90_000, Some(100_000), 0.9, true)]
    #[test_case(10, None, 10_000, Some(100_000), 0.1, false)]
    #[test_case(10, Some(100), 10_000, None, 0.1, false)]
    #[test_case(10, None, 10_000, None, 0.0, false)]
    #[tokio::test]
    async fn load_math(
        requests: u64,
        max_outstanding_requests: Option<u64>,
        bytes: u64,
        max_outstanding_bytes: Option<u64>,
        expected_load: f64,
        expected_is_loaded: bool,
    ) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("ignored").await?);
        let pool = StreamPool {
            inner: transport,
            next_stream_id: AtomicU64::new(1),
            streams: Mutex::new(Vec::new()),
            max_streams: 10,
            max_outstanding_requests,
            max_outstanding_bytes,
            load_threshold: 0.2,
        };

        let s = pool.new_stream_entry();
        s.outstanding_requests.store(requests, Ordering::Relaxed);
        s.outstanding_bytes.store(bytes, Ordering::Relaxed);

        assert_eq!(pool.normalize_load(&s), expected_load);
        assert_eq!(pool.is_loaded(&s), expected_is_loaded);
        Ok(())
    }

    #[tokio::test]
    async fn empty_pool_get_basic() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = StreamPool::new(transport, 10);

        let s1 = pool.get();
        assert_eq!(s1.id, 1);
        assert_eq!(pool.stream_ids(), [1]);

        let s2 = pool.get();
        assert_eq!(s2.id, 1);
        assert_eq!(pool.stream_ids(), [1]);

        // Use the stream handles to send requests on the same underlying gRPC
        // stream.

        // write 1, from stream 1
        let (resp_tx1, resp_rx1) = oneshot::channel();
        let write1 = WriteRequest {
            req: test_request(1),
            resp_tx: resp_tx1,
        };
        s1.req_tx.send(write1)?;

        // write 2, from stream 2
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let write2 = WriteRequest {
            req: test_request(2),
            resp_tx: resp_tx2,
        };
        s2.req_tx.send(write2)?;

        // resp 1
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp1 = resp_rx1.await??;
        assert_eq!(resp1, test_response(1));

        // resp 2
        response_tx.send(Ok(convert(&test_response(2)))).await?;
        let resp2 = resp_rx2.await??;
        assert_eq!(resp2, test_response(2));

        Ok(())
    }

    #[tokio::test]
    async fn empty_pool_get_lock_contention() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("ignored").await?);
        let pool = Arc::new(StreamPool {
            inner: transport,
            next_stream_id: AtomicU64::new(1),
            streams: Mutex::new(Vec::new()),
            max_streams: 10,
            // Disable load tracking. We should never scale past a single stream.
            max_outstanding_requests: None,
            max_outstanding_bytes: None,
            load_threshold: 0.2,
        });

        let mut streams = JoinSet::new();
        for _ in 0..1000 {
            let p = pool.clone();
            streams.spawn(async move { p.get() });
        }
        // Verify each stream handle is for ID 1.
        while let Some(s) = streams.join_next().await {
            assert_eq!(s?.id, 1);
        }
        // Verify only one stream is created total.
        assert_eq!(pool.stream_ids(), [1]);

        Ok(())
    }

    #[tokio::test]
    async fn get_least_loaded() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("ignored").await?);
        let pool = StreamPool::new(transport, 10);

        // Manually seed the pool
        for load in [8, 2, 2, 3, 1, 9] {
            let s = pool.new_stream_entry();
            s.outstanding_requests.store(load, Ordering::Relaxed);
            pool.streams.lock().unwrap().push(s);
        }

        let s = pool.get();
        assert_eq!(s.id, 5);

        Ok(())
    }

    #[tokio::test]
    async fn get_should_grow() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("ignored").await?);
        let pool = Arc::new(StreamPool {
            inner: transport,
            next_stream_id: AtomicU64::new(1),
            streams: Mutex::new(Vec::new()),
            max_streams: 10,
            max_outstanding_requests: Some(3),
            max_outstanding_bytes: None,
            load_threshold: 0.5,
        });

        let s = pool.get();
        assert_eq!(s.id, 1);
        assert_eq!(pool.stream_ids(), [1]);

        // Simulate an in-flight request on the stream. It should not be loaded
        // yet.
        s.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        let s = pool.get();
        assert_eq!(s.id, 1);
        assert_eq!(pool.stream_ids(), [1]);

        // Simulate another in-flight request on the stream. Now it should be at
        // load. We should grow the pool.
        s.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        let s = pool.get();
        assert_eq!(s.id, 2);
        assert_eq!(pool.stream_ids(), [1, 2]);

        // Simulate an in-flight request on the second stream. It should not be
        // loaded yet.
        s.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        let s = pool.get();
        assert_eq!(s.id, 2);
        assert_eq!(pool.stream_ids(), [1, 2]);

        // Simulate another in-flight request on the second stream. Now it should be at
        // load. We should grow the pool again.
        s.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        let s = pool.get();
        assert_eq!(s.id, 3);
        assert_eq!(pool.stream_ids(), [1, 2, 3]);

        Ok(())
    }

    #[tokio::test]
    async fn fully_loaded_get() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("ignored").await?);
        let pool = Arc::new(StreamPool {
            inner: transport,
            next_stream_id: AtomicU64::new(1),
            streams: Mutex::new(Vec::new()),
            max_streams: 6,
            max_outstanding_requests: Some(10),
            max_outstanding_bytes: None,
            load_threshold: 0.2,
        });

        // Manually seed the pool to its limit (`max_streams`). Note that all
        // streams are already at load.
        for load in [8, 5, 3, 8, 9, 11] {
            let s = pool.new_stream_entry();
            s.outstanding_requests.store(load, Ordering::Relaxed);
            pool.streams.lock().unwrap().push(s);
        }
        assert_eq!(pool.streams.lock().unwrap().len(), 6);

        // We should return the least loaded stream without growing the pool.
        let s = pool.get();
        assert_eq!(s.id, 3);
        assert_eq!(pool.streams.lock().unwrap().len(), 6);

        Ok(())
    }

    #[tokio::test]
    async fn evict_basic() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("ignored").await?);
        let pool = StreamPool::new(transport, 10);

        // Manually seed the pool
        for load in [1, 2, 3, 4, 5, 6] {
            let s = pool.new_stream_entry();
            s.outstanding_requests.store(load, Ordering::Relaxed);
            pool.streams.lock().unwrap().push(s);
        }
        assert_eq!(pool.stream_ids(), [1, 2, 3, 4, 5, 6]);

        let s = pool.evict_and_replace(3);
        assert_eq!(s.id, 7);
        assert_eq!(pool.normalize_load(&s), 0.0);
        assert_eq!(pool.stream_ids(), [1, 2, 4, 5, 6, 7]);

        let s = pool.evict_and_replace(6);
        assert_eq!(s.id, 8);
        assert_eq!(pool.normalize_load(&s), 0.0);
        assert_eq!(pool.stream_ids(), [1, 2, 4, 5, 7, 8]);

        Ok(())
    }

    #[tokio::test]
    async fn evict_lock_contention() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("ignored").await?);
        let pool = Arc::new(StreamPool::new(transport, 10));

        // Manually seed the pool
        let s = pool.new_stream_entry();
        s.outstanding_requests.store(1, Ordering::Relaxed);
        pool.streams.lock().unwrap().push(s);

        let mut streams = JoinSet::new();
        for _ in 0..1000 {
            let p = pool.clone();
            streams.spawn(async move { p.evict_and_replace(1) });
        }
        // Verify each stream handle is for ID 2.
        while let Some(s) = streams.join_next().await {
            assert_eq!(s?.id, 2);
        }
        // Verify the pool stays at one stream total.
        assert_eq!(pool.stream_ids(), [2]);

        Ok(())
    }

    // Returns the stream IDs in the pool, in order.
    impl StreamPool {
        fn stream_ids(&self) -> Vec<u64> {
            let mut ids: Vec<_> = self.streams.lock().unwrap().iter().map(|s| s.id).collect();
            ids.sort();
            ids
        }
    }
}
