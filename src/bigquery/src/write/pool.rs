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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// TODO(#5381) - assign a unique ID, and hook up to a `Runner`
/// An entry in the stream pool serviced by a `Runner`.
#[derive(Clone, Debug)]
pub(crate) struct StreamEntry {
    /// The number of outstanding requests on this stream.
    pub(crate) outstanding_requests: Arc<AtomicU64>,

    /// The total outstanding bytes on this stream.
    pub(crate) outstanding_bytes: Arc<AtomicU64>,
}

/// A pool of open streams that supports multiplexing, load balancing.
#[derive(Debug)]
pub(crate) struct StreamPool {
    // TODO(#5381) - add the streams
    max_outstanding_requests: Option<u64>,
    max_outstanding_bytes: Option<u64>,
    load_threshold: f64,
}

impl StreamPool {
    // TODO(#5381) - add `pub(crate) fn get(&self) -> StreamEntry`

    // TODO(#5381) - add `pub(crate) fn evict_and_replace(&self, id: u64) -> StreamEntry`

    fn new_stream_entry(&self) -> StreamEntry {
        // TODO(#5381) - assign an ID and attach to a `Runner`.
        StreamEntry {
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
    use super::*;
    use test_case::test_case;

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
        let pool = StreamPool {
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
}
