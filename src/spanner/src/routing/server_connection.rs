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

//! Server connection wrapper and inflight request tracking for location-aware routing.

// TODO(location-aware-routing): Remove allow(dead_code) once location_router.rs integrates ServerConnection.
#![allow(dead_code)]

use crate::client::Channel;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

const STATE_READY: u8 = 0;
const STATE_TRANSIENT_FAILURE: u8 = 1;
const STATE_UNHEALTHY: u8 = 2;

/// A Spanner server connection wrapper for location-aware routing.
///
/// Wraps a gRPC [`Channel`] connected to a specific Spanner server node address and tracks
/// connectivity health state and active inflight requests.
///
/// # Thread Safety
///
/// All health transitions and request counter modifications are lock-free and thread-safe.
#[derive(Clone, Debug)]
pub(crate) struct ServerConnection {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    address: String,
    channel: Channel,
    state: AtomicU8,
    active_requests: AtomicUsize,
}

/// RAII guard that decrements the active request count of a [`ServerConnection`] when dropped.
#[must_use = "if unused the request count will decrement immediately"]
pub(crate) struct ActiveRequestGuard {
    inner: Arc<Inner>,
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        let _ =
            self.inner
                .active_requests
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                    Some(val.saturating_sub(1))
                });
    }
}

impl ServerConnection {
    /// Creates a new `ServerConnection` wrapping the given address and channel in the `READY` state.
    pub(crate) fn new(address: String, channel: Channel) -> Self {
        Self {
            inner: Arc::new(Inner {
                address,
                channel,
                state: AtomicU8::new(STATE_READY),
                active_requests: AtomicUsize::new(0),
            }),
        }
    }

    /// Returns the network address of this server in `"host:port"` format.
    pub(crate) fn address(&self) -> &str {
        &self.inner.address
    }

    /// Returns a reference to the wrapped gRPC [`Channel`].
    pub(crate) fn channel(&self) -> &Channel {
        &self.inner.channel
    }

    /// Returns whether this connection is in the `READY` state and eligible for location-aware routing.
    pub(crate) fn is_healthy(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == STATE_READY
    }

    /// Returns whether this connection is in the `TRANSIENT_FAILURE` state.
    pub(crate) fn is_transient_failure(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == STATE_TRANSIENT_FAILURE
    }

    /// Marks this connection as `READY`.
    pub(crate) fn set_ready(&self) {
        self.inner.state.store(STATE_READY, Ordering::Release);
    }

    /// Marks this connection as in `TRANSIENT_FAILURE`.
    pub(crate) fn set_transient_failure(&self) {
        self.inner
            .state
            .store(STATE_TRANSIENT_FAILURE, Ordering::Release);
    }

    /// Marks this connection as `UNHEALTHY`.
    pub(crate) fn set_unhealthy(&self) {
        self.inner.state.store(STATE_UNHEALTHY, Ordering::Release);
    }

    /// Increments the active inflight request count for this connection.
    pub(crate) fn increment_active_requests(&self) {
        self.inner.active_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the active inflight request count for this connection without underflow.
    pub(crate) fn decrement_active_requests(&self) {
        let _ =
            self.inner
                .active_requests
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                    Some(val.saturating_sub(1))
                });
    }

    /// Returns the current number of active inflight requests on this connection.
    pub(crate) fn active_request_count(&self) -> usize {
        self.inner.active_requests.load(Ordering::Relaxed)
    }

    /// Increments the active request count and returns an RAII guard that automatically decrements
    /// the count when dropped.
    pub(crate) fn acquire_request_guard(&self) -> ActiveRequestGuard {
        self.increment_active_requests();
        ActiveRequestGuard {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[derive(Debug)]
    struct DummyStub;
    impl crate::generated::gapic_dataplane::stub::Spanner for DummyStub {}

    fn create_test_connection(address: &str) -> ServerConnection {
        let channel = Channel::new_for_test(DummyStub);
        ServerConnection::new(address.to_string(), channel)
    }

    #[test]
    fn test_server_connection_health_state_transitions() {
        let conn = create_test_connection("10.0.0.1:15000");
        assert_eq!(conn.address(), "10.0.0.1:15000");
        assert!(conn.is_healthy());
        assert!(!conn.is_transient_failure());

        conn.set_transient_failure();
        assert!(!conn.is_healthy());
        assert!(conn.is_transient_failure());

        conn.set_unhealthy();
        assert!(!conn.is_healthy());
        assert!(!conn.is_transient_failure());

        conn.set_ready();
        assert!(conn.is_healthy());
        assert!(!conn.is_transient_failure());
    }

    #[test]
    fn test_server_connection_active_requests_and_underflow_protection() {
        let conn = create_test_connection("10.0.0.1:15000");
        assert_eq!(conn.active_request_count(), 0);

        conn.increment_active_requests();
        conn.increment_active_requests();
        conn.increment_active_requests();
        assert_eq!(conn.active_request_count(), 3);

        conn.decrement_active_requests();
        assert_eq!(conn.active_request_count(), 2);

        conn.decrement_active_requests();
        conn.decrement_active_requests();
        assert_eq!(conn.active_request_count(), 0);

        // Verify saturating subtraction prevents wrapping underflow.
        conn.decrement_active_requests();
        conn.decrement_active_requests();
        assert_eq!(conn.active_request_count(), 0);
    }

    #[test]
    fn test_server_connection_channel_accessor() {
        let conn = create_test_connection("10.0.0.1:15000");
        let channel = conn.channel();
        assert!(format!("{:?}", channel).contains("Channel"));
    }

    #[test]
    fn test_server_connection_acquire_request_guard() {
        let conn = create_test_connection("10.0.0.1:15000");
        assert_eq!(conn.active_request_count(), 0);

        {
            let _guard1 = conn.acquire_request_guard();
            assert_eq!(conn.active_request_count(), 1);

            {
                let _guard2 = conn.acquire_request_guard();
                assert_eq!(conn.active_request_count(), 2);
            }

            assert_eq!(conn.active_request_count(), 1);
        }

        assert_eq!(conn.active_request_count(), 0);
    }

    #[test]
    fn test_server_connection_concurrent_guards() {
        let conn = create_test_connection("10.0.0.1:15000");
        let worker_count = 10;
        let iterations = 100;
        let barrier = Barrier::new(worker_count);

        thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..iterations {
                        let _guard = conn.acquire_request_guard();
                        assert!(conn.active_request_count() > 0);
                    }
                });
            }
        });

        assert_eq!(conn.active_request_count(), 0);
    }

    #[tokio::test]
    async fn test_server_connection_guard_static_lifetime() {
        let conn = create_test_connection("10.0.0.1:15000");
        let guard = conn.acquire_request_guard();
        assert_eq!(conn.active_request_count(), 1);

        let conn_clone = conn.clone();
        tokio::spawn(async move {
            let _moved_guard = guard;
            assert_eq!(conn_clone.active_request_count(), 1);
        })
        .await
        .expect("spawned task should complete");

        assert_eq!(conn.active_request_count(), 0);
    }
}
