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

//! Thread-safe cache of Spanner server node connections for location-aware routing.

// TODO(location-aware-routing): Remove allow(dead_code) once location_router.rs integrates ConnectionCache.
#![allow(dead_code)]

use crate::client::Channel;
use crate::routing::server_connection::ServerConnection;
use gaxi::options::ClientConfig;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Cache for server connections used in location-aware routing.
///
/// Stores and manages [`ServerConnection`] instances such that repeated calls with the same address
/// return the same connection wrapper.
///
/// # Thread Safety
///
/// This cache is thread-safe and allows concurrent lookups across multiple operations.
#[derive(Debug)]
pub(crate) struct ConnectionCache {
    default_connection: ServerConnection,
    servers: RwLock<HashMap<String, Arc<tokio::sync::OnceCell<ServerConnection>>>>,
}

impl ConnectionCache {
    /// Creates a new `ConnectionCache` with the specified default fallback connection.
    pub(crate) fn new(default_connection: ServerConnection) -> Self {
        let default_cell = Arc::new(tokio::sync::OnceCell::from(default_connection.clone()));
        let mut map = HashMap::new();
        map.insert(default_connection.address().to_string(), default_cell);
        Self {
            default_connection,
            servers: RwLock::new(map),
        }
    }

    /// Returns a reference to the default fallback server connection.
    pub(crate) fn default_connection(&self) -> &ServerConnection {
        &self.default_connection
    }

    /// Returns a cached connection for the given address without creating it if missing.
    ///
    /// This method is used by location-aware routing to avoid foreground connection creation on the
    /// hot RPC request path.
    pub(crate) fn get_if_present(&self, address: &str) -> Option<ServerConnection> {
        let guard = self
            .servers
            .read()
            .expect("connection cache read lock poisoned");
        guard.get(address).and_then(|cell| cell.get().cloned())
    }

    /// Returns a cached connection for the given address, creating and caching a new connection
    /// asynchronously if needed.
    pub(crate) async fn get(
        &self,
        address: &str,
        config: &ClientConfig,
    ) -> crate::ClientBuilderResult<ServerConnection> {
        let cell = {
            let guard = self
                .servers
                .read()
                .expect("connection cache read lock poisoned");
            if let Some(cell) = guard.get(address) {
                if let Some(connection) = cell.get() {
                    return Ok(connection.clone());
                }
                cell.clone()
            } else {
                drop(guard);
                let mut guard = self
                    .servers
                    .write()
                    .expect("connection cache write lock poisoned");
                guard
                    .entry(address.to_string())
                    .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                    .clone()
            }
        };

        cell.get_or_try_init(|| async {
            let mut ep_config = config.clone();
            let addr = address.to_string();
            ep_config.endpoint = Some(addr.clone());
            let channel = Channel::create(&ep_config).await?;
            Ok(ServerConnection::new(addr, channel))
        })
        .await
        .cloned()
    }

    /// Evicts a server connection from the cache.
    ///
    /// If `address` matches the default connection's address, this method does nothing and returns
    /// `false`. Otherwise, returns `true` if a connection was removed from the cache.
    pub(crate) fn evict(&self, address: &str) -> bool {
        if self.default_connection.address() == address {
            return false;
        }
        let mut guard = self
            .servers
            .write()
            .expect("connection cache write lock poisoned");
        guard.remove(address).is_some()
    }

    /// Returns the number of cached server connections (including the default connection).
    pub(crate) fn len(&self) -> usize {
        let guard = self
            .servers
            .read()
            .expect("connection cache read lock poisoned");
        guard.values().filter(|cell| cell.get().is_some()).count()
    }

    /// Returns whether the cache is empty.
    pub(crate) fn is_empty(&self) -> bool {
        let guard = self
            .servers
            .read()
            .expect("connection cache read lock poisoned");
        !guard.values().any(|cell| cell.get().is_some())
    }

    /// Clears all cached server connections while preserving the default fallback connection.
    pub(crate) fn clear(&self) {
        let mut guard = self
            .servers
            .write()
            .expect("connection cache write lock poisoned");
        let default_address = self.default_connection.address();
        guard.retain(|k, _| k == default_address);
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
    fn test_connection_cache_default_connection_and_get_if_present() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_conn.clone());

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        assert_eq!(
            cache.default_connection().address(),
            "spanner.googleapis.com:443"
        );

        let cached_default = cache
            .get_if_present("spanner.googleapis.com:443")
            .expect("default connection should be in cache");
        assert_eq!(cached_default.address(), "spanner.googleapis.com:443");

        assert!(cache.get_if_present("10.0.0.1:15000").is_none());
    }

    #[test]
    fn test_connection_cache_eviction_and_protection_of_default() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_conn);

        // Manually insert a tablet connection into the cache for testing eviction.
        let tablet_conn = create_test_connection("10.0.0.1:15000");
        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            let cell = Arc::new(tokio::sync::OnceCell::new());
            let _ = cell.set(tablet_conn.clone());
            guard.insert(tablet_conn.address().to_string(), cell);
        }
        assert_eq!(cache.len(), 2);

        // Evicting the default connection should be ignored.
        assert!(!cache.evict("spanner.googleapis.com:443"));
        assert_eq!(cache.len(), 2);

        // Evicting a normal tablet connection should succeed.
        assert!(cache.evict("10.0.0.1:15000"));
        assert_eq!(cache.len(), 1);
        assert!(cache.get_if_present("10.0.0.1:15000").is_none());
    }

    #[test]
    fn test_connection_cache_clear_preserves_default() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_conn);

        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            let cell1 = Arc::new(tokio::sync::OnceCell::new());
            let _ = cell1.set(create_test_connection("10.0.0.1:15000"));
            guard.insert("10.0.0.1:15000".to_string(), cell1);

            let cell2 = Arc::new(tokio::sync::OnceCell::new());
            let _ = cell2.set(create_test_connection("10.0.0.2:15000"));
            guard.insert("10.0.0.2:15000".to_string(), cell2);
        }
        assert_eq!(cache.len(), 3);

        cache.clear();
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.default_connection().address(),
            "spanner.googleapis.com:443"
        );
        assert!(cache.get_if_present("spanner.googleapis.com:443").is_some());
        assert!(cache.get_if_present("10.0.0.1:15000").is_none());
        assert!(cache.get_if_present("10.0.0.2:15000").is_none());
    }

    #[test]
    fn test_connection_cache_concurrent_access() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_conn);
        let worker_count = 10;
        let iterations = 100;
        let barrier = Barrier::new(worker_count);

        thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..iterations {
                        assert!(cache.get_if_present("spanner.googleapis.com:443").is_some());
                        assert!(!cache.evict("spanner.googleapis.com:443"));
                    }
                });
            }
        });

        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn test_connection_cache_get_cached() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_conn);
        let config = ClientConfig::default();

        let ep = cache
            .get("spanner.googleapis.com:443", &config)
            .await
            .expect("cached default connection");
        assert_eq!(ep.address(), "spanner.googleapis.com:443");
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn test_connection_cache_concurrent_get_stampede_prevention() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = Arc::new(ConnectionCache::new(default_conn));
        let config = ClientConfig::default();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let config_clone = config.clone();
            handles.push(tokio::spawn(async move {
                cache_clone
                    .get("http://10.0.0.1:15000", &config_clone)
                    .await
                    .expect("should obtain connection")
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.expect("task should complete"));
        }

        for conn in &results {
            assert_eq!(conn.address(), "http://10.0.0.1:15000");
        }
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_connection_cache_uninitialized_cell_not_counted_in_len() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_conn);
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            guard.insert(
                "10.0.0.1:15000".to_string(),
                Arc::new(tokio::sync::OnceCell::new()),
            );
        }

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        assert!(cache.get_if_present("10.0.0.1:15000").is_none());
    }

    #[test]
    fn test_connection_cache_evict_uninitialized_cell() {
        let default_conn = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_conn);

        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            guard.insert(
                "10.0.0.1:15000".to_string(),
                Arc::new(tokio::sync::OnceCell::new()),
            );
        }

        assert_eq!(cache.len(), 1);
        assert!(cache.evict("10.0.0.1:15000"));
        assert_eq!(cache.len(), 1);
        assert!(cache.get_if_present("10.0.0.1:15000").is_none());
    }
}
