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

use crate::ClientBuilderResult;
use crate::client::Channel;
use crate::omni::{InstanceType, TlsConfig};
use crate::routing::server_connection::ServerConnection;
use gaxi::grpc::tonic::transport::ClientTlsConfig;
use gaxi::options::ClientConfig;
use google_cloud_gax::client_builder::Extensions;
use http::HeaderMap;
use http::uri::Scheme;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use tokio::sync::OnceCell;
use url::{Host, Url};

const SCHEME_HTTP: &str = "http";
const SCHEME_HTTPS: &str = "https";

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
    default_key: String,
    servers: RwLock<HashMap<String, Arc<OnceCell<ServerConnection>>>>,
}

impl ConnectionCache {
    /// Creates a new `ConnectionCache` with the specified default fallback connection.
    pub(crate) fn new(default_connection: ServerConnection) -> Self {
        let default_cell = Arc::new(OnceCell::from(default_connection.clone()));
        let mut cache = Self {
            default_connection,
            default_key: String::new(),
            servers: RwLock::new(HashMap::new()),
        };
        let key = cache.cache_key(cache.default_connection.address());
        cache.default_key = key.clone();
        cache
            .servers
            .get_mut()
            .expect("uncontended lock on initialization")
            .insert(key, default_cell);
        cache
    }

    /// Returns a reference to the default fallback server connection.
    pub(crate) fn default_connection(&self) -> &ServerConnection {
        &self.default_connection
    }

    /// Returns whether the default connection uses plaintext (HTTP).
    fn default_is_plaintext(&self) -> bool {
        if let Some(scheme) = endpoint_scheme(self.default_connection.address()) {
            return scheme == Scheme::HTTP;
        }
        is_loopback_address(self.default_connection.address())
    }

    /// Returns the effective scheme (`"http"` or `"https"`) for an address in the context of this cache.
    fn effective_scheme(&self, address: &str) -> &'static str {
        match endpoint_scheme(address) {
            Some(scheme) if scheme == Scheme::HTTP => SCHEME_HTTP,
            Some(_) => SCHEME_HTTPS,
            None if self.default_is_plaintext() => SCHEME_HTTP,
            None => SCHEME_HTTPS,
        }
    }

    /// Derives the canonical cache lookup key for an address, including the effective scheme
    /// and lowercase normalized host:port.
    fn cache_key(&self, address: &str) -> String {
        let scheme = self.effective_scheme(address);
        let trimmed = address.trim();
        let Some(url) = parse_endpoint_url(address, scheme) else {
            return format!("{scheme}://{}", trimmed.to_ascii_lowercase());
        };
        let Some(host) = url.host_str() else {
            return format!("{scheme}://{}", trimmed.to_ascii_lowercase());
        };
        if let Some(port) = url.port_or_known_default() {
            return format!("{scheme}://{}:{port}", host.to_ascii_lowercase());
        }
        format!("{scheme}://{}", host.to_ascii_lowercase())
    }

    /// Returns whether the given address matches the default connection address.
    pub(crate) fn is_default_address(&self, address: &str) -> bool {
        if address == self.default_connection.address() {
            return true;
        }
        self.default_key == self.cache_key(address)
    }

    /// Returns a cached connection for the given address without creating it if missing.
    ///
    /// This method is used by location-aware routing to avoid foreground connection creation on the
    /// hot RPC request path.
    pub(crate) fn get_if_present(&self, address: &str) -> Option<ServerConnection> {
        if self.is_default_address(address) {
            return Some(self.default_connection.clone());
        }
        let key = self.cache_key(address);
        let guard = self
            .servers
            .read()
            .expect("connection cache read lock poisoned");
        guard.get(&key).and_then(|cell| cell.get().cloned())
    }

    /// Returns a cached connection for the given address, creating and caching a new connection
    /// asynchronously if needed.
    pub(crate) async fn get(
        &self,
        address: &str,
        config: &ClientConfig,
    ) -> ClientBuilderResult<ServerConnection> {
        if self.is_default_address(address) {
            return Ok(self.default_connection.clone());
        }

        let key = self.cache_key(address);
        let cell = {
            let guard = self
                .servers
                .read()
                .expect("connection cache read lock poisoned");
            if let Some(cell) = guard.get(&key) {
                if let Some(connection) = cell.get() {
                    return Ok(connection.clone());
                }
                Arc::clone(cell)
            } else {
                drop(guard);
                let mut guard = self
                    .servers
                    .write()
                    .expect("connection cache write lock poisoned");
                Arc::clone(
                    guard
                        .entry(key)
                        .or_insert_with(|| Arc::new(OnceCell::new())),
                )
            }
        };

        cell.get_or_try_init(|| async {
            let endpoint_config =
                prepare_routed_endpoint_config(config, self.default_connection.address(), address);
            let channel = Channel::create(&endpoint_config, 0).await?;
            Ok(ServerConnection::new(address.to_string(), channel))
        })
        .await
        .cloned()
    }

    /// Evicts a server connection from the cache.
    ///
    /// If `address` matches the default connection's address, this method does nothing and returns
    /// `false`. Otherwise, returns `true` if a connection was removed from the cache.
    pub(crate) fn evict(&self, address: &str) -> bool {
        if self.is_default_address(address) {
            return false;
        }
        let key = self.cache_key(address);
        let mut guard = self
            .servers
            .write()
            .expect("connection cache write lock poisoned");
        guard.remove(&key).is_some()
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
        self.len() == 0
    }

    /// Clears all cached server connections while preserving the default fallback connection.
    pub(crate) fn clear(&self) {
        let mut guard = self
            .servers
            .write()
            .expect("connection cache write lock poisoned");
        guard.retain(|key, _| key == &self.default_key);
    }
}

/// Parses an endpoint address into a [`Url`], supplying `default_scheme` if none is present
/// and handling bracketed or bare IPv6 addresses.
fn parse_endpoint_url(address: &str, default_scheme: &str) -> Option<Url> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(IpAddr::V6(ipv6)) = trimmed.parse::<IpAddr>() {
        return Url::parse(&format!("{default_scheme}://[{ipv6}]")).ok();
    }
    let url = if trimmed.contains("://") {
        Url::parse(trimmed).ok()?
    } else {
        Url::parse(&format!("{default_scheme}://{trimmed}")).ok()?
    };
    url.has_host().then_some(url)
}

/// Extracts the hostname (domain or IP address) from an endpoint address.
///
/// Supports addresses with or without schemes, as well as bracketed or bare IPv6 addresses:
/// - `"spanner.googleapis.com:443"` -> `"spanner.googleapis.com"`
/// - `"https://omni.spanner.internal:8443"` -> `"omni.spanner.internal"`
/// - `"[::1]:8443"` -> `"::1"`
/// - `"::1"` -> `"::1"`
/// - `"2001:db8::1"` -> `"2001:db8::1"`
fn extract_host(address: &str) -> Option<String> {
    let url = parse_endpoint_url(address, SCHEME_HTTP)?;
    match url.host()? {
        Host::Domain(domain) => Some(domain.to_ascii_lowercase()),
        Host::Ipv4(ip) => Some(ip.to_string()),
        Host::Ipv6(ip) => Some(ip.to_string()),
    }
}

/// Returns the scheme (e.g. `http` or `https`) if explicitly specified in the endpoint string.
fn endpoint_scheme(endpoint: &str) -> Option<Scheme> {
    let url = Url::parse(endpoint.trim()).ok()?;
    if !url.has_host() {
        return None;
    }
    match url.scheme() {
        SCHEME_HTTP => Some(Scheme::HTTP),
        SCHEME_HTTPS => Some(Scheme::HTTPS),
        _ => None,
    }
}

/// Constructs the dial URI string for Tonic using the appropriate scheme.
fn dial_endpoint(routed_address: &str, is_plaintext: bool) -> String {
    let scheme = if is_plaintext {
        SCHEME_HTTP
    } else {
        SCHEME_HTTPS
    };
    let trimmed = routed_address.trim();
    if let Some((_existing_scheme, authority_and_path)) = trimmed.split_once("://") {
        return format!("{scheme}://{authority_and_path}");
    }
    format!("{scheme}://{trimmed}")
}

/// Returns whether an address points to a local loopback host (e.g. emulator or local test server).
fn is_loopback_address(address: &str) -> bool {
    let Some(host) = extract_host(address) else {
        return false;
    };
    let host = host.trim_end_matches('.');
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Prepares a [`ClientConfig`] for dialing a routed tablet server endpoint.
///
/// Ensures that:
/// 1. The routed endpoint URI has an appropriate scheme (`https://` or `http://`), which Tonic requires.
/// 2. TLS certificate verification and SNI match the default service host when TLS is enabled.
/// 3. Any parent TLS configuration is stripped when dialing a plaintext endpoint.
pub(crate) fn prepare_routed_endpoint_config(
    config: &ClientConfig,
    default_address: &str,
    routed_address: &str,
) -> ClientConfig {
    let mut endpoint_config = config.clone();

    // Determine whether this connection is plaintext or TLS.
    // Plaintext is used if the routed address explicitly requests `http://`,
    // or if the default connection or client config specifies plaintext and no TLS is configured.
    let is_plaintext = match endpoint_scheme(routed_address) {
        Some(scheme) => scheme == Scheme::HTTP,
        None => {
            let is_http = |endpoint: &str| {
                endpoint_scheme(endpoint).is_some_and(|scheme| scheme == Scheme::HTTP)
            };
            let no_tls_configured = config.extensions.get::<TlsConfig>().is_none()
                && config.extensions.get::<ClientTlsConfig>().is_none();

            is_http(default_address)
                || config.endpoint.as_deref().is_some_and(is_http)
                || (no_tls_configured
                    && (is_loopback_address(default_address)
                        || config.endpoint.as_deref().is_some_and(is_loopback_address)))
        }
    };

    endpoint_config.endpoint = Some(dial_endpoint(routed_address, is_plaintext));

    if is_plaintext {
        // Strip any TLS configuration when dialing a plaintext endpoint to prevent GAX from
        // rejecting the connection with "cannot configure TLS on non-HTTPS endpoint".
        // Note: Extensions does not expose a remove::<T>() method, so non-TLS extensions
        // used by the Spanner client (InstanceType and HeaderMap) are selectively retained.
        let mut extensions = Extensions::new();
        if let Some(instance_type) = config.extensions.get::<InstanceType>() {
            extensions.insert(*instance_type);
        }
        if let Some(headers) = config.extensions.get::<HeaderMap>() {
            extensions.insert(headers.clone());
        }
        endpoint_config.extensions = extensions;
        return endpoint_config;
    }

    let default_domain =
        extract_host(default_address).filter(|host| host.parse::<IpAddr>().is_err());

    let tonic_tls = if let Some(omni_tls) = config.extensions.get::<TlsConfig>() {
        let mut omni_tls = omni_tls.clone();
        if omni_tls.domain_name_override().is_none()
            && let Some(domain) = default_domain.as_deref()
        {
            omni_tls = omni_tls.with_domain_name_override(domain);
        }
        let tonic_tls = omni_tls.to_tonic_client_tls_config();
        endpoint_config.extensions.insert(omni_tls);
        tonic_tls
    } else if let Some(existing_tonic_tls) = config.extensions.get::<ClientTlsConfig>() {
        Some(existing_tonic_tls.clone())
    } else if let Some(domain) = default_domain {
        let omni_tls = TlsConfig::new().with_domain_name_override(domain);
        let tonic_tls = omni_tls.to_tonic_client_tls_config();
        endpoint_config.extensions.insert(omni_tls);
        tonic_tls
    } else {
        None
    };

    if let Some(tonic_tls) = tonic_tls {
        endpoint_config.extensions.insert(tonic_tls);
    }

    endpoint_config
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{AUTHORIZATION, HeaderValue};
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
    fn traits() {
        static_assertions::assert_impl_all!(ConnectionCache: Send, Sync, std::fmt::Debug);
    }

    #[test]
    fn connection_cache_default_connection_and_get_if_present() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection.clone());

        assert_eq!(
            cache.len(),
            1,
            "cache should contain only default connection on creation"
        );
        assert!(!cache.is_empty(), "cache should not be empty");
        assert_eq!(
            cache.default_connection().address(),
            "spanner.googleapis.com:443",
            "default connection address should match"
        );
        assert!(
            cache.is_default_address("spanner.googleapis.com:443"),
            "must return true for default address"
        );
        assert!(
            !cache.is_default_address("10.0.0.1:15000"),
            "must return false for non-default address"
        );

        let cached_default = cache
            .get_if_present("spanner.googleapis.com:443")
            .expect("default connection should be in cache");
        assert_eq!(
            cached_default.address(),
            "spanner.googleapis.com:443",
            "cached connection address should match"
        );

        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "un-cached tablet connection should return None"
        );
    }

    #[test]
    fn connection_cache_eviction_and_protection_of_default() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);

        // Manually insert a tablet connection into the cache for testing eviction.
        let tablet_connection = create_test_connection("10.0.0.1:15000");
        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            let cell = Arc::new(OnceCell::new());
            let _ = cell.set(tablet_connection.clone());
            guard.insert(cache.cache_key(tablet_connection.address()), cell);
        }
        assert_eq!(
            cache.len(),
            2,
            "cache should contain default and tablet connections"
        );

        // Evicting the default connection should be ignored.
        assert!(
            !cache.evict("spanner.googleapis.com:443"),
            "evicting default connection should return false"
        );
        assert_eq!(
            cache.len(),
            2,
            "default connection must not be evicted from cache"
        );

        // Evicting a normal tablet connection should succeed.
        assert!(
            cache.evict("10.0.0.1:15000"),
            "evicting tablet connection should return true"
        );
        assert_eq!(
            cache.len(),
            1,
            "cache should only retain default connection after tablet eviction"
        );
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "evicted tablet connection should not be found in cache"
        );

        // Evicting an address not present in the cache should return false.
        assert!(
            !cache.evict("unknown:15000"),
            "evicting address not present in cache must return false"
        );
    }

    #[test]
    fn connection_cache_clear_preserves_default() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);

        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            let cell1 = Arc::new(OnceCell::new());
            let _ = cell1.set(create_test_connection("10.0.0.1:15000"));
            guard.insert(cache.cache_key("10.0.0.1:15000"), cell1);

            let cell2 = Arc::new(OnceCell::new());
            let _ = cell2.set(create_test_connection("10.0.0.2:15000"));
            guard.insert(cache.cache_key("10.0.0.2:15000"), cell2);
        }
        assert_eq!(
            cache.len(),
            3,
            "cache should contain default and 2 tablet connections"
        );

        cache.clear();
        assert_eq!(
            cache.len(),
            1,
            "cache clear must retain only default connection"
        );
        assert_eq!(
            cache.default_connection().address(),
            "spanner.googleapis.com:443",
            "default connection address must remain preserved"
        );
        assert!(
            cache.get_if_present("spanner.googleapis.com:443").is_some(),
            "default connection should still be present in cache"
        );
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "first tablet connection should be cleared from cache"
        );
        assert!(
            cache.get_if_present("10.0.0.2:15000").is_none(),
            "second tablet connection should be cleared from cache"
        );
    }

    #[test]
    fn connection_cache_concurrent_access() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);
        let worker_count = 10;
        let iterations = 100;
        let barrier = Barrier::new(worker_count);

        thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..iterations {
                        assert!(
                            cache.get_if_present("spanner.googleapis.com:443").is_some(),
                            "concurrent lookup for default connection must succeed"
                        );
                        assert!(
                            !cache.evict("spanner.googleapis.com:443"),
                            "concurrent eviction of default connection must return false"
                        );
                    }
                });
            }
        });

        assert_eq!(
            cache.len(),
            1,
            "cache length must remain 1 after concurrent lookups"
        );
    }

    #[tokio::test]
    async fn connection_cache_get_cached() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);
        let config = ClientConfig::default();

        let connection = cache
            .get("spanner.googleapis.com:443", &config)
            .await
            .expect("cached default connection");
        assert_eq!(
            connection.address(),
            "spanner.googleapis.com:443",
            "retrieved connection address must match default"
        );
        assert_eq!(
            cache.len(),
            1,
            "cache length should remain 1 after get of default connection"
        );
    }

    #[tokio::test]
    async fn connection_cache_concurrent_get_stampede_prevention() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = Arc::new(ConnectionCache::new(default_connection));
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

        for connection in &results {
            assert_eq!(
                connection.address(),
                "http://10.0.0.1:15000",
                "connection address must match requested endpoint"
            );
        }
        assert_eq!(
            cache.len(),
            2,
            "cache length must be 2 after stampede prevention (default + 1 tablet)"
        );
    }

    #[test]
    fn connection_cache_uninitialized_cell_not_counted_in_len() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);
        assert_eq!(cache.len(), 1, "cache should initially have 1 connection");
        assert!(!cache.is_empty(), "cache should not be empty");

        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            guard.insert(cache.cache_key("10.0.0.1:15000"), Arc::new(OnceCell::new()));
        }

        assert_eq!(
            cache.len(),
            1,
            "uninitialized OnceCell must not be counted in cache length"
        );
        assert!(!cache.is_empty(), "cache should not be empty");
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "uninitialized OnceCell must not be returned by get_if_present"
        );
    }

    #[test]
    fn connection_cache_evict_uninitialized_cell() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);

        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            guard.insert(cache.cache_key("10.0.0.1:15000"), Arc::new(OnceCell::new()));
        }

        assert_eq!(
            cache.len(),
            1,
            "cache length should be 1 before evicting uninitialized cell"
        );
        assert!(
            cache.evict("10.0.0.1:15000"),
            "evict should return true when removing uninitialized cell"
        );
        assert_eq!(
            cache.len(),
            1,
            "cache length should remain 1 after evicting uninitialized cell"
        );
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "evicted uninitialized cell should not be present"
        );
    }

    #[test]
    fn extract_host() {
        assert_eq!(
            super::extract_host("spanner.googleapis.com:443"),
            Some("spanner.googleapis.com".to_string()),
            "should extract host by stripping port"
        );
        assert_eq!(
            super::extract_host("SPANNER.GOOGLEAPIS.COM:443"),
            Some("spanner.googleapis.com".to_string()),
            "should lowercase extracted domain host"
        );
        assert_eq!(
            super::extract_host("spanner.googleapis.com"),
            Some("spanner.googleapis.com".to_string()),
            "host without port should remain unchanged"
        );
        assert_eq!(
            super::extract_host("https://spanner.googleapis.com:443/"),
            Some("spanner.googleapis.com".to_string()),
            "should strip scheme and port"
        );
        assert_eq!(
            super::extract_host("https://spanner.googleapis.com:443/some/path?query=1#frag"),
            Some("spanner.googleapis.com".to_string()),
            "should ignore path, query and fragment"
        );
        assert_eq!(
            super::extract_host("[::1]:8443"),
            Some("::1".to_string()),
            "should extract IPv6 address from bracketed host:port"
        );
        assert_eq!(
            super::extract_host("[::1]"),
            Some("::1".to_string()),
            "should extract IPv6 address from bracketed host without port"
        );
        assert_eq!(
            super::extract_host("::1"),
            Some("::1".to_string()),
            "should extract bare IPv6 address without port or brackets"
        );
        assert_eq!(
            super::extract_host("2001:db8::1"),
            Some("2001:db8::1".to_string()),
            "should extract unbracketed IPv6 address"
        );
        assert_eq!(
            super::extract_host("http://[2001:db8::1]:8443"),
            Some("2001:db8::1".to_string()),
            "should extract full IPv6 address from URI"
        );
        assert_eq!(
            super::extract_host("omni.spanner.internal/"),
            Some("omni.spanner.internal".to_string()),
            "should strip trailing slash when extracting host"
        );
        assert_eq!(
            super::extract_host("omni.spanner.internal/path"),
            Some("omni.spanner.internal".to_string()),
            "should strip path when extracting host"
        );
        assert_eq!(
            super::extract_host(""),
            None,
            "empty address should return None"
        );
        assert_eq!(
            super::extract_host("   "),
            None,
            "whitespace address should return None"
        );
        assert_eq!(
            super::extract_host("10.0.0.1:8443"),
            Some("10.0.0.1".to_string()),
            "should extract IPv4 host from host:port"
        );
        assert_eq!(
            super::extract_host("10.0.0.1"),
            Some("10.0.0.1".to_string()),
            "should extract bare IPv4 host"
        );
        assert_eq!(
            super::extract_host(":8080"),
            None,
            "port without host should return None"
        );
        assert_eq!(
            super::extract_host("http://"),
            None,
            "scheme without host should return None"
        );
        assert_eq!(
            super::extract_host("http://:8080"),
            None,
            "scheme with empty host should return None"
        );
    }

    #[test]
    fn is_loopback_address() {
        assert!(
            super::is_loopback_address("localhost:9010"),
            "localhost with port must be loopback"
        );
        assert!(
            super::is_loopback_address("LOCALHOST:9010"),
            "uppercase localhost must be loopback"
        );
        assert!(
            super::is_loopback_address("http://localhost:9010/"),
            "localhost URI must be loopback"
        );
        assert!(
            super::is_loopback_address("127.0.0.1:9010"),
            "127.0.0.1 must be loopback"
        );
        assert!(
            super::is_loopback_address("127.0.0.2:9010"),
            "127.0.0.2 in loopback subnet must be loopback"
        );
        assert!(
            super::is_loopback_address("[::1]:9010"),
            "bracketed IPv6 ::1 must be loopback"
        );
        assert!(
            super::is_loopback_address("::1"),
            "bare IPv6 ::1 must be loopback"
        );
        assert!(
            super::is_loopback_address("test.localhost:8080"),
            "subdomain of .localhost must be loopback"
        );
        assert!(
            super::is_loopback_address("localhost.:9010"),
            "localhost with trailing dot must be loopback"
        );
        assert!(
            super::is_loopback_address("test.localhost.:8080"),
            "subdomain of .localhost with trailing dot must be loopback"
        );
        assert!(
            !super::is_loopback_address("localhost.example.com:9010"),
            "domain ending with example.com must not be loopback"
        );
        assert!(
            !super::is_loopback_address("notlocalhost:9010"),
            "notlocalhost must not be loopback"
        );
        assert!(
            !super::is_loopback_address("spanner.googleapis.com:443"),
            "spanner remote endpoint must not be loopback"
        );
        assert!(
            !super::is_loopback_address("10.0.0.1:15000"),
            "private IP must not be loopback"
        );
        assert!(
            !super::is_loopback_address(""),
            "empty address must not be loopback"
        );
        assert!(
            !super::is_loopback_address(":8080"),
            "port without host must not be loopback"
        );
    }

    #[test]
    fn is_default_address() {
        let default_connection = create_test_connection("https://omni.example.com:8443/");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            cache.is_default_address("https://omni.example.com:8443/"),
            "should match exact default connection address"
        );
        assert!(
            cache.is_default_address("  https://omni.example.com:8443/  "),
            "should match address with leading and trailing whitespace"
        );
        assert!(
            cache.is_default_address("omni.example.com:8443"),
            "should match normalized address without scheme and trailing slash"
        );
        assert!(
            cache.is_default_address("OMNI.EXAMPLE.COM:8443"),
            "should match case-insensitively"
        );
        assert!(
            !cache.is_default_address("http://omni.example.com:8443/"),
            "should not match when scheme differs (http vs https)"
        );
        assert!(
            !cache.is_default_address("10.0.0.1:8443"),
            "should not match different tablet address"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_standard_tls() {
        let config = ClientConfig::default();
        let default_address = "omni.spanner.internal:8443";
        let routed_address = "10.0.0.1:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.1:8443"),
            "routed endpoint dial URI must use https scheme"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must be inserted for TLS hostname verification"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_omni_mtls() {
        let mut config = ClientConfig::default();
        let omni_tls = TlsConfig::new()
            .with_root_certificate_pem(b"test ca pem")
            .with_client_certificate_pem(b"test client cert", b"test client key");
        config.extensions.insert(omni_tls);

        let default_address = "omni.spanner.internal:8443";
        let routed_address = "10.0.0.2:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.2:8443"),
            "routed endpoint dial URI must use https scheme"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must be present in extensions for mTLS with custom root CA"
        );

        let updated_omni_tls = endpoint_config
            .extensions
            .get::<TlsConfig>()
            .expect("TlsConfig must be retained in extensions");
        assert_eq!(
            updated_omni_tls.domain_name_override(),
            Some("omni.spanner.internal"),
            "domain name override must be derived from default endpoint host and carried over"
        );
        assert_eq!(
            updated_omni_tls.client_certificate(),
            Some(b"test client cert".as_slice()),
            "client certificate must be preserved in carried over TlsConfig"
        );
        assert_eq!(
            updated_omni_tls.client_private_key(),
            Some(b"test client key".as_slice()),
            "client private key must be preserved in carried over TlsConfig"
        );
        assert_eq!(
            updated_omni_tls.root_certificate(),
            Some(b"test ca pem".as_slice()),
            "root certificate CA must be preserved in carried over TlsConfig"
        );
    }

    const SAMPLE_CA_PEM: &[u8] = include_bytes!("../../../../testdata/tls/ca_cert.pem");
    const SAMPLE_CERT_PEM: &[u8] = include_bytes!("../../../../testdata/tls/server_cert.pem");
    const SAMPLE_KEY_PEM: &[u8] = include_bytes!("../../../../testdata/tls/server_key.pem");

    #[tokio::test]
    async fn connection_cache_get_creates_and_caches_routed_endpoint_with_omni_mtls() {
        let default_connection = create_test_connection("https://omni.spanner.internal:8443");
        let cache = ConnectionCache::new(default_connection);

        let mut config = ClientConfig::default();
        let omni_tls = TlsConfig::new()
            .with_root_certificate_pem(SAMPLE_CA_PEM)
            .with_client_certificate_pem(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM);
        config.extensions.insert(omni_tls);

        let routed_address = "10.0.0.2:8443";
        let connection = cache
            .get(routed_address, &config)
            .await
            .expect("cache.get should succeed constructing routed connection with mTLS");

        assert_eq!(
            connection.address(),
            routed_address,
            "created connection address must match requested routed address"
        );
        assert_eq!(
            cache.len(),
            2,
            "cache should now contain default connection and the newly created routed connection"
        );
        assert!(
            cache.get_if_present(routed_address).is_some(),
            "get_if_present should return the newly created connection"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_omni_mtls_client_identity_without_ca() {
        let mut config = ClientConfig::default();
        let omni_tls =
            TlsConfig::new().with_client_certificate_pem(b"test client cert", b"test client key");
        config.extensions.insert(omni_tls);

        let default_address = "omni.spanner.internal:8443";
        let routed_address = "10.0.0.2:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.2:8443"),
            "routed endpoint dial URI must use https scheme"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must be present in extensions for mTLS with system roots"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_omni_tls_existing_domain_override() {
        let mut config = ClientConfig::default();
        let omni_tls = TlsConfig::new()
            .with_root_certificate_pem(b"test ca pem")
            .with_domain_name_override("custom.spanner.domain");
        config.extensions.insert(omni_tls);

        let default_address = "omni.spanner.internal:8443";
        let routed_address = "10.0.0.2:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.2:8443"),
            "routed endpoint dial URI must use https scheme"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must be present in extensions"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_existing_client_tls_config() {
        let mut config = ClientConfig::default();
        config
            .extensions
            .insert(ClientTlsConfig::new().with_enabled_roots());

        let default_address = "omni.spanner.internal:8443";
        let routed_address = "10.0.0.3:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.3:8443"),
            "routed endpoint dial URI must use https scheme"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must be preserved in extensions"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_plaintext_strips_parent_tls() {
        let mut config = ClientConfig::default();
        let omni_tls = TlsConfig::new().with_root_certificate_pem(b"test ca pem");
        config.extensions.insert(omni_tls);
        config
            .extensions
            .insert(ClientTlsConfig::new().with_enabled_roots());

        let default_address = "omni.spanner.internal:8443";
        let routed_address = "http://10.0.0.1:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("http://10.0.0.1:8443"),
            "routed endpoint dial URI must use http scheme"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_none(),
            "ClientTlsConfig must be stripped from extensions for plaintext endpoint"
        );
        assert!(
            endpoint_config.extensions.get::<TlsConfig>().is_none(),
            "TlsConfig must be stripped from extensions for plaintext endpoint"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_plaintext_preserves_non_tls_extensions() {
        let mut config = ClientConfig::default();
        config.extensions.insert(InstanceType::Omni);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer test-token"));
        config.extensions.insert(headers);
        config
            .extensions
            .insert(TlsConfig::new().with_root_certificate_pem(b"test ca"));
        config
            .extensions
            .insert(ClientTlsConfig::new().with_enabled_roots());

        let default_address = "omni.spanner.internal:8443";
        let routed_address = "http://10.0.0.1:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("http://10.0.0.1:8443"),
            "routed endpoint dial URI must use http scheme"
        );
        assert_eq!(
            endpoint_config.extensions.get::<InstanceType>(),
            Some(&InstanceType::Omni),
            "InstanceType extension must be preserved when dialing plaintext"
        );
        assert!(
            endpoint_config.extensions.get::<HeaderMap>().is_some(),
            "HeaderMap extension must be preserved when dialing plaintext"
        );
        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_none(),
            "ClientTlsConfig must be stripped from extensions for plaintext endpoint"
        );
        assert!(
            endpoint_config.extensions.get::<TlsConfig>().is_none(),
            "TlsConfig must be stripped from extensions for plaintext endpoint"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_schemeless_localhost_plaintext() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("localhost:9010".to_string());

        let default_address = "localhost:9010";
        let routed_address = "127.0.0.1:9010";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("http://127.0.0.1:9010"),
            "schemeless localhost endpoint must dial over http"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_none(),
            "ClientTlsConfig must NOT be present for schemeless localhost endpoint"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_schemeless_ipv6_loopback_plaintext() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("[::1]:9010".to_string());

        let default_address = "[::1]:9010";
        let routed_address = "[::1]:9011";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("http://[::1]:9011"),
            "schemeless IPv6 loopback endpoint must dial over http"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_none(),
            "ClientTlsConfig must NOT be present for schemeless IPv6 loopback endpoint"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_localhost_with_tls_not_plaintext() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("localhost:9010".to_string());
        config
            .extensions
            .insert(TlsConfig::new().with_root_certificate_pem(b"test ca pem"));

        let default_address = "localhost:9010";
        let routed_address = "127.0.0.1:9010";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://127.0.0.1:9010"),
            "endpoint must dial over https when TLS is explicitly configured on loopback"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must be present when TLS is explicitly configured"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_tls_preserved_when_default_host_unextractable() {
        let mut config = ClientConfig::default();
        config
            .extensions
            .insert(TlsConfig::new().with_root_certificate_pem(b"test ca pem"));

        let default_address = "";
        let routed_address = "10.0.0.1:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.1:8443"),
            "endpoint must dial over https"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must not be dropped when default host is unextractable"
        );
    }

    #[tokio::test]
    async fn connection_cache_get_default_address_bypasses_init() {
        let default_connection = create_test_connection("https://omni.spanner.internal:8443/");
        let cache = ConnectionCache::new(default_connection.clone());
        let config = ClientConfig::default();

        let connection = cache
            .get("omni.spanner.internal:8443", &config)
            .await
            .expect("should obtain default connection");

        assert_eq!(
            connection.address(),
            "https://omni.spanner.internal:8443/",
            "address must match default connection"
        );
        assert_eq!(
            cache.len(),
            1,
            "cache length should remain 1 without adding duplicates"
        );
        assert!(
            cache.get_if_present("omni.spanner.internal:8443").is_some(),
            "get_if_present should match default connection with normalized address"
        );
    }

    #[test]
    fn connection_cache_get_if_present_default_address_normalization() {
        let default_connection = create_test_connection("https://omni.spanner.internal:8443/");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            cache
                .get_if_present("https://omni.spanner.internal:8443/")
                .is_some(),
            "should match exact default address"
        );
        assert!(
            cache.get_if_present("omni.spanner.internal:8443").is_some(),
            "should match default address without scheme and trailing slash"
        );
        assert!(
            cache
                .get_if_present("HTTPS://OMNI.SPANNER.INTERNAL:8443/")
                .is_some(),
            "should match default address with uppercase scheme and domain"
        );
        assert!(
            cache
                .get_if_present("http://omni.spanner.internal:8443/")
                .is_none(),
            "should not match default address when scheme differs"
        );
    }

    #[test]
    fn connection_cache_evict_default_address_protection_with_normalization() {
        let default_connection = create_test_connection("https://omni.spanner.internal:8443/");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            !cache.evict("omni.spanner.internal:8443"),
            "evicting normalized default address must return false"
        );
        assert!(
            !cache.evict("HTTPS://OMNI.SPANNER.INTERNAL:8443/"),
            "evicting uppercase default address must return false"
        );
        assert_eq!(
            cache.len(),
            1,
            "default connection must not be evicted from cache"
        );
        assert!(
            cache.get_if_present("omni.spanner.internal:8443").is_some(),
            "default connection should still be present in cache"
        );
    }

    #[test]
    fn connection_cache_tablet_lookup_and_evict_normalization() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);

        let tablet_connection = create_test_connection("https://Tablet.Omni.Internal:8443/");
        {
            let mut guard = cache.servers.write().expect("write lock poisoned");
            let cell = Arc::new(OnceCell::new());
            let _ = cell.set(tablet_connection.clone());
            guard.insert(cache.cache_key(tablet_connection.address()), cell);
        }

        assert!(
            cache.get_if_present("tablet.omni.internal:8443").is_some(),
            "get_if_present should find tablet connection with lowercase address without scheme"
        );
        assert!(
            cache
                .get_if_present("HTTPS://TABLET.OMNI.INTERNAL:8443/")
                .is_some(),
            "get_if_present should find tablet connection with uppercase address with scheme"
        );
        assert!(
            cache
                .get_if_present("http://tablet.omni.internal:8443")
                .is_none(),
            "get_if_present should not find tablet connection when scheme differs (http vs https)"
        );

        assert!(
            cache.evict("tablet.omni.internal:8443"),
            "evict should succeed using normalized address"
        );
        assert_eq!(
            cache.len(),
            1,
            "cache should only contain default connection after evicting tablet"
        );
        assert!(
            cache.get_if_present("tablet.omni.internal:8443").is_none(),
            "tablet connection should no longer be present in cache"
        );
    }

    #[tokio::test]
    async fn connection_cache_get_tablet_prevents_duplicate_connections() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);
        let config = ClientConfig::default();

        let connection1 = cache
            .get("10.0.0.1:15000", &config)
            .await
            .expect("should obtain tablet connection");
        let connection2 = cache
            .get("https://10.0.0.1:15000", &config)
            .await
            .expect("should obtain existing tablet connection with scheme");

        assert_eq!(
            connection1.address(),
            "10.0.0.1:15000",
            "first connection retains initial address"
        );
        assert_eq!(
            cache.len(),
            2,
            "cache length must be 2 (default + 1 tablet) without duplicates"
        );
        assert_eq!(
            connection1.address(),
            connection2.address(),
            "repeated get with normalized address returns the same cached connection"
        );
    }

    #[tokio::test]
    async fn connection_cache_get_tablet_different_scheme_creates_separate_connections() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);
        let config = ClientConfig::default();

        let tls_connection = cache
            .get("https://10.0.0.1:15000", &config)
            .await
            .expect("should obtain TLS tablet connection");
        let plaintext_connection = cache
            .get("http://10.0.0.1:15000", &config)
            .await
            .expect("should obtain plaintext tablet connection");

        assert_eq!(
            tls_connection.address(),
            "https://10.0.0.1:15000",
            "TLS connection address matches request"
        );
        assert_eq!(
            plaintext_connection.address(),
            "http://10.0.0.1:15000",
            "plaintext connection address matches request"
        );
        assert_eq!(
            cache.len(),
            3,
            "cache length must be 3 (default + TLS tablet + plaintext tablet)"
        );
    }

    #[test]
    fn connection_cache_schemeless_default_rejects_http() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            cache.is_default_address("spanner.googleapis.com:443"),
            "schemeless default address matches itself"
        );
        assert!(
            cache.is_default_address("https://spanner.googleapis.com:443"),
            "default address matches https explicitly"
        );
        assert!(
            !cache.is_default_address("http://spanner.googleapis.com:443"),
            "http scheme must not match TLS default connection"
        );
    }

    #[tokio::test]
    async fn connection_cache_get_channel_create_error_propagates_and_retries() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);
        let config = ClientConfig::default();

        // An invalid address with illegal characters causes Channel::create to fail.
        let invalid_address = "invalid host:not a port";
        let result1 = cache.get(invalid_address, &config).await;
        assert!(
            result1.is_err(),
            "channel creation with invalid address must return an error"
        );
        assert_eq!(
            cache.len(),
            1,
            "failed channel creation must not increase cached connections count"
        );
        assert!(
            cache.get_if_present(invalid_address).is_none(),
            "uninitialized cell must not be returned by get_if_present"
        );

        // Retrying the lookup runs initialization again rather than returning a cached error.
        let result2 = cache.get(invalid_address, &config).await;
        assert!(
            result2.is_err(),
            "retry of failed connection creation should attempt again and propagate the error"
        );
        assert_eq!(
            cache.len(),
            1,
            "cache length must still be 1 after retry failure"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_remote_http_default_propagates_plaintext() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("http://remote.spanner.internal:8080".to_string());

        let default_address = "http://remote.spanner.internal:8080";
        let routed_address = "10.0.0.1:8080";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("http://10.0.0.1:8080"),
            "remote http default endpoint must propagate plaintext http scheme to routed endpoint"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_none(),
            "ClientTlsConfig must NOT be present when default uses http"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_localhost_uppercase_schemeless_plaintext() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("LOCALHOST:9010".to_string());

        let default_address = "LOCALHOST:9010";
        let routed_address = "127.0.0.1:9010";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("http://127.0.0.1:9010"),
            "uppercase LOCALHOST default endpoint must dial routed loopback over http"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_none(),
            "ClientTlsConfig must NOT be present for uppercase LOCALHOST endpoint"
        );
    }

    #[test]
    fn connection_cache_default_port_normalization() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            cache.is_default_address("https://spanner.googleapis.com"),
            "default address with port 443 must match https address without port"
        );
        assert!(
            cache.is_default_address("https://spanner.googleapis.com:443"),
            "default address must match https address with explicit port 443"
        );
        assert!(
            cache
                .get_if_present("https://spanner.googleapis.com")
                .is_some(),
            "get_if_present should find default connection using normalized port 443"
        );
    }

    #[test]
    fn connection_cache_with_plaintext_default_propagates_http() {
        let default_connection = create_test_connection("http://remote.spanner.internal:8080");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            cache.default_is_plaintext(),
            "default connection with explicit http scheme must be detected as plaintext"
        );
        assert_eq!(
            cache.effective_scheme("10.0.0.1:8080"),
            "http",
            "effective scheme for tablet must inherit http from default connection"
        );
        assert_eq!(
            cache.cache_key("10.0.0.1:8080"),
            "http://10.0.0.1:8080",
            "cache key for tablet must use http scheme"
        );
    }

    #[test]
    fn connection_cache_with_loopback_default_propagates_http() {
        let default_connection = create_test_connection("localhost:9010");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            cache.default_is_plaintext(),
            "schemeless localhost default connection must be detected as plaintext"
        );
        assert_eq!(
            cache.effective_scheme("127.0.0.1:9010"),
            "http",
            "effective scheme for tablet must inherit http from loopback default"
        );
        assert_eq!(
            cache.cache_key("127.0.0.1:9010"),
            "http://127.0.0.1:9010",
            "cache key for tablet must use http scheme"
        );
    }

    #[test]
    fn connection_cache_with_loopback_https_default_preserves_tls() {
        let default_connection = create_test_connection("https://localhost:9010");
        let cache = ConnectionCache::new(default_connection);

        assert!(
            !cache.default_is_plaintext(),
            "https localhost default connection must NOT be detected as plaintext"
        );
        assert_eq!(
            cache.effective_scheme("127.0.0.1:9010"),
            "https",
            "effective scheme for tablet must inherit https from https loopback default"
        );
        assert_eq!(
            cache.cache_key("127.0.0.1:9010"),
            "https://127.0.0.1:9010",
            "cache key for tablet must use https scheme"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_ip_default_host_does_not_override_domain() {
        let mut config = ClientConfig::default();
        let omni_tls = TlsConfig::new().with_root_certificate_pem(b"test ca pem");
        config.extensions.insert(omni_tls);

        let default_address = "10.0.0.1:8443";
        let routed_address = "10.0.0.2:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.2:8443"),
            "routed endpoint dial URI must use https scheme"
        );

        let tls_extension = endpoint_config.extensions.get::<ClientTlsConfig>();
        assert!(
            tls_extension.is_some(),
            "ClientTlsConfig must be present in extensions"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_localhost_with_client_tls_config_not_plaintext() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("localhost:9010".to_string());
        config
            .extensions
            .insert(ClientTlsConfig::new().with_enabled_roots());

        let default_address = "localhost:9010";
        let routed_address = "127.0.0.1:9010";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://127.0.0.1:9010"),
            "endpoint must dial over https when ClientTlsConfig is explicitly configured on loopback"
        );

        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_some(),
            "ClientTlsConfig must be present in extensions"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_explicit_https_routed_address() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("http://localhost:9010".to_string());

        let default_address = "http://localhost:9010";
        let routed_address = "https://10.0.0.1:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.1:8443"),
            "explicit https routed address must force https dial scheme even if default is http"
        );
    }

    #[test]
    fn prepare_routed_endpoint_config_fallback_when_default_host_unextractable_without_tls() {
        let config = ClientConfig::default();
        let default_address = "";
        let routed_address = "10.0.0.1:8443";

        let endpoint_config =
            prepare_routed_endpoint_config(&config, default_address, routed_address);

        assert_eq!(
            endpoint_config.endpoint.as_deref(),
            Some("https://10.0.0.1:8443"),
            "endpoint must dial over https"
        );
        assert!(
            endpoint_config
                .extensions
                .get::<ClientTlsConfig>()
                .is_none(),
            "ClientTlsConfig should be None in fallback branch without TLS config and without default domain"
        );
    }

    #[test]
    fn endpoint_scheme() {
        assert_eq!(
            super::endpoint_scheme("http://localhost:9010"),
            Some(Scheme::HTTP),
            "http URL scheme"
        );
        assert_eq!(
            super::endpoint_scheme("https://spanner.googleapis.com"),
            Some(Scheme::HTTPS),
            "https URL scheme"
        );
        assert_eq!(
            super::endpoint_scheme("HTTP://LOCALHOST:9010"),
            Some(Scheme::HTTP),
            "uppercase http scheme"
        );
        assert_eq!(
            super::endpoint_scheme("HTTPS://SPANNER.GOOGLEAPIS.COM"),
            Some(Scheme::HTTPS),
            "uppercase https scheme"
        );
        assert_eq!(
            super::endpoint_scheme("spanner.googleapis.com:443"),
            None,
            "schemeless host:port"
        );
        assert_eq!(
            super::endpoint_scheme("localhost:9010"),
            None,
            "schemeless localhost"
        );
        assert_eq!(
            super::endpoint_scheme("[::1]:8443"),
            None,
            "schemeless IPv6"
        );
        assert_eq!(super::endpoint_scheme(""), None, "empty string");
        assert_eq!(
            super::endpoint_scheme("ftp://example.com"),
            None,
            "non-http/https scheme"
        );
    }

    #[test]
    fn dial_endpoint() {
        assert_eq!(
            super::dial_endpoint("10.0.0.1:15000", true),
            "http://10.0.0.1:15000",
            "plaintext dial endpoint without scheme"
        );
        assert_eq!(
            super::dial_endpoint("10.0.0.1:15000", false),
            "https://10.0.0.1:15000",
            "TLS dial endpoint without scheme"
        );
        assert_eq!(
            super::dial_endpoint("https://10.0.0.1:15000", true),
            "http://10.0.0.1:15000",
            "plaintext dial endpoint replacing existing https"
        );
        assert_eq!(
            super::dial_endpoint("http://10.0.0.1:15000", false),
            "https://10.0.0.1:15000",
            "TLS dial endpoint replacing existing http"
        );
    }

    #[test]
    fn connection_cache_bare_ipv6_normalization() {
        let default_connection = create_test_connection("https://[::1]:443");
        let cache = ConnectionCache::new(default_connection);

        assert_eq!(
            cache.cache_key("::1"),
            "https://[::1]:443",
            "bare unbracketed IPv6 must normalize to bracketed IPv6 with port"
        );
        assert_eq!(
            cache.cache_key("[::1]"),
            "https://[::1]:443",
            "bracketed IPv6 without port must normalize with default port"
        );
        assert!(
            cache.is_default_address("::1"),
            "bare IPv6 ::1 must match default address https://[::1]:443"
        );

        let plaintext_cache = ConnectionCache::new(create_test_connection("http://[::1]:80"));
        assert_eq!(
            plaintext_cache.cache_key("::1"),
            "http://[::1]:80",
            "bare unbracketed IPv6 on plaintext cache must normalize to http port 80"
        );
        assert!(
            plaintext_cache.is_default_address("::1"),
            "bare IPv6 ::1 must match default address http://[::1]:80"
        );
    }

    #[test]
    fn connection_cache_key_unknown_scheme_fallback() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = ConnectionCache::new(default_connection);

        assert_eq!(
            cache.cache_key("custom://myhost"),
            "https://myhost",
            "URL with unknown scheme and no default port formats host"
        );
    }
}
