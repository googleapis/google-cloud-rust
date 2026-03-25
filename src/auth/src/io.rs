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

//! Trait-based abstractions for external I/O in the auth crate.
//!
//! This module defines provider traits for environment variable reads,
//! filesystem reads, and HTTP requests. Users can implement these traits
//! to control how the auth crate performs I/O operations.

use std::future::Future;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::Arc;

/// Abstracts environment variable reads.
///
/// The default implementation delegates to `std::env::var`.
/// Implement this trait to control how the auth crate resolves
/// environment variables like `GOOGLE_APPLICATION_CREDENTIALS`.
pub trait EnvProvider: Send + Sync + UnwindSafe + RefUnwindSafe + std::fmt::Debug {
    /// Reads an environment variable by name.
    /// Returns `None` if the variable is not set.
    fn var(&self, name: &str) -> Option<String>;
}

/// Default implementation using `std::env::var`.
#[derive(Debug, Clone)]
pub struct DefaultEnvProvider;

impl EnvProvider for DefaultEnvProvider {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Abstracts filesystem read operations.
///
/// The default implementation delegates to `std::fs::read_to_string`.
/// Implement this trait to control how the auth crate loads files
/// such as ADC credential files.
pub trait FsProvider: Send + Sync + UnwindSafe + RefUnwindSafe + std::fmt::Debug {
    /// Reads the entire contents of a file as a string.
    fn read_to_string(&self, path: &str) -> std::io::Result<String>;
}

/// Default implementation using `std::fs::read_to_string`.
#[derive(Debug, Clone)]
pub struct DefaultFsProvider;

impl FsProvider for DefaultFsProvider {
    fn read_to_string(&self, path: &str) -> std::io::Result<String> {
        std::fs::read_to_string(path)
    }
}

/// HTTP method for auth requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
}

/// A simple HTTP request used by the auth crate.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub query_params: Vec<(String, String)>,
}

impl HttpRequest {
    /// Creates a GET request to the given URL.
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: HttpMethod::Get,
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
            query_params: Vec::new(),
        }
    }

    /// Creates a POST request to the given URL.
    pub fn post(url: impl Into<String>) -> Self {
        Self {
            method: HttpMethod::Post,
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
            query_params: Vec::new(),
        }
    }

    /// Creates a PUT request to the given URL.
    pub fn put(url: impl Into<String>) -> Self {
        Self {
            method: HttpMethod::Put,
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
            query_params: Vec::new(),
        }
    }

    /// Adds a header to the request.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Adds a query parameter to the request.
    pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.query_params.push((name.into(), value.into()));
        self
    }

    /// Sets the request body.
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    /// Sets a JSON body and adds the `content-type: application/json` header.
    pub fn json(self, body: impl Into<Vec<u8>>) -> Self {
        self.header("content-type", "application/json").body(body)
    }

    /// Sets a form-encoded body and adds the `content-type: application/x-www-form-urlencoded` header.
    pub fn form(self, body: impl Into<Vec<u8>>) -> Self {
        self.header("content-type", "application/x-www-form-urlencoded")
            .body(body)
    }

    /// Copies headers from an `http::HeaderMap` into this request.
    pub fn headers_from_map(mut self, map: &http::HeaderMap) -> Self {
        for (name, value) in map.iter() {
            if let Ok(v) = value.to_str() {
                self.headers.push((name.to_string(), v.to_string()));
            }
        }
        self
    }
}

/// A simple HTTP response returned by the auth crate.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Returns `true` if the status code indicates success (2xx).
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Returns `true` if the status code indicates a transient/retryable error.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.status,
            http::StatusCode::REQUEST_TIMEOUT
                | http::StatusCode::TOO_MANY_REQUESTS
                | http::StatusCode::INTERNAL_SERVER_ERROR
                | http::StatusCode::SERVICE_UNAVAILABLE
        )
    }

    /// Deserializes the response body as JSON into the given type.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> serde_json::Result<T> {
        serde_json::from_slice(&self.body)
    }

    /// Returns the response body as a UTF-8 string.
    pub fn text(&self) -> std::result::Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.body.clone())
    }
}

/// Abstracts HTTP request execution.
///
/// The default implementation delegates to `reqwest::Client`.
/// Implement this trait to control how the auth crate makes HTTP
/// requests for token exchanges, metadata service calls, etc.
pub trait HttpClientProvider: Send + Sync + UnwindSafe + RefUnwindSafe + std::fmt::Debug {
    /// Executes an HTTP request and returns the response.
    fn execute(
        &self,
        request: HttpRequest,
    ) -> impl Future<Output = Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>>> + Send;
}

/// Default implementation using `reqwest::Client`.
#[derive(Debug, Clone)]
pub struct DefaultHttpClientProvider {
    client: reqwest::Client,
}

// SAFETY: `reqwest::Client` is internally `Arc<ClientInner>` — it is
// effectively immutable once constructed and safe to use across
// `catch_unwind` boundaries.
impl UnwindSafe for DefaultHttpClientProvider {}
impl RefUnwindSafe for DefaultHttpClientProvider {}

impl Default for DefaultHttpClientProvider {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl HttpClientProvider for DefaultHttpClientProvider {
    async fn execute(
        &self,
        request: HttpRequest,
    ) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
        let method = match request.method {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
        };

        let mut builder = self.client.request(method, &request.url);
        if !request.query_params.is_empty() {
            builder = builder.query(&request.query_params);
        }
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body);
        }

        let resp = builder.send().await?;

        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.bytes().await?.to_vec();

        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

/// A shared, cloneable environment variable provider.
///
/// Wraps an `Arc<dyn EnvProvider>`. Construct via [`SharedEnvProvider::new`].
#[derive(Clone, Debug)]
pub struct SharedEnvProvider(Arc<dyn EnvProvider>);

impl SharedEnvProvider {
    /// Creates a new shared provider from any type implementing [`EnvProvider`].
    pub fn new<P: EnvProvider + 'static>(provider: P) -> Self {
        Self(Arc::new(provider))
    }

    /// Reads an environment variable by name, delegating to the inner provider.
    pub(crate) fn var(&self, name: &str) -> Option<String> {
        self.0.var(name)
    }
}

impl EnvProvider for SharedEnvProvider {
    fn var(&self, name: &str) -> Option<String> {
        self.0.var(name)
    }
}

impl Default for SharedEnvProvider {
    fn default() -> Self {
        Self::new(DefaultEnvProvider)
    }
}

/// A shared, cloneable filesystem provider.
///
/// Wraps an `Arc<dyn FsProvider>`. Construct via [`SharedFsProvider::new`].
#[derive(Clone, Debug)]
pub struct SharedFsProvider(Arc<dyn FsProvider>);

impl SharedFsProvider {
    /// Creates a new shared provider from any type implementing [`FsProvider`].
    pub fn new<P: FsProvider + 'static>(provider: P) -> Self {
        Self(Arc::new(provider))
    }

    /// Reads the entire contents of a file as a string, delegating to the inner provider.
    pub(crate) fn read_to_string(&self, path: &str) -> std::io::Result<String> {
        self.0.read_to_string(path)
    }
}

impl FsProvider for SharedFsProvider {
    fn read_to_string(&self, path: &str) -> std::io::Result<String> {
        self.0.read_to_string(path)
    }
}

impl Default for SharedFsProvider {
    fn default() -> Self {
        Self::new(DefaultFsProvider)
    }
}

/// A shared, cloneable HTTP client provider.
///
/// Wraps an `Arc<dyn HttpClientProvider>`. Construct via [`SharedHttpClientProvider::new`].
#[derive(Clone, Debug)]
pub struct SharedHttpClientProvider(Arc<dyn dynamic::HttpClientProvider>);

impl SharedHttpClientProvider {
    /// Creates a new shared provider from any type implementing [`HttpClientProvider`].
    pub fn new<P: HttpClientProvider + 'static>(provider: P) -> Self {
        Self(Arc::new(provider))
    }

    /// Executes an HTTP request, delegating to the inner provider.
    pub(crate) async fn execute(
        &self,
        request: HttpRequest,
    ) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
        self.0.execute(request).await
    }
}

impl HttpClientProvider for SharedHttpClientProvider {
    async fn execute(
        &self,
        request: HttpRequest,
    ) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
        self.0.execute(request).await
    }
}

impl Default for SharedHttpClientProvider {
    fn default() -> Self {
        Self::new(DefaultHttpClientProvider::default())
    }
}

/// Holds the I/O provider configuration for credential construction.
///
/// Passed through the credential construction chain so that all components
/// use the same set of providers.
#[derive(Clone, Debug, Default)]
pub(crate) struct IoConfig {
    pub(crate) env: SharedEnvProvider,
    pub(crate) fs: SharedFsProvider,
    pub(crate) http: SharedHttpClientProvider,
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- HttpRequest builder tests --

    #[test]
    fn http_request_get() {
        let req = HttpRequest::get("https://example.com");
        assert_eq!(req.method, HttpMethod::Get);
        assert_eq!(req.url, "https://example.com");
        assert!(req.headers.is_empty());
        assert!(req.body.is_empty());
        assert!(req.query_params.is_empty());
    }

    #[test]
    fn http_request_post() {
        let req = HttpRequest::post("https://example.com");
        assert_eq!(req.method, HttpMethod::Post);
    }

    #[test]
    fn http_request_put() {
        let req = HttpRequest::put("https://example.com");
        assert_eq!(req.method, HttpMethod::Put);
    }

    #[test]
    fn http_request_header() {
        let req = HttpRequest::get("https://example.com")
            .header("authorization", "Bearer tok")
            .header("x-custom", "val");
        assert_eq!(req.headers.len(), 2);
        assert_eq!(
            req.headers[0],
            ("authorization".into(), "Bearer tok".into())
        );
        assert_eq!(req.headers[1], ("x-custom".into(), "val".into()));
    }

    #[test]
    fn http_request_query() {
        let req = HttpRequest::get("https://example.com")
            .query("key", "value")
            .query("a", "b");
        assert_eq!(req.query_params.len(), 2);
        assert_eq!(req.query_params[0], ("key".into(), "value".into()));
    }

    #[test]
    fn http_request_body() {
        let req = HttpRequest::post("https://example.com").body(b"hello".to_vec());
        assert_eq!(req.body, b"hello");
    }

    #[test]
    fn http_request_json() {
        let req = HttpRequest::post("https://example.com").json(b"{}".to_vec());
        assert_eq!(req.body, b"{}");
        assert!(
            req.headers
                .iter()
                .any(|(k, v)| k == "content-type" && v == "application/json")
        );
    }

    #[test]
    fn http_request_form() {
        let req = HttpRequest::post("https://example.com").form(b"k=v".to_vec());
        assert_eq!(req.body, b"k=v");
        assert!(
            req.headers
                .iter()
                .any(|(k, v)| k == "content-type" && v == "application/x-www-form-urlencoded")
        );
    }

    #[test]
    fn http_request_headers_from_map() {
        let mut map = http::HeaderMap::new();
        map.insert("x-foo", http::HeaderValue::from_static("bar"));
        let req = HttpRequest::get("https://example.com").headers_from_map(&map);
        assert_eq!(req.headers, vec![("x-foo".into(), "bar".into())]);
    }

    // -- HttpResponse tests --

    fn make_response(status: u16, body: &[u8]) -> HttpResponse {
        HttpResponse {
            status: http::StatusCode::from_u16(status).unwrap(),
            headers: http::HeaderMap::new(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn http_response_is_success() {
        assert!(make_response(200, b"").is_success());
        assert!(make_response(201, b"").is_success());
        assert!(!make_response(400, b"").is_success());
        assert!(!make_response(500, b"").is_success());
    }

    #[test]
    fn http_response_is_retryable() {
        assert!(make_response(408, b"").is_retryable());
        assert!(make_response(429, b"").is_retryable());
        assert!(make_response(500, b"").is_retryable());
        assert!(make_response(503, b"").is_retryable());
        assert!(!make_response(200, b"").is_retryable());
        assert!(!make_response(400, b"").is_retryable());
        assert!(!make_response(404, b"").is_retryable());
        assert!(!make_response(502, b"").is_retryable());
    }

    #[test]
    fn http_response_json() {
        let resp = make_response(200, br#"{"key":"value"}"#);
        let parsed: serde_json::Value = resp.json().unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn http_response_json_error() {
        let resp = make_response(200, b"not json");
        let result: serde_json::Result<serde_json::Value> = resp.json();
        assert!(result.is_err());
    }

    #[test]
    fn http_response_text() {
        let resp = make_response(200, b"hello world");
        assert_eq!(resp.text().unwrap(), "hello world");
    }

    #[test]
    fn http_response_text_invalid_utf8() {
        let resp = make_response(200, &[0xff, 0xfe]);
        assert!(resp.text().is_err());
    }

    // -- Custom provider tests --

    #[derive(Debug)]
    struct FakeEnv;
    impl EnvProvider for FakeEnv {
        fn var(&self, name: &str) -> Option<String> {
            match name {
                "TEST_KEY" => Some("test_value".into()),
                _ => None,
            }
        }
    }

    #[derive(Debug)]
    struct FakeFs;
    impl FsProvider for FakeFs {
        fn read_to_string(&self, path: &str) -> std::io::Result<String> {
            if path == "/fake/file.txt" {
                Ok("fake contents".into())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "not found",
                ))
            }
        }
    }

    #[test]
    fn shared_env_provider_delegates() {
        let shared = SharedEnvProvider::new(FakeEnv);
        assert_eq!(shared.var("TEST_KEY"), Some("test_value".into()));
        assert_eq!(shared.var("MISSING"), None);
    }

    #[test]
    fn shared_env_provider_implements_trait() {
        let shared = SharedEnvProvider::new(FakeEnv);
        let as_trait: &dyn EnvProvider = &shared;
        assert_eq!(as_trait.var("TEST_KEY"), Some("test_value".into()));
    }

    #[test]
    fn shared_env_provider_clone() {
        let shared = SharedEnvProvider::new(FakeEnv);
        let cloned = shared.clone();
        assert_eq!(cloned.var("TEST_KEY"), Some("test_value".into()));
    }

    #[test]
    fn shared_fs_provider_delegates() {
        let shared = SharedFsProvider::new(FakeFs);
        assert_eq!(
            shared.read_to_string("/fake/file.txt").unwrap(),
            "fake contents"
        );
        assert!(shared.read_to_string("/missing").is_err());
    }

    #[test]
    fn shared_fs_provider_implements_trait() {
        let shared = SharedFsProvider::new(FakeFs);
        let as_trait: &dyn FsProvider = &shared;
        assert_eq!(
            as_trait.read_to_string("/fake/file.txt").unwrap(),
            "fake contents"
        );
    }

    #[derive(Debug)]
    struct FakeHttp;
    impl HttpClientProvider for FakeHttp {
        async fn execute(
            &self,
            _request: HttpRequest,
        ) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
            Ok(make_response(200, b"ok"))
        }
    }

    #[tokio::test]
    async fn shared_http_provider_delegates() {
        let shared = SharedHttpClientProvider::new(FakeHttp);
        let resp = shared
            .execute(HttpRequest::get("https://example.com"))
            .await
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
        assert_eq!(resp.body, b"ok");
    }

    #[tokio::test]
    async fn shared_http_provider_implements_trait() {
        let shared = SharedHttpClientProvider::new(FakeHttp);
        let resp = HttpClientProvider::execute(&shared, HttpRequest::get("https://example.com"))
            .await
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
    }

    // -- IoConfig defaults --

    #[test]
    fn io_config_default() {
        let config = IoConfig::default();
        // Default env provider reads real env vars — just verify it doesn't panic.
        let _ = config.env.var("PATH");
        // Default fs provider reads real files — just verify a missing file errors.
        assert!(config.fs.read_to_string("/nonexistent-path-12345").is_err());
    }
}

pub(crate) mod dynamic {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    /// A dyn-compatible, crate-private version of `HttpClientProvider`.
    ///
    /// The public `HttpClientProvider` uses RPITIT (`-> impl Future<...>`)
    /// which is not dyn-compatible. This trait uses `#[async_trait]` to
    /// produce a boxed future, enabling storage as `Arc<dyn HttpClientProvider>`.
    ///
    /// `EnvProvider` and `FsProvider` are synchronous and already
    /// dyn-compatible, so they don't need dynamic counterparts.
    #[async_trait::async_trait]
    pub trait HttpClientProvider:
        Send + Sync + UnwindSafe + RefUnwindSafe + std::fmt::Debug
    {
        async fn execute(
            &self,
            request: super::HttpRequest,
        ) -> Result<super::HttpResponse, Box<dyn std::error::Error + Send + Sync>>;
    }

    /// The public HttpClientProvider implements the dyn-compatible HttpClientProvider.
    #[async_trait::async_trait]
    impl<T> HttpClientProvider for T
    where
        T: super::HttpClientProvider + Send + Sync,
    {
        async fn execute(
            &self,
            request: super::HttpRequest,
        ) -> Result<super::HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
            T::execute(self, request).await
        }
    }
}
