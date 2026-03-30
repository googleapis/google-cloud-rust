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

use crate::observability::attributes::{GCP_CLIENT_REPO_GOOGLEAPIS, RPC_SYSTEM_HTTP};
#[cfg(feature = "_internal-http-client")]
use crate::observability::http_tracing::sanitize_url;
use crate::options::InstrumentationClientInfo;
#[cfg(feature = "_internal-http-client")]
use google_cloud_gax::error::Error;
use http::Uri;
use reqwest::Method;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

const HTTPS_PORT: u16 = 443;

tokio::task_local! {
    static RECORDER: RequestRecorder;
}

/// Capture telemetry information for a typical client request.
///
/// In this document we use the naming conventions from go/clo:product-requirements-v1
///
/// We want the client library to emit telemetry signals (spans, duration metrics, and logs) for
/// each client-level request (T3) and transport-level request (T4). To meet the requirements we
/// need to capture information as the request makes progress. For example, the client request
/// telemetry includes information about the last low-level request, such as the remote server IP
/// address and port. It is difficult to carry this information through the different layers without
/// breaking changes APIs.
///
/// This type solves that problem by setting a task-local (think "thread local" but for
/// asynchronous tasks) variable valid for the full request. Each layer adds information to this
/// variable. Once the telemetry layer is ready to emit a signal it consults the variable and uses
/// the latest snapshot to populate the attributes of the signal.
///
/// Client library implementations must initialize a `RequestRecorder` and bring it into scope
/// before calling its transport-level stub (and therefore before making any requests).
///
/// The transport-level stub in the client library must call `on_client_request()` before calling the
/// transport client.
///
/// The transport clients ([ReqwestClient] and [GrpcClient]) must make matched calls to
/// `on_<transport>_request` and one of `on_<transport>_response` or `on_<transport>_error()`. Any
/// instrumentation on these clients can read the values directly from the `RequestRecorder` that is
/// in scope.
///
/// The client library tracing-level stub reads the values directly from the `RequestRecorder` when
/// the transport-level stub future is satisfied.
///
/// # Example
/// ```
/// # use google_cloud_gax_internal::observability::RequestRecorder;
/// use google_cloud_gax_internal::options::InstrumentationClientInfo;
/// async fn telemetry_layer() -> google_cloud_gax::Result<String> {
///     let recorder = RequestRecorder::new(info());
///     // Calls `transport_layer()` and capture all the information about the client and transport layers.
///     recorder.scope(transport_layer()).await
/// }
///
/// fn info() -> InstrumentationClientInfo {
/// # panic!();
/// }
/// async fn transport_layer() -> google_cloud_gax::Result<String> {
/// # panic!("")
/// }
/// ```
///
/// [ReqwestClient]: crate::http::ReqwestClient
/// [GrpcClient]: crate::grpc::GrpcClient
#[derive(Clone, Debug)]
pub struct RequestRecorder {
    inner: Arc<Mutex<ClientSnapshot>>,
}

impl RequestRecorder {
    /// Creates a new request recorder based on the client library instrumentation in `info`.
    pub fn new(info: InstrumentationClientInfo) -> Self {
        let inner = ClientSnapshot::new(info);
        let inner = Arc::new(Mutex::new(inner));
        Self { inner }
    }

    /// Runs a `future` in the scope of a request recorder.
    pub fn scope<F, R>(self, future: F) -> impl Future<Output = Result<R, Error>>
    where
        F: Future<Output = Result<R, Error>>,
    {
        RECORDER.scope(self, future)
    }

    /// Returns the current scope.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax_internal::observability::RequestRecorder;
    /// use google_cloud_gax::options::RequestOptions;
    /// async fn sample(options: &RequestOptions, request: reqwest::RequestBuilder) -> anyhow::Result<()> {
    ///     let response = request.send().await?;
    ///     if let Some(current) = RequestRecorder::current() {
    ///         current.on_http_response(&response);
    ///     }
    ///     // ... do something with `response` ...
    ///     Ok(())
    /// }
    /// ```
    pub fn current() -> Option<Self> {
        RECORDER.try_get().ok()
    }

    /// Returns the data captured for the client layer.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax_internal::observability::RequestRecorder;
    /// async fn emit_client_log<T>(result: &google_cloud_gax::Result<T>) {
    ///     let Err(e) = result else { return; };
    ///     let Some(recorder) = RequestRecorder::current() else { return; };
    ///     let snapshot = recorder.client_snapshot();
    ///     tracing::error!(
    ///         { "url.domain" } = snapshot.default_host(),
    ///         // use more things from snapshot here.
    ///     );
    /// }
    /// ```
    pub fn client_snapshot(&self) -> ClientSnapshot {
        let guard = self.inner.lock().expect("never poisoned");
        guard.clone()
    }

    /// Call before starting the retry loop the client request.
    ///
    /// # Parameters
    /// - `rpc_method`: the name (in gRPC format) of the RPC being invoked. Some client
    ///   requests do not use a RPC. For example, storage `read_object()` calls a plain HTTP
    ///   GET request. Most client requests use a single RPC, for example:
    ///   `google.storage.v2.Storage/BidiStreamingRead`,
    ///   `google.cloud.secretmanager.v1.SecretManagerService/ListSecrets`
    pub fn on_client_request(&self, attributes: ClientRequestAttributes) {
        let mut guard = self.inner.lock().expect("never poisoned");
        guard.rpc_method = attributes.rpc_method;
        guard.url_template = attributes.url_template;
        guard.resource_name = attributes.resource_name;
    }

    /// Call before issuing a HTTP request to capture its data.
    #[cfg(feature = "_internal-http-client")]
    pub fn on_http_request(&self, request: &reqwest::Request) {
        let mut guard = self.inner.lock().expect("never poisoned");
        let snapshot = TransportSnapshot {
            start: Instant::now(),
            network_peer_address: None,
            rpc_system: Some(RPC_SYSTEM_HTTP),
            http_method: Some(request.method().clone()),
            http_status_code: None,
            url: Some(sanitize_url(request.url()).to_string()),
        };
        guard.transport_snapshot = Some(snapshot);
    }

    /// Call when receiving a HTTP response to capture its data.
    ///
    /// In this context, responses that return an error status code are considered successful,
    /// we just need them to capture their data for the spans and metrics.
    #[cfg(feature = "_internal-http-client")]
    pub fn on_http_response(&self, response: &reqwest::Response) {
        let mut guard = self.inner.lock().expect("never poisoned");
        guard.attempt_count += 1;
        if let Some(s) = guard.transport_snapshot.as_mut() {
            s.network_peer_address = response.remote_addr();
            s.http_status_code = Some(response.status().as_u16());
        }
    }

    /// Call when it was not possible to send an HTTP request.
    #[cfg(feature = "_internal-http-client")]
    pub fn on_http_error(&self, _err: &Error) {
        let mut guard = self.inner.lock().expect("never poisoned");
        guard.attempt_count += 1;
    }
}

/// The attributes captured at the start of the request.
///
/// # Example
/// ```
/// # use google_cloud_gax_internal::observability::ClientRequestAttributes;
/// let attributes = ClientRequestAttributes::default()
///     .set_rpc_method("google.test.v1.TestService/SomeMethod")
///     .set_url_template("/v42/{parent}")
///     .set_resource_name("//test.googleapis.com/projects/my-project".to_string());
/// ```
///
/// The generated code can provide these attributes at the beginning of the request, in the transport.rs file.
///
/// For hand-crafted clients, the client layer can provide these attributes just after initializing the request recorder.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClientRequestAttributes {
    pub rpc_method: Option<&'static str>,
    pub url_template: Option<&'static str>,
    pub resource_name: Option<String>,
}

impl ClientRequestAttributes {
    pub fn set_rpc_method(mut self, v: &'static str) -> Self {
        self.rpc_method = Some(v);
        self
    }

    pub fn set_url_template(mut self, v: &'static str) -> Self {
        self.url_template = Some(v);
        self
    }

    pub fn set_resource_name(mut self, v: String) -> Self {
        self.resource_name = Some(v);
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ClientSnapshot {
    start: Instant,
    info: InstrumentationClientInfo,
    rpc_method: Option<&'static str>,
    url_template: Option<&'static str>,
    resource_name: Option<String>,
    attempt_count: u32,
    transport_snapshot: Option<TransportSnapshot>,
}

impl ClientSnapshot {
    fn new(info: InstrumentationClientInfo) -> Self {
        let start = Instant::now();
        Self {
            start,
            info,
            rpc_method: None,
            url_template: None,
            resource_name: None,
            attempt_count: 0_u32,
            transport_snapshot: None,
        }
    }

    /// Returns the client request duration.
    ///
    /// This measures the time since the instance was created. Client libraries should initialize an
    /// instance at the beginning of the request, before any RPCs or attempts to create or fetch
    /// authentication tokens.
    pub fn client_duration(&self) -> Duration {
        self.start.elapsed()
    }

    /// Returns the default host (e.g. `storage.googleapis.com`).
    ///
    /// Use with the "url.domain" attribute.
    pub fn default_host(&self) -> &'static str {
        self.info.default_host
    }

    /// Returns the service name (e.g. `storage`).
    ///
    /// Use with the "gcp.client.service" attribute.
    pub fn service_name(&self) -> &'static str {
        self.info.service_name
    }

    /// Returns the service version (e.g. `1.2.3`).
    ///
    /// Use with the "gcp.client.version" attribute.
    pub fn client_version(&self) -> &'static str {
        self.info.client_version
    }

    /// Returns the GitHub repository.
    ///
    /// Use with the "gcp.client.repo" attribute.
    pub fn client_repo(&self) -> &'static str {
        GCP_CLIENT_REPO_GOOGLEAPIS
    }

    /// Returns the Rust crate.
    ///
    /// Use as instrumentation name, and with the "gcp.client.artifact" attribute.
    pub fn client_artifact(&self) -> &'static str {
        self.info.client_artifact
    }

    /// Returns the RPC system (HTTP or gRPC) used in the last low-level request.
    ///
    /// Use with the "rpc.system.name" attribute.
    pub fn rpc_system(&self) -> Option<&'static str> {
        self.transport_snapshot.as_ref().and_then(|s| s.rpc_system)
    }

    /// Returns the server address used in the last low-level request.
    ///
    /// If no address is known, use the target address from `info.default_host`.
    ///
    /// Use with the "server.address" attribute.
    pub fn server_address(&self) -> String {
        if let Some(uri) = self.sanitized_url().and_then(|u| u.parse::<Uri>().ok()) {
            if let Some(host) = uri.authority().map(|a| a.host().to_string()) {
                return host;
            }
        }
        self.info.default_host.to_string()
    }

    /// Returns the server port used in the last low-level request.
    ///
    /// If no port is known, use the port implied by `info.default_host`.
    ///
    /// Use with the "server.port" attribute after casting to `i64`.
    pub fn server_port(&self) -> u16 {
        if let Some(uri) = self.sanitized_url().and_then(|u| u.parse::<Uri>().ok()) {
            if let Some(host) = uri.authority().and_then(|a| a.port_u16()) {
                return host;
            }
        }
        HTTPS_PORT
    }

    /// Returns the URL template (e.g. "/v1/storage/b/{bucket}") used in the last low-level request.
    ///
    /// Use with the "url.template" attribute.
    pub fn url_template(&self) -> Option<&'static str> {
        self.url_template
    }

    /// Returns the resource name (e.g. "//storage.googleapis.com/projects/_/buckets/my-bucket").
    ///
    /// Use with the "gcp.resource.destination.id" attribute.
    pub fn resource_name(&self) -> Option<&str> {
        self.resource_name.as_deref()
    }

    /// Returns the RPC method (e.g. "cloud.google.secretmanager.v1.SecretManager/GetSecret") used in the request.
    ///
    /// Use with the "rpc.method" attribute.
    pub fn rpc_method(&self) -> Option<&'static str> {
        self.rpc_method
    }

    /// Returns the HTTP status code (e.g. 404) returned in the last request.
    ///
    /// Note that this may not be populated for gRPC requests.
    ///
    /// Use with the "http.response.status_code" attribute after casting to `i64`.
    pub fn http_status_code(&self) -> Option<u16> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.http_status_code)
    }

    /// Returns the HTTP method (e.g. POST) used in the last request.
    ///
    /// Note that this may not be populated for gRPC requests.
    ///
    /// Use with the "http.request.method" attribute.
    pub fn http_method(&self) -> Option<&str> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.http_method.as_ref().map(|m| m.as_str()))
    }

    /// Returns the "resend count" of the last request, if it was a retry.
    ///
    /// The resend count of the initial attempt is `None`, and starts at 1 for each retry attempt
    /// made.
    ///
    /// Use with the "http.request.resend_count" attribute after casting to `i64`.
    pub fn http_resend_count(&self) -> Option<u32> {
        if self.attempt_count <= 1 {
            return None;
        }
        Some(self.attempt_count - 1)
    }

    /// Returns the sanitized (but otherwise full) URL used in the last request.
    ///
    /// Note that this may not be populated for gRPC requests.
    ///
    /// Use with the "url.full" attribute.
    pub fn sanitized_url(&self) -> Option<&str> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.url.as_deref())
    }

    /// Returns the network peer address.
    ///
    /// Use with the "network.peer.address" attribute.
    pub fn network_peer_address(&self) -> Option<String> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.network_peer_address)
            .map(|a| a.ip().to_string())
    }

    /// Returns the network peer port.
    ///
    /// Use with the "network.peer.port" attribute.
    pub fn network_peer_port(&self) -> Option<i64> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.network_peer_address)
            .map(|a| a.port() as i64)
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TransportSnapshot {
    start: Instant,
    network_peer_address: Option<SocketAddr>,
    rpc_system: Option<&'static str>,
    http_method: Option<Method>,
    http_status_code: Option<u16>,
    url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::super::tests::TEST_INFO;
    use super::*;
    use httptest::matchers::request::method_path;
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use pretty_assertions::assert_eq;

    const TEST_METHOD_NAME: &str = "google.test.v1.Service/SomeMethod";
    const TEST_PATH_TEMPLATE: &str = "/v42/{parent}";
    const TEST_RESOURCE_NAME: &str = "//test.googleapis.com/test-only";
    const STORAGE_PATH_TEMPLATE: &str = "/v1/storage/b/{bucket}/o/{object}";

    async fn simulate_http_client_gaxi(url: &str) -> Result<String, Error> {
        let client = reqwest::Client::new();
        let current = RequestRecorder::current().expect("current recorder should be available");
        let request = client
            .get(url)
            .build()
            .map_err(Error::io)
            .inspect_err(|e| current.on_http_error(e))?;
        current.on_http_request(&request);
        let response = client
            .execute(request)
            .await
            .map_err(Error::io)
            .inspect_err(|e| current.on_http_error(e))?;
        current.on_http_response(&response);
        Err(Error::deser("fake error"))
    }

    async fn simulate_http_client_transport_layer(url: &str) -> Result<String, Error> {
        let current = RequestRecorder::current().expect("current recorder should be available");
        // The generator knows the RPC method and determines the URL path template and resource.
        current.on_client_request(
            ClientRequestAttributes::default()
                .set_rpc_method(TEST_METHOD_NAME)
                .set_url_template(TEST_PATH_TEMPLATE)
                .set_resource_name(TEST_RESOURCE_NAME.to_string()),
        );
        simulate_http_client_gaxi(url).await
    }

    #[tokio::test(start_paused = true)]
    async fn http_full_cycle() {
        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", "/v1/test-only"))
                .respond_with(status_code(404).body("NOT FOUND")),
        );

        let url = server.url_str("/v1/test-only");

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        // Normally this code would be in the `tracing.rs` layer. Inline it here so we can examine the
        // effects on the `RequestRecorder`.
        let got = scoped
            .scope(simulate_http_client_transport_layer(&url))
            .await;
        assert!(
            matches!(got, Err(ref e) if e.is_deserialization()),
            "{got:?}"
        );
        let snap = recorder.client_snapshot();

        assert_eq!(snap.start, Instant::now(), "{snap:?}");
        assert_eq!(snap.rpc_method(), Some(TEST_METHOD_NAME), "{snap:?}");
        assert_eq!(snap.url_template(), Some(TEST_PATH_TEMPLATE), "{snap:?}");
        assert_eq!(snap.resource_name(), Some(TEST_RESOURCE_NAME), "{snap:?}");
        assert_eq!(snap.rpc_system(), Some("http"), "{snap:?}");
        assert_eq!(snap.sanitized_url(), Some(url.as_str()), "{snap:?}");

        assert_eq!(snap.attempt_count, 1, "{snap:?}");
        assert_eq!(snap.http_status_code(), Some(404), "{snap:?}");
        let addr = server.addr();
        assert_eq!(snap.server_address(), addr.ip().to_string(), "{snap:?}");
        assert_eq!(snap.server_port(), addr.port(), "{snap:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn http_cannot_send() {
        const BAD_URL: &str = "https://127.0.0.1:1/v1/test-only";

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        // Normally this code would be in the `tracing.rs` layer. Inline it here so we can examine the
        // effects on the `RequestRecorder`.
        let got = scoped
            .scope(simulate_http_client_transport_layer(BAD_URL))
            .await;
        assert!(matches!(got, Err(ref e) if e.is_io()), "{got:?}");
        let snap = recorder.client_snapshot();

        assert_eq!(snap.start, Instant::now(), "{snap:?}");
        assert_eq!(snap.rpc_method(), Some(TEST_METHOD_NAME), "{snap:?}");
        assert_eq!(snap.url_template(), Some(TEST_PATH_TEMPLATE), "{snap:?}");
        assert_eq!(snap.rpc_system(), Some("http"), "{snap:?}");
        assert_eq!(snap.sanitized_url(), Some(BAD_URL), "{snap:?}");

        assert_eq!(snap.attempt_count, 1, "{snap:?}");
        assert!(snap.http_status_code().is_none(), "{snap:?}");
        assert_eq!(snap.server_address().as_str(), "127.0.0.1", "{snap:?}");
        assert_eq!(snap.server_port(), 1, "{snap:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn http_bad_url() {
        const BAD_URL: &str = "bad-url";

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        // Normally this code would be in the `tracing.rs` layer. Inline it here so we can examine the
        // effects on the `RequestRecorder`.
        let got = scoped
            .scope(simulate_http_client_transport_layer(BAD_URL))
            .await;
        assert!(matches!(got, Err(ref e) if e.is_io()), "{got:?}");
        let snap = recorder.client_snapshot();

        assert_eq!(snap.start, Instant::now(), "{snap:?}");
        assert_eq!(snap.rpc_method(), Some(TEST_METHOD_NAME), "{snap:?}");
        assert_eq!(snap.url_template(), Some(TEST_PATH_TEMPLATE), "{snap:?}");
        assert!(snap.rpc_system().is_none(), "{snap:?}");
        assert!(snap.sanitized_url().is_none(), "{snap:?}");

        assert_eq!(snap.attempt_count, 1, "{snap:?}");
        assert!(snap.http_status_code().is_none(), "{snap:?}");
        assert_eq!(snap.server_address().as_str(), "example.com", "{snap:?}");
        assert_eq!(snap.server_port(), 443, "{snap:?}");
    }

    async fn simulate_storage_client_transport_layer(url: &str) -> Result<String, Error> {
        let current = RequestRecorder::current().expect("current recorder should be available");
        // The generator knows the RPC method and determines the URL path template and resource.
        current.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template(STORAGE_PATH_TEMPLATE)
                .set_resource_name(
                    "//storage.googleapis.com/projects/_/buckets/my-bucket".to_string(),
                ),
        );
        simulate_http_client_gaxi(url).await
    }

    #[tokio::test(start_paused = true)]
    async fn storage_full_cycle() {
        const PATH: &str = "/v1/storage/b/my-bucket/o/my-object";
        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", PATH))
                .respond_with(status_code(404).body("NOT FOUND")),
        );

        let url = server.url_str(PATH);

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        // Normally this code would be in the `storage/src/transport.rs` file. Inline it here so we can examine the
        // effects on the `RequestRecorder`.
        let got = scoped
            .scope(simulate_storage_client_transport_layer(&url))
            .await;
        assert!(
            matches!(got, Err(ref e) if e.is_deserialization()),
            "{got:?}"
        );
        let snap = recorder.client_snapshot();

        assert_eq!(snap.start, Instant::now(), "{snap:?}");
        assert!(snap.rpc_method().is_none(), "{snap:?}");
        assert_eq!(snap.url_template(), Some(STORAGE_PATH_TEMPLATE), "{snap:?}");
        assert_eq!(snap.rpc_system(), Some("http"), "{snap:?}");
        assert_eq!(snap.sanitized_url(), Some(url.as_str()), "{snap:?}");
        assert_eq!(snap.attempt_count, 1, "{snap:?}");
        assert_eq!(snap.http_status_code(), Some(404), "{snap:?}");
        let addr = server.addr();
        assert_eq!(snap.server_address(), addr.ip().to_string(), "{snap:?}");
        assert_eq!(snap.server_port(), addr.port(), "{snap:?}");
    }

    #[test]
    fn url_is_sanitized() -> anyhow::Result<()> {
        const RAW_URL: &str = "https://127.0.0.1:1/v42/unused?Signature=ABC&upload_id=123";
        const WANT_URL: &str =
            "https://127.0.0.1:1/v42/unused?Signature=REDACTED&upload_id=REDACTED";

        let url = reqwest::Url::parse(RAW_URL)?;
        let request = reqwest::Request::new(reqwest::Method::GET, url);
        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_http_request(&request);
        let snap = recorder.client_snapshot();
        assert_eq!(snap.sanitized_url(), Some(WANT_URL), "{snap:?}");
        Ok(())
    }

    #[test]
    fn address_sources() -> anyhow::Result<()> {
        const RAW_URL: &str = "https://127.0.0.1:1/v42/unused";

        let recorder = RequestRecorder::new(TEST_INFO);
        let snap = recorder.client_snapshot();
        assert_eq!(snap.server_address(), TEST_INFO.default_host, "{snap:?}");
        assert_eq!(snap.server_port(), HTTPS_PORT, "{snap:?}");

        let url = reqwest::Url::parse(RAW_URL)?;
        let request = reqwest::Request::new(reqwest::Method::GET, url);
        recorder.on_http_request(&request);
        let snap = recorder.client_snapshot();
        assert_eq!(snap.server_address(), "127.0.0.1", "{snap:?}");
        assert_eq!(snap.server_port(), 1, "{snap:?}");

        {
            let mut guard = recorder.inner.lock().expect("never poisoned");
            let s = guard.transport_snapshot.as_mut().expect("already set");
            s.network_peer_address = Some(std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::new(127, 0, 0, 234),
                234,
            )));
        }
        let snap = recorder.client_snapshot();
        assert_eq!(
            snap.network_peer_address(),
            Some("127.0.0.234".to_string()),
            "{snap:?}"
        );
        assert_eq!(snap.network_peer_port(), Some(234), "{snap:?}");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn client_duration() {
        const DURATION: Duration = Duration::from_millis(123456);
        let recorder = RequestRecorder::new(TEST_INFO);
        tokio::time::sleep(DURATION).await;
        let snap = recorder.client_snapshot();
        assert_eq!(snap.client_duration(), DURATION, "{snap:?}");
    }
}
