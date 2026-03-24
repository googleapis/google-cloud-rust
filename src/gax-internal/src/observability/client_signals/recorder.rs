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

use crate::observability::attributes::RPC_SYSTEM_HTTP;
use crate::options::InstrumentationClientInfo;
#[cfg(feature = "_internal-http-client")]
use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
use reqwest::Method;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

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

    /// Call before issuing a HTTP request to capture its data.
    #[cfg(feature = "_internal-http-client")]
    pub fn on_http_request(&self, options: &RequestOptions, request: &reqwest::Request) {
        let mut guard = self.inner.lock().expect("never poisoned");
        let snapshot = TransportSnapshot {
            start: Instant::now(),
            server_address: None,
            url_template: options.get_extension::<PathTemplate>().map(|e| e.0),
            rpc_system: Some(RPC_SYSTEM_HTTP),
            rpc_method: None,
            http_method: Some(request.method().clone()),
            http_status_code: None,
            url: Some(request.url().to_string()),
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
            s.server_address = response.remote_addr();
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

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ClientSnapshot {
    pub start: Instant,
    pub info: InstrumentationClientInfo,
    pub attempt_count: u32,
    pub transport_snapshot: Option<TransportSnapshot>,
}

impl ClientSnapshot {
    fn new(info: InstrumentationClientInfo) -> Self {
        let start = Instant::now();
        Self {
            start,
            info,
            attempt_count: 0_u32,
            transport_snapshot: None,
        }
    }

    /// Returns the default host (e.g. `storage.googleapis.com`).
    ///
    /// Use with the "url.domain" attribute.
    pub fn default_host(&self) -> &'static str {
        self.info.default_host
    }

    /// Returns the RPC system (HTTP or gRPC) used in the last low-level request.
    ///
    /// Use with the "rpc.system.name" attribute.
    pub fn rpc_system(&self) -> Option<&'static str> {
        self.transport_snapshot.as_ref().and_then(|s| s.rpc_system)
    }

    /// Returns the server address used in the last low-level request.
    pub fn server_address(&self) -> Option<SocketAddr> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.server_address)
    }

    /// Returns the URL template (e.g. "/v1/storage/b/{bucket}") used in the last low-level request.
    ///
    /// Use with the "url.template" attribute.
    pub fn url_template(&self) -> Option<&'static str> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.url_template)
    }

    /// Returns the RPC method (e.g. "cloud.google.secretmanager.v1.SecretManager/GetSecret") used in the request.
    ///
    /// Use with the "rpc.method" attribute.
    pub fn rpc_method(&self) -> Option<&'static str> {
        self.transport_snapshot.as_ref().and_then(|s| s.rpc_method)
    }

    /// Returns the HTTP status code (e.g. 404) returned in the last request.
    ///
    /// Note that this may not be populated for gRPC requests.
    ///
    /// Use with the "rpc.method" attribute.
    pub fn http_status_code(&self) -> Option<u16> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.http_status_code)
    }

    /// Returns the full URL used in the last request.
    ///
    /// Note that this may not be populated for gRPC requests.
    ///
    /// Use with the "rpc.method" attribute.
    pub fn url(&self) -> Option<&str> {
        self.transport_snapshot
            .as_ref()
            .and_then(|s| s.url.as_deref())
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TransportSnapshot {
    start: Instant,
    server_address: Option<SocketAddr>,
    rpc_system: Option<&'static str>,
    rpc_method: Option<&'static str>,
    url_template: Option<&'static str>,
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

    #[tokio::test]
    async fn scope() {
        let recorder = RequestRecorder::new(TEST_INFO);

        let scoped = recorder.clone();
        let got = scoped
            .scope(async {
                let current =
                    RequestRecorder::current().expect("current recorder should be available");
                let snap = current.client_snapshot();
                assert_eq!(snap.attempt_count, 0, "{snap:?}");
                assert_eq!(snap.default_host(), TEST_INFO.default_host, "{snap:?}");
                current.on_http_error(&Error::deser("cannot deserialize"));
                Ok(123)
            })
            .await;

        assert!(matches!(got, Ok(ref v) if v == &123), "{got:?}");
        let snap = recorder.client_snapshot();
        assert_eq!(snap.attempt_count, 1, "{snap:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn on_http_request() -> anyhow::Result<()> {
        let recorder = RequestRecorder::new(TEST_INFO);
        let options = RequestOptions::default().insert_extension(PathTemplate("/v7/{funny}"));
        let client = reqwest::Client::new();
        let request = client.get("http://127.0.0.1:1/v7/will-not-work").build()?;

        recorder.on_http_request(&options, &request);
        let snap = recorder.client_snapshot();
        assert_eq!(snap.start, Instant::now(), "{snap:?}");
        assert_eq!(snap.url_template(), Some("/v7/{funny}"), "{snap:?}");
        assert_eq!(snap.rpc_system(), Some("http"), "{snap:?}");
        assert!(snap.rpc_method().is_none(), "{snap:?}");
        assert!(snap.http_status_code().is_none(), "{snap:?}");
        assert_eq!(
            snap.url(),
            Some("http://127.0.0.1:1/v7/will-not-work"),
            "{snap:?}"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn on_http_response() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", "/v1/test-only"))
                .respond_with(status_code(404).body("NOT FOUND")),
        );
        let client = reqwest::Client::new();
        let request = client.get(server.url_str("/v1/test-only")).build()?;
        let options = RequestOptions::default();

        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_http_request(&options, &request);
        let snap = recorder.client_snapshot();
        assert_eq!(snap.attempt_count, 0, "{snap:?}");

        let response = client.execute(request).await?;
        recorder.on_http_response(&response);
        let snap = recorder.client_snapshot();
        assert_eq!(snap.attempt_count, 1, "{snap:?}");
        assert_eq!(snap.http_status_code(), Some(404), "{snap:?}");
        assert_eq!(snap.server_address(), Some(server.addr()), "{snap:?}");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn on_http_error() -> anyhow::Result<()> {
        let recorder = RequestRecorder::new(TEST_INFO);
        let snap = recorder.client_snapshot();
        assert_eq!(snap.attempt_count, 0, "{snap:?}");
        recorder.on_http_error(&Error::deser("fake error"));
        let snap = recorder.client_snapshot();
        assert_eq!(snap.attempt_count, 1, "{snap:?}");
        recorder.on_http_error(&Error::deser("fake error"));
        Ok(())
    }
}
