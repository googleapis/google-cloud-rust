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

//! Implements [WithClientLogging] a decorator for [Future] adding error logs.
//!
//! This is a private module, it is not exposed in the public API.

use super::RequestRecorder;

use crate::observability::attributes::keys::{
    ERROR_TYPE, GCP_CLIENT_ARTIFACT, GCP_CLIENT_REPO, GCP_CLIENT_SERVICE, GCP_CLIENT_VERSION,
    GCP_ERRORS_DOMAIN, GCP_ERRORS_METADATA, HTTP_REQUEST_METHOD, HTTP_REQUEST_RESEND_COUNT,
    RPC_RESPONSE_STATUS_CODE, RPC_SERVICE, RPC_SYSTEM_NAME, SERVER_ADDRESS, SERVER_PORT, URL_FULL,
};
use crate::observability::errors::ErrorType;
use google_cloud_gax::error::Error;
use opentelemetry_semantic_conventions::attribute::{RPC_METHOD, URL_DOMAIN, URL_TEMPLATE};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// A tentative name for the error logs.
pub const NAME: &str = "experimental.client.request.error";
// A tentative target for the error logs.
pub const TARGET: &str = "experimental.client.request";

/// A future instrumented to generate the client request logs.
///
/// Decorates the `F` future, which represents a pending client request,
/// to emit the error logs. Typically this is used in the tracing layer:
///
/// ```ignore
/// # struct Client;
/// # impl Client {
/// #[tracing::instrument(level = tracing::Level::DEBUG, ret)]
/// async fn echo(
///     &self,
///     req: crate::model::EchoRequest,
///     options: crate::RequestOptions,
/// ) -> Result<crate::Response<crate::model::EchoResponse>> {
///     use google_cloud_gax_internal::observability::client_signals::WithClientLogging;
///     let pending = self.inner.echo(req, options);
///     WithClientLogging::new(pending).await
/// }
/// # }
/// ```
///
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WithClientLogging<F> {
    #[pin]
    inner: F,
}

impl<F, R> WithClientLogging<F>
where
    F: Future<Output = Result<R, Error>>,
{
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F, R> Future for WithClientLogging<F>
where
    F: Future<Output = Result<R, Error>>,
{
    type Output = <F as Future>::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let output = futures::ready!(this.inner.poll(cx));
        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return Poll::Ready(output);
        };
        match &output {
            Ok(_) => (),
            Err(error) => {
                let rpc_status_code = error.status().map(|s| s.code.name());
                let error_type = ErrorType::from_gax_error(error);
                let error_info = error.status().and_then(|s| {
                    s.details.iter().find_map(|d| match d {
                        google_cloud_gax::error::rpc::StatusDetails::ErrorInfo(i) => Some(i),
                        _ => None,
                    })
                });
                let error_domain = error_info.map(|i| i.domain.as_str());
                let error_metadata = error_info.and_then(|i| {
                    if i.metadata.is_empty() {
                        None
                    } else {
                        serde_json::to_string(&i.metadata).ok()
                    }
                });

                // TODO(#4795) - use the correct name and target
                tracing::event!(
                    name: NAME,
                    target: TARGET,
                    tracing::Level::WARN,
                    { RPC_SYSTEM_NAME } = snapshot.rpc_system(),
                    { RPC_SERVICE } = snapshot.service_name(),
                    { RPC_METHOD } = snapshot.rpc_method(),
                    { GCP_CLIENT_VERSION } = snapshot.client_version(),
                    { GCP_CLIENT_REPO } = snapshot.client_repo(),
                    { GCP_CLIENT_ARTIFACT } = snapshot.client_artifact(),
                    { URL_DOMAIN } = snapshot.default_host(),
                    // TODO(#5152) - sanitize the URL.
                    { URL_FULL } = snapshot.sanitized_url(),
                    { URL_TEMPLATE } = snapshot.url_template(),
                    { RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                    { ERROR_TYPE } = error_type.as_str(),
                    { SERVER_ADDRESS } = snapshot.server_address(),
                    { SERVER_PORT } = snapshot.server_port(),
                    { HTTP_REQUEST_METHOD } = snapshot.http_method(),
                    { HTTP_REQUEST_RESEND_COUNT } = snapshot.http_resend_count(),
                    { GCP_CLIENT_SERVICE } = snapshot.service_name(),
                    { GCP_ERRORS_DOMAIN } = error_domain,
                    { GCP_ERRORS_METADATA } = error_metadata,
                    "{error:?}"
                );
            }
        }
        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        TEST_INFO, TEST_METHOD, TEST_URL_TEMPLATE, recorded_request_transport_stub,
    };
    use super::*;
    use google_cloud_test_utils::tracing::Buffer;
    use httptest::Expectation;
    use httptest::Server;
    use httptest::matchers::request::method_path;
    use httptest::responders::status_code;
    use pretty_assertions::assert_eq;
    use serde_json::Value;
    use serde_json::json;
    use tracing::Level;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::fmt::format::FmtSpan;

    #[tokio::test]
    async fn no_recorder() -> anyhow::Result<()> {
        let (_guard, buffer) = capture_logs();

        let logging = WithClientLogging::new(async { Ok(123) });
        let got = logging.await;
        assert!(matches!(got, Ok(123)), "{got:?}");
        let contents = String::from_utf8(buffer.captured())?;
        assert!(contents.is_empty(), "{contents}");
        Ok(())
    }

    #[tokio::test]
    async fn ok() -> anyhow::Result<()> {
        let (_guard, buffer) = capture_logs();

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        let logging = WithClientLogging::new(async {
            let _current =
                RequestRecorder::current().expect("current recorder should be available");
            Ok(123)
        });
        let got = scoped.scope(logging).await;
        assert!(matches!(got, Ok(123)), "{got:?}");
        let contents = String::from_utf8(buffer.captured())?;
        assert!(contents.is_empty(), "{contents}");
        Ok(())
    }

    #[tokio::test]
    async fn error_with_partial_recorder() -> anyhow::Result<()> {
        const BAD_URL: &str = "https://127.0.0.1:1";

        let (_guard, buffer) = capture_logs();
        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        let logging = WithClientLogging::new(recorded_request_transport_stub(BAD_URL));
        let got = scoped.scope(logging).await;
        assert!(got.is_err(), "{got:?}");
        let parsed = extract_captured_log(buffer)?;
        let mut object = parsed
            .as_object()
            .unwrap_or_else(|| panic!("error is serialized as JSON object, got: {parsed:?}"))
            .clone();
        // Extract the fields to check them in detail later.
        let mut fields = object
            .remove("fields")
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_else(|| panic!("serialized error should have fields"));
        assert!(object.remove("timestamp").is_some(), "{parsed:?}");
        let want = json!({
            "level": "WARN",
            "target": "experimental.client.request",
        });
        assert_eq!(Some(&object), want.as_object(), "{parsed:?}");

        // Don't care about the formatted message, this is not a test for Error formatting.
        assert!(fields.remove("message").is_some(), "{parsed:?}");
        let want = json!({
            "error.type": "CLIENT_CONNECTION_ERROR",
            "rpc.system.name": "http",
            "rpc.method": TEST_METHOD,
            "rpc.service": "test-service",
            "url.domain": "example.com",
            "url.template": TEST_URL_TEMPLATE,
            "gcp.client.artifact": "test-artifact",
            "gcp.client.repo": "googleapis/google-cloud-rust",
            "gcp.client.version": "1.2.3",
            "gcp.client.service": "test-service",
            "url.full": format!("{}/", BAD_URL),
            "server.address": "127.0.0.1",
            "server.port": 1,
            "http.request.method": "GET",
        });
        assert_eq!(Some(&fields), want.as_object(), "{parsed:?}");

        Ok(())
    }

    #[tokio::test]
    async fn error_with_full_recorder() -> anyhow::Result<()> {
        let (_guard, buffer) = capture_logs();

        const PATH: &str = "/v1/projects/test-only:test_method";

        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", PATH))
                .respond_with(status_code(404).body("NOT FOUND")),
        );
        let url = server.url(PATH).to_string();

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        let got = scoped
            .scope(WithClientLogging::new(recorded_request_transport_stub(
                &url,
            )))
            .await;
        assert!(matches!(got, Err(ref e) if e.is_transport()), "{got:?}");
        let parsed = extract_captured_log(buffer)?;
        let mut object = parsed
            .as_object()
            .unwrap_or_else(|| panic!("error is serialized as JSON object, got: {parsed:?}"))
            .clone();
        // Extract the fields to check them in detail later.
        let mut fields = object
            .remove("fields")
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_else(|| panic!("serialized error should have fields"));
        // Don't care about the timestamp value, just that it exists.
        assert!(object.remove("timestamp").is_some(), "{parsed:?}");
        let want = json!({
            "level": "WARN",
            "target": "experimental.client.request",
        });
        assert_eq!(Some(&object), want.as_object(), "{parsed:?}");

        // Don't care about the formatted message, this is not a test for Error formatting.
        assert!(fields.remove("message").is_some(), "{parsed:?}");
        let want = json!({
            "rpc.system.name": "http",
            "rpc.method": TEST_METHOD,
            "rpc.service": "test-service",
            "url.domain": "example.com",
            "url.template": TEST_URL_TEMPLATE,
            "error.type": "404",
            "gcp.client.artifact": "test-artifact",
            "gcp.client.repo": "googleapis/google-cloud-rust",
            "gcp.client.version": "1.2.3",
            "gcp.client.service": "test-service",
            "url.full": url,
            "server.address": server.addr().ip().to_string(),
            "server.port": server.addr().port(),
            "http.request.method": "GET",
        });
        assert_eq!(Some(&fields), want.as_object(), "{parsed:?}");

        Ok(())
    }

    fn capture_logs() -> (DefaultGuard, Buffer) {
        let buffer = Buffer::default();
        let make_writer = {
            let shared = buffer.clone();
            move || shared.clone()
        };
        let subscriber = tracing_subscriber::fmt()
            .with_span_events(FmtSpan::NONE)
            .with_level(true)
            .with_writer(make_writer)
            .json()
            .with_max_level(Level::WARN)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (guard, buffer)
    }

    #[track_caller]
    fn extract_captured_log(buffer: Buffer) -> anyhow::Result<Value> {
        let contents = String::from_utf8(buffer.captured())?;
        let mut s = contents.split('\n');
        let parsed = match (s.next(), s.next(), s.next()) {
            (Some(line), Some(""), None) => serde_json::from_str::<Value>(line)?,
            _ => panic!("unexpected number of lines: {contents}"),
        };
        Ok(parsed)
    }
}
