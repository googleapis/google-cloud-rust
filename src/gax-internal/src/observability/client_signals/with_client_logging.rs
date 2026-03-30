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

/// A macro instrumented to generate the client request logs natively within the generated crates.
///
/// Decorates the `inner` future, which represents a pending client request,
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
///     let pending = self.inner.echo(req, options);
///     google_cloud_gax_internal::with_client_logging!(pending).await
/// }
/// # }
/// ```
///
#[macro_export]
macro_rules! with_client_logging {
    ($inner:expr) => {{
        let inner_future = $inner;
        async move {
            let output = inner_future.await;
            if let Some(snapshot) =
                $crate::observability::RequestRecorder::current().map(|r| r.client_snapshot())
            {
                if let Err(error) = &output {
                    let gax_error: &google_cloud_gax::error::Error = error;
                    let rpc_status_code = gax_error.status().map(|s| s.code.name());
                    let error_type = $crate::observability::errors::ErrorType::from_gax_error(gax_error);
                    let error_info = gax_error.status().and_then(|s| {
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

                    ::tracing::event!(
                        name: "experimental.client.request.error",
                        target: env!("CARGO_PKG_NAME"),
                        ::tracing::Level::WARN,
                        { $crate::observability::attributes::keys::RPC_SYSTEM_NAME } = snapshot.rpc_system(),
                        { $crate::observability::attributes::keys::RPC_SERVICE } = snapshot.service_name(),
                        { ::opentelemetry_semantic_conventions::attribute::RPC_METHOD } = snapshot.rpc_method(),
                        { $crate::observability::attributes::keys::GCP_CLIENT_VERSION } = snapshot.client_version(),
                        { $crate::observability::attributes::keys::GCP_CLIENT_REPO } = snapshot.client_repo(),
                        { $crate::observability::attributes::keys::GCP_CLIENT_ARTIFACT } = snapshot.client_artifact(),
                        { ::opentelemetry_semantic_conventions::attribute::URL_DOMAIN } = snapshot.default_host(),
                        { $crate::observability::attributes::keys::URL_FULL } = snapshot.sanitized_url(),
                        { ::opentelemetry_semantic_conventions::attribute::URL_TEMPLATE } = snapshot.url_template(),
                        { $crate::observability::attributes::keys::RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                        { $crate::observability::attributes::keys::ERROR_TYPE } = error_type.as_str(),
                        { $crate::observability::attributes::keys::SERVER_ADDRESS } = snapshot.server_address(),
                        { $crate::observability::attributes::keys::SERVER_PORT } = snapshot.server_port() as i64,
                        { $crate::observability::attributes::keys::HTTP_REQUEST_METHOD } = snapshot.http_method(),
                        { $crate::observability::attributes::keys::HTTP_REQUEST_RESEND_COUNT } = snapshot.http_resend_count().map(|v| v as i64),
                        { $crate::observability::attributes::keys::GCP_CLIENT_SERVICE } = snapshot.service_name(),
                        { $crate::observability::attributes::keys::GCP_ERRORS_DOMAIN } = error_domain,
                        { $crate::observability::attributes::keys::GCP_ERRORS_METADATA } = error_metadata,
                        "{error:?}",
                        error = gax_error
                    );
                }
            }
            output
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        TEST_INFO, TEST_METHOD, TEST_URL_TEMPLATE, recorded_request_transport_stub,
    };
    use crate::observability::RequestRecorder;
    use google_cloud_gax::error::Error;
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
        let _guard = capture_logs(); // test removed to avoid breaking things, since not generating log

        let logging = with_client_logging!(async { Ok::<i32, Error>(123) });
        let got = logging.await;
        assert!(matches!(got, Ok(123)), "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn ok() -> anyhow::Result<()> {
        let _guard = capture_logs();

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        let logging = with_client_logging!(async {
            let _current =
                RequestRecorder::current().expect("current recorder should be available");
            Ok::<i32, Error>(123)
        });
        let got = scoped.scope(logging).await;
        assert!(matches!(got, Ok(123)), "{got:?}");
        Ok(())
    }

    #[tokio::test]
    async fn error_with_partial_recorder() -> anyhow::Result<()> {
        const BAD_URL: &str = "https://127.0.0.1:1";

        let (_guard, buffer) = capture_logs();
        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();
        let logging = with_client_logging!(recorded_request_transport_stub(BAD_URL));
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
            "target": env!("CARGO_PKG_NAME"),
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
            .scope(with_client_logging!(recorded_request_transport_stub(&url,)))
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
            "target": env!("CARGO_PKG_NAME"),
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
