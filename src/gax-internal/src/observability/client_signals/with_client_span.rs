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

//! Implements [WithClientSpan] a decorator for [Future] adding span attributes.
//!
//! This is a private module, it is not exposed in the public API.

use super::RequestRecorder;
use crate::observability::attributes::SCHEMA_URL_VALUE;
use crate::observability::attributes::keys::{
    ERROR_TYPE, GCP_CLIENT_ARTIFACT, GCP_CLIENT_REPO, GCP_CLIENT_VERSION, GCP_SCHEMA_URL,
    HTTP_REQUEST_METHOD, HTTP_REQUEST_RESEND_COUNT, NETWORK_PEER_ADDRESS, NETWORK_PEER_PORT,
    OTEL_STATUS_CODE, OTEL_STATUS_DESCRIPTION, RPC_RESPONSE_STATUS_CODE, RPC_SYSTEM,
};
use crate::observability::attributes::otel_status_codes;
use crate::observability::errors::ErrorType;
use google_cloud_gax::error::Error;
use opentelemetry_semantic_conventions::attribute::{
    HTTP_RESPONSE_STATUS_CODE, RPC_METHOD, SERVER_ADDRESS, SERVER_PORT, URL_FULL,
};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::Span;

/// A future instrumented to add span attributes.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WithClientSpan<F> {
    #[pin]
    inner: F,
    span: Span,
}

impl<F, R> WithClientSpan<F>
where
    F: Future<Output = Result<R, Error>>,
{
    pub fn new(span: Span, inner: F) -> Self {
        Self { inner, span }
    }
}

impl<F, R> Future for WithClientSpan<F>
where
    F: Future<Output = Result<R, Error>>,
{
    type Output = <F as Future>::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let span = self.span.clone();
        let this = self.project();
        let output = futures::ready!(this.inner.poll(cx));

        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return Poll::Ready(output);
        };

        match &output {
            Ok(_) => {
                tracing::record_all!(
                    span,
                    { RPC_SYSTEM } = snapshot.rpc_system(),
                    { RPC_METHOD } = snapshot.rpc_method(),
                    { GCP_CLIENT_VERSION } = snapshot.client_version(),
                    { GCP_CLIENT_REPO } = snapshot.client_repo(),
                    { GCP_CLIENT_ARTIFACT } = snapshot.client_artifact(),
                    { GCP_SCHEMA_URL } = SCHEMA_URL_VALUE,
                    { URL_FULL } = snapshot.sanitized_url(),
                    { SERVER_ADDRESS } = snapshot.server_address(),
                    { SERVER_PORT } = snapshot.server_port() as i64,
                    { NETWORK_PEER_ADDRESS } = snapshot.network_peer_address(),
                    { NETWORK_PEER_PORT } = snapshot.network_peer_port(),
                    { HTTP_RESPONSE_STATUS_CODE } = snapshot.http_status_code().map(|v| v as i64),
                    { HTTP_REQUEST_METHOD } = snapshot.http_method(),
                    { HTTP_REQUEST_RESEND_COUNT } = snapshot.http_resend_count().map(|v| v as i64),
                    { OTEL_STATUS_CODE } = otel_status_codes::UNSET
                );
            }
            Err(error) => {
                let rpc_status_code = error.status().map(|s| s.code.name());
                let error_type = ErrorType::from_gax_error(error);

                tracing::record_all!(
                    span,
                    { RPC_SYSTEM } = snapshot.rpc_system(),
                    { RPC_METHOD } = snapshot.rpc_method(),
                    { GCP_CLIENT_VERSION } = snapshot.client_version(),
                    { GCP_CLIENT_REPO } = snapshot.client_repo(),
                    { GCP_CLIENT_ARTIFACT } = snapshot.client_artifact(),
                    { GCP_SCHEMA_URL } = SCHEMA_URL_VALUE,
                    { URL_FULL } = snapshot.sanitized_url(),
                    { RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                    { ERROR_TYPE } = error_type.as_str(),
                    { SERVER_ADDRESS } = snapshot.server_address(),
                    { SERVER_PORT } = snapshot.server_port() as i64,
                    { NETWORK_PEER_ADDRESS } = snapshot.network_peer_address(),
                    { NETWORK_PEER_PORT } = snapshot.network_peer_port(),
                    { HTTP_RESPONSE_STATUS_CODE } = snapshot.http_status_code().map(|v| v as i64),
                    { HTTP_REQUEST_METHOD } = snapshot.http_method(),
                    { HTTP_REQUEST_RESEND_COUNT } = snapshot.http_resend_count().map(|v| v as i64),
                    { OTEL_STATUS_CODE } = otel_status_codes::ERROR,
                    { OTEL_STATUS_DESCRIPTION } = error.to_string()
                );
            }
        }
        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        SignalProviders, TEST_INFO, TEST_METHOD, TEST_URL_TEMPLATE, recorded_request_transport_stub,
    };
    use super::*;
    use crate::observability::ClientRequestAttributes;
    use crate::observability::attributes::GCP_CLIENT_REPO_GOOGLEAPIS;
    use httptest::{Expectation, Server, matchers::request::method_path, responders::status_code};
    use opentelemetry::trace::{SpanKind, Status};
    use pretty_assertions::{Comparison, assert_eq};
    use std::collections::BTreeSet;
    use std::future::ready;

    #[tokio::test(start_paused = true)]
    async fn poll_ok() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let span = crate::client_request_signals!(info: TEST_INFO, method: "__test__");

        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_rpc_method(TEST_METHOD)
                .set_url_template(TEST_URL_TEMPLATE)
                .set_resource_name("//test.googleapis.com/test-only".to_string()),
        );

        let request = reqwest::Client::new().get("https://example.com/").build()?;
        recorder.on_http_request(&request);

        let scoped = recorder.clone();
        let future = ready(Ok::<String, Error>("hello world".to_string()));
        let future = scoped.scope(WithClientSpan::new(span.clone(), future));
        let result = future.await;
        assert!(
            matches!(result, Ok(ref s) if s == "hello world"),
            "{result:?}"
        );

        drop(span);
        providers.force_flush()?;
        let captured = providers.trace_exporter.get_finished_spans()?;
        let record = match &captured[..] {
            [record] => record,
            _ => panic!("expected a single capture: {captured:#?}"),
        };

        let got = BTreeSet::from_iter(
            record
                .attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );
        assert_eq!(record.name, concat!(env!("CARGO_CRATE_NAME"), "::__test__"));
        assert_eq!(record.span_kind, SpanKind::Internal);
        assert_eq!(record.status, Status::Unset);
        let want = BTreeSet::from_iter(
            [
                ("rpc.system", "http"),
                ("gcp.client.service", TEST_INFO.service_name),
                ("gcp.client.repo", GCP_CLIENT_REPO_GOOGLEAPIS),
                ("gcp.client.artifact", TEST_INFO.client_artifact),
                ("gcp.client.version", TEST_INFO.client_version),
                ("rpc.method", "google.test.v1.Service/TestMethod"),
                ("http.request.method", "GET"),
                ("server.address", "example.com"),
                ("server.port", "443"),
                ("url.full", "https://example.com/"),
            ]
            .map(|(k, v)| (k, v.to_string())),
        );
        let diff = want.difference(&got).collect::<Vec<_>>();
        assert!(
            diff.is_empty(),
            "diff={diff:?}\n{}",
            Comparison::new(&want, &got)
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn poll_err() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let span = crate::client_request_signals!(info: TEST_INFO, method: "__test__");

        const PATH: &str = "/v1/projects/test-only:test_method";
        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", PATH))
                .respond_with(status_code(404).body("NOT FOUND")),
        );
        let url = server.url(PATH).to_string();

        let recorder = RequestRecorder::new(TEST_INFO);
        let scoped = recorder.clone();

        let future = recorded_request_transport_stub(&url);
        let future = scoped.scope(WithClientSpan::new(span.clone(), future));
        let result = future.await;
        assert!(
            matches!(result, Err(ref e) if e.is_transport()),
            "{result:?}"
        );

        drop(span);
        providers.force_flush()?;
        let captured = providers.trace_exporter.get_finished_spans()?;
        let record = match &captured[..] {
            [record] => record,
            _ => panic!("expected a single capture: {captured:#?}"),
        };

        // Assert some key attributes
        let got = BTreeSet::from_iter(
            record
                .attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );
        assert_eq!(record.name, concat!(env!("CARGO_CRATE_NAME"), "::__test__"));
        assert_eq!(record.span_kind, SpanKind::Internal);
        assert!(
            matches!(record.status, Status::Error { ref description } if description.contains("SIMULATED NOT FOUND")),
            "{record:#?}"
        );
        let want = BTreeSet::from_iter(
            [
                ("rpc.system", "http"),
                ("gcp.client.service", TEST_INFO.service_name),
                ("gcp.client.repo", GCP_CLIENT_REPO_GOOGLEAPIS),
                ("gcp.client.artifact", TEST_INFO.client_artifact),
                ("gcp.client.version", TEST_INFO.client_version),
                ("rpc.method", "google.test.v1.Service/TestMethod"),
                ("error.type", "404"),
                ("http.request.method", "GET"),
                ("http.response.status_code", "404"),
            ]
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .chain([
                ("server.address", server.addr().ip().to_string()),
                ("server.port", server.addr().port().to_string()),
                ("url.full", url.to_string()),
            ]),
        );
        let diff = want.difference(&got).collect::<Vec<_>>();
        assert!(
            diff.is_empty(),
            "diff={diff:?}\n{}",
            Comparison::new(&want, &got)
        );

        Ok(())
    }
}
