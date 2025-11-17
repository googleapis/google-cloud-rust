// Copyright 2025 Google LLC
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

use crate::observability::attributes::{self, keys::*, otel_status_codes};
use crate::observability::errors::ErrorType;
use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// A wrapper for the attempt count to be stored in request extensions.
#[derive(Clone, Copy, Debug)]
pub struct AttemptCount(pub i64);

/// A Tower layer that adds structured tracing to gRPC requests that is compatible with OpenTelemetry.
///
/// This layer is responsible for wrapping the inner service with a
/// [`TracingTowerService`], which intercepts requests and creates tracing spans.
///
/// It is typically used with [`tower::ServiceBuilder`] to add tracing middleware
/// to a gRPC client.
#[derive(Clone, Debug, Default)]
pub struct TracingTowerLayer {
    inner: Arc<TracingTowerLayerInner>,
}

#[derive(Debug, Default)]
struct TracingTowerLayerInner {
    server_address: String,
    server_port: Option<i64>,
    url_domain: String,
    instrumentation: Option<&'static crate::options::InstrumentationClientInfo>,
}

impl TracingTowerLayer {
    /// Creates a new `TracingTowerLayer`.
    pub fn new(
        uri: &http::Uri,
        default_domain: String,
        instrumentation: Option<&'static crate::options::InstrumentationClientInfo>,
    ) -> Self {
        let host = uri.host().unwrap_or("").to_string();
        let port = uri.port_u16().or_else(|| match uri.scheme_str() {
            Some("https") => Some(443),
            Some("http") => Some(80),
            _ => None,
        });
        Self {
            inner: Arc::new(TracingTowerLayerInner {
                server_address: host,
                server_port: port.map(|p| p as i64),
                url_domain: default_domain,
                instrumentation,
            }),
        }
    }
}

impl<S> Layer<S> for TracingTowerLayer {
    type Service = TracingTowerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingTowerService {
            inner,
            layer: self.clone(),
        }
    }
}

/// A Tower service that intercepts gRPC requests to create tracing spans.
///
/// This service wraps an inner service and instruments the returned future with
/// a tracing span. The span is named "grpc.request" and is created at the `INFO`
/// level.
#[derive(Clone, Debug)]
pub struct TracingTowerService<S> {
    inner: S,
    layer: TracingTowerLayer,
}

impl<S, B, ResBody> Service<http::Request<B>> for TracingTowerService<S>
where
    S: Service<http::Request<B>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let attempt_count = req.extensions().get::<AttemptCount>().map(|a| a.0);
        let span = create_grpc_span(req.uri(), &self.layer.inner, attempt_count);
        let future = self.inner.call(req);
        ResponseFuture {
            inner: future,
            span,
        }
    }
}

/// A future that wraps the inner service's future and records the status code on completion.
#[pin_project::pin_project]
pub struct ResponseFuture<F> {
    #[pin]
    inner: F,
    span: tracing::Span,
}

impl<F, ResBody, Error> Future for ResponseFuture<F>
where
    F: Future<Output = Result<http::Response<ResBody>, Error>>,
    Error: std::fmt::Display,
{
    type Output = Result<http::Response<ResBody>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.span.enter();

        // Note: `futures_util::ready!` will immediately return `Poll::Pending` if the future is not ready.
        let result = futures_util::ready!(this.inner.poll(cx));

        match &result {
            Ok(response) => record_response_status(this.span, response),
            Err(e) => record_error_status(this.span, e),
        }
        Poll::Ready(result)
    }
}

fn record_response_status<ResBody>(span: &tracing::Span, response: &http::Response<ResBody>) {
    // Check for "OK" status (missing or "0") directly to avoid
    // the potential overhead of `tonic::Status::from_header_map` (parsing, decoding) in the success path.
    if response
        .headers()
        .get("grpc-status")
        .is_none_or(|v| v == "0")
    {
        span.record(otel_attr::RPC_GRPC_STATUS_CODE, 0_i64);
        return;
    }

    // This is also (eventually) called in Tonic before returning a result in the API, but it's important to
    // include any error information inside the span (with API-level detail).
    if let Some(status) = tonic::Status::from_header_map(response.headers()) {
        let code = status.code();
        span.record(otel_attr::RPC_GRPC_STATUS_CODE, i32::from(code) as i64);
        if code != tonic::Code::Ok {
            span.record(OTEL_STATUS_CODE, otel_status_codes::ERROR);
            let gax_error = crate::grpc::from_status::to_gax_error(status);
            span.record(
                otel_trace::ERROR_TYPE,
                ErrorType::from_gax_error(&gax_error).as_str(),
            );
        }
    }
}

fn record_error_status<Error: std::fmt::Display>(span: &tracing::Span, error: &Error) {
    span.record(OTEL_STATUS_CODE, otel_status_codes::ERROR);
    let gax_error = gax::error::Error::io(error.to_string());
    span.record(
        otel_trace::ERROR_TYPE,
        ErrorType::from_gax_error(&gax_error).as_str(),
    );
    span.record(OTEL_STATUS_DESCRIPTION, error.to_string());
}

fn create_grpc_span(
    uri: &http::Uri,
    layer_inner: &TracingTowerLayerInner,
    attempt_count: Option<i64>,
) -> tracing::Span {
    let (rpc_service, rpc_method) =
        parse_method(uri.path()).unwrap_or_else(|_| ("unknown".to_string(), "unknown".to_string()));
    let span_name = uri.path().trim_start_matches('/');

    let (service, version, repo, artifact) = if let Some(info) = layer_inner.instrumentation {
        (
            Some(info.service_name),
            Some(info.client_version),
            Some("googleapis/google-cloud-rust"),
            Some(info.client_artifact),
        )
    } else {
        (None, None, None, None)
    };

    let resend_count = attempt_count.filter(|&c| c > 0);

    let span = tracing::info_span!(
        "grpc.request",
        { OTEL_NAME } = span_name,
        { otel_trace::RPC_SYSTEM } = attributes::RPC_SYSTEM_GRPC,
        { OTEL_KIND } = attributes::OTEL_KIND_CLIENT,
        { otel_trace::RPC_SERVICE } = rpc_service,
        { otel_trace::RPC_METHOD } = rpc_method,
        { otel_trace::SERVER_ADDRESS } = layer_inner.server_address,
        { otel_trace::SERVER_PORT } = layer_inner.server_port,
        { otel_attr::URL_DOMAIN } = layer_inner.url_domain,
        // Standard attributes that will be populated later
        { otel_attr::RPC_GRPC_STATUS_CODE } = tracing::field::Empty,
        { GRPC_STATUS } = tracing::field::Empty,
        { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
        { OTEL_STATUS_DESCRIPTION } = tracing::field::Empty,
        { otel_trace::ERROR_TYPE } = tracing::field::Empty,
        // Client library metadata
        { GCP_CLIENT_SERVICE } = service,
        { GCP_CLIENT_VERSION } = version,
        { GCP_CLIENT_REPO } = repo,
        { GCP_CLIENT_ARTIFACT } = artifact,
        { GCP_GRPC_RESEND_COUNT } = resend_count,
    );

    span
}

fn parse_method(path: &str) -> Result<(String, String), &'static str> {
    let path = path.trim_start_matches('/');
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() == 2 {
        Ok((parts[0].to_string(), parts[1].to_string()))
    } else {
        Err("invalid path format")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use std::collections::HashMap;

    #[test]
    fn test_parse_method() {
        assert_eq!(
            parse_method("/google.pubsub.v1.Publisher/Publish"),
            Ok((
                "google.pubsub.v1.Publisher".to_string(),
                "Publish".to_string()
            ))
        );
        assert_eq!(
            parse_method("google.pubsub.v1.Publisher/Publish"),
            Ok((
                "google.pubsub.v1.Publisher".to_string(),
                "Publish".to_string()
            ))
        );
        assert!(parse_method("/invalid/path/format").is_err());
        assert!(parse_method("invalid").is_err());
    }

    #[test]
    fn test_layer_new() {
        let uri = "https://pubsub.googleapis.com".parse().unwrap();
        let layer = TracingTowerLayer::new(&uri, "pubsub.googleapis.com".to_string(), None);
        assert_eq!(layer.inner.server_address, "pubsub.googleapis.com");
        assert_eq!(layer.inner.server_port, Some(443));
        assert_eq!(layer.inner.url_domain, "pubsub.googleapis.com");

        let uri = "http://localhost:8080".parse().unwrap();
        let layer = TracingTowerLayer::new(&uri, "localhost".to_string(), None);
        assert_eq!(layer.inner.server_address, "localhost");
        assert_eq!(layer.inner.server_port, Some(8080));
        assert_eq!(layer.inner.url_domain, "localhost");
    }

    #[test]
    fn test_layer_new_with_different_domain() {
        let uri = "http://localhost:8080".parse().unwrap();
        let layer = TracingTowerLayer::new(&uri, "example.com".to_string(), None);
        assert_eq!(layer.inner.server_address, "localhost");
        assert_eq!(layer.inner.server_port, Some(8080));
        assert_eq!(layer.inner.url_domain, "example.com");
    }

    #[test]
    fn test_layer_new_schemes() {
        let uri = "http://example.com".parse().unwrap();
        let layer = TracingTowerLayer::new(&uri, "example.com".to_string(), None);
        assert_eq!(layer.inner.server_port, Some(80));

        let uri = "ftp://example.com".parse().unwrap();
        let layer = TracingTowerLayer::new(&uri, "example.com".to_string(), None);
        assert_eq!(layer.inner.server_port, None);
    }

    #[test]
    fn test_create_grpc_span() {
        let guard = TestLayer::initialize();
        let uri = http::Uri::from_static(
            "https://pubsub.googleapis.com/google.pubsub.v1.Publisher/Publish",
        );
        let endpoint_uri = "https://pubsub.googleapis.com".parse().unwrap();
        let layer =
            TracingTowerLayer::new(&endpoint_uri, "pubsub.googleapis.com".to_string(), None);
        // First attempt (0) should not have resend count
        let _span = create_grpc_span(&uri, &layer.inner, Some(0));

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1);
        let span = &captured[0];
        assert_eq!(span.name, "grpc.request");

        let expected_attributes: HashMap<String, AttributeValue> = [
            (OTEL_NAME, "google.pubsub.v1.Publisher/Publish".into()),
            (
                otel_trace::RPC_SYSTEM,
                crate::observability::attributes::RPC_SYSTEM_GRPC.into(),
            ),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.pubsub.v1.Publisher".into()),
            (otel_trace::RPC_METHOD, "Publish".into()),
            (otel_trace::SERVER_ADDRESS, "pubsub.googleapis.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_attr::URL_DOMAIN, "pubsub.googleapis.com".into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(span.attributes, expected_attributes);
    }

    #[test]
    fn test_create_grpc_span_with_metadata() {
        use crate::options::InstrumentationClientInfo;
        let guard = TestLayer::initialize();
        let uri = http::Uri::from_static(
            "https://pubsub.googleapis.com/google.pubsub.v1.Publisher/Publish",
        );
        let endpoint_uri = "https://pubsub.googleapis.com".parse().unwrap();

        static TEST_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
            service_name: "test-service",
            client_version: "1.0.0",
            client_artifact: "test-artifact",
            default_host: "example.com",
        };

        let layer = TracingTowerLayer::new(
            &endpoint_uri,
            "pubsub.googleapis.com".to_string(),
            Some(&TEST_INFO),
        );
        // Retry attempt (1) should have resend count 1
        let _span = create_grpc_span(&uri, &layer.inner, Some(1));

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1);
        let span = &captured[0];
        assert_eq!(span.name, "grpc.request");

        let expected_attributes: HashMap<String, AttributeValue> = [
            (OTEL_NAME, "google.pubsub.v1.Publisher/Publish".into()),
            (
                otel_trace::RPC_SYSTEM,
                crate::observability::attributes::RPC_SYSTEM_GRPC.into(),
            ),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.pubsub.v1.Publisher".into()),
            (otel_trace::RPC_METHOD, "Publish".into()),
            (otel_trace::SERVER_ADDRESS, "pubsub.googleapis.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_attr::URL_DOMAIN, "pubsub.googleapis.com".into()),
            (GCP_CLIENT_SERVICE, "test-service".into()),
            (GCP_CLIENT_VERSION, "1.0.0".into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_ARTIFACT, "test-artifact".into()),
            (GCP_GRPC_RESEND_COUNT, 1_i64.into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(span.attributes, expected_attributes);
    }
}
