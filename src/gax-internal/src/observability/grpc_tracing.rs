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
pub struct AttemptCount(i64);

impl AttemptCount {
    pub fn new(value: i64) -> Self {
        Self(value)
    }
    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

/// A wrapper for the resource name to be stored in request extensions.
#[derive(Clone, Debug)]
pub struct ResourceName(String);

impl ResourceName {
    pub fn new(value: String) -> Self {
        Self(value)
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// A type alias for the response body that can be either an instrumented body or a raw body.
///
/// This allows us to return a single type from both the tracing and non-tracing paths
/// without using `Box<dyn Body>`, which avoids heap allocation and dynamic dispatch overhead.
///
/// * `Either::Left`: The body is wrapped in `InstrumentedBody` (tracing enabled).
/// * `Either::Right`: The body is the raw `B` (tracing disabled).
pub type OptionallyTracedBody<B> = http_body_util::Either<InstrumentedBody<B>, B>;

/// A wrapper around the response body that keeps the span active while streaming.
#[pin_project::pin_project]
pub struct InstrumentedBody<B> {
    #[pin]
    inner: B,
    span: tracing::Span,
}

impl<B> http_body::Body for InstrumentedBody<B>
where
    B: http_body::Body<Data = bytes::Bytes, Error = tonic::Status>,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, B::Error>>> {
        let this = self.project();
        let _guard = this.span.enter();
        let result = futures::ready!(this.inner.poll_frame(cx));

        match &result {
            Some(Ok(frame)) => {
                if let Some(trailers) = frame.trailers_ref() {
                    record_status_from_headers(this.span, trailers);
                }
            }
            Some(Err(e)) => {
                record_error_status(this.span, e);
            }
            None => {}
        }
        Poll::Ready(result)
    }
}

impl<B> InstrumentedBody<B> {
    pub fn new(inner: B, span: tracing::Span) -> Self {
        Self { inner, span }
    }
}

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
    ResBody: http_body::Body<Data = bytes::Bytes, Error = tonic::Status> + Send + 'static,
{
    // We return `OptionallyTracedBody` which is `Either<InstrumentedBody<ResBody>, ResBody>`.
    // In this case (TracingTowerService), we always return `Either::Left(InstrumentedBody)`.
    type Response = http::Response<OptionallyTracedBody<ResBody>>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let attempt_count = req.extensions().get::<AttemptCount>().map(|a| a.as_i64());
        let resource_name = req.extensions().get::<ResourceName>().map(|r| r.as_str());
        let span = create_grpc_span(req.uri(), &self.layer.inner, attempt_count, resource_name);
        let future = self.inner.call(req);
        ResponseFuture {
            inner: future,
            span,
            completed: false,
        }
    }
}

/// A service that wraps the response body in `Either::Right` to match the `OptionallyTracedBody` type.
/// Used to unify the response type with `TracingTowerService` when tracing is disabled.
#[derive(Clone, Debug, Default)]
pub struct NoTracingTowerLayer;

impl<S> Layer<S> for NoTracingTowerLayer {
    type Service = NoTracingTowerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NoTracingTowerService::new(inner)
    }
}

#[derive(Clone, Debug)]
pub struct NoTracingTowerService<S> {
    inner: S,
}

impl<S> NoTracingTowerService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, Req, ResBody> Service<Req> for NoTracingTowerService<S>
where
    S: Service<Req, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: http_body::Body<Data = bytes::Bytes, Error = tonic::Status> + Send + 'static,
{
    // We return `OptionallyTracedBody` which is `Either<InstrumentedBody<ResBody>, ResBody>`.
    // In this case (NoTracingTowerService), we always return `Either::Right(ResBody)`.
    // This matches the return type of `TracingTowerService`, allowing them to be used interchangeably
    // in `tower::util::Either` without boxing.
    type Response = http::Response<OptionallyTracedBody<ResBody>>;
    type Error = S::Error;
    type Future = NoTracingFuture<S::Future, ResBody>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        NoTracingFuture {
            inner: self.inner.call(req),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[pin_project::pin_project]
pub struct NoTracingFuture<F, B> {
    #[pin]
    inner: F,
    _phantom: std::marker::PhantomData<B>,
}

impl<F, B, E> Future for NoTracingFuture<F, B>
where
    F: Future<Output = Result<http::Response<B>, E>>,
    B: http_body::Body<Data = bytes::Bytes, Error = tonic::Status> + Send + 'static,
{
    type Output = Result<http::Response<OptionallyTracedBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use http_body_util::Either;
        let this = self.project();
        let result = futures::ready!(this.inner.poll(cx));
        Poll::Ready(result.map(|r| r.map(Either::Right)))
    }
}

/// A future that wraps the inner service's future and records the status code on completion.
#[pin_project::pin_project(PinnedDrop)]
pub struct ResponseFuture<F> {
    #[pin]
    inner: F,
    span: tracing::Span,
    completed: bool,
}

#[pin_project::pinned_drop]
impl<F> PinnedDrop for ResponseFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        if !self.completed {
            self.span.record(OTEL_STATUS_CODE, otel_status_codes::ERROR);
            self.span.record(
                otel_trace::ERROR_TYPE,
                attributes::error_type_values::CLIENT_CANCELLED,
            );
        }
    }
}

impl<F, ResBody, Error> Future for ResponseFuture<F>
where
    F: Future<Output = Result<http::Response<ResBody>, Error>>,
    Error: std::fmt::Display,
    ResBody: http_body::Body<Data = bytes::Bytes, Error = tonic::Status> + Send + 'static,
{
    type Output = Result<http::Response<OptionallyTracedBody<ResBody>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use http_body_util::Either;
        let this = self.project();
        let _guard = this.span.enter();

        // Note: `futures::ready!` will immediately return `Poll::Pending` if the future is not ready.
        // Crucially, this causes `_guard` to be dropped, which exits the span.
        // This ensures we don't hold the span open while waiting for I/O.
        let result = futures::ready!(this.inner.poll(cx));

        // If we get here, the future is ready.
        *this.completed = true;

        match result {
            Ok(response) => {
                record_status_from_headers(this.span, response.headers());
                let (parts, body) = response.into_parts();
                let instrumented_body = InstrumentedBody::new(body, this.span.clone());
                Poll::Ready(Ok(http::Response::from_parts(
                    parts,
                    Either::Left(instrumented_body),
                )))
            }
            Err(e) => {
                record_error_status(this.span, &e);
                Poll::Ready(Err(e))
            }
        }
    }
}

fn record_status_from_headers(span: &tracing::Span, headers: &http::HeaderMap) {
    // Check for "OK" status (missing or "0") directly to avoid
    // the potential overhead of `tonic::Status::from_header_map` (parsing, decoding) in the success path.
    if headers.get("grpc-status").is_none_or(|v| v == "0") {
        span.record(otel_attr::RPC_GRPC_STATUS_CODE, 0_i64);
        return;
    }

    // This is also (eventually) called in Tonic before returning a result in the API, but it's important to
    // include any error information inside the span (with API-level detail).
    if let Some(status) = tonic::Status::from_header_map(headers) {
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
}

fn create_grpc_span(
    uri: &http::Uri,
    layer_inner: &TracingTowerLayerInner,
    attempt_count: Option<i64>,
    resource_name: Option<&str>,
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
        { otel_trace::ERROR_TYPE } = tracing::field::Empty,
        // Client library metadata
        { GCP_CLIENT_SERVICE } = service,
        { GCP_CLIENT_VERSION } = version,
        { GCP_CLIENT_REPO } = repo,
        { GCP_CLIENT_ARTIFACT } = artifact,
        { GCP_GRPC_RESEND_COUNT } = resend_count,
        { GCP_RESOURCE_NAME } = resource_name,
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
        let _span = create_grpc_span(&uri, &layer.inner, Some(0), None);

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
        let _span = create_grpc_span(&uri, &layer.inner, Some(1), None);

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

    #[test]
    fn test_create_grpc_span_with_resource_name() {
        let guard = TestLayer::initialize();
        let uri = http::Uri::from_static(
            "https://pubsub.googleapis.com/google.pubsub.v1.Publisher/Publish",
        );
        let endpoint_uri = "https://pubsub.googleapis.com".parse().unwrap();
        let layer =
            TracingTowerLayer::new(&endpoint_uri, "pubsub.googleapis.com".to_string(), None);

        let resource_name = "projects/my-project/topics/my-topic";
        let _span = create_grpc_span(&uri, &layer.inner, None, Some(resource_name));

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
            (GCP_RESOURCE_NAME, resource_name.into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(span.attributes, expected_attributes);
    }

    #[test]
    fn test_record_status_from_headers_ok() {
        let guard = TestLayer::initialize();
        let span = tracing::info_span!(
            "test_span",
            { otel_attr::RPC_GRPC_STATUS_CODE } = tracing::field::Empty,
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
        );
        let _enter = span.enter();

        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", "0".parse().unwrap());

        record_status_from_headers(&span, &headers);

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1);
        let span_data = &captured[0];

        let status_code = span_data.attributes.get(otel_attr::RPC_GRPC_STATUS_CODE);
        assert_eq!(status_code, Some(&AttributeValue::from(0_i64)));

        // OTEL_STATUS_CODE should not be set to ERROR
        if let Some(val) = span_data.attributes.get(OTEL_STATUS_CODE) {
            assert_ne!(val, &AttributeValue::from("ERROR"));
        }
    }

    #[test]
    fn test_record_status_from_headers_error() {
        let guard = TestLayer::initialize();
        let span = tracing::info_span!(
            "test_span",
            { otel_attr::RPC_GRPC_STATUS_CODE } = tracing::field::Empty,
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { otel_trace::ERROR_TYPE } = tracing::field::Empty,
        );
        let _enter = span.enter();

        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", "3".parse().unwrap()); // INVALID_ARGUMENT
        headers.insert("grpc-message", "invalid argument".parse().unwrap());

        record_status_from_headers(&span, &headers);

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1);
        let span_data = &captured[0];

        let status_code = span_data.attributes.get(otel_attr::RPC_GRPC_STATUS_CODE);
        assert_eq!(status_code, Some(&AttributeValue::from(3_i64)));

        let otel_status = span_data.attributes.get(OTEL_STATUS_CODE);
        assert_eq!(otel_status, Some(&AttributeValue::from("ERROR")));

        let error_type = span_data.attributes.get(otel_trace::ERROR_TYPE);
        assert_eq!(error_type, Some(&AttributeValue::from("INVALID_ARGUMENT")));
    }

    #[test]
    fn test_record_error_status() {
        let guard = TestLayer::initialize();
        let span = tracing::info_span!(
            "test_span",
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { otel_trace::ERROR_TYPE } = tracing::field::Empty,
        );
        let _enter = span.enter();

        let error = tonic::Status::internal("internal error");
        record_error_status(&span, &error);

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1);
        let span_data = &captured[0];

        let otel_status = span_data.attributes.get(OTEL_STATUS_CODE);
        assert_eq!(otel_status, Some(&AttributeValue::from("ERROR")));

        let error_type = span_data.attributes.get(otel_trace::ERROR_TYPE);
        // record_error_status converts the error to an IO error, which maps to CLIENT_CONNECTION_ERROR
        assert_eq!(
            error_type,
            Some(&AttributeValue::from("CLIENT_CONNECTION_ERROR"))
        );
    }
}
