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

use crate::observability::attributes::{self, keys::*};
use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::Instrument;

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
    // We use `Box<dyn Future...>` (type erasure) here to simplify the type signature.
    // Without this, we would need to explicitly name the complex type returned by
    // `.instrument()` (and any implementation changes in `call`), which can be verbose and brittle.
    //
    // The allocation cost is negligible as `call` is invoked once per RPC (or stream initialization),
    // not per message in a streaming call.
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let attempt_count = req.extensions().get::<AttemptCount>().map(|a| a.0);
        let span = create_grpc_span(req.uri(), &self.layer.inner, attempt_count);
        Box::pin(self.inner.call(req).instrument(span))
    }
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
        { OTEL_STATUS_CODE } = tracing::field::Empty,
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
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(span.attributes, expected_attributes);
    }
}
