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

mod duration_metric;
mod recorder;
mod transport_metric;
mod with_client_logging;
mod with_client_metric;
mod with_client_span;
mod with_transport_logging;
mod with_transport_metric;
mod with_transport_span;

pub use duration_metric::DurationMetric;
pub use recorder::{ClientRequestAttributes, RequestRecorder};
pub use transport_metric::TransportMetric;
pub use with_client_logging::WithClientLogging;
pub use with_client_metric::WithClientMetric;
pub use with_client_span::WithClientSpan;
pub use with_transport_logging::WithTransportLogging;
pub use with_transport_metric::WithTransportMetric;
pub use with_transport_span::WithTransportSpan;

/// Creates a [Span] and decorated future for a client request.
///
/// # Parameters
/// * `metric`: a handle to [DurationMetric] used to measure the request duration.
/// * `info`: a reference to the [InstrumentationClientInfo] structure for this
///   client.
/// * `method` (`&' static str`): the name of the **Rust** method.
///   Examples:
///   - `"client::SecretManagerService::create_secret"`
///   - `"client::Storage::open_object"`
///   - `"client::PredictionService::predict"`
/// * `inner` (`impl Future<Output = google_cloud_gax::Result<T>>`): the pending RPC.
///
/// This is typically used in the body of the `Tracing` stub, to simplify the
/// code. The body of the tracing function would be:
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
///     let (_span, pending) = google_cloud_gax_internal::client_request_signals!(
///         metric: self.duration.clone(),            // Duration metric handle
///         info: *info::INSTRUMENTATION_CLIENT_INFO, // Instrumentation for the crate
///         method: "client::Client::echo",           // The Rust method name
///         self.inner(req, options)
///     );
///     pending.await
/// }
/// # }
/// ```
///
/// [InstrumentationClientInfo]: [crate::options::InstrumentationClientInfo]
/// [Span]: [tracing::Span]
#[macro_export]
macro_rules! client_request_signals {
    (metric: $metric:expr, info: $info:expr, method: $method:literal, $inner:expr) => {{
        use ::tracing::instrument::Instrument;
        let span = $crate::client_request_signals!(info: $info, method: $method);
        let recorder = $crate::observability::RequestRecorder::new($info);
        let pending = recorder
            .scope($crate::observability::WithClientSpan::new(
                span.clone(),
                $crate::observability::WithClientMetric::new(
                    $metric,
                    $crate::observability::WithClientLogging::new($inner),
                ),
            ))
            .instrument(span.clone());
        (span, pending)
    }};
    (info: $info:expr, method: $method:literal) => {{
        use ::tracing::field::Empty;
        // We use string literals for all the field names because it narrows the public API for
        // `google-cloud-gax-internal`. The exception are these values, which we expect may change
        // from time to time.
        use $crate::observability::{GCP_CLIENT_REPO_GOOGLEAPIS, SCHEMA_URL_VALUE};
        tracing::info_span!(
            "client_request",
             "otel.name"             = concat!(env!("CARGO_CRATE_NAME"), "::", $method),
             "otel.kind"             = "Internal",
             "rpc.system.name"       = "http", // Default to HTTP, can be overridden
             "gcp.client.service"    = $info.service_name,
             "gcp.client.repo"       = GCP_CLIENT_REPO_GOOGLEAPIS,
             "gcp.client.artifact"   = $info.client_artifact,
             "gcp.client.version"    = $info.client_version,
             "gcp.schema.url"        = SCHEMA_URL_VALUE,
             "otel.status_code"      = "UNSET",
            // Fields to be recorded later
            "rpc.method"                  = Empty,
            "otel.status_description"     = Empty,
            "error.type"                  = Empty,
            "server.address"              = Empty,
            "server.port"                 = Empty,
            "network.peer.address"        = Empty,
            "network.peer.port"           = Empty,
            "url.full"                    = Empty,
            "http.request.method"         = Empty,
            "http.request.resend_count"   = Empty,
            "http.response.status_code"   = Empty,
            "gcp.resource.destination.id" = Empty,
        )
    }};
}

#[cfg(test)]
mod tests {
    use super::duration_metric::BOUNDARIES;
    use super::with_client_logging::{NAME, TARGET};
    use super::{ClientRequestAttributes, RequestRecorder};
    use crate::observability::DurationMetric;
    use crate::observability::attributes::SCHEMA_URL_VALUE;
    use crate::options::InstrumentationClientInfo;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::error::Error;
    use grpc_server::google::test::v1::{EchoRequest, EchoResponse};
    use httptest::matchers::request::method_path;
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use opentelemetry::logs::AnyValue;
    use opentelemetry::trace::TraceId;
    use opentelemetry::trace::{Status as SpanStatus, TracerProvider};
    use opentelemetry::{InstrumentationScope, KeyValue};
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_sdk::logs::{
        BatchLogProcessor, InMemoryLogExporter, SdkLogRecord, SdkLoggerProvider,
    };
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::trace::{BatchSpanProcessor, InMemorySpanExporter, SdkTracerProvider};
    use pretty_assertions::assert_eq;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::Registry;
    use tracing_subscriber::prelude::*;

    pub(crate) static TEST_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
        service_name: "test-service",
        client_version: "1.2.3",
        client_artifact: "test-artifact",
        default_host: "example.com",
    };
    pub(crate) static TEST_URL_TEMPLATE: &str = "/v1/projects/{}:test_method";
    pub(crate) static TEST_METHOD: &str = "google.test.v1.Service/TestMethod";
    pub(crate) const TEST_REQUEST_DURATION: Duration = Duration::from_millis(750);
    const COMMON_ATTRIBUTES: [(&str, &str); 3] = [
        ("rpc.system.name", "http"),
        ("url.domain", "example.com"),
        ("url.template", TEST_URL_TEMPLATE),
    ];

    // Simulate the transport HTTP client for a request that fills the `RequestRecorder` data.
    async fn recorded_request_transport_client(url: &str) -> Result<String, Error> {
        let recorder = RequestRecorder::current().expect("current recorder should be available");
        let client = reqwest::Client::new();
        let request = client
            .get(url)
            .build()
            .map_err(Error::io)
            .inspect_err(|e| recorder.on_http_error(e))?;

        recorder.on_http_request(&request);
        let response = client
            .execute(request)
            .await
            .map_err(Error::io)
            .inspect_err(|e| recorder.on_http_error(e))?;
        tokio::time::sleep(TEST_REQUEST_DURATION).await;
        recorder.on_http_response(&response);
        Err(Error::http(
            response.status().as_u16(),
            response.headers().clone(),
            bytes::Bytes::from_owner("SIMULATED NOT FOUND"),
        ))
    }

    // Simulate the transport stub for a request that fills the `RequestRecorder` data.
    pub(crate) async fn recorded_request_transport_stub(url: &str) -> Result<String, Error> {
        let recorder = RequestRecorder::current().expect("current recorder should be available");
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_rpc_method(TEST_METHOD)
                .set_url_template(TEST_URL_TEMPLATE)
                .set_resource_name("//test.googleapis.com/test-only".to_string()),
        );
        recorded_request_transport_client(url).await
    }

    #[tokio::test(start_paused = true)]
    async fn client_request() -> anyhow::Result<()> {
        const PATH: &str = "/v1/projects/test-only:test_method";

        let signals = SignalProviders::new();
        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", PATH))
                .respond_with(status_code(404).body("NOT FOUND")),
        );
        let url = server.url(PATH).to_string();

        // In a real client this is created during the `tracing::Client`
        // initialization.
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );

        // Simulate a client call, this simulates a call that takes 750ms and then returns an error.
        let (span, pending) = crate::client_request_signals!(
            metric: metric.clone(),
            info: TEST_INFO,
            method: "FakeClient::some_rust_function",
            recorded_request_transport_stub(&url));
        let result = pending.await;
        assert!(result.is_err(), "{result:?}");
        drop(span);

        // Flush the trace, logs and metric providers so we can collect the data.
        signals.force_flush()?;

        const FULL_METHOD: &str = concat!(
            env!("CARGO_CRATE_NAME"),
            "::",
            "FakeClient::some_rust_function"
        );
        // Verify the metrics include the data we want.
        let metrics = signals.metric_exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            "gcp.client.request.duration",
            1_u64..=1_u64,
            &[
                ("rpc.system.name", "http"),
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("http.response.status_code", "404"),
                ("error.type", "404"),
                ("server.address", server.addr().ip().to_string().as_str()),
                ("server.port", server.addr().port().to_string().as_str()),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
            ],
        );

        // Find the span.
        let spans = signals.trace_exporter.get_finished_spans()?;
        let span = spans
            .iter()
            .find(|s| s.name.as_ref() == FULL_METHOD)
            .unwrap_or_else(|| panic!("expected one span named 'client_request', spans={spans:?}"));
        let trace_id = span.span_context.trace_id();
        let got = BTreeSet::from_iter(
            span.attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );
        let want = BTreeSet::from_iter(
            [
                ("rpc.system.name", "http"),
                ("rpc.method", TEST_METHOD),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("code.module.name", module_path!()),
                ("error.type", "404"),
                ("http.request.method", "GET"),
                ("http.response.status_code", "404"),
                ("server.address", server.addr().ip().to_string().as_str()),
                ("server.port", server.addr().port().to_string().as_str()),
                (
                    "network.peer.address",
                    server.addr().ip().to_string().as_str(),
                ),
                (
                    "network.peer.port",
                    server.addr().port().to_string().as_str(),
                ),
                ("url.full", url.as_str()),
            ]
            .map(|(k, v)| (k, v.to_string())),
        );
        let missing = want.difference(&got).collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "missing = {missing:?}\nwant = {want:?}\ngot  = {got:?}"
        );
        assert!(matches!(span.status, SpanStatus::Error { .. }), "{span:#?}");

        // Verify the logs include the entry for this error.
        let captured = signals.logs_exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET))
            .unwrap_or_else(|| panic!("missing log for target {TARGET} in {captured:#?}"));
        check_log_record(
            &record.record,
            trace_id,
            &[
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
                ("gcp.client.service", "test-service"),
                ("rpc.method", TEST_METHOD),
                ("error.type", "404"),
                ("http.request.method", "GET"),
                ("server.address", server.addr().ip().to_string().as_str()),
                ("server.port", server.addr().port().to_string().as_str()),
                (
                    "network.peer.address",
                    server.addr().ip().to_string().as_str(),
                ),
                (
                    "network.peer.port",
                    server.addr().port().to_string().as_str(),
                ),
                ("url.full", url.as_str()),
            ],
        );
        Ok(())
    }

    // Simulate the transport stub for a request that fills the `RequestRecorder` data for gRPC.
    #[cfg(feature = "_internal-grpc-client")]
    pub(crate) async fn recorded_request_grpc_stub(url: &str) -> Result<String, Error> {
        let recorder = RequestRecorder::current().expect("current recorder should be available");
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_rpc_method(TEST_METHOD)
                .set_url_template(TEST_URL_TEMPLATE)
                .set_resource_name("//test.googleapis.com/test-only".to_string()),
        );

        let mut config = crate::options::ClientConfig::default();
        config.tracing = true;
        // Don't retry, just fail once
        config.retry_policy = Some(std::sync::Arc::new(
            google_cloud_gax::retry_policy::NeverRetry,
        ));

        config.cred = Some(Anonymous::new().build());

        let client = crate::grpc::Client::new(config, url)
            .await
            .map_err(|e| Error::io(e.to_string()))?;

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = EchoRequest {
            message: "test message".into(),
            ..Default::default()
        };

        tokio::time::sleep(TEST_REQUEST_DURATION).await;

        let response: Result<tonic::Response<EchoResponse>, google_cloud_gax::error::Error> =
            client
                .execute::<EchoRequest, EchoResponse>(
                    extensions,
                    // Direct tonic endpoint that does not exist to trigger an error
                    http::uri::PathAndQuery::from_static(
                        "/google.test.v1.EchoService/NonExistentMethod",
                    ),
                    request,
                    google_cloud_gax::options::RequestOptions::default(),
                    "test-client",
                    "",
                )
                .await;

        response.map(|_| "SUCCESS".to_string())
    }

    #[cfg(feature = "_internal-grpc-client")]
    pub(crate) async fn recorded_request_grpc_stub_retry(url: &str) -> Result<String, Error> {
        use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
        use std::sync::Arc;

        let recorder = RequestRecorder::current().expect("current recorder should be available");
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_rpc_method(TEST_METHOD)
                .set_url_template(TEST_URL_TEMPLATE)
                .set_resource_name("//test.googleapis.com/test-only".to_string()),
        );

        let mut config = crate::options::ClientConfig::default();
        config.tracing = true;
        config.retry_policy = Some(Arc::new(AlwaysRetry.with_attempt_limit(3)));

        config.cred = Some(Anonymous::new().build());

        let client = crate::grpc::Client::new(config, url)
            .await
            .map_err(|e| Error::io(e.to_string()))?;

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = EchoRequest {
            message: "test message".into(),
            ..Default::default()
        };

        let response: Result<tonic::Response<EchoResponse>, google_cloud_gax::error::Error> =
            client
                .execute::<EchoRequest, EchoResponse>(
                    extensions,
                    http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                    request,
                    google_cloud_gax::options::RequestOptions::default(),
                    "test-client",
                    "",
                )
                .await;

        response.map(|_| "SUCCESS".to_string())
    }

    #[cfg(feature = "_internal-grpc-client")]
    pub(crate) async fn recorded_request_grpc_stub_success(url: &str) -> Result<String, Error> {
        let recorder = RequestRecorder::current().expect("current recorder should be available");
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_rpc_method(TEST_METHOD)
                .set_url_template(TEST_URL_TEMPLATE)
                .set_resource_name("//test.googleapis.com/test-only".to_string()),
        );

        use std::sync::Arc;
        let mut config = crate::options::ClientConfig::default();
        config.tracing = true;
        config.retry_policy = Some(Arc::new(google_cloud_gax::retry_policy::NeverRetry));

        config.cred = Some(Anonymous::new().build());

        let client = crate::grpc::Client::new(config, url)
            .await
            .map_err(|e| Error::io(e.to_string()))?;

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = EchoRequest {
            message: "test message".into(),
            ..Default::default()
        };

        tokio::time::sleep(TEST_REQUEST_DURATION).await;

        let response: Result<tonic::Response<EchoResponse>, google_cloud_gax::error::Error> =
            client
                .execute::<EchoRequest, EchoResponse>(
                    extensions,
                    http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                    request,
                    google_cloud_gax::options::RequestOptions::default(),
                    "test-client",
                    "",
                )
                .await;

        response.map(|_| "SUCCESS".to_string())
    }

    #[cfg(feature = "_internal-grpc-client")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_client_request() -> anyhow::Result<()> {
        let (endpoint, _server) = grpc_server::start_echo_server().await?;
        let signals = SignalProviders::new();

        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );

        let (span, pending) = crate::client_request_signals!(
            metric: metric.clone(),
            info: TEST_INFO,
            method: "FakeGrpcClient::some_rust_function",
            recorded_request_grpc_stub(&endpoint)
        );
        let result = pending.await;
        assert!(result.is_err(), "{result:?}");
        drop(span);

        signals.force_flush()?;

        const FULL_METHOD: &str = concat!(
            env!("CARGO_CRATE_NAME"),
            "::",
            "FakeGrpcClient::some_rust_function"
        );

        let metrics = signals.metric_exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            "gcp.client.request.duration",
            1_u64..=1_u64,
            &[
                ("rpc.system.name", "grpc"),
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("rpc.response.status_code", "UNIMPLEMENTED"),
                ("error.type", "UNIMPLEMENTED"),
                ("server.address", "example.com"),
                ("server.port", "443"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
            ],
        );

        // Verify the span exists.
        let spans = signals.trace_exporter.get_finished_spans()?;
        let span = spans
            .iter()
            .find(|s| s.name.as_ref() == FULL_METHOD)
            .unwrap_or_else(|| panic!("expected one span named 'client_request', spans={spans:?}"));
        let trace_id = span.span_context.trace_id();
        assert!(matches!(span.status, SpanStatus::Error { .. }), "{span:#?}");

        // Verify the logs.
        let captured = signals.logs_exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET))
            .unwrap_or_else(|| panic!("missing log for target {TARGET} in {captured:#?}"));

        check_log_record_grpc(
            &record.record,
            trace_id,
            &[
                ("rpc.system.name", "grpc"),
                ("http.request.method", "POST"),
                ("url.full", "/google.test.v1.EchoService/NonExistentMethod"),
                ("url.template", TEST_URL_TEMPLATE),
                ("url.domain", "example.com"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
                ("gcp.client.service", "test-service"),
                ("rpc.method", TEST_METHOD),
                ("rpc.response.status_code", "UNIMPLEMENTED"),
                ("error.type", "UNIMPLEMENTED"),
                ("server.address", "example.com"),
                ("server.port", "443"),
            ],
        );

        Ok(())
    }

    #[cfg(feature = "_internal-grpc-client")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_client_request_retry() -> anyhow::Result<()> {
        let (endpoint, _server) = grpc_server::start_fixed_responses(vec![
            Err(tonic::Status::unavailable("try again")),
            Ok(tonic::Response::new(EchoResponse {
                message: "success".into(),
                ..Default::default()
            })),
        ])
        .await?;

        let signals = SignalProviders::new();

        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );

        let (span, pending) = crate::client_request_signals!(
            metric: metric.clone(),
            info: TEST_INFO,
            method: "FakeGrpcClient::some_rust_function_retry",
            recorded_request_grpc_stub_retry(&endpoint)
        );
        let result = pending.await;
        assert!(result.is_ok(), "{result:?}");
        drop(span);

        signals.force_flush()?;

        let spans = signals.trace_exporter.get_finished_spans()?;

        // Assert that at least one span has resend_count = 1
        let retry_span = spans.iter().find(|s| {
            s.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "gcp.grpc.resend_count" && kv.value.to_string() == "1")
        });
        assert!(
            retry_span.is_some(),
            "expected a span with resend_count=1, spans={spans:#?}"
        );

        Ok(())
    }

    #[cfg(feature = "_internal-grpc-client")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_client_request_success() -> anyhow::Result<()> {
        let (endpoint, _server) = grpc_server::start_echo_server().await?;
        let signals = SignalProviders::new();

        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );

        let (span, pending) = crate::client_request_signals!(
            metric: metric.clone(),
            info: TEST_INFO,
            method: "FakeGrpcClient::some_rust_function_success",
            recorded_request_grpc_stub_success(&endpoint)
        );
        let result = pending.await;
        assert!(result.is_ok(), "{result:?}");
        drop(span);

        signals.force_flush()?;

        const FULL_METHOD: &str = concat!(
            env!("CARGO_CRATE_NAME"),
            "::",
            "FakeGrpcClient::some_rust_function_success"
        );

        let metrics = signals.metric_exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            "gcp.client.request.duration",
            1_u64..=1_u64,
            &[
                ("rpc.system.name", "grpc"),
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("rpc.response.status_code", "OK"),
                ("server.address", "example.com"),
                ("server.port", "443"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
            ],
        );

        // Verify the span exists.
        let spans = signals.trace_exporter.get_finished_spans()?;
        let span = spans
            .iter()
            .find(|s| s.name.as_ref() == FULL_METHOD)
            .unwrap_or_else(|| panic!("expected one span named 'client_request', spans={spans:?}"));
        assert!(matches!(span.status, SpanStatus::Unset), "{span:#?}");

        // Verify span attributes
        let got = BTreeSet::from_iter(
            span.attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );
        let want = BTreeSet::from_iter(
            [
                ("rpc.system.name", "grpc"),
                ("rpc.method", TEST_METHOD),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("server.address", "example.com"),
                ("server.port", "443"),
                ("url.full", "/google.test.v1.EchoService/Echo"),
            ]
            .map(|(k, v)| (k, v.to_string())),
        );
        let missing = want.difference(&got).collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "missing span attributes = {missing:?}\nwant = {want:?}\ngot  = {got:?}"
        );

        // Verify NO logs are recorded for success.
        let captured = signals.logs_exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET));
        assert!(
            record.is_none(),
            "expected no logs for success, found {record:#?}"
        );

        Ok(())
    }

    pub struct SignalProviders {
        pub trace_exporter: InMemorySpanExporter,
        pub trace_provider: SdkTracerProvider,
        pub logs_exporter: InMemoryLogExporter,
        pub logs_provider: SdkLoggerProvider,
        pub metric_exporter: InMemoryMetricExporter,
        pub metric_provider: SdkMeterProvider,
        // To make the tests hermetic we need to avoid the global `tracing` subscriber. This is a
        // per-thread guard. It works as long as all the tests are single-threaded (the default).
        pub _guard: DefaultGuard,
    }

    impl SignalProviders {
        pub fn new() -> Self {
            // We need a tracing exporter and provider or the traces get no trace ids.
            let trace_exporter = InMemorySpanExporter::default();
            let trace_provider = SdkTracerProvider::builder()
                .with_span_processor(BatchSpanProcessor::builder(trace_exporter.clone()).build())
                .build();

            // We also need a logging exporter to capture the `tracing::event!()` logs
            // as they are forwarded to OpenTelemetry logs.
            let logs_exporter = InMemoryLogExporter::default();
            let logs_provider = SdkLoggerProvider::builder()
                .with_log_processor(BatchLogProcessor::builder(logs_exporter.clone()).build())
                .build();
            // Using a per-thread guard is Okay because all the tests in this module are single-threaded.
            let guard = tracing::subscriber::set_default(
                Registry::default()
                    .with(OpenTelemetryTracingBridge::new(&logs_provider))
                    .with(
                        tracing_opentelemetry::layer()
                            .with_tracer(trace_provider.tracer("test-only")),
                    ),
            );

            let metric_exporter = InMemoryMetricExporter::default();
            let metric_provider = SdkMeterProvider::builder()
                .with_reader(PeriodicReader::builder(metric_exporter.clone()).build())
                .build();

            SignalProviders {
                trace_exporter,
                trace_provider,
                logs_exporter,
                logs_provider,
                metric_exporter,
                metric_provider,
                _guard: guard,
            }
        }

        pub fn force_flush(&self) -> anyhow::Result<()> {
            self.trace_provider.force_flush()?;
            self.logs_provider.force_flush()?;
            self.metric_provider.force_flush()?;
            Ok(())
        }
    }

    #[track_caller]
    pub fn check_metric_scope(metrics: &Vec<ResourceMetrics>) {
        let got = match &metrics[..] {
            [g] => g,
            _ => panic!("expected a single metric, metrics={metrics:?}"),
        };

        let mut m = got.scope_metrics();
        let got = match (m.next(), m.next()) {
            (Some(g), None) => g,
            _ => panic!("expected a single scoped metric, got={metrics:?}"),
        };
        let scope = got.scope();
        let want = InstrumentationScope::builder("test-artifact")
            .with_version("1.2.3")
            .with_schema_url(SCHEMA_URL_VALUE)
            .with_attributes([
                KeyValue::new("gcp.client.artifact", "test-artifact"),
                KeyValue::new("gcp.client.service", "test-service"),
                KeyValue::new("gcp.client.repo", "googleapis/google-cloud-rust"),
            ])
            .build();
        assert_eq!(scope, &want, "{got:?}");
    }

    #[track_caller]
    pub fn check_metric_data<R>(
        metrics: &Vec<ResourceMetrics>,
        expected_name: &str,
        want_count: R,
        want_attributes: &[(&'static str, &str)],
    ) where
        R: std::ops::RangeBounds<u64>,
    {
        let mut iter = metrics
            .iter()
            .flat_map(|s| s.scope_metrics())
            .flat_map(|r| r.metrics());
        let actual = match (iter.next(), iter.next()) {
            (Some(a), None) => a,
            _ => panic!(
                "expected a single metric after flattening scopes and resources, metric={metrics:?}"
            ),
        };
        assert_eq!(actual.name(), expected_name);
        assert_eq!(actual.unit(), "s");
        let histo = match actual.data() {
            AggregatedMetrics::F64(MetricData::Histogram(h)) => h,
            _ => panic!("expected a f64 histogram, got={actual:?}"),
        };
        let mut m = histo.data_points();
        let point = match (m.next(), m.next()) {
            (Some(p), None) => p,
            _ => panic!("expected a single data point, histo={histo:?}"),
        };
        let attr = BTreeSet::from_iter(
            point
                .attributes()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );
        let want = BTreeSet::from_iter(want_attributes.iter().map(|(k, v)| (*k, v.to_string())));
        let diff = attr.symmetric_difference(&want).collect::<Vec<_>>();
        assert_eq!(attr, want, "diff={diff:?}");

        let bucket = point
            .bucket_counts()
            // The first bucket is "counting the values below the first boundary".
            .skip(1)
            .zip(point.bounds())
            .find(|(count, _bound)| *count >= 1_u64);
        // Find the expected bucket
        let secs = TEST_REQUEST_DURATION.as_secs_f64();
        let (low, high) = BOUNDARIES
            .windows(2)
            .map(|a| (a[0], a[1]))
            .find(|(a, b)| (*a..*b).contains(&secs))
            .unwrap_or_else(|| {
                panic!(
                    "expected DELAY ({}) to match of the buckets in {BOUNDARIES:?}",
                    secs
                )
            });
        assert!(
            bucket.is_some_and(|(c, b)| want_count.contains(&c) && b == low),
            "mismatched bucket {bucket:?} want (1, {low})\nfound=[{low}, {high})\n{point:?}"
        );
    }

    #[track_caller]
    pub fn check_log_record_grpc(
        record: &SdkLogRecord,
        trace_id: TraceId,
        extra_attributes: &[(&'static str, &str)],
    ) {
        fn format_value(any: &AnyValue) -> String {
            match any {
                AnyValue::Int(v) => v.to_string(),
                AnyValue::Double(v) => v.to_string(),
                AnyValue::String(v) => v.to_string(),
                AnyValue::Boolean(v) => v.to_string(),
                _ => "unexpected AnyValue variant".to_string(),
            }
        }
        assert_eq!(record.event_name(), Some(NAME), "{record:?}");
        assert_eq!(
            record.target().map(|s| s.as_ref()),
            Some(TARGET),
            "{record:?}"
        );
        assert_eq!(record.severity_text(), Some("WARN"), "{record:?}");
        assert_eq!(
            record.trace_context().map(|c| c.trace_id),
            Some(trace_id),
            "{record:?}"
        );
        let got = BTreeSet::from_iter(
            record
                .attributes_iter()
                .map(|(k, v)| (k.as_str(), format_value(v))),
        );
        let want = BTreeSet::from_iter(extra_attributes.iter().map(|(k, v)| (*k, v.to_string())));
        let diff = got.symmetric_difference(&want).collect::<Vec<_>>();
        assert_eq!(got, want, "diff={diff:?}");
    }

    #[track_caller]
    pub fn check_log_record(
        record: &SdkLogRecord,
        trace_id: TraceId,
        extra_attributes: &[(&'static str, &str)],
    ) {
        fn format_value(any: &AnyValue) -> String {
            match any {
                AnyValue::Int(v) => v.to_string(),
                AnyValue::Double(v) => v.to_string(),
                AnyValue::String(v) => v.to_string(),
                AnyValue::Boolean(v) => v.to_string(),
                _ => "unexpected AnyValue variant".to_string(),
            }
        }
        assert_eq!(record.event_name(), Some(NAME), "{record:?}");
        assert_eq!(
            record.target().map(|s| s.as_ref()),
            Some(TARGET),
            "{record:?}"
        );
        assert_eq!(record.severity_text(), Some("WARN"), "{record:?}");
        assert_eq!(
            record.trace_context().map(|c| c.trace_id),
            Some(trace_id),
            "{record:?}"
        );
        let got = BTreeSet::from_iter(
            record
                .attributes_iter()
                .map(|(k, v)| (k.as_str(), format_value(v))),
        );
        let want = BTreeSet::from_iter(
            COMMON_ATTRIBUTES
                .iter()
                .chain(extra_attributes)
                .map(|(k, v)| (*k, v.to_string())),
        );
        let diff = got.symmetric_difference(&want).collect::<Vec<_>>();
        assert_eq!(got, want, "diff={diff:?}");
    }
}
