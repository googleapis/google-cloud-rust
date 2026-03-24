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

mod client_signals_ext;
mod duration_metric;
mod request_start;
mod with_client_signals;

pub use client_signals_ext::ClientSignalsExt;
pub use duration_metric::DurationMetric;
pub use request_start::RequestStart;
pub use with_client_signals::WithClientSignals;

/// An extension to disable terminal actionable error logging.
///
/// If this extension is present in the `RequestOptions` supplied to a GAX call,
/// the terminal application logs will be suppressed.
#[derive(Clone, Copy, Debug)]
pub struct SuppressActionableErrorLog;

/// Creates a [Span] and [RequestStart] for a client request.
///
/// # Parameters
/// * `info`: a reference to the [InstrumentationClientInfo] structure for this
///   client.
/// * `client` (`&'static str`): the name of the method in the generated (or
///   hand-crafted) library. Examples:
///   - `"client::SecretManagerService"`
///   - `"client::Storage"`
///   - `"client::PredictionService"`
/// * `method` (`&' static str`): the name of the method in the client struct.
///   Examples:
///   - `"create_secret"`
///   - `"open_object"`
///   - `"read_object"`
///   - `"predict"`
/// * `rpc_method`: the fully qualified gRPC method, in gRPC notation. Examples:
///   - `Some("google.cloud.secretmanager.v1.SecretManagerService/CreateSecret")`
///   - `Some("google.storage.v2.Storage/BidiReadObject")`
///   - `None` -> use with `read_object` because there is no "RPC"
///   - `Some("google.cloud.aiplatform.v1.PredictionService/Predic")
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
///     use google_cloud_gax_internal::observability::ClientSignalsExt as _;
///     let (start, span) = google_cloud_gax_internal::client_request_signals!(
///         "client::Echo",
///         "echo",
///         &info::INSTRUMENTATION_CLIENT_INFO,
///         &options
///     );
///     self.inner
///         .echo(req, options)
///         .instrument_client(self.duration.clone(), start, span)
///         .await
/// }
/// # }
/// ```
///
/// [InstrumentationClientInfo]: [crate::options::InstrumentationClientInfo]
/// [Span]: [tracing::Span]
#[macro_export]
macro_rules! client_request_signals {
    ($info:expr, $options:expr, $client:literal, $method:literal, $rpc_method:expr) => {{
        use $crate::observability::attributes::keys::*;
        use $crate::observability::attributes::otel_status_codes;
        use $crate::observability::attributes::{
            GCP_CLIENT_LANGUAGE_RUST, GCP_CLIENT_REPO_GOOGLEAPIS, OTEL_KIND_INTERNAL,
            RPC_SYSTEM_HTTP,
        };
        let start = $crate::observability::RequestStart::new(
            $info,
            $options,
            concat!(env!("CARGO_CRATE_NAME"), "::", $client, "::", $method),
        );
        let span = ::tracing::info_span!(
            "client_request",
            "gax.client.span" = true, // Marker field
            { OTEL_NAME } = concat!(env!("CARGO_CRATE_NAME"), "::", $client, "::", $method),
            { OTEL_KIND } = OTEL_KIND_INTERNAL,
            { RPC_SYSTEM } = RPC_SYSTEM_HTTP, // Default to HTTP, can be overridden
            { RPC_SERVICE } = $info.service_name,
            { RPC_METHOD } = ::tracing::field::Empty,
            { GCP_CLIENT_SERVICE } = $info.service_name,
            { GCP_CLIENT_VERSION } = $info.client_version,
            { GCP_CLIENT_REPO } = GCP_CLIENT_REPO_GOOGLEAPIS,
            { GCP_CLIENT_ARTIFACT } = $info.client_artifact,
            { GCP_CLIENT_LANGUAGE } = GCP_CLIENT_LANGUAGE_RUST,
            // Fields to be recorded later
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { SERVER_ADDRESS } = ::tracing::field::Empty,
            { SERVER_PORT } = ::tracing::field::Empty,
            { URL_FULL } = ::tracing::field::Empty,
            { HTTP_REQUEST_METHOD } = ::tracing::field::Empty,
            { HTTP_RESPONSE_STATUS_CODE } = ::tracing::field::Empty,
            { HTTP_REQUEST_RESEND_COUNT } = ::tracing::field::Empty,
        );
        if let Some(m) = $rpc_method {
            span.record(RPC_METHOD, m);
        }
        (start, span)
    }};
}

/// Creates a new tracing span for a client request.
///
/// This span represents the logical request operation and is used to track
/// the overall duration and status of the request, including retries.
///
/// # Example
///
/// ```
/// let span = client_request_span!("client::Client", "upload_chunk", &HIDDEN_DETAIL);
/// # use std::sync::LazyLock;
/// # use google_cloud_gax_internal::client_request_span;
/// # use google_cloud_gax_internal::options::InstrumentationClientInfo;
/// # static HIDDEN_DETAIL: LazyLock<InstrumentationClientInfo> =
/// #     LazyLock::new(|| InstrumentationClientInfo::default());
/// ```
#[macro_export]
macro_rules! client_request_span {
    ($client:expr, $method:expr, $info:expr) => {{
        use $crate::observability::attributes::keys::*;
        use $crate::observability::attributes::otel_status_codes;
        use $crate::observability::attributes::{
            GCP_CLIENT_LANGUAGE_RUST, GCP_CLIENT_REPO_GOOGLEAPIS, OTEL_KIND_INTERNAL,
            RPC_SYSTEM_HTTP,
        };
        tracing::info_span!(
            "client_request",
            "gax.client.span" = true, // Marker field
            { OTEL_NAME } = concat!(env!("CARGO_CRATE_NAME"), "::", $client, "::", $method),
            { OTEL_KIND } = OTEL_KIND_INTERNAL,
            { RPC_SYSTEM } = RPC_SYSTEM_HTTP, // Default to HTTP, can be overridden
            { RPC_SERVICE } = $info.service_name,
            { RPC_METHOD } = $method,
            { GCP_CLIENT_SERVICE } = $info.service_name,
            { GCP_CLIENT_VERSION } = $info.client_version,
            { GCP_CLIENT_REPO } = GCP_CLIENT_REPO_GOOGLEAPIS,
            { GCP_CLIENT_ARTIFACT } = $info.client_artifact,
            { GCP_CLIENT_LANGUAGE } = GCP_CLIENT_LANGUAGE_RUST,
            // Fields to be recorded later
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { SERVER_ADDRESS } = ::tracing::field::Empty,
            { SERVER_PORT } = ::tracing::field::Empty,
            { URL_FULL } = ::tracing::field::Empty,
            { HTTP_REQUEST_METHOD } = ::tracing::field::Empty,
            { HTTP_RESPONSE_STATUS_CODE } = ::tracing::field::Empty,
            { HTTP_REQUEST_RESEND_COUNT } = ::tracing::field::Empty,
        )
    }};
}

#[cfg(test)]
mod tests {
    use super::duration_metric::BOUNDARIES;
    use super::with_client_signals::{NAME, TARGET};
    use crate::observability::DurationMetric;
    use crate::options::InstrumentationClientInfo;
    use google_cloud_gax::error::Error;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
    use opentelemetry::TraceId;
    use opentelemetry::logs::AnyValue;
    use opentelemetry::trace::{Status as SpanStatus, TracerProvider};
    use opentelemetry::{InstrumentationScope, KeyValue};
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_sdk::logs::{
        BatchLogProcessor, InMemoryLogExporter, SdkLogRecord, SdkLoggerProvider,
    };
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::trace::{BatchSpanProcessor, InMemorySpanExporter, SdkTracerProvider};
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
    pub(crate) static URL_TEMPLATE: &str = "/v1/projects/{}:test_method";
    pub(crate) static METHOD: &str = "test-method";
    pub(crate) const DELAY: Duration = Duration::from_millis(750);
    const COMMON_ATTRIBUTES: [(&str, &str); 3] = [
        ("rpc.system.name", "http"),
        ("url.domain", "example.com"),
        ("url.template", URL_TEMPLATE),
    ];

    async fn inner_echo(_options: &RequestOptions) -> Result<String, Error> {
        tokio::time::sleep(DELAY).await;
        let error = Error::service(
            Status::default()
                .set_code(Code::NotFound)
                .set_message("NOT FOUND"),
        );
        Err(error)
    }

    async fn tracing_echo(
        metric: &DurationMetric,
        options: &RequestOptions,
    ) -> Result<String, Error> {
        use crate::observability::ClientSignalsExt as _;
        let (start, span) = crate::client_request_signals!(
            &TEST_INFO,
            &options,
            "Client",
            "echo",
            Some("google.test.v7.Client/Echo")
        );
        inner_echo(options)
            .instrument_client(metric.clone(), start, span)
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn all_signals_go() -> anyhow::Result<()> {
        let signals = SignalProviders::new();

        // In a real client this is created during the `tracing::Client`
        // initialization.
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );
        let options = RequestOptions::default().insert_extension(PathTemplate(URL_TEMPLATE));
        // Simulate a client call, this simulates a call that takes 750ms and then returns an error.
        let result = tracing_echo(&metric, &options).await;
        assert!(result.is_err(), "{result:?}");

        // Flush the trace, logs and metric providers so we can collect the data.
        signals.force_flush()?;

        const FULL_METHOD: &str = concat!(env!("CARGO_CRATE_NAME"), "::Client::echo");
        // Verify the metrics include the data we want.
        let metrics = signals.metric_exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            1_u64..=1_u64,
            &[
                ("rpc.method", FULL_METHOD),
                ("rpc.response.status_code", "NOT_FOUND"),
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
                ("gax.client.span", "true"),
                ("rpc.system", "http"),
                ("rpc.service", "test-service"),
                ("rpc.method", "google.test.v7.Client/Echo"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.client.language", "rust"),
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
                ("rpc.method", FULL_METHOD),
                ("rpc.response.status_code", "NOT_FOUND"),
                ("exception.type", "NOT_FOUND"),
                (
                    "exception.message",
                    "the service reports an error with code NOT_FOUND described as: NOT FOUND",
                ),
            ],
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
            .with_attributes([
                KeyValue::new("gcp.client.artifact", "test-artifact"),
                KeyValue::new("gcp.client.version", "1.2.3"),
                KeyValue::new("gcp.client.service", "test-service"),
                KeyValue::new("gcp.client.repo", "googleapis/google-cloud-rust"),
            ])
            .build();
        assert_eq!(scope, &want, "{got:?}");
    }

    #[track_caller]
    pub fn check_metric_data<R>(
        metrics: &Vec<ResourceMetrics>,
        want_count: R,
        want_attributes: &[(&'static str, &'static str)],
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
        let want = BTreeSet::from_iter(
            COMMON_ATTRIBUTES
                .iter()
                .chain(want_attributes)
                .map(|(k, v)| (*k, v.to_string())),
        );
        let diff = attr.symmetric_difference(&want).collect::<Vec<_>>();
        assert_eq!(attr, want, "diff={diff:?}");

        let bucket = point
            .bucket_counts()
            // The first bucket is "counting the values below the first boundary".
            .skip(1)
            .zip(point.bounds())
            .find(|(count, _bound)| *count >= 1_u64);
        // Find the expected bucket
        let secs = DELAY.as_secs_f64();
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
    pub fn check_log_record(
        record: &SdkLogRecord,
        trace_id: TraceId,
        extra_attributes: &[(&'static str, &'static str)],
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
