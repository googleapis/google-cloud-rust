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

//! Implements [WithClientSignals].
//!
//! This is a private module, it is not exposed in the public API.

use super::DurationMetric;
use super::RequestStart;
use crate::observability::attributes::RPC_SYSTEM_HTTP;
use crate::observability::attributes::keys::{RPC_RESPONSE_STATUS_CODE, RPC_SYSTEM_NAME};
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use opentelemetry_semantic_conventions::attribute;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::Instrument;
use tracing::Span;
use tracing::instrument::Instrumented;

// A tentative name for the error logs.
const NAME: &str = "experimental.client.request.error";
// A tentative target for the error logs.
const TARGET: &str = "experimental.client.request";

/// A future instrumented to generate the client request telemetry.
///
/// Decorates the `F` future, which represents a pending client request,
/// to emit the span, duration metric, and error logs. Typically this
/// is used in the tracing layer:
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
///     use google_cloud_gax_internal::observability::{DurationMetric, RequestStart};
///     use google_cloud_gax_internal::observability::client_signals::WithClientSignals;
///     let metric: DurationMetric = self.metric.clone();
///     let start = RequestStart::new(&info::INSTRUMENTATION_CLIENT_INFO, &options, "echo");
///     let span = tracing::info_span!("client_request",
///         "gax.client.span" = true
///         // ... many more attributes ...
///     );
///     let pending = self.inner.echo(req, options);
///     WithClientSignals::new(pending, metric, start, span).await
/// }
/// # }
/// ```
///
/// The final code will use a macro to create the `(start, span)` pair. The
/// macro captures several attribute values that are only available from the
/// callsite.
///
/// The final code also uses an extension trait to simplify the call to decorate
/// the `self.inner.echo()` future.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WithClientSignals<F> {
    #[pin]
    inner: F,
    metric: DurationMetric,
    start: RequestStart,
    span: Span,
}

impl<F, R> WithClientSignals<Instrumented<F>>
where
    F: Future<Output = Result<R, Error>>,
{
    #[allow(dead_code)]
    pub(crate) fn new(inner: F, metric: DurationMetric, start: RequestStart, span: Span) -> Self {
        let inner = inner.instrument(span.clone());
        Self {
            inner,
            metric,
            start,
            span,
        }
    }
}

impl<F, R> Future for WithClientSignals<Instrumented<F>>
where
    F: Future<Output = Result<R, Error>>,
{
    type Output = <F as Future>::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let span = self.span.clone();
        let start = self.start;
        let metric = self.metric.clone();
        let this = self.project();
        let output = futures::ready!(this.inner.poll(cx));
        // Record the metric and log the value in the context of the span.
        let _enter = span.entered();
        match &output {
            Ok(_) => metric.record_ok(start),
            Err(error) => {
                let rpc_status_code = error
                    .status()
                    .map(|s| s.code.name())
                    .unwrap_or(Code::Unknown.name());
                if let Some(http_code) = error.http_status_code() {
                    // TODO(#4795) - use the correct name and target
                    tracing::event!(
                        name: NAME,
                        target: TARGET,
                        tracing::Level::ERROR,
                        { RPC_SYSTEM_NAME } = RPC_SYSTEM_HTTP,
                        { attribute::URL_DOMAIN } = start.info().default_host,
                        { attribute::URL_TEMPLATE } = start.url_template(),
                        { attribute::RPC_METHOD } = start.method(),
                        { RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                        { attribute::HTTP_RESPONSE_STATUS_CODE } = http_code,
                        "{error:?}"
                    );
                } else {
                    tracing::event!(
                        name: NAME,
                        target: TARGET,
                        tracing::Level::ERROR,
                        { RPC_SYSTEM_NAME } = RPC_SYSTEM_HTTP,
                        { attribute::URL_DOMAIN } = start.info().default_host,
                        { attribute::URL_TEMPLATE } = start.url_template(),
                        { attribute::RPC_METHOD } = start.method(),
                        { RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                        "{error:?}"
                    );
                }
                metric.record_error(start, error)
            }
        }
        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::InstrumentationClientInfo;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
    use opentelemetry::logs::AnyValue;
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_sdk::logs::{
        BatchLogProcessor, InMemoryLogExporter, SdkLogRecord, SdkLoggerProvider,
    };
    use opentelemetry_sdk::trace::{BatchSpanProcessor, InMemorySpanExporter, SdkTracerProvider};
    use std::collections::BTreeSet;
    use std::future::ready;
    use tracing::subscriber::DefaultGuard;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::Registry;
    use tracing_subscriber::prelude::*;

    static TEST_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
        service_name: "test-service",
        client_version: "1.2.3",
        client_artifact: "test-artifact",
        default_host: "example.com",
    };
    static URL_TEMPLATE: &str = "/v1/projects/{}:test_method";
    static METHOD: &str = "test-method";

    const COMMON_ATTRIBUTES: [(&str, &str); 4] = [
        ("rpc.system.name", "http"),
        ("url.domain", "example.com"),
        ("url.template", URL_TEMPLATE),
        ("rpc.method", METHOD),
    ];

    // The tests run serially because the tracing subscriber is global, yuck.
    #[tokio::test(start_paused = true)]
    async fn poll_ok() -> anyhow::Result<()> {
        let (exporter, provider, _guard) = init_logger();

        let span = tracing::info_span!("test-span");
        let metric = DurationMetric::new(&TEST_INFO);
        let options = RequestOptions::default().insert_extension(PathTemplate(URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, METHOD);
        let future = ready(Ok::<String, Error>("hello world".to_string()));
        let future = WithClientSignals::new(future, metric.clone(), start, span.clone());
        let result = future.await;
        assert!(
            matches!(result, Ok(ref s) if s == "hello world"),
            "{result:?}"
        );

        provider.force_flush()?;
        let logs = exporter.get_emitted_logs()?;
        let record = logs
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET));
        assert!(record.is_none(), "{record:?}\nlogs={logs:#?}");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn poll_err_disabled_span() -> anyhow::Result<()> {
        let (exporter, provider, _guard) = init_logger();

        let span = Span::none();
        let metric = DurationMetric::new(&TEST_INFO);
        let options = RequestOptions::default().insert_extension(PathTemplate(URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, METHOD);
        let future = ready(Err::<String, Error>(not_found()));
        let future = WithClientSignals::new(future, metric.clone(), start, span.clone());
        let result = future.await;
        assert!(
            matches!(result, Err(ref e) if e.status() == not_found().status()),
            "{result:?}"
        );

        provider.force_flush()?;
        let captured = exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET))
            .unwrap_or_else(|| panic!("missing log for target {TARGET} in {captured:#?}"));
        assert!(
            record.record.trace_context().is_none(),
            "{record:?}\nspan={span:?}"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn poll_err() -> anyhow::Result<()> {
        let (exporter, provider, _guard) = init_logger();

        let span = crate::client_request_span!("TestClient", "test_method", &TEST_INFO);
        let _enter = span.enter();
        let metric = DurationMetric::new(&TEST_INFO);
        let options = RequestOptions::default().insert_extension(PathTemplate(URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, METHOD);
        let future = ready(Err::<String, Error>(not_found()));
        let future = WithClientSignals::new(future, metric.clone(), start, span.clone());
        let result = future.await;
        assert!(
            matches!(result, Err(ref e) if e.status() == not_found().status()),
            "{result:?}"
        );

        provider.force_flush()?;
        let captured = exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET))
            .unwrap_or_else(|| panic!("missing log for target {TARGET} in {captured:#?}"));
        check_log_record(
            &record.record,
            &span,
            &[("rpc.response.status_code", "NOT_FOUND")],
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn poll_err_http() -> anyhow::Result<()> {
        let (exporter, provider, _guard) = init_logger();

        let span = tracing::info_span!("test-span");
        let metric = DurationMetric::new(&TEST_INFO);
        let options = RequestOptions::default().insert_extension(PathTemplate(URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, METHOD);
        let future = ready(Err::<String, Error>(http_too_many_requests()));
        let future = WithClientSignals::new(future, metric.clone(), start, span.clone());
        let result = future.await;
        assert!(
            matches!(result, Err(ref e) if e.http_status_code() == http_too_many_requests().http_status_code()),
            "{result:?}"
        );

        provider.force_flush()?;
        let captured = exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET))
            .unwrap_or_else(|| panic!("missing log for target {TARGET} in {captured:#?}"));
        check_log_record(
            &record.record,
            &span,
            &[
                ("rpc.response.status_code", "UNKNOWN"),
                ("http.response.status_code", "429"),
            ],
        );

        Ok(())
    }

    #[track_caller]
    fn check_log_record(
        record: &SdkLogRecord,
        span: &Span,
        extra_attributes: &[(&'static str, &'static str)],
    ) {
        assert_eq!(record.event_name(), Some(NAME), "{record:?}");
        assert_eq!(
            record.target().map(|s| s.as_ref()),
            Some(TARGET),
            "{record:?}"
        );
        assert_eq!(record.severity_text(), Some("ERROR"), "{record:?}");
        let trace_id = span.context().span().span_context().trace_id();
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

    fn format_value(any: &AnyValue) -> String {
        match any {
            AnyValue::Int(v) => v.to_string(),
            AnyValue::Double(v) => v.to_string(),
            AnyValue::String(v) => v.to_string(),
            AnyValue::Boolean(v) => v.to_string(),
            _ => "unexpected AnyValue variant".to_string(),
        }
    }

    fn init_logger() -> (InMemoryLogExporter, SdkLoggerProvider, DefaultGuard) {
        // We need a tracing exporter and provider or the traces get no trace ids.
        let trace_exporter = InMemorySpanExporter::default();
        let trace_provider = SdkTracerProvider::builder()
            .with_span_processor(BatchSpanProcessor::builder(trace_exporter.clone()).build())
            .build();

        // We also need a logging exporter to capture the `tracing::event!()` logs
        // as they are forwarded to OpenTelemetry logs.
        let exporter = InMemoryLogExporter::default();
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(BatchLogProcessor::builder(exporter.clone()).build())
            .build();
        // Using a per-thread guard is Okay because all the tests in this module are single-threaded.
        let guard = tracing::subscriber::set_default(
            Registry::default()
                .with(OpenTelemetryTracingBridge::new(&provider))
                .with(
                    tracing_opentelemetry::layer().with_tracer(trace_provider.tracer("test-only")),
                ),
        );

        (exporter, provider, guard)
    }

    fn not_found() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::NotFound)
                .set_message("NOT FOUND"),
        )
    }

    fn http_too_many_requests() -> Error {
        Error::http(429, http::HeaderMap::new(), bytes::Bytes::new())
    }
}
