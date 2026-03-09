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
use crate::observability::attributes::keys::OTEL_STATUS_DESCRIPTION;
use crate::observability::attributes::keys::{
    OTEL_STATUS_CODE, RPC_RESPONSE_STATUS_CODE, RPC_SYSTEM_NAME,
};
use crate::observability::attributes::otel_status_codes;
use crate::observability::errors::ErrorType;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use opentelemetry_semantic_conventions::attribute::{
    ERROR_TYPE, HTTP_RESPONSE_STATUS_CODE, RPC_METHOD, URL_DOMAIN, URL_TEMPLATE,
};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::Instrument;
use tracing::Span;
use tracing::instrument::Instrumented;

// A tentative name for the error logs.
pub const NAME: &str = "experimental.client.request.error";
// A tentative target for the error logs.
pub const TARGET: &str = "experimental.client.request";

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
    #[pin]
    metric: DurationMetric,
    #[pin]
    start: RequestStart,
    span: Span,
}

impl<F, R> WithClientSignals<Instrumented<F>>
where
    F: Future<Output = Result<R, Error>>,
{
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
        let this = self.project();
        let output = futures::ready!(this.inner.poll(cx));
        // Record the metric and log the value in the context of the span.
        let span = span.entered();
        match &output {
            Ok(_) => {
                span.record(OTEL_STATUS_CODE, otel_status_codes::OK);
                this.metric.record_ok(&this.start)
            }
            Err(error) => {
                let error_type = ErrorType::from_gax_error(error);
                tracing::record_all!(
                    span,
                    { OTEL_STATUS_CODE } = otel_status_codes::ERROR,
                    { OTEL_STATUS_DESCRIPTION } = error.to_string(),
                    { ERROR_TYPE } = error_type.as_str()
                );
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
                        { URL_DOMAIN } = this.start.info().default_host,
                        { URL_TEMPLATE } = this.start.url_template(),
                        { RPC_METHOD } = this.start.method(),
                        { RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                        { HTTP_RESPONSE_STATUS_CODE } = http_code,
                        "{error:?}"
                    );
                } else {
                    tracing::event!(
                        name: NAME,
                        target: TARGET,
                        tracing::Level::ERROR,
                        { RPC_SYSTEM_NAME } = RPC_SYSTEM_HTTP,
                        { URL_DOMAIN } = this.start.info().default_host,
                        { URL_TEMPLATE } = this.start.url_template(),
                        { RPC_METHOD } = this.start.method(),
                        { RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                        "{error:?}"
                    );
                }
                this.metric.record_error(&this.start, error)
            }
        }
        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{METHOD, TEST_INFO, URL_TEMPLATE};
    use super::super::tests::{SignalProviders, check_log_record};
    use super::*;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
    use opentelemetry::trace::{Status as TraceStatus, TraceContextExt};
    use opentelemetry::{TraceId, Value};
    use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
    use opentelemetry_sdk::trace::SpanData;
    use std::collections::BTreeSet;
    use std::future::ready;
    use tracing::subscriber::DefaultGuard;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    // The tests run serially because the tracing subscriber is global, yuck.
    #[tokio::test(start_paused = true)]
    async fn poll_ok() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let span = tracing::info_span!(
            "client_request",
            // Fields to be recorded later
            { OTEL_STATUS_CODE } = ::tracing::field::Empty,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty
        );
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
        let trace_id = trace_id(&span);

        drop(span);
        providers.force_flush()?;
        let logs = providers.logs_exporter.get_emitted_logs()?;
        let record = logs
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET));
        assert!(record.is_none(), "{record:?}\nlogs={logs:#?}");

        let captured = providers.trace_exporter.get_finished_spans()?;
        let span = match &captured[..] {
            [s] => s,
            _ => panic!("expected exactly one span, got={captured:?}"),
        };
        // The tracing-opentelemetry subscriber converts `otel.status_code` and
        // `otel.status_description` into `span.status`.
        assert_eq!(span.status, TraceStatus::Ok);
        check_span(span, trace_id, &[]);
        let found = span
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == ERROR_TYPE);
        assert!(found.is_none(), "{found:?}");

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
        let providers = SignalProviders::new();

        let span = tracing::info_span!(
            "client_request",
            // Fields to be recorded later
            { OTEL_STATUS_CODE } = ::tracing::field::Empty,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty
        );
        let trace_id = trace_id(&span);
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

        drop(span);
        providers.force_flush()?;
        let captured = providers.logs_exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET))
            .unwrap_or_else(|| panic!("missing log for target {TARGET} in {captured:#?}"));
        check_log_record(
            &record.record,
            trace_id,
            &[
                ("rpc.method", METHOD),
                ("rpc.response.status_code", "NOT_FOUND"),
            ],
        );

        let captured = providers.trace_exporter.get_finished_spans()?;
        let span = match &captured[..] {
            [s] => s,
            _ => panic!("expected exactly one span, got={captured:?}"),
        };
        // The tracing-opentelemetry subscriber converts `otel.status_code` and
        // `otel.status_description` into `span.status`.
        assert_eq!(
            span.status,
            TraceStatus::Error {
                description: not_found().to_string().into()
            }
        );
        check_span(span, trace_id, &[("error.type", "NOT_FOUND")]);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn poll_err_http() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let span = tracing::info_span!(
            "client_request",
            // Fields to be recorded later
            { OTEL_STATUS_CODE } = ::tracing::field::Empty,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty
        );
        let trace_id = trace_id(&span);
        let metric = DurationMetric::new(&TEST_INFO);
        let options = RequestOptions::default().insert_extension(PathTemplate(URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, METHOD);
        let future = ready(Err::<String, Error>(http_too_many_requests()));
        let future = WithClientSignals::new(future, metric.clone(), start, span);
        let result = future.await;
        assert!(
            matches!(result, Err(ref e) if e.http_status_code() == http_too_many_requests().http_status_code()),
            "{result:?}"
        );

        providers.force_flush()?;

        let captured = providers.logs_exporter.get_emitted_logs()?;
        let record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == TARGET))
            .unwrap_or_else(|| panic!("missing log for target {TARGET} in {captured:#?}"));
        check_log_record(
            &record.record,
            trace_id,
            &[
                ("rpc.method", METHOD),
                ("rpc.response.status_code", "UNKNOWN"),
                ("http.response.status_code", "429"),
            ],
        );

        let captured = providers.trace_exporter.get_finished_spans()?;
        let span = match &captured[..] {
            [s] => s,
            _ => panic!("expected exactly one span, got={captured:?}"),
        };
        // The tracing-opentelemetry subscriber converts `otel.status_code` and
        // `otel.status_description` into `span.status`.
        assert_eq!(
            span.status,
            TraceStatus::Error {
                description: http_too_many_requests().to_string().into()
            }
        );
        check_span(span, trace_id, &[("error.type", "429")]);

        Ok(())
    }

    #[track_caller]
    pub fn check_span(
        data: &SpanData,
        trace_id: TraceId,
        attributes: &[(&'static str, &'static str)],
    ) {
        fn format_value(value: &Value) -> String {
            match value {
                Value::Bool(v) => v.to_string(),
                Value::I64(v) => v.to_string(),
                Value::F64(v) => v.to_string(),
                Value::String(v) => v.to_string(),
                _ => "unexpected Value variant".to_string(),
            }
        }
        assert_eq!(data.span_context.trace_id(), trace_id, "{data:?}");
        let unsorted = data
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), format_value(&kv.value)))
            .collect::<Vec<_>>();
        let got = BTreeSet::from_iter(
            data.attributes
                .iter()
                .map(|kv| (kv.key.as_str(), format_value(&kv.value))),
        );
        let want = BTreeSet::from_iter(attributes.iter().map(|(k, v)| (*k, v.to_string())));
        let missing = want.difference(&got).collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "missing = {missing:?}\nwant = {want:?}\ngot  = {got:?}\nunsorted = {unsorted:?}\nspan = {data:?}"
        );
    }

    fn trace_id(span: &Span) -> TraceId {
        span.context().span().span_context().trace_id()
    }

    fn init_logger() -> (InMemoryLogExporter, SdkLoggerProvider, DefaultGuard) {
        let providers = SignalProviders::new();
        (
            providers.logs_exporter,
            providers.logs_provider,
            providers.guard,
        )
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
