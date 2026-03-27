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
use crate::observability::attributes::keys::OTEL_STATUS_DESCRIPTION;
use crate::observability::attributes::keys::{
    GCP_CLIENT_ARTIFACT, GCP_CLIENT_LANGUAGE, GCP_CLIENT_REPO, GCP_CLIENT_SERVICE,
    GCP_CLIENT_VERSION, GCP_ERRORS_DOMAIN, GCP_ERRORS_METADATA, OTEL_STATUS_CODE,
    RPC_RESPONSE_STATUS_CODE, RPC_SYSTEM_NAME,
};
use crate::observability::attributes::otel_status_codes;
use crate::observability::attributes::{
    GCP_CLIENT_LANGUAGE_RUST, GCP_CLIENT_REPO_GOOGLEAPIS, RPC_SYSTEM_HTTP,
};
use crate::observability::errors::ErrorType;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use opentelemetry_semantic_conventions::attribute::{
    ERROR_TYPE, EXCEPTION_MESSAGE, EXCEPTION_TYPE, HTTP_RESPONSE_STATUS_CODE, RPC_METHOD,
    URL_DOMAIN, URL_TEMPLATE,
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
            Ok(_) => this.metric.record_ok(&this.start),
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
                let error_str = error_type.as_str();
                let err_msg = error.to_string();

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
                if !this.start.disable_actionable_error_logging() {
                    tracing::event!(
                        name: NAME,
                        target: TARGET,
                        tracing::Level::WARN,
                        { RPC_SYSTEM_NAME } = RPC_SYSTEM_HTTP,
                        { URL_DOMAIN } = this.start.info().default_host,
                        { URL_TEMPLATE } = this.start.url_template(),
                        { RPC_METHOD } = this.start.method(),
                        { GCP_CLIENT_VERSION } = this.start.info().client_version,
                        { GCP_CLIENT_REPO } = GCP_CLIENT_REPO_GOOGLEAPIS,
                        { GCP_CLIENT_ARTIFACT } = this.start.info().client_artifact,
                        { GCP_CLIENT_SERVICE } = this.start.info().service_name,
                        { GCP_CLIENT_LANGUAGE } = GCP_CLIENT_LANGUAGE_RUST,
                        { GCP_ERRORS_DOMAIN } = error_domain,
                        { GCP_ERRORS_METADATA } = error_metadata,
                        { RPC_RESPONSE_STATUS_CODE } = rpc_status_code,
                        { EXCEPTION_TYPE } = error_str,
                        { EXCEPTION_MESSAGE } = err_msg,
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
    use super::super::tests::{SignalProviders, check_log_record};
    use super::super::tests::{TEST_INFO, TEST_METHOD, TEST_URL_TEMPLATE};
    use super::*;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
    use opentelemetry::trace::{Status as TraceStatus, TraceContextExt};
    use opentelemetry::{TraceId, Value};
    use opentelemetry_sdk::trace::SpanData;
    use std::collections::BTreeSet;
    use std::future::ready;
    use std::sync::Arc;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    // The tests run serially because the tracing subscriber is global, yuck.
    #[tokio::test(start_paused = true)]
    async fn poll_ok() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let span = tracing::info_span!(
            "client_request",
            // Fields to be recorded later
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty
        );
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(providers.metric_provider.clone()),
        );
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
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
        assert_eq!(span.status, TraceStatus::Unset);
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
        let providers = SignalProviders::new();

        let span = Span::none();
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(providers.metric_provider.clone()),
        );
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
        let future = ready(Err::<String, Error>(not_found()));
        let future = WithClientSignals::new(future, metric.clone(), start, span.clone());
        let result = future.await;
        assert!(
            matches!(result, Err(ref e) if e.status() == not_found().status()),
            "{result:?}"
        );

        providers.force_flush()?;
        let captured = providers.logs_exporter.get_emitted_logs()?;
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
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty
        );
        let trace_id = trace_id(&span);
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(providers.metric_provider.clone()),
        );
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
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
                ("rpc.method", TEST_METHOD),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.language", "rust"),
                ("rpc.response.status_code", "NOT_FOUND"),
                ("exception.type", "NOT_FOUND"),
                (
                    "exception.message",
                    "the service reports an error with code NOT_FOUND described as: NOT FOUND",
                ),
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
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty
        );
        let trace_id = trace_id(&span);
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(providers.metric_provider.clone()),
        );
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
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
                ("rpc.method", TEST_METHOD),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.language", "rust"),
                ("rpc.response.status_code", "UNKNOWN"),
                ("exception.type", "429"),
                (
                    "exception.message",
                    "the HTTP transport reports a [429] error: ",
                ),
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

    #[tokio::test(start_paused = true)]
    async fn poll_err_suppresses_actionable_logs() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let span = tracing::info_span!(
            "client_request",
            { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty
        );

        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(providers.metric_provider.clone()),
        );
        use google_cloud_gax::options::internal::RequestOptionsExt;
        let options = RequestOptions::default()
            .insert_extension(PathTemplate(TEST_URL_TEMPLATE))
            .insert_extension(crate::observability::client_signals::SuppressActionableErrorLog);
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);

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
            .find(|r| r.record.target().is_some_and(|v| v == TARGET));

        assert!(
            record.is_none(),
            "unexpected actionable log record found: {record:?}"
        );

        Ok(())
    }
}
