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

use super::{DurationMetric, RequestStart, WithClientSignals};
use google_cloud_gax::error::Error;
use std::future::Future;
use tracing::Span;

/// Extends the `std::future::Future` types to provide client request telemetry.
///
/// Wraps a future representing an client request to associate a span with a
/// duration metric, and produce a log message (also associated to the span) on
/// errors.
///
/// An instrumented client would use this as part of the tracing layer, for example:
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
///     use google_cloud_gax_internal::observability::MetricDuration;
///     let (start, span) = google_cloud_gax_internal::client_request_signals!(
///         "client::Echo",
///         "echo",
///         &info::INSTRUMENTATION_CLIENT_INFO,
///         &options
///     );
///     let duration: MetricDuration  = self.duration.clone();
///     self.inner
///         .echo(req, options)
///         .instrument_client(duration, start, span)
///         .await
/// }
/// # }
/// ```
pub trait ClientSignalsExt: Sized + sealed::ClientSignalsExt {
    type Inner;
    fn instrument_client(
        self,
        metric: DurationMetric,
        start: RequestStart,
        span: Span,
    ) -> WithClientSignals<Self::Inner>;
}

mod sealed {
    /// Prevents implementation outside the crate.
    pub trait ClientSignalsExt {}
}

impl<T, R> sealed::ClientSignalsExt for T where T: Future<Output = Result<R, Error>> {}
impl<T, R> ClientSignalsExt for T
where
    T: Future<Output = Result<R, Error>>,
{
    type Inner = tracing::instrument::Instrumented<Self>;
    fn instrument_client(
        self,
        metric: DurationMetric,
        start: RequestStart,
        span: Span,
    ) -> WithClientSignals<Self::Inner> {
        WithClientSignals::new(self, metric, start, span)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::*;
    use super::*;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
    use std::future::ready;
    use tracing::instrument::Instrumented;

    #[tokio::test(start_paused = true)]
    async fn basic() -> anyhow::Result<()> {
        let metric = DurationMetric::new(&TEST_INFO);

        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = super::RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
        let span = tracing::info_span!("test-span");

        let future = ready(Ok::<String, Error>("hello".to_string()));
        let instrumented: WithClientSignals<Instrumented<_>> =
            future.instrument_client(metric, start, span);
        let result = instrumented.await;
        assert!(matches!(result, Ok(ref s) if s == "hello"), "{result:?}");
        Ok(())
    }
}
