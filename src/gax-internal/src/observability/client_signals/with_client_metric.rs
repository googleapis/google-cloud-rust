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

//! Implements [WithClientMetric] a decorator for [Future] adding duration metrics.
//!
//! This is a private module, it is not exposed in the public API.

use crate::observability::DurationMetric;
use google_cloud_gax::error::Error;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A future instrumented to generate the client request duration.
///
/// Decorates the `F` future, which represents a pending client request,
/// to emit record the request duration. Typically this is used in the tracing layer:
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
///     use google_cloud_gax_internal::observability::client_signals::WithClientMetric;
///     let pending = self.inner.echo(req, options);
///     WithClientMetric::new(self.metric.clone(), pending).await
/// }
/// # }
/// ```
///
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WithClientMetric<F> {
    #[pin]
    inner: F,
    #[pin]
    metric: DurationMetric,
}

impl<F, R> WithClientMetric<F>
where
    F: Future<Output = Result<R, Error>>,
{
    pub fn new(metric: DurationMetric, inner: F) -> Self {
        Self { metric, inner }
    }
}

impl<F, R> Future for WithClientMetric<F>
where
    F: Future<Output = Result<R, Error>>,
{
    type Output = <F as Future>::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let output = futures::ready!(this.inner.poll(cx));
        match &output {
            Ok(_) => this.metric.with_recorder_ok(),
            Err(error) => this.metric.with_recorder_error(error),
        }
        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        SignalProviders, TEST_INFO, TEST_METHOD, TEST_REQUEST_DURATION, check_metric_data,
        check_metric_scope, recorded_request_transport_stub,
    };
    use super::*;
    use crate::observability::RequestRecorder;
    use crate::observability::client_signals::tests::TEST_URL_TEMPLATE;
    use httptest::matchers::request::method_path;
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use std::sync::Arc;

    #[tokio::test]
    async fn no_recorder() -> anyhow::Result<()> {
        let signals = SignalProviders::new();
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );
        let got = WithClientMetric::new(metric, async { Ok(123) }).await;
        assert!(matches!(got, Ok(123)), "{got:?}");

        signals.force_flush()?;
        // Verify the metrics include the data we want.
        let metrics = signals.metric_exporter.get_finished_metrics()?;
        assert!(metrics.is_empty(), "{metrics:?}");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn ok_no_annotations() -> anyhow::Result<()> {
        let signals = SignalProviders::new();
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );

        let recorder = RequestRecorder::new(TEST_INFO);
        let got = recorder
            .scope(WithClientMetric::new(metric, async {
                let _current =
                    RequestRecorder::current().expect("current recorder should be available");
                tokio::time::sleep(TEST_REQUEST_DURATION).await;
                Ok(123)
            }))
            .await;
        assert!(matches!(got, Ok(123)), "{got:?}");
        signals.force_flush()?;
        // Verify the metrics include the data we want.
        let metrics = signals.metric_exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            1_u64..=1_u64,
            &[
                ("rpc.response.status_code", "OK"),
                ("url.domain", "example.com"),
            ],
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn err_with_annotations() -> anyhow::Result<()> {
        let signals = SignalProviders::new();
        let metric = DurationMetric::new_with_provider(
            &TEST_INFO,
            Arc::new(signals.metric_provider.clone()),
        );
        const PATH: &str = "/v1/projects/test-only:test_method";

        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", PATH))
                .respond_with(status_code(404).body("NOT FOUND")),
        );
        let url = server.url(PATH).to_string();

        let recorder = RequestRecorder::new(TEST_INFO);
        let got = recorder
            .scope(WithClientMetric::new(
                metric,
                recorded_request_transport_stub(&url),
            ))
            .await;
        assert!(got.is_err(), "{got:?}");
        signals.force_flush()?;
        // Verify the metrics include the data we want.
        let metrics = signals.metric_exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            1_u64..=1_u64,
            &[
                ("rpc.method", TEST_METHOD),
                ("rpc.system.name", "http"),
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("http.response.status_code", "404"),
            ],
        );
        Ok(())
    }
}
