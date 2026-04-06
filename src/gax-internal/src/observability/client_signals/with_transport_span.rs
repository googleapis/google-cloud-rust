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

//! Implements [WithTransportSpan] a decorator for [Future] adding span attributes.
//!
//! This is a private module, it is not exposed in the public API.

use super::RequestRecorder;
use crate::observability::attributes::keys::*;
use crate::observability::attributes::otel_status_codes;
use crate::observability::errors::ErrorType;
use google_cloud_gax::error::Error;
use opentelemetry_semantic_conventions::attribute::HTTP_RESPONSE_STATUS_CODE;
use opentelemetry_semantic_conventions::trace::{HTTP_RESPONSE_BODY_SIZE, URL_SCHEME};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::Span;

/// A future instrumented to add span attributes for transport attempts.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WithTransportSpan<F> {
    #[pin]
    inner: F,
    span: Span,
}

impl<F> WithTransportSpan<F>
where
    F: Future<Output = Result<reqwest::Response, Error>>,
{
    pub fn new(span: Span, inner: F) -> Self {
        Self { inner, span }
    }
}

impl<F> Future for WithTransportSpan<F>
where
    F: Future<Output = Result<reqwest::Response, Error>>,
{
    type Output = <F as Future>::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let span = self.span.clone();
        let this = self.project();
        let output = futures::ready!(this.inner.poll(cx));

        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return Poll::Ready(output);
        };

        match &output {
            Ok(_) => {
                tracing::record_all!(
                    span,
                    { HTTP_RESPONSE_STATUS_CODE } = snapshot.http_status_code().map(|v| v as i64),
                    { NETWORK_PEER_ADDRESS } = snapshot.network_peer_address(),
                    { NETWORK_PEER_PORT } = snapshot.network_peer_port(),
                    { HTTP_RESPONSE_BODY_SIZE } = snapshot.http_response_body_size(),
                    { HTTP_REQUEST_RESEND_COUNT } = snapshot.http_resend_count().map(|v| v as i64),
                    { URL_SCHEME } = snapshot.url_scheme()
                );
            }
            Err(err) => {
                let error_type = ErrorType::from_gax_error(err);
                tracing::record_all!(
                    span,
                    { OTEL_STATUS_CODE } = otel_status_codes::ERROR,
                    { HTTP_RESPONSE_STATUS_CODE } = err.http_status_code().map(|v| v as i64),
                    { ERROR_TYPE } = error_type.as_str(),
                    { OTEL_STATUS_DESCRIPTION } = err.to_string(),
                    { HTTP_REQUEST_RESEND_COUNT } = snapshot.http_resend_count().map(|v| v as i64)
                );
                crate::observability::errors::emit_error_log(&span, err);
            }
        }
        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::http_tracing::create_http_attempt_span;
    use super::super::tests::{SignalProviders, TEST_INFO};
    use super::*;
    use google_cloud_gax::options::RequestOptions;
    use httptest::{Expectation, Server, matchers::request::method_path, responders::status_code};
    use opentelemetry::trace::SpanKind;
    use pretty_assertions::assert_eq;
    use std::collections::BTreeSet;

    #[tokio::test(start_paused = true)]
    async fn poll_ok() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", "/"))
                .respond_with(status_code(200).insert_header("Content-Length", "2").body("OK")),
        );
        let url = server.url("/").to_string();

        let request = reqwest::Client::new().get(&url).build()?;
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, Some(&TEST_INFO), 0);

        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_http_request(&request);

        let client = reqwest::Client::new();
        let pending = async move {
            let res = client
                .execute(request)
                .await
                .map_err(|e| google_cloud_gax::error::Error::io(e.to_string()));
            if let Some(recorder) = RequestRecorder::current() {
                if let Ok(r) = &res {
                    recorder.on_http_response(r);
                }
            }
            res
        };

        let future = recorder
            .clone()
            .scope(WithTransportSpan::new(span.clone(), pending));
        let result = future.await;
        assert!(result.is_ok(), "{result:?}");

        drop(span);
        providers.force_flush()?;
        let captured = providers.trace_exporter.get_finished_spans()?;
        let record = match &captured[..] {
            [record] => record,
            _ => panic!("expected a single capture: {captured:#?}"),
        };

        let got = BTreeSet::from_iter(
            record
                .attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );

        assert_eq!(record.span_kind, SpanKind::Client);
        assert!(got.contains(&("http.response.status_code", "200".to_string())));
        assert!(got.contains(&("http.response.body_size", "2".to_string())));
        assert!(got.contains(&("url.scheme", "http".to_string())));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn poll_err() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let server = Server::run();
        let url = server.url("/").to_string();

        let request = reqwest::Client::new().get(&url).build()?;
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, Some(&TEST_INFO), 0);

        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_http_request(&request);

        let pending = async move {
            // Simulate a failure by returning an error directly
            let err = google_cloud_gax::error::Error::http(
                404,
                http::HeaderMap::new(),
                bytes::Bytes::new(),
            );
            if let Some(recorder) = RequestRecorder::current() {
                recorder.on_http_error(&err);
            }
            Err(err)
        };

        let future = recorder
            .clone()
            .scope(WithTransportSpan::new(span.clone(), pending));
        let result = future.await;
        assert!(result.is_err(), "{result:?}");

        drop(span);
        providers.force_flush()?;
        let captured = providers.trace_exporter.get_finished_spans()?;
        let record = match &captured[..] {
            [record] => record,
            _ => panic!("expected a single capture: {captured:#?}"),
        };

        let got = BTreeSet::from_iter(
            record
                .attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );

        assert_eq!(record.span_kind, SpanKind::Client);
        assert!(got.contains(&("http.response.status_code", "404".to_string())));
        assert!(got.contains(&("error.type", "404".to_string())));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn poll_ok_retry() -> anyhow::Result<()> {
        let providers = SignalProviders::new();

        let server = Server::run();
        server.expect(
            Expectation::matching(method_path("GET", "/"))
                .respond_with(status_code(200).body("OK")),
        );
        let url = server.url("/").to_string();

        let request = reqwest::Client::new().get(&url).build()?;
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, Some(&TEST_INFO), 1);

        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_http_request(&request);

        // Simulate a previous failed attempt
        let err = google_cloud_gax::error::Error::io("simulated".to_string());
        recorder.on_http_error(&err);

        let client = reqwest::Client::new();
        let pending = async move {
            let res = client
                .execute(request)
                .await
                .map_err(|e| google_cloud_gax::error::Error::io(e.to_string()));
            if let Some(recorder) = RequestRecorder::current() {
                if let Ok(r) = &res {
                    recorder.on_http_response(r);
                }
            }
            res
        };

        let future = recorder
            .clone()
            .scope(WithTransportSpan::new(span.clone(), pending));
        let result = future.await;
        assert!(result.is_ok(), "{result:?}");

        drop(span);
        providers.force_flush()?;
        let captured = providers.trace_exporter.get_finished_spans()?;
        let record = match &captured[..] {
            [record] => record,
            _ => panic!("expected a single capture: {captured:#?}"),
        };

        let got = BTreeSet::from_iter(
            record
                .attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.to_string())),
        );

        assert!(got.contains(&("http.request.resend_count", "1".to_string())));

        Ok(())
    }
}
