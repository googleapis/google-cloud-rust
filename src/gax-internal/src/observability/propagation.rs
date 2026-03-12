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

//! Trace context propagation utilities.
//!
//! This module provides a simple `HeaderInjector` that allows the OpenTelemetry
//! global propagator to inject W3C trace context headers into `http::HeaderMap`.
//! This allows us to propagate distributed tracing context (like `traceparent`)
//! to backend services without adding an extra dependency on `opentelemetry-http`.
//!
//! Note: Users must align their application's `opentelemetry` and
//! `tracing-opentelemetry` versions with the versions used by this library.
//! Cargo treats 0.x releases as major versions, so a version mismatch will
//! result in disconnected global contexts and trace propagation will fail.

use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};
use opentelemetry::Context;
use opentelemetry::propagation::{Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::str::FromStr;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[allow(dead_code)]
pub(crate) struct HeaderInjector<'a>(pub &'a mut HeaderMap);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(key), Ok(value)) = (HeaderName::from_str(key), HeaderValue::from_str(&value)) {
            self.0.insert(key, value);
        }
    }
}

/// Injects the OpenTelemetry context from the given tracing span into the provided HTTP headers.
#[allow(dead_code)]
pub(crate) fn inject_context(span: &Span, headers: &mut HeaderMap) {
    let mut context = span.context();

    // If the tracing span doesn't have a valid trace ID (e.g., the user isn't
    // using the tracing_opentelemetry subscriber bridge), fall back to the global
    // OTel context (for pure opentelemetry_sdk users).
    if !context.span().span_context().is_valid() {
        context = Context::current();
    }

    let propagator = TraceContextPropagator::new();
    propagator.inject_context(&context, &mut HeaderInjector(headers));
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceFlags, TraceId, TraceState, TracerProvider,
    };
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing::Dispatch;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn injector_valid_headers() {
        let mut headers = HeaderMap::new();
        let mut injector = HeaderInjector(&mut headers);

        injector.set(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0by902b7-01".to_string(),
        );
        injector.set(
            "tracestate",
            "rojo=00f067aa0by902b7,congo=t61rcWkgMzE".to_string(),
        );

        assert_eq!(
            headers.get("traceparent").unwrap().to_str().unwrap(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0by902b7-01"
        );
        assert_eq!(
            headers.get("tracestate").unwrap().to_str().unwrap(),
            "rojo=00f067aa0by902b7,congo=t61rcWkgMzE"
        );
    }

    #[test]
    fn injector_invalid_key() {
        let mut headers = HeaderMap::new();
        let mut injector = HeaderInjector(&mut headers);

        // Invalid characters in header key
        injector.set("invalid key\n", "value".to_string());

        assert!(headers.is_empty(), "{headers:?}");
    }

    #[test]
    fn injector_invalid_value() {
        let mut headers = HeaderMap::new();
        let mut injector = HeaderInjector(&mut headers);

        // Invalid characters in header value
        injector.set("valid-key", "invalid\nvalue".to_string());

        assert!(headers.is_empty(), "{headers:?}");
    }

    fn set_up_otel_and_tracing() -> Dispatch {
        let tracer_provider = SdkTracerProvider::builder().build();
        let tracer = tracer_provider.tracer("test");

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(telemetry);
        Dispatch::new(subscriber)
    }

    #[test]
    fn inject_context_success() {
        let dispatcher = set_up_otel_and_tracing();

        let mut headers = HeaderMap::new();

        tracing::dispatcher::with_default(&dispatcher, || {
            let span = tracing::info_span!("test_inject_context_span");
            let _enter = span.enter();

            inject_context(&span, &mut headers);
        });

        assert!(
            matches!(
                headers.get("traceparent").map(|v| v.to_str()),
                Some(Ok(v)) if v.starts_with("00-")
            ),
            "Headers: {:?}",
            headers
        );
    }

    #[test]
    fn inject_context_fallback_to_opentelemetry_context() {
        let trace_id = TraceId::from_hex("00000000000000000000000000000001").unwrap();
        let span_id = SpanId::from_hex("0000000000000002").unwrap();
        let span_context = SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let otel_context = Context::new().with_remote_span_context(span_context);

        // Create a tracing span without the tracing_opentelemetry subscriber active
        let span = tracing::info_span!("test_fallback_span");

        let _guard = otel_context.attach();

        let mut headers = HeaderMap::new();

        // Inject should fail to pull from `span` and fallback to `otel_context`
        inject_context(&span, &mut headers);

        assert!(
            matches!(
                headers.get("traceparent").map(|v| v.to_str()),
                Some(Ok(
                    "00-00000000000000000000000000000001-0000000000000002-01"
                ))
            ),
            "Headers: {:?}",
            headers
        );
    }
}
