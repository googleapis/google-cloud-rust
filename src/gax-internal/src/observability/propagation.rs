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

#[allow(dead_code)]
pub(crate) struct HeaderInjector<'a>(pub &'a mut http::HeaderMap);

impl<'a> opentelemetry::propagation::Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = http::header::HeaderName::from_bytes(key.as_bytes()) {
            if let Ok(val) = http::header::HeaderValue::from_str(&value) {
                self.0.insert(name, val);
            }
        }
    }
}

/// Injects the OpenTelemetry context from the given tracing span into the provided HTTP headers.
#[allow(dead_code)]
pub(crate) fn inject_context(span: &tracing::Span, headers: &mut http::HeaderMap) {
    let context = tracing_opentelemetry::OpenTelemetrySpanExt::context(span);
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(headers))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::propagation::Injector;
    use http::HeaderMap;

    #[test]
    fn test_injector_valid_headers() {
        let mut headers = HeaderMap::new();
        let mut injector = HeaderInjector(&mut headers);

        injector.set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0by902b7-01".to_string());
        injector.set("tracestate", "rojo=00f067aa0by902b7,congo=t61rcWkgMzE".to_string());

        assert_eq!(headers.get("traceparent").unwrap().to_str().unwrap(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0by902b7-01");
        assert_eq!(headers.get("tracestate").unwrap().to_str().unwrap(), "rojo=00f067aa0by902b7,congo=t61rcWkgMzE");
    }

    #[test]
    fn test_injector_invalid_key() {
        let mut headers = HeaderMap::new();
        let mut injector = HeaderInjector(&mut headers);

        // Invalid characters in header key
        injector.set("invalid key \n", "value".to_string());

        assert!(headers.is_empty());
    }

    #[test]
    fn test_injector_invalid_value() {
        let mut headers = HeaderMap::new();
        let mut injector = HeaderInjector(&mut headers);

        // Invalid characters in header value
        injector.set("valid-key", "invalid\nvalue".to_string());

        assert!(headers.is_empty());
    }

    #[test]
    fn test_inject_context_success() {
        // 1. Setup a global propagator for the test
        let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
        opentelemetry::global::set_text_map_propagator(propagator);

        // 2. Setup tracing with OpenTelemetry layer
        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        use opentelemetry::trace::TracerProvider as _;
        let tracer = tracer_provider.tracer("test");

        use tracing_subscriber::layer::SubscriberExt;
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(telemetry);
        let dispatcher = tracing::Dispatch::new(subscriber);

        let mut headers = HeaderMap::new();

        // 3. Run inside the dispatcher so the span gets the OTel context
        tracing::dispatcher::with_default(&dispatcher, || {
            let span = tracing::info_span!("test_inject_context_span");
            let _enter = span.enter();

            // 4. Inject the context into empty headers
            inject_context(&span, &mut headers);
        });

        // 5. Verify the traceparent header was successfully injected
        assert!(headers.contains_key("traceparent"), "Headers: {:?}", headers);
        let traceparent = headers.get("traceparent").unwrap().to_str().unwrap();
        assert!(traceparent.starts_with("00-"));
    }

    #[test]
    fn test_inject_context_no_propagator() {
        // Clear any global propagator that might be set by other tests
        // (OpenTelemetry uses a NoopTextMapPropagator by default)
        opentelemetry::global::set_text_map_propagator(opentelemetry::propagation::TextMapCompositePropagator::new(vec![]));

        let span = tracing::info_span!("test_no_propagator_span");
        let mut headers = HeaderMap::new();
        
        inject_context(&span, &mut headers);

        // Without a valid global propagator, it should silently do nothing (no headers added)
        assert!(headers.is_empty());
    }
}
