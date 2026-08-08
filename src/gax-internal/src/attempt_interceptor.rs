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

//! Types and traits for intercepting and modifying outgoing RPC attempts.

use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use http::HeaderMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

/// A callback invoked on outgoing RPC attempts, allowing modification of gRPC headers and tracking attempt lifecycle.
///
/// # Unary vs. Streaming RPC Lifecycles
/// - [`intercept`](Self::intercept): Invoked for **all** outgoing RPCs (unary and streaming) before transmission,
///   allowing headers (such as authentication or request IDs) to be modified.
/// - [`on_attempt_start`](Self::on_attempt_start) and [`on_attempt_complete`](Self::on_attempt_complete): Lifecycle
///   hooks invoked specifically for **unary RPC attempts** managed by the GAX retry loop, allowing attempt
///   duration, response headers, and attempt outcomes to be measured.
///
/// Streaming RPCs have lifecycles that extend across stream consumption beyond the initial request dispatch;
/// their requests invoke [`intercept`](Self::intercept) during stream establishment, while stream iteration
/// and retries are managed by the higher-level streaming layer.
pub trait AttemptInterceptor: Debug + Send + Sync {
    /// Intercepts and modifies the headers of an outgoing RPC attempt.
    ///
    /// This method is invoked for all outgoing RPCs (both unary and streaming).
    ///
    /// * `headers`: The mutable map of headers to be sent with the request.
    /// * `attempt`: The 1-based attempt number for the current RPC.
    fn intercept(&self, _headers: &mut HeaderMap, _attempt: u32) {}

    /// Callback invoked before a unary RPC attempt is dispatched by the GAX retry loop.
    ///
    /// Allows header mutation and returns the start [`Instant`] of the attempt.
    /// The default implementation delegates to [`self.intercept(headers, attempt)`](Self::intercept)
    /// and returns [`Instant::now()`].
    ///
    /// * `method`: The gRPC method path (e.g. `"/google.spanner.v1.Spanner/ExecuteSql"`).
    /// * `attempt`: The 1-based attempt number for the current RPC.
    /// * `headers`: The mutable map of headers to be sent with the request.
    /// * `options`: The request options associated with the RPC.
    fn on_attempt_start(
        &self,
        _method: &str,
        attempt: u32,
        headers: &mut HeaderMap,
        _options: &RequestOptions,
    ) -> Instant {
        self.intercept(headers, attempt);
        Instant::now()
    }

    /// Callback invoked when a unary RPC attempt completes (either successfully or with an error).
    ///
    /// * `method`: The gRPC method path (e.g. `"/google.spanner.v1.Spanner/ExecuteSql"`).
    /// * `attempt`: The 1-based attempt number for the RPC.
    /// * `start_time`: The instant returned by [`on_attempt_start`](Self::on_attempt_start).
    /// * `response_headers`: The response metadata headers returned by the server, if available.
    /// * `error`: The error returned by this attempt, if the attempt failed.
    /// * `options`: The request options associated with the RPC.
    fn on_attempt_complete(
        &self,
        _method: &str,
        _attempt: u32,
        _start_time: Instant,
        _response_headers: Option<&HeaderMap>,
        _error: Option<&Error>,
        _options: &RequestOptions,
    ) {
    }
}

impl AttemptInterceptor for Vec<Arc<dyn AttemptInterceptor>> {
    /// Callback invoked before a unary RPC attempt is dispatched.
    ///
    /// # Note
    /// Any custom `Instant` returned by individual interceptors in the `Vec` is discarded.
    /// The composite implementation always returns a fresh `Instant::now()`.
    fn on_attempt_start(
        &self,
        method: &str,
        attempt: u32,
        headers: &mut HeaderMap,
        options: &RequestOptions,
    ) -> Instant {
        for interceptor in self {
            interceptor.on_attempt_start(method, attempt, headers, options);
        }
        Instant::now()
    }

    fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
        for interceptor in self {
            interceptor.intercept(headers, attempt);
        }
    }

    fn on_attempt_complete(
        &self,
        method: &str,
        attempt: u32,
        start_time: Instant,
        response_headers: Option<&HeaderMap>,
        error: Option<&Error>,
        options: &RequestOptions,
    ) {
        for interceptor in self {
            interceptor.on_attempt_complete(
                method,
                attempt,
                start_time,
                response_headers,
                error,
                options,
            );
        }
    }
}

impl<T: AttemptInterceptor + ?Sized> AttemptInterceptor for Arc<T> {
    fn on_attempt_start(
        &self,
        method: &str,
        attempt: u32,
        headers: &mut HeaderMap,
        options: &RequestOptions,
    ) -> Instant {
        (**self).on_attempt_start(method, attempt, headers, options)
    }

    fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
        (**self).intercept(headers, attempt);
    }

    fn on_attempt_complete(
        &self,
        method: &str,
        attempt: u32,
        start_time: Instant,
        response_headers: Option<&HeaderMap>,
        error: Option<&Error>,
        options: &RequestOptions,
    ) {
        (**self).on_attempt_complete(
            method,
            attempt,
            start_time,
            response_headers,
            error,
            options,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_debug<T: Debug>() {}
    fn assert_attempt_interceptor<T: AttemptInterceptor>() {}

    #[test]
    fn traits() {
        assert_send::<Arc<dyn AttemptInterceptor>>();
        assert_sync::<Arc<dyn AttemptInterceptor>>();
        assert_debug::<Arc<dyn AttemptInterceptor>>();
        assert_attempt_interceptor::<Arc<dyn AttemptInterceptor>>();

        assert_send::<Vec<Arc<dyn AttemptInterceptor>>>();
        assert_sync::<Vec<Arc<dyn AttemptInterceptor>>>();
        assert_debug::<Vec<Arc<dyn AttemptInterceptor>>>();
        assert_attempt_interceptor::<Vec<Arc<dyn AttemptInterceptor>>>();
    }

    #[derive(Debug, Default)]
    struct MockInterceptor {
        intercept_count: AtomicU32,
        complete_count: AtomicU32,
    }

    impl AttemptInterceptor for MockInterceptor {
        fn intercept(&self, _headers: &mut HeaderMap, _attempt: u32) {
            self.intercept_count.fetch_add(1, Ordering::SeqCst);
        }

        fn on_attempt_complete(
            &self,
            _method: &str,
            _attempt: u32,
            _start_time: Instant,
            _response_headers: Option<&HeaderMap>,
            _error: Option<&Error>,
            _options: &RequestOptions,
        ) {
            self.complete_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn default_on_attempt_start_calls_intercept() {
        let interceptor = MockInterceptor::default();
        let mut headers = HeaderMap::new();
        let options = RequestOptions::default();
        let _start = interceptor.on_attempt_start("test_method", 1, &mut headers, &options);
        assert_eq!(interceptor.intercept_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn vec_interceptor_forwards_to_all() {
        let first = Arc::new(MockInterceptor::default());
        let second = Arc::new(MockInterceptor::default());
        let interceptors: Vec<Arc<dyn AttemptInterceptor>> = vec![first.clone(), second.clone()];
        let mut headers = HeaderMap::new();
        let options = RequestOptions::default();

        let start = interceptors.on_attempt_start("test_method", 1, &mut headers, &options);
        assert_eq!(first.intercept_count.load(Ordering::SeqCst), 1);
        assert_eq!(second.intercept_count.load(Ordering::SeqCst), 1);

        interceptors.on_attempt_complete("test_method", 1, start, Some(&headers), None, &options);
        assert_eq!(first.complete_count.load(Ordering::SeqCst), 1);
        assert_eq!(second.complete_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn arc_interceptor_forwards_all_methods() {
        let mock = Arc::new(MockInterceptor::default());
        let arc_interceptor: Arc<dyn AttemptInterceptor> = mock.clone();
        let mut headers = HeaderMap::new();
        let options = RequestOptions::default();

        let start = arc_interceptor.on_attempt_start("test_method", 1, &mut headers, &options);
        assert_eq!(mock.intercept_count.load(Ordering::SeqCst), 1);

        arc_interceptor.intercept(&mut headers, 2);
        assert_eq!(mock.intercept_count.load(Ordering::SeqCst), 2);

        arc_interceptor.on_attempt_complete(
            "test_method",
            1,
            start,
            Some(&headers),
            None,
            &options,
        );
        assert_eq!(mock.complete_count.load(Ordering::SeqCst), 1);
    }
}
