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

use http::HeaderMap;
use std::sync::Arc;

/// A callback invoked on every RPC attempt, allowing modification of gRPC headers.
/// The callback receives the header map and the current 1-based attempt number.
pub trait AttemptInterceptor: std::fmt::Debug + Send + Sync {
    /// Intercepts and modifies the headers of an outgoing RPC attempt.
    ///
    /// `headers` is the mutable map of headers to be sent with the request.
    /// `attempt` is the 1-based attempt number for the current RPC.
    fn intercept(&self, headers: &mut HeaderMap, attempt: u32);
}

impl AttemptInterceptor for Vec<Arc<dyn AttemptInterceptor>> {
    fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
        for interceptor in self {
            interceptor.intercept(headers, attempt);
        }
    }
}

impl<T: AttemptInterceptor> AttemptInterceptor for Option<T> {
    fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
        if let Some(interceptor) = self {
            interceptor.intercept(headers, attempt);
        }
    }
}

impl<T: AttemptInterceptor + ?Sized> AttemptInterceptor for Arc<T> {
    fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
        self.as_ref().intercept(headers, attempt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderName, HeaderValue};

    #[derive(Debug)]
    struct AddHeaderInterceptor {
        name: &'static str,
        value: &'static str,
    }

    impl AttemptInterceptor for AddHeaderInterceptor {
        fn intercept(&self, headers: &mut HeaderMap, _attempt: u32) {
            headers.insert(
                HeaderName::from_static(self.name),
                HeaderValue::from_static(self.value),
            );
        }
    }

    #[derive(Debug)]
    struct AppendAttemptInterceptor;

    impl AttemptInterceptor for AppendAttemptInterceptor {
        fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
            headers.insert(
                HeaderName::from_static("x-attempt"),
                HeaderValue::from_str(&attempt.to_string()).expect("valid attempt number"),
            );
        }
    }

    #[test]
    fn test_single_interceptor() {
        let interceptor = AddHeaderInterceptor {
            name: "x-test",
            value: "hello",
        };
        let mut headers = HeaderMap::new();
        interceptor.intercept(&mut headers, 1);
        assert_eq!(
            headers
                .get("x-test")
                .expect("header x-test should be present"),
            "hello"
        );
    }

    #[test]
    fn test_vec_interceptor_sequential() {
        let interceptors: Vec<Arc<dyn AttemptInterceptor>> = vec![
            Arc::new(AddHeaderInterceptor {
                name: "x-first",
                value: "1",
            }),
            Arc::new(AddHeaderInterceptor {
                name: "x-second",
                value: "2",
            }),
        ];
        let mut headers = HeaderMap::new();
        interceptors.intercept(&mut headers, 1);
        assert_eq!(
            headers
                .get("x-first")
                .expect("header x-first should be present"),
            "1"
        );
        assert_eq!(
            headers
                .get("x-second")
                .expect("header x-second should be present"),
            "2"
        );
    }

    #[test]
    fn test_option_interceptor() {
        let mut headers = HeaderMap::new();
        let no_interceptor: Option<AddHeaderInterceptor> = None;
        no_interceptor.intercept(&mut headers, 1);
        assert!(headers.is_empty());

        let some_interceptor = Some(AddHeaderInterceptor {
            name: "x-test",
            value: "hello",
        });
        some_interceptor.intercept(&mut headers, 1);
        assert_eq!(
            headers
                .get("x-test")
                .expect("header x-test should be present"),
            "hello"
        );
    }

    #[test]
    fn test_attempt_number_propagation() {
        let interceptor = AppendAttemptInterceptor;
        let mut headers = HeaderMap::new();
        interceptor.intercept(&mut headers, 42);
        assert_eq!(
            headers
                .get("x-attempt")
                .expect("header x-attempt should be present"),
            "42"
        );
    }

    #[test]
    fn test_dyn_interceptor() {
        let interceptor: Arc<dyn AttemptInterceptor> = Arc::new(AddHeaderInterceptor {
            name: "x-test",
            value: "hello",
        });
        let mut headers = HeaderMap::new();
        // This calls the intercept method on Arc<dyn AttemptInterceptor>
        interceptor.intercept(&mut headers, 1);
        assert_eq!(
            headers
                .get("x-test")
                .expect("header x-test should be present"),
            "hello"
        );
    }

    fn assert_interceptor<I: AttemptInterceptor>(_interceptor: I) {}

    #[test]
    fn test_trait_bound() {
        let interceptor: Option<Arc<dyn AttemptInterceptor>> = None;
        assert_interceptor(interceptor);
    }
}
