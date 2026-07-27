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
