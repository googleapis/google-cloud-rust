// Copyright 2025 Google LLC
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

use super::redirect::is_redirect;
use gax::error::Error;
use gax::retry_policy::RetryPolicy;
use gax::retry_result::RetryResult;
use gax::retry_state::RetryState;
use gax::throttle_result::ThrottleResult;
use std::sync::Arc;

/// Decorate the retry policy to continue on redirect errors.
///
/// The bidi streaming read API uses errors to redirect requests. We want to
/// ignore these errors in the retry loop while respecting any limits set by the
/// application.
///
/// The client library uses this policy to decorate whatever policy set by the
/// application. If the policy is exhausted, or the error is transient, then
/// the decorator has no effect. If the error is "permanent", but happens to be
/// a redirect, then it is treated as retryable.
#[derive(Clone, Debug)]
pub struct RetryRedirect<T> {
    inner: T,
}

impl<T> RetryRedirect<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl RetryPolicy for RetryRedirect<Arc<dyn RetryPolicy + 'static>> {
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        match self.inner.on_error(state, error) {
            RetryResult::Permanent(e) if is_redirect(&e) => RetryResult::Continue(e),
            // Exhausted(), Continue() and other permanent errors pass thru.
            result => result,
        }
    }

    fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult {
        self.inner.on_throttle(state, error)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{permanent_error, redirect_status, transient_error};
    use super::*;
    use crate::retry_policy::RetryableErrors;
    use gax::throttle_result::ThrottleResult;
    use gaxi::grpc::from_status::to_gax_error;

    #[test]
    fn retry_redirect() {
        use gax::retry_policy::RetryPolicyExt;
        let inner: Arc<dyn RetryPolicy + 'static> = Arc::new(RetryableErrors.with_attempt_limit(3));
        let p = RetryRedirect::new(inner);

        let result = p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));
        assert!(matches!(&result, RetryResult::Continue(_)), "{result:?}");
        let result = p.on_error(
            &RetryState::new(false).set_attempt_count(5_u32),
            to_gax_error(redirect_status("r1")),
        );
        assert!(matches!(&result, RetryResult::Continue(_)), "{result:?}");

        let result = p.on_error(&RetryState::new(true), transient_error());
        assert!(matches!(&result, RetryResult::Continue(_)), "{result:?}");
        let result = p.on_error(
            &RetryState::new(true).set_attempt_count(5_u32),
            transient_error(),
        );
        assert!(matches!(&result, RetryResult::Exhausted(_)), "{result:?}");

        let result = p.on_error(&RetryState::new(true), permanent_error());
        assert!(matches!(&result, RetryResult::Permanent(_)), "{result:?}");

        let t = p.on_throttle(&RetryState::new(true), to_gax_error(redirect_status("r1")));
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");

        let t = p.on_throttle(&RetryState::new(true), transient_error());
        assert!(matches!(t, ThrottleResult::Continue(_)), "{t:?}");
    }
}
