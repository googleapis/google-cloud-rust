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

use super::redirect::is_redirect;
use google_cloud_gax::error::Error;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// Maximum redirects followed per connect or reconnect cycle before giving up.
///
/// Reaching the target backend normally takes 1 redirect (or 2 if that backend is draining), so 3
/// prevents infinite redirect loops while leaving 1 extra redirect of headroom.
pub(super) const MAX_REDIRECTS_FOLLOWED: u32 = 3;

/// Decorates a [`RetryPolicy`] to follow `BidiWriteObject` routing redirects.
///
/// GCS signals routing changes via `Aborted` errors carrying a `BidiWriteObjectRedirectedError`.
/// Because a redirect is a routing update rather than a failed write attempt, this decorator:
/// - Passes non-redirect errors through to the inner [`RetryPolicy`] unchanged.
/// - Overrides redirect errors to [`RetryResult::Continue`] up to [`MAX_REDIRECTS_FOLLOWED`] times,
///   even if the inner policy treats `Aborted` as permanent or has reached its attempt limit.
#[derive(Debug)]
pub struct RetryRedirect<T> {
    inner: T,
    redirects_followed: AtomicU32,
}

impl<T> RetryRedirect<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            redirects_followed: AtomicU32::new(0),
        }
    }
}

impl RetryPolicy for RetryRedirect<Arc<dyn RetryPolicy + 'static>> {
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        let redirect = is_redirect(&error);
        let result = self.inner.on_error(state, error);
        if !redirect {
            return result;
        }
        // Redirects are control flow, not failures. Count every redirect (including when the inner
        // policy returns `Continue`) and cap consecutive redirects at `MAX_REDIRECTS_FOLLOWED`.
        if self.redirects_followed.fetch_add(1, Ordering::Relaxed) < MAX_REDIRECTS_FOLLOWED {
            let (RetryResult::Continue(e) | RetryResult::Permanent(e) | RetryResult::Exhausted(e)) =
                result;
            RetryResult::Continue(e)
        } else {
            match result {
                RetryResult::Continue(e) | RetryResult::Exhausted(e) => RetryResult::Exhausted(e),
                RetryResult::Permanent(e) => RetryResult::Permanent(e),
            }
        }
    }

    fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult {
        self.inner.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        self.inner.remaining_time(state)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{permanent_error, redirect_status, transient_error};
    use super::*;
    use crate::retry_policy::RetryableErrors;
    use gaxi::grpc::from_status::to_gax_error;
    use google_cloud_gax::throttle_result::ThrottleResult;

    #[test]
    fn retry_redirect() {
        use google_cloud_gax::retry_policy::RetryPolicyExt;
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

    #[test]
    fn retry_redirect_with_never_retry() {
        use google_cloud_gax::retry_policy::NeverRetry;
        // `NeverRetry` reports every error as exhausted, including redirects.
        let inner: Arc<dyn RetryPolicy + 'static> = Arc::new(NeverRetry);
        let p = RetryRedirect::new(inner);

        let result = p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));
        assert!(matches!(&result, RetryResult::Continue(_)), "{result:?}");

        let result = p.on_error(&RetryState::new(true), transient_error());
        assert!(matches!(&result, RetryResult::Exhausted(_)), "{result:?}");
    }

    #[test]
    fn redirect_budget_reinstates_exhausted_verdict() {
        use google_cloud_gax::retry_policy::NeverRetry;
        // Arrange.
        let inner: Arc<dyn RetryPolicy + 'static> = Arc::new(NeverRetry);
        let p = RetryRedirect::new(inner);

        // Act.
        // Redirects within the budget are followed regardless of the inner verdict; the one past it
        // is not.
        for i in 0..MAX_REDIRECTS_FOLLOWED {
            let result = p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));
            assert!(
                matches!(&result, RetryResult::Continue(_)),
                "redirect {i}: {result:?}"
            );
        }
        let result = p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));

        // Assert.
        // `NeverRetry` reports errors as exhausted, so that is what stands.
        assert!(matches!(&result, RetryResult::Exhausted(_)), "{result:?}");
    }

    #[test]
    fn redirect_budget_reinstates_permanent_verdict() {
        // Arrange.
        // `RetryableErrors` treats the `Aborted` status carrying a redirect as permanent, so the
        // budget must reinstate `Permanent` rather than rewriting the verdict.
        let inner: Arc<dyn RetryPolicy + 'static> = Arc::new(RetryableErrors);
        let p = RetryRedirect::new(inner);

        // Act.
        for _ in 0..MAX_REDIRECTS_FOLLOWED {
            p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));
        }
        let result = p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));

        // Assert.
        assert!(matches!(&result, RetryResult::Permanent(_)), "{result:?}");
    }

    #[test]
    fn redirect_budget_caps_continue_verdict() {
        use google_cloud_gax::retry_policy::AlwaysRetry;
        // Arrange.
        // `AlwaysRetry` returns `Continue` for every error, including redirects.
        let inner: Arc<dyn RetryPolicy + 'static> = Arc::new(AlwaysRetry);
        let p = RetryRedirect::new(inner);

        // Act.
        for i in 0..MAX_REDIRECTS_FOLLOWED {
            let result = p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));
            assert!(
                matches!(&result, RetryResult::Continue(_)),
                "redirect {i}: {result:?}"
            );
        }
        let result = p.on_error(&RetryState::new(true), to_gax_error(redirect_status("r1")));

        // Assert.
        assert!(matches!(&result, RetryResult::Exhausted(_)), "{result:?}");
    }

    #[test]
    fn remaining_time_delegates_to_inner_policy() {
        use google_cloud_gax::retry_policy::RetryPolicyExt;
        // Arrange.
        let limit = Duration::from_secs(60);
        let limited: Arc<dyn RetryPolicy + 'static> =
            Arc::new(RetryableErrors.with_time_limit(limit));
        let unlimited: Arc<dyn RetryPolicy + 'static> = Arc::new(RetryableErrors);

        // Act.
        let remaining = RetryRedirect::new(limited).remaining_time(&RetryState::new(true));
        let unbounded = RetryRedirect::new(unlimited).remaining_time(&RetryState::new(true));

        // Assert.
        assert!(
            remaining.is_some_and(|r| r <= limit),
            "expected a bound no larger than the inner limit, got {remaining:?}"
        );
        assert_eq!(unbounded, None);
    }
}
