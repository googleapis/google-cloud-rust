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

use super::{Error, RetryPolicy, RetryResult, RetryState, ThrottleResult};
use crate::error::rpc::Code;
use std::time::Duration;

/// A retry policy decorator continues on `ResourceExhausted` or `TOO_MANY_REQUESTS`.
///
/// This policy returns [RetryResult::Continue] when the error is a
/// `ResourceExhausted` (or `TOO_MANY_REQUESTS` if received from the HTTP layer).
/// Otherwise it returns the result from the inner retry policy.
///
/// # Parameters
/// * `P` - the inner retry policy.
#[derive(Debug)]
pub struct TooManyRequests<P>
where
    P: RetryPolicy,
{
    inner: P,
}

impl<P> TooManyRequests<P>
where
    P: RetryPolicy,
{
    /// Creates a new instance with a custom inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::{TooManyRequests, RetryPolicy};
    /// use google_cloud_gax::retry_policy::Aip194Strict;
    /// use google_cloud_gax::retry_state::RetryState;
    /// let policy = TooManyRequests::new(Aip194Strict);
    /// assert!(policy.on_error(&RetryState::new(true).set_attempt_count(1_u32), too_many()).is_continue());
    /// assert!(policy.on_error(&RetryState::new(true).set_attempt_count(2_u32), permanent()).is_permanent());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn too_many() -> Error { Error::service(Status::default().set_code(Code::ResourceExhausted)) }
    /// fn permanent() -> Error { Error::service(Status::default().set_code(Code::PermissionDenied)) }
    /// ```
    pub fn new(inner: P) -> Self {
        Self { inner }
    }

    fn is_resource_exhausted(e: &Error) -> bool {
        e.status()
            .is_some_and(|s| s.code == Code::ResourceExhausted)
    }

    fn is_too_many_requests(e: &Error) -> bool {
        e.http_status_code()
            .is_some_and(|code| code == http::StatusCode::TOO_MANY_REQUESTS.as_u16())
    }
}

impl<P> RetryPolicy for TooManyRequests<P>
where
    P: RetryPolicy,
{
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        if Self::is_resource_exhausted(&error) || Self::is_too_many_requests(&error) {
            return RetryResult::Continue(error);
        }
        self.inner.on_error(state, error)
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
    use super::super::tests::{MockPolicy, idempotent_state};
    use super::*;
    use crate::error::rpc::Status;
    use crate::retry_policy::NeverRetry;
    use std::time::Instant;

    fn too_many_requests() -> Error {
        Error::service(Status::default().set_code(Code::ResourceExhausted))
    }

    fn too_many_requests_http() -> Error {
        Error::http(
            http::StatusCode::TOO_MANY_REQUESTS.as_u16(),
            http::HeaderMap::new(),
            bytes::Bytes::new(),
        )
    }

    fn permanent() -> Error {
        Error::service(Status::default().set_code(Code::PermissionDenied))
    }

    fn transient() -> Error {
        Error::service(Status::default().set_code(Code::Unavailable))
    }

    #[test]
    fn on_error() {
        let policy = TooManyRequests::new(NeverRetry);
        let result = policy.on_error(&idempotent_state(Instant::now()), too_many_requests());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");
        let result = policy.on_error(&idempotent_state(Instant::now()), too_many_requests_http());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");
        let result = policy.on_error(&idempotent_state(Instant::now()), permanent());
        assert!(matches!(result, RetryResult::Exhausted(_)), "{result:?}");
    }

    #[test]
    fn ext() {
        use super::super::RetryPolicyExt;
        let policy = NeverRetry.continue_on_too_many_requests();
        let result = policy.on_error(&idempotent_state(Instant::now()), too_many_requests());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");
        let result = policy.on_error(&idempotent_state(Instant::now()), too_many_requests_http());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");
        let result = policy.on_error(&idempotent_state(Instant::now()), permanent());
        assert!(matches!(result, RetryResult::Exhausted(_)), "{result:?}");
    }

    #[test]
    fn forwards() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, e| RetryResult::Permanent(e));
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, e| ThrottleResult::Exhausted(e));
        mock.expect_remaining_time().times(1).returning(|_| None);

        let policy = TooManyRequests::new(mock);
        let result = policy.on_error(&idempotent_state(Instant::now()), transient());
        assert!(matches!(result, RetryResult::Permanent(_)), "{result:?}");
        let result = policy.on_error(&idempotent_state(Instant::now()), too_many_requests());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");

        let result = policy.on_throttle(&idempotent_state(Instant::now()), transient());
        assert!(matches!(result, ThrottleResult::Exhausted(_)));
        let result = policy.on_throttle(&idempotent_state(Instant::now()), too_many_requests());
        assert!(matches!(result, ThrottleResult::Exhausted(_)));

        let result = policy.remaining_time(&idempotent_state(Instant::now()));
        assert!(result.is_none(), "{result:?}");
    }
}
