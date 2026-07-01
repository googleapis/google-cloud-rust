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
use std::time::Duration;

/// A retry policy decorator that continues on client timeouts.
///
/// This policy returns [RetryResult::Continue] when the error is a client-side timeout and the
/// request is idempotent. Otherwise it returns the result from the inner retry policy.
///
/// # Parameters
/// * `P` - the inner retry policy.
#[derive(Debug)]
pub struct ClientTimeout<P>
where
    P: RetryPolicy,
{
    inner: P,
}

impl<P> ClientTimeout<P>
where
    P: RetryPolicy,
{
    /// Decorate an existing retry policy to ignore client-side timeouts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::{ClientTimeout, RetryPolicy};
    /// use google_cloud_gax::retry_policy::Aip194Strict;
    /// use google_cloud_gax::retry_state::RetryState;
    /// let policy = ClientTimeout::new(Aip194Strict);
    /// assert!(policy.on_error(&RetryState::new(true).set_attempt_count(1_u32), timeout()).is_continue());
    /// assert!(policy.on_error(&RetryState::new(true).set_attempt_count(2_u32), permanent()).is_permanent());
    ///
    /// use google_cloud_gax::error::Error;
    /// # use google_cloud_gax::error::rpc::{Code, Status};
    /// fn timeout() -> Error {
    /// # Error::timeout("test-only")
    /// }
    /// fn permanent() -> Error {
    /// # Error::service(Status::default().set_code(Code::PermissionDenied))
    /// }
    /// ```
    pub fn new(inner: P) -> Self {
        Self { inner }
    }
}

impl<P> RetryPolicy for ClientTimeout<P>
where
    P: RetryPolicy,
{
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        if state.idempotent && error.is_timeout() {
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
    use super::super::tests::{MockPolicy, idempotent_state, non_idempotent_state};
    use super::*;
    use crate::error::rpc::{Code, Status};
    use crate::retry_policy::NeverRetry;
    use std::time::Instant;

    fn timeout() -> Error {
        Error::timeout("test-only")
    }

    fn permanent() -> Error {
        Error::service(Status::default().set_code(Code::PermissionDenied))
    }

    fn transient() -> Error {
        Error::service(Status::default().set_code(Code::Unavailable))
    }

    #[test]
    fn on_error() {
        let policy = ClientTimeout::new(NeverRetry);
        let result = policy.on_error(&idempotent_state(Instant::now()), timeout());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");
        let result = policy.on_error(&non_idempotent_state(Instant::now()), timeout());
        assert!(matches!(result, RetryResult::Exhausted(_)), "{result:?}");
        let result = policy.on_error(&idempotent_state(Instant::now()), permanent());
        assert!(matches!(result, RetryResult::Exhausted(_)), "{result:?}");
    }

    #[test]
    fn ext() {
        use super::super::RetryPolicyExt;
        let policy = NeverRetry.continue_on_client_timeout();
        let result = policy.on_error(&idempotent_state(Instant::now()), timeout());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");
        let result = policy.on_error(&non_idempotent_state(Instant::now()), timeout());
        assert!(matches!(result, RetryResult::Exhausted(_)), "{result:?}");
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

        let policy = ClientTimeout::new(mock);
        let result = policy.on_error(&idempotent_state(Instant::now()), transient());
        assert!(matches!(result, RetryResult::Permanent(_)), "{result:?}");
        let result = policy.on_error(&idempotent_state(Instant::now()), timeout());
        assert!(matches!(result, RetryResult::Continue(_)), "{result:?}");

        let result = policy.on_throttle(&idempotent_state(Instant::now()), transient());
        assert!(matches!(result, ThrottleResult::Exhausted(_)));
        let result = policy.on_throttle(&idempotent_state(Instant::now()), timeout());
        assert!(matches!(result, ThrottleResult::Exhausted(_)));

        let result = policy.remaining_time(&idempotent_state(Instant::now()));
        assert!(result.is_none(), "{result:?}");
    }
}
