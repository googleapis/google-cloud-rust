// Copyright 2024 Google LLC
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

//! Defines traits for retry policies and some common implementations.
//!
//! The client libraries automatically retry RPCs when they fail due to
//! transient errors and the RPC is idempotent, that is, it is safe to perform
//! the RPC more than once.
//!
//! Applications may override the default behavior and maybe retry operations
//! that, while not safe in general, may be safe given how the application
//! manages resources.
//!
//! This module defines the traits for retry policies and some common
//! implementations.

use crate::error::rpc::Status;
use crate::error::{Error, HttpError};
use std::cell::Cell;

/// The result of a retry policy decision.
///
/// If the caller should continue retrying the policy returns
/// [Continue][std::ops::ControlFlow::Continue]. If the caller
/// should stop retrying, the policy returns
/// [Break][std::ops::ControlFlow::Break].
///
/// In both variants the result includes an error. This is useful when retry
/// policies are composed. The inner policy returns `Continue` based on the
/// error type, and the outer policy may return `Break` based on the number
/// errors, or the elapsed time.
pub type RetryFlow = std::ops::ControlFlow<Error, ()>;

/// Controls the retry loop behavior on
pub trait RetryPolicy: Clone {
    /// Query the retry policy after an error.
    ///
    /// # Parameters
    /// * `idempotent` - if `true` assume the operation is idempotent. Many more
    ///   errors are retryable on idempotent operations.
    /// * `error` - the last error received from a request. Not all are server
    ///   errors. The client library may have been unable to send or complete
    ///   the RPC before the server returned an error.
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow;

    /// The remaining time in the retry policy.
    ///
    /// For policies based on time, this returns the remaining time in the
    /// policy. The retry loop can use this value to adjust the next RPC
    /// timeout. For policies that are not time based this returns `None`.
    fn remaining_time(&self) -> Option<std::time::Duration>;
}

/// A retry policy that strictly follows [AIP-194].
///
/// This policy should be decorated to limit the number of retry attempts or the
/// duration of the retry loop.
///
/// The policy interprets AIP-194 **strictly**, the retry decision for
/// server-side errors are based only on the status code, and the only retryable
/// status code is "UNAVAILABLE".
#[derive(Clone)]
pub struct Aip194Strict;

impl RetryPolicy for Aip194Strict {
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow {
        if let Some(http) = error.as_inner::<crate::error::HttpError>() {
            if !idempotent {
                return RetryFlow::Break(error);
            }
            return if match_status_code_string(http, "UNAVAILABLE") {
                RetryFlow::Continue(())
            } else {
                RetryFlow::Break(error)
            };
        }
        use crate::error::ErrorKind;
        match error.kind() {
            ErrorKind::Rpc | ErrorKind::Io => {
                if idempotent {
                    RetryFlow::Continue(())
                } else {
                    RetryFlow::Break(error)
                }
            }
            ErrorKind::Authentication => {
                // This indicates the operation never left the client, so it
                // safe to retry
                RetryFlow::Continue(())
            }
            ErrorKind::Serde => RetryFlow::Break(error),
            ErrorKind::Other => RetryFlow::Break(error),
        }
    }

    fn remaining_time(&self) -> Option<std::time::Duration> {
        None
    }
}

// A helper function to simplify `Api194Strict::on_error()`:
fn match_status_code_string(http: &HttpError, code: &str) -> bool {
    Status::try_from(http)
        .ok()
        .map(|v| v.status.as_deref() == Some(code))
        .unwrap_or(false)
}

/// A retry policy decorator that limits the number of errors.
///
/// This policy decorates an inner policy and limits the duration of retry
/// loops. Once the loop exceeds its duration limit, this policy always returns
/// [Break][std::ops::ControlFlow::Break]. Before this deadline is reached, the
/// policy returns of `P::on_error()`
///
/// # Parameters
/// * `P` - the inner retry policy.
#[derive(Clone)]
pub struct LimitedElapsedTime<P>
where
    P: RetryPolicy,
{
    inner: P,
    deadline: std::time::Instant,
}

impl<P> LimitedElapsedTime<P>
where
    P: RetryPolicy,
{
    pub fn new(inner: P, maximum_duration: std::time::Duration) -> Self {
        Self {
            inner,
            deadline: std::time::Instant::now() + maximum_duration,
        }
    }

    fn on_error_now(&self, now: std::time::Instant, idempotent: bool, error: Error) -> RetryFlow {
        if now >= self.deadline {
            return RetryFlow::Break(error);
        }
        self.inner.on_error(idempotent, error)
    }

    fn remaining_time_now(&self, now: std::time::Instant) -> Option<std::time::Duration> {
        let remaining = self.deadline.saturating_duration_since(now);
        if let Some(inner) = self.inner.remaining_time() {
            return Some(std::cmp::min(remaining, inner));
        }
        Some(remaining)
    }
}

impl<P> RetryPolicy for LimitedElapsedTime<P>
where
    P: RetryPolicy,
{
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow {
        self.on_error_now(std::time::Instant::now(), idempotent, error)
    }

    fn remaining_time(&self) -> Option<std::time::Duration> {
        self.remaining_time_now(std::time::Instant::now())
    }
}

/// A retry policy decorator that limits the number of errors.
///
/// This policy decorates an inner policy and limits the total number of errors.
/// Once the maximum error count is reached this policy always returns
/// [Break][std::ops::ControlFlow::Break]. Before the maximum is reached, the
/// policy returns of `P::on_error()`
///
/// # Parameters
/// * `P` - the inner retry policy.
#[derive(Clone)]
pub struct LimitedErrorCount<P>
where
    P: RetryPolicy,
{
    inner: P,
    maximum_error_count: i32,
    error_count: Cell<i32>,
}

impl<P> LimitedErrorCount<P>
where
    P: RetryPolicy,
{
    pub fn new(inner: P, maximum_error_count: i32) -> Self {
        Self {
            inner,
            maximum_error_count,
            error_count: Cell::new(0),
        }
    }
}

impl<P> RetryPolicy for LimitedErrorCount<P>
where
    P: RetryPolicy,
{
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow {
        let count = self.error_count.get().saturating_add(1);
        self.error_count.set(count);
        if count > self.maximum_error_count {
            return RetryFlow::Break(error);
        }
        self.inner.on_error(idempotent, error)
    }

    fn remaining_time(&self) -> Option<std::time::Duration> {
        self.inner.remaining_time()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::rpc::Status;

    #[test]
    fn aip194_strict() {
        let p = Aip194Strict;

        assert!(p.on_error(true, unavailable()).is_continue());
        assert!(p.on_error(false, unavailable()).is_break());

        assert!(p.on_error(true, permission_denied()).is_break());
        assert!(p.on_error(false, permission_denied()).is_break());

        assert!(p.on_error(true, Error::io("err".to_string())).is_continue());
        assert!(p.on_error(false, Error::io("err".to_string())).is_break());

        assert!(p
            .on_error(true, Error::authentication("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(false, Error::authentication("err".to_string()))
            .is_continue());

        assert!(p.on_error(true, Error::serde("err".to_string())).is_break());
        assert!(p
            .on_error(false, Error::serde("err".to_string()))
            .is_break());
        assert!(p.on_error(true, Error::other("err".to_string())).is_break());
        assert!(p
            .on_error(false, Error::other("err".to_string()))
            .is_break());

        assert!(p.remaining_time().is_none());
    }

    fn from_status(status: Status) -> Error {
        use std::collections::HashMap;
        let payload = serde_json::to_value(&status)
            .ok()
            .map(|v| serde_json::json!({"error": v}));
        let payload = payload.map(|v| v.to_string());
        let payload = payload.map(bytes::Bytes::from_owner);
        let http = crate::error::HttpError::new(status.code as u16, HashMap::new(), payload);
        Error::rpc(http)
    }

    fn unavailable() -> Error {
        let mut status = Status::default();
        status.code = 503;
        status.message = "SERVICE UNAVAILABLE".to_string();
        status.status = Some("UNAVAILABLE".to_string());
        from_status(status)
    }

    fn permission_denied() -> Error {
        let mut status = Status::default();
        status.code = 403;
        status.message = "PERMISSION DENIED".to_string();
        status.status = Some("PERMISSION_DENIED".to_string());
        from_status(status)
    }

    mockall::mock! {
        Policy {}
        impl RetryPolicy for Policy {
            fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow;

            /// The remaining time in the retry policy.
            ///
            /// For policies based on time, this returns the remaining time in the
            /// policy. The retry loop can use this value to adjust the next RPC
            /// timeout. For policies that are not time based this returns `None`.
            fn remaining_time(&self) -> Option<std::time::Duration>;
        }
    }

    impl Clone for MockPolicy {
        fn clone(&self) -> Self {
            MockPolicy::new()
        }
    }

    use std::time::Duration;

    #[test]
    fn test_limited_time_forwards() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _| RetryFlow::Continue(()));
        mock.expect_remaining_time().times(1).returning(|| None);

        let policy = LimitedElapsedTime::new(mock, Duration::from_secs(60));
        let rf = policy.on_error(true, Error::other("err".to_string()));
        assert!(rf.is_continue());

        let rt = policy.remaining_time();
        assert!(rt.is_some());
    }

    #[test]
    fn test_limited_time_inner_continues() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _| RetryFlow::Continue(()));

        let policy = LimitedElapsedTime::new(mock, Duration::from_secs(60));
        let rf = policy.on_error_now(
            policy.deadline - Duration::from_secs(10),
            true,
            Error::other("err".to_string()),
        );
        assert!(rf.is_continue());

        let rf = policy.on_error_now(
            policy.deadline + Duration::from_secs(10),
            true,
            Error::other("err".to_string()),
        );
        assert!(rf.is_break());
    }

    #[test]
    fn test_limited_time_inner_breaks() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1)
            .returning(|_, e| RetryFlow::Break(e));
        let policy = LimitedElapsedTime::new(mock, Duration::from_secs(60));

        let rf = policy.on_error_now(
            policy.deadline - Duration::from_secs(10),
            false,
            Error::other("err".to_string()),
        );
        assert!(rf.is_break());

        let rf = policy.on_error_now(
            policy.deadline + Duration::from_secs(10),
            false,
            Error::other("err".to_string()),
        );
        assert!(rf.is_break());
    }

    #[test]
    fn test_limited_time_remaining_inner_longer() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|| Some(Duration::from_secs(50)));
        let policy = LimitedElapsedTime::new(mock, Duration::from_secs(60));

        let remaining = policy.remaining_time_now(policy.deadline - Duration::from_secs(10));
        assert_eq!(remaining, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_limited_time_remaining_inner_shorter() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|| Some(Duration::from_secs(5)));
        let policy = LimitedElapsedTime::new(mock, Duration::from_secs(60));

        let remaining = policy.remaining_time_now(policy.deadline - Duration::from_secs(10));
        assert_eq!(remaining, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_limited_time_remaining_inner_is_none() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time().times(1).returning(|| None);
        let policy = LimitedElapsedTime::new(mock, Duration::from_secs(60));

        let remaining = policy.remaining_time_now(policy.deadline - Duration::from_secs(10));
        assert_eq!(remaining, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_limited_error_count_on_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _| RetryFlow::Continue(()));

        let policy = LimitedErrorCount::new(mock, 3);
        assert!(policy
            .on_error(true, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(true, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(true, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(true, Error::other("err".to_string()))
            .is_break());
    }

    #[test]
    fn test_limited_error_count_remaining_none() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time().times(1).returning(|| None);
        let policy = LimitedErrorCount::new(mock, 3);

        assert!(policy.remaining_time().is_none());
    }

    #[test]
    fn test_limited_error_count_remaining_some() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|| Some(Duration::from_secs(123)));
        let policy = LimitedErrorCount::new(mock, 3);

        assert_eq!(policy.remaining_time(), Some(Duration::from_secs(123)));
    }
}
