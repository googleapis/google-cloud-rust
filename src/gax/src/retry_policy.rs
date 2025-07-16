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
//! The client libraries automatically retry RPCs when (1) they fail due to
//! transient errors **and** the RPC is [idempotent], (2) or failed before an
//! RPC was started. That is, when it is safe to attempt the RPC more than once.
//!
//! Applications may override the default behavior, increasing the retry
//! attempts, or changing what errors are considered safe to retry.
//!
//! This module defines the traits for retry policies and some common
//! implementations.
//!
//! To configure the default throttler for a client, use
//! [ClientBuilder::with_retry_policy]. To configure the retry policy used for
//! a specific request, use [RequestOptionsBuilder::with_retry_policy].
//!
//! [ClientBuilder::with_retry_policy]: crate::client_builder::ClientBuilder::with_retry_policy
//! [RequestOptionsBuilder::with_retry_policy]: crate::options::RequestOptionsBuilder::with_retry_policy
//!
//! # Examples
//!
//! Create a policy that only retries transient errors, and retries for at
//! most 10 seconds or at most 5 attempts: whichever limit is reached first
//! stops the retry loop.
//! ```
//! # use google_cloud_gax::retry_policy::*;
//! use std::time::Duration;
//! let policy = Aip194Strict.with_time_limit(Duration::from_secs(10)).with_attempt_limit(5);
//! ```
//!
//! Create a policy that retries on any error (even when unsafe to do so),
//! and stops retrying after 5 attempts or 10 seconds, whichever limit is
//! reached first stops the retry loop.
//! ```
//! # use google_cloud_gax::retry_policy::*;
//! use std::time::Duration;
//! let policy = AlwaysRetry.with_time_limit(Duration::from_secs(10)).with_attempt_limit(5);
//! ```
//!
//! [idempotent]: https://en.wikipedia.org/wiki/Idempotence

use crate::error::{CredentialsError, Error};
use crate::retry_result::RetryResult;
use crate::throttle_result::ThrottleResult;
use std::sync::Arc;
use std::time::Duration;

/// Determines how errors are handled in the retry loop.
///
/// Implementations of this trait determine if errors are retryable, and for how
/// long the retry loop may continue.
pub trait RetryPolicy: Send + Sync + std::fmt::Debug {
    /// Query the retry policy after an error.
    ///
    /// # Parameters
    /// * `loop_start` - when the retry loop started.
    /// * `attempt_count` - the number of attempts. This includes the initial
    ///   attempt. This method called after the first attempt, so the
    ///   value is always non-zero.
    /// * `idempotent` - if `true` assume the operation is idempotent. Many more
    ///   errors are retryable on idempotent operations.
    /// * `error` - the last error when attempting the request.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn on_error(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryResult;

    /// Query the retry policy after a retry attempt is throttled.
    ///
    /// Retry attempts may be throttled before they are even sent out. The retry
    /// policy may choose to treat these as normal errors, consuming attempts,
    /// or may prefer to ignore them and always return [RetryResult::Continue].
    ///
    /// # Parameters
    /// * `loop_start` - when the retry loop started.
    /// * `attempt_count` - the number of attempts. This method is never called
    ///   before the first attempt.
    /// * `error` - the previous error that caused the retry attempt. Throttling
    ///   only applies to retry attempts, and a retry attempt implies that a
    ///   previous attempt failed. The retry policy should preserve this error.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn on_throttle(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        error: Error,
    ) -> ThrottleResult {
        ThrottleResult::Continue(error)
    }

    /// The remaining time in the retry policy.
    ///
    /// For policies based on time, this returns the remaining time in the
    /// policy. The retry loop can use this value to adjust the next RPC
    /// timeout. For policies that are not time based this returns `None`.
    ///
    /// # Parameters
    /// * `loop_start` - when the retry loop started.
    /// * `attempt_count` - the number of attempts. This method is called before
    ///   the first attempt, so the first value is zero.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn remaining_time(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
    ) -> Option<Duration> {
        None
    }
}

/// A helper type to use [RetryPolicy] in client and request options.
#[derive(Clone, Debug)]
pub struct RetryPolicyArg(Arc<dyn RetryPolicy>);

impl<T> std::convert::From<T> for RetryPolicyArg
where
    T: RetryPolicy + 'static,
{
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl std::convert::From<Arc<dyn RetryPolicy>> for RetryPolicyArg {
    fn from(value: Arc<dyn RetryPolicy>) -> Self {
        Self(value)
    }
}

impl From<RetryPolicyArg> for Arc<dyn RetryPolicy> {
    fn from(value: RetryPolicyArg) -> Arc<dyn RetryPolicy> {
        value.0
    }
}

/// Extension trait for [`RetryPolicy`]
pub trait RetryPolicyExt: RetryPolicy + Sized {
    /// Decorate a [`RetryPolicy`] to limit the total elapsed time in the retry loop.
    ///
    /// While the time spent in the retry loop (including time in backoff) is
    /// less than the prescribed duration the `on_error()` method returns the
    /// results of the inner policy. After that time it returns
    /// [Exhausted][RetryResult::Exhausted] if the inner policy returns
    /// [Continue][RetryResult::Continue].
    ///
    /// The `remaining_time()` function returns the remaining time. This is
    /// always [Duration::ZERO] once or after the policy's expiration time is
    /// reached.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::*;
    /// let d = std::time::Duration::from_secs(10);
    /// let policy = Aip194Strict.with_time_limit(d);
    /// assert!(policy.remaining_time(std::time::Instant::now(), 0) <= Some(d));
    /// ```
    fn with_time_limit(self, maximum_duration: Duration) -> LimitedElapsedTime<Self> {
        LimitedElapsedTime::custom(self, maximum_duration)
    }

    /// Decorate a [RetryPolicy] to limit the number of retry attempts.
    ///
    /// This policy decorates an inner policy and limits the total number of
    /// attempts. Note that `on_error()` is not called before the initial
    /// (non-retry) attempt. Therefore, setting the maximum number of attempts
    /// to 0 or 1 results in no retry attempts.
    ///
    /// The policy passes through the results from the inner policy as long as
    /// `attempt_count < maximum_attempts`. Once the maximum number of attempts
    /// is reached, the policy returns [Exhausted][RetryResult::Exhausted] if the
    /// inner policy returns [Continue][RetryResult::Continue].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::*;
    /// use std::time::Instant;
    /// let policy = Aip194Strict.with_attempt_limit(3);
    /// assert_eq!(policy.remaining_time(Instant::now(), 0), None);
    /// assert!(policy.on_error(Instant::now(), 0, true, transient_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 1, true, transient_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, true, transient_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 3, true, transient_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
    /// ```
    fn with_attempt_limit(self, maximum_attempts: u32) -> LimitedAttemptCount<Self> {
        LimitedAttemptCount::custom(self, maximum_attempts)
    }
}

impl<T: RetryPolicy> RetryPolicyExt for T {}

/// A retry policy that strictly follows [AIP-194].
///
/// This policy must be decorated to limit the number of retry attempts or the
/// duration of the retry loop.
///
/// The policy interprets AIP-194 **strictly**, the retry decision for
/// server-side errors are based only on the status code, and the only retryable
/// status code is "UNAVAILABLE".
///
/// # Example
/// ```
/// # use google_cloud_gax::retry_policy::*;
/// use std::time::Instant;
/// let policy = Aip194Strict;
/// assert!(policy.on_error(Instant::now(), 0, true, transient_error()).is_continue());
/// assert!(policy.on_error(Instant::now(), 0, true, permanent_error()).is_permanent());
///
/// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
/// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
/// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::PermissionDenied)) }
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct Aip194Strict;

impl RetryPolicy for Aip194Strict {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryResult {
        if error.is_transient_and_before_rpc() {
            return RetryResult::Continue(error);
        }
        if !idempotent {
            return RetryResult::Permanent(error);
        }
        if error.is_io() {
            return RetryResult::Continue(error);
        }
        if let Some(status) = error.status() {
            return if status.code == crate::error::rpc::Code::Unavailable {
                RetryResult::Continue(error)
            } else {
                RetryResult::Permanent(error)
            };
        }

        match error.http_status_code() {
            Some(code) if code == http::StatusCode::SERVICE_UNAVAILABLE.as_u16() => {
                RetryResult::Continue(error)
            }
            _ => RetryResult::Permanent(error),
        }
    }
}

/// A retry policy that retries all errors.
///
/// This policy must be decorated to limit the number of retry attempts or the
/// duration of the retry loop.
///
/// The policy retries all errors. This may be useful if the service guarantees
/// idempotency, maybe through the use of request ids.
///
/// # Example
/// ```
/// # use google_cloud_gax::retry_policy::*;
/// use std::time::Instant;
/// let policy = AlwaysRetry;
/// assert!(policy.on_error(Instant::now(), 0, true, transient_error()).is_continue());
/// assert!(policy.on_error(Instant::now(), 0, true, permanent_error()).is_continue());
///
/// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
/// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
/// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::PermissionDenied)) }
/// ```
#[derive(Clone, Debug)]
pub struct AlwaysRetry;

impl RetryPolicy for AlwaysRetry {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        _idempotent: bool,
        error: Error,
    ) -> RetryResult {
        RetryResult::Continue(error)
    }
}

/// A retry policy that never retries.
///
/// This policy is useful when the client already has (or may already have) a
/// retry policy configured, and you want to avoid retrying a particular method.
///
/// # Example
/// ```
/// # use google_cloud_gax::retry_policy::*;
/// use std::time::Instant;
/// let policy = NeverRetry;
/// assert!(policy.on_error(Instant::now(), 0, true, transient_error()).is_exhausted());
/// assert!(policy.on_error(Instant::now(), 0, true, permanent_error()).is_exhausted());
///
/// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
/// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
/// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::PermissionDenied)) }
/// ```
#[derive(Clone, Debug)]
pub struct NeverRetry;

impl RetryPolicy for NeverRetry {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        _idempotent: bool,
        error: Error,
    ) -> RetryResult {
        RetryResult::Exhausted(error)
    }
}

#[derive(thiserror::Error, Debug)]
pub struct LimitedElapsedTimeError {
    maximum_duration: Duration,
    #[source]
    source: Error,
}

impl LimitedElapsedTimeError {
    pub(crate) fn new(maximum_duration: Duration, source: Error) -> Self {
        Self {
            maximum_duration,
            source,
        }
    }

    /// Returns the maximum number of attempts in the exhausted policy.
    pub fn maximum_duration(&self) -> Duration {
        self.maximum_duration
    }
}

impl std::fmt::Display for LimitedElapsedTimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "retry policy is exhausted after {}s, the last retry attempt was throttled",
            self.maximum_duration.as_secs_f64()
        )
    }
}

/// A retry policy decorator that limits the total time in the retry loop.
///
/// This policy decorates an inner policy and limits the duration of retry
/// loops. While the time spent in the retry loop (including time in backoff)
/// is less than the prescribed duration the `on_error()` method returns the
/// results of the inner policy. After that time it returns
/// [Exhausted][RetryResult::Exhausted] if the inner policy returns
/// [Continue][RetryResult::Continue].
///
/// The `remaining_time()` function returns the remaining time. This is always
/// [Duration::ZERO] once or after the policy's deadline is reached.
///
/// # Parameters
/// * `P` - the inner retry policy, defaults to [Aip194Strict].
#[derive(Debug)]
pub struct LimitedElapsedTime<P = Aip194Strict>
where
    P: RetryPolicy,
{
    inner: P,
    maximum_duration: Duration,
}

impl LimitedElapsedTime {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::*;
    /// let d = std::time::Duration::from_secs(10);
    /// let policy = LimitedElapsedTime::new(d);
    /// assert!(policy.remaining_time(std::time::Instant::now(), 0) <= Some(d));
    /// ```
    pub fn new(maximum_duration: Duration) -> Self {
        Self {
            inner: Aip194Strict,
            maximum_duration,
        }
    }
}

impl<P> LimitedElapsedTime<P>
where
    P: RetryPolicy,
{
    /// Creates a new instance with a custom inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::*;
    /// # use google_cloud_gax::error;
    /// use std::time::{Duration, Instant};
    /// let d = Duration::from_secs(10);
    /// let policy = AlwaysRetry.with_time_limit(d);
    /// assert!(policy.remaining_time(Instant::now(), 0) <= Some(d));
    /// assert!(policy.on_error(Instant::now(), 1, false, permanent_error()).is_continue());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
    /// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::PermissionDenied)) }
    /// ```
    pub fn custom(inner: P, maximum_duration: Duration) -> Self {
        Self {
            inner,
            maximum_duration,
        }
    }

    fn error_if_exhausted(&self, loop_start: std::time::Instant, error: Error) -> ThrottleResult {
        let deadline = loop_start + self.maximum_duration;
        let now = tokio::time::Instant::now().into_std();
        if now < deadline {
            ThrottleResult::Continue(error)
        } else {
            ThrottleResult::Exhausted(Error::exhausted(LimitedElapsedTimeError::new(
                self.maximum_duration,
                error,
            )))
        }
    }
}

impl<P> RetryPolicy for LimitedElapsedTime<P>
where
    P: RetryPolicy + 'static,
{
    fn on_error(
        &self,
        start: std::time::Instant,
        count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryResult {
        match self.inner.on_error(start, count, idempotent, error) {
            RetryResult::Permanent(e) => RetryResult::Permanent(e),
            RetryResult::Exhausted(e) => RetryResult::Exhausted(e),
            RetryResult::Continue(e) => {
                if tokio::time::Instant::now().into_std() >= start + self.maximum_duration {
                    RetryResult::Exhausted(e)
                } else {
                    RetryResult::Continue(e)
                }
            }
        }
    }

    fn on_throttle(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        error: Error,
    ) -> ThrottleResult {
        match self.inner.on_throttle(loop_start, attempt_count, error) {
            ThrottleResult::Continue(e) => self.error_if_exhausted(loop_start, e),
            ThrottleResult::Exhausted(e) => ThrottleResult::Exhausted(e),
        }
    }

    fn remaining_time(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> Option<Duration> {
        let deadline = loop_start + self.maximum_duration;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now().into_std());
        if let Some(inner) = self.inner.remaining_time(loop_start, attempt_count) {
            return Some(std::cmp::min(remaining, inner));
        }
        Some(remaining)
    }
}

/// A retry policy decorator that limits the number of attempts.
///
/// This policy decorates an inner policy and limits the total number of
/// attempts. Note that `on_error()` is not called before the initial
/// (non-retry) attempt. Therefore, setting the maximum number of attempts to 0
/// or 1 results in no retry attempts.
///
/// The policy passes through the results from the inner policy as long as
/// `attempt_count < maximum_attempts`. However, once the maximum number of
/// attempts is reached, the policy replaces any [Continue][RetryResult::Continue]
/// result with [Exhausted][RetryResult::Exhausted].
///
/// # Parameters
/// * `P` - the inner retry policy.
#[derive(Debug)]
pub struct LimitedAttemptCount<P = Aip194Strict>
where
    P: RetryPolicy,
{
    inner: P,
    maximum_attempts: u32,
}

impl LimitedAttemptCount {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::*;
    /// let policy = LimitedAttemptCount::new(5);
    /// ```
    pub fn new(maximum_attempts: u32) -> Self {
        Self {
            inner: Aip194Strict,
            maximum_attempts,
        }
    }
}

impl<P> LimitedAttemptCount<P>
where
    P: RetryPolicy,
{
    /// Creates a new instance with a custom inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_policy::*;
    /// use std::time::Instant;
    /// let policy = LimitedAttemptCount::custom(AlwaysRetry, 2);
    /// assert!(policy.on_error(Instant::now(), 1, false, permanent_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, false, permanent_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::PermissionDenied)) }
    /// ```
    pub fn custom(inner: P, maximum_attempts: u32) -> Self {
        Self {
            inner,
            maximum_attempts,
        }
    }
}

impl<P> RetryPolicy for LimitedAttemptCount<P>
where
    P: RetryPolicy,
{
    fn on_error(
        &self,
        start: std::time::Instant,
        count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryResult {
        match self.inner.on_error(start, count, idempotent, error) {
            RetryResult::Permanent(e) => RetryResult::Permanent(e),
            RetryResult::Exhausted(e) => RetryResult::Exhausted(e),
            RetryResult::Continue(e) => {
                if count >= self.maximum_attempts {
                    RetryResult::Exhausted(e)
                } else {
                    RetryResult::Continue(e)
                }
            }
        }
    }

    fn on_throttle(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        error: Error,
    ) -> ThrottleResult {
        // The retry loop only calls `on_throttle()` if the policy has not
        // been exhausted.
        assert!(attempt_count < self.maximum_attempts);
        self.inner.on_throttle(loop_start, attempt_count, error)
    }

    fn remaining_time(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> Option<Duration> {
        self.inner.remaining_time(loop_start, attempt_count)
    }
}

const AUTH_ERROR_MESSAGE: &str = "Authentication error: This will not be retried. To configure retries for authentication, \
                please construct the credential instance directly using its builder (e.g., user_account::Builder) \
                and provide the retry policy there.";

/// A retry policy that wraps another policy and prevents retrying authentication errors.
#[derive(Debug, Clone)]
pub(crate) struct DontRetryAuthPolicy(pub(crate) Arc<dyn RetryPolicy>);

impl RetryPolicy for DontRetryAuthPolicy {
    fn on_error(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryResult {
        if error.is_authentication() {
            let new_error = Error::authentication(CredentialsError::new(
                false,
                AUTH_ERROR_MESSAGE,
                error,
            ));
            return RetryResult::Permanent(new_error);
        }
        self.0
            .on_error(loop_start, attempt_count, idempotent, error)
    }


    fn on_throttle(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        error: Error,
    ) -> ThrottleResult {
        self.0.on_throttle(loop_start, attempt_count, error)
    }

    fn remaining_time(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> Option<Duration> {
        self.0.remaining_time(loop_start, attempt_count)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::error::Error as StdError;

    // Verify `RetryPolicyArg` can be converted from the desired types.
    #[test]
    fn retry_policy_arg() {
        let policy = LimitedAttemptCount::new(3);
        let _ = RetryPolicyArg::from(policy);

        let policy: Arc<dyn RetryPolicy> = Arc::new(LimitedAttemptCount::new(3));
        let _ = RetryPolicyArg::from(policy);
    }

    #[test]
    fn aip194_strict() {
        let p = Aip194Strict;

        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, true, unavailable()).is_continue());
        assert!(p.on_error(now, 0, false, unavailable()).is_permanent());
        assert!(matches!(
            p.on_throttle(now, 0, unavailable()),
            ThrottleResult::Continue(_)
        ));

        assert!(p.on_error(now, 0, true, permission_denied()).is_permanent());
        assert!(
            p.on_error(now, 0, false, permission_denied())
                .is_permanent()
        );

        assert!(p.on_error(now, 0, true, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, false, http_unavailable()).is_permanent());
        assert!(matches!(
            p.on_throttle(now, 0, http_unavailable()),
            ThrottleResult::Continue(_)
        ));

        assert!(
            p.on_error(now, 0, true, http_permission_denied())
                .is_permanent()
        );
        assert!(
            p.on_error(now, 0, false, http_permission_denied())
                .is_permanent()
        );

        assert!(
            p.on_error(now, 0, true, Error::io("err".to_string()))
                .is_continue()
        );
        assert!(
            p.on_error(now, 0, false, Error::io("err".to_string()))
                .is_permanent()
        );

        assert!(p.on_error(now, 0, true, pre_rpc_transient()).is_continue());
        assert!(p.on_error(now, 0, false, pre_rpc_transient()).is_continue());

        assert!(p.on_error(now, 0, true, Error::ser("err")).is_permanent());
        assert!(p.on_error(now, 0, false, Error::ser("err")).is_permanent());
        assert!(p.on_error(now, 0, true, Error::deser("err")).is_permanent());
        assert!(
            p.on_error(now, 0, false, Error::deser("err"))
                .is_permanent()
        );

        assert!(p.remaining_time(now, 0).is_none());
    }

    #[test]
    fn always_retry() {
        let p = AlwaysRetry;

        let now = std::time::Instant::now();
        assert!(p.remaining_time(now, 0).is_none());
        assert!(p.on_error(now, 0, true, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, false, http_unavailable()).is_continue());
        assert!(matches!(
            p.on_throttle(now, 0, http_unavailable()),
            ThrottleResult::Continue(_)
        ));

        assert!(p.on_error(now, 0, true, unavailable()).is_continue());
        assert!(p.on_error(now, 0, false, unavailable()).is_continue());
    }

    #[test_case::test_case(true, Error::io("err"))]
    #[test_case::test_case(true, pre_rpc_transient())]
    #[test_case::test_case(true, Error::ser("err"))]
    #[test_case::test_case(false, Error::io("err"))]
    #[test_case::test_case(false, pre_rpc_transient())]
    #[test_case::test_case(false, Error::ser("err"))]
    fn always_retry_error_kind(idempotent: bool, error: Error) {
        let p = AlwaysRetry;
        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, idempotent, error).is_continue());
    }

    #[test]
    fn never_retry() {
        let p = NeverRetry;

        let now = std::time::Instant::now();
        assert!(p.remaining_time(now, 0).is_none());
        assert!(p.on_error(now, 0, true, http_unavailable()).is_exhausted());
        assert!(p.on_error(now, 0, false, http_unavailable()).is_exhausted());
        assert!(matches!(
            p.on_throttle(now, 0, http_unavailable()),
            ThrottleResult::Continue(_)
        ));

        assert!(p.on_error(now, 0, true, unavailable()).is_exhausted());
        assert!(p.on_error(now, 0, false, unavailable()).is_exhausted());

        assert!(
            p.on_error(now, 0, true, http_permission_denied())
                .is_exhausted()
        );
        assert!(
            p.on_error(now, 0, false, http_permission_denied())
                .is_exhausted()
        );
    }

    #[test_case::test_case(true, Error::io("err"))]
    #[test_case::test_case(true, pre_rpc_transient())]
    #[test_case::test_case(true, Error::ser("err"))]
    #[test_case::test_case(false, Error::io("err"))]
    #[test_case::test_case(false, pre_rpc_transient())]
    #[test_case::test_case(false, Error::ser("err"))]
    fn never_retry_error_kind(idempotent: bool, error: Error) {
        let p = NeverRetry;
        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, idempotent, error).is_exhausted());
    }

    fn pre_rpc_transient() -> Error {
        use crate::error::CredentialsError;
        Error::authentication(CredentialsError::from_msg(true, "err"))
    }

    fn http_unavailable() -> Error {
        Error::http(
            503_u16,
            HeaderMap::new(),
            bytes::Bytes::from_owner("SERVICE UNAVAILABLE".to_string()),
        )
    }

    fn http_permission_denied() -> Error {
        Error::http(
            403_u16,
            HeaderMap::new(),
            bytes::Bytes::from_owner("PERMISSION DENIED".to_string()),
        )
    }

    fn unavailable() -> Error {
        use crate::error::rpc::Code;
        let status = crate::error::rpc::Status::default()
            .set_code(Code::Unavailable)
            .set_message("UNAVAILABLE");
        Error::service(status)
    }

    fn permission_denied() -> Error {
        use crate::error::rpc::Code;
        let status = crate::error::rpc::Status::default()
            .set_code(Code::PermissionDenied)
            .set_message("PERMISSION_DENIED");
        Error::service(status)
    }

    mockall::mock! {
        #[derive(Debug)]
        Policy {}
        impl RetryPolicy for Policy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, idempotent: bool, error: Error) -> RetryResult;
            fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32, error: Error) -> ThrottleResult;
            fn remaining_time(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Duration>;
        }
    }

    #[test]
    fn limited_elapsed_time_error() {
        let limit = Duration::from_secs(123) + Duration::from_millis(567);
        let err = LimitedElapsedTimeError::new(limit, unavailable());
        assert_eq!(err.maximum_duration(), limit);
        let fmt = err.to_string();
        assert!(fmt.contains("123.567s"), "display={fmt}, debug={err:?}");
        assert!(err.source().is_some(), "{err:?}");
    }

    #[test]
    fn test_limited_time_forwards() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, _, e| RetryResult::Continue(e));
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, _, e| ThrottleResult::Continue(e));
        mock.expect_remaining_time().times(1).returning(|_, _| None);

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(now, 0, true, transient_error());
        assert!(rf.is_continue());

        let rt = policy.remaining_time(now, 0);
        assert!(rt.is_some());

        let e = policy.on_throttle(now, 0, transient_error());
        assert!(matches!(e, ThrottleResult::Continue(_)));
    }

    #[test]
    fn test_limited_time_on_throttle_continue() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, _, e| ThrottleResult::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        // Before the policy expires the inner result is returned verbatim.
        let rf = policy.on_throttle(now - Duration::from_secs(50), 1, unavailable());
        assert!(matches!(rf, ThrottleResult::Continue(_)), "{rf:?}");

        // After the policy expires the innter result is always "exhausted".
        let rf = policy.on_throttle(now - Duration::from_secs(70), 1, unavailable());
        assert!(matches!(rf, ThrottleResult::Exhausted(_)), "{rf:?}");
    }

    #[test]
    fn test_limited_time_on_throttle_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, _, e| ThrottleResult::Exhausted(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        // Before the policy expires the inner result is returned verbatim.
        let rf = policy.on_throttle(now - Duration::from_secs(50), 1, unavailable());
        assert!(matches!(rf, ThrottleResult::Exhausted(_)), "{rf:?}");
    }

    #[test]
    fn test_limited_time_inner_continues() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, _, e| RetryResult::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(now - Duration::from_secs(10), 1, true, transient_error());
        assert!(rf.is_continue());

        let rf = policy.on_error(now - Duration::from_secs(70), 1, true, transient_error());
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_time_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, _, e| RetryResult::Permanent(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error(now - Duration::from_secs(10), 1, false, transient_error());
        assert!(rf.is_permanent());

        let rf = policy.on_error(now + Duration::from_secs(10), 1, false, transient_error());
        assert!(rf.is_permanent());
    }

    #[test]
    fn test_limited_time_inner_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, _, e| RetryResult::Exhausted(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error(now - Duration::from_secs(10), 1, false, transient_error());
        assert!(rf.is_exhausted());

        let rf = policy.on_error(now + Duration::from_secs(10), 1, false, transient_error());
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_time_remaining_inner_longer() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|_, _| Some(Duration::from_secs(30)));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let remaining = policy.remaining_time(now - Duration::from_secs(55), 0);
        assert!(remaining <= Some(Duration::from_secs(5)), "{remaining:?}");
    }

    #[test]
    fn test_limited_time_remaining_inner_shorter() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|_, _| Some(Duration::from_secs(5)));
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let now = std::time::Instant::now();
        let remaining = policy.remaining_time(now - Duration::from_secs(5), 0);
        assert!(remaining <= Some(Duration::from_secs(10)), "{remaining:?}");
    }

    #[test]
    fn test_limited_time_remaining_inner_is_none() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time().times(1).returning(|_, _| None);
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let now = std::time::Instant::now();
        let remaining = policy.remaining_time(now - Duration::from_secs(50), 0);
        assert!(remaining <= Some(Duration::from_secs(10)), "{remaining:?}");
    }

    #[test]
    fn test_limited_attempt_count_on_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, _, e| RetryResult::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(
            policy
                .on_error(now, 1, true, transient_error())
                .is_continue()
        );
        assert!(
            policy
                .on_error(now, 2, true, transient_error())
                .is_continue()
        );
        assert!(
            policy
                .on_error(now, 3, true, transient_error())
                .is_exhausted()
        );
    }

    #[test]
    fn test_limited_attempt_count_on_throttle_continue() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, _, e| ThrottleResult::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(matches!(
            policy.on_throttle(now, 2, unavailable()),
            ThrottleResult::Continue(_)
        ));
    }

    #[test]
    fn test_limited_attempt_count_on_throttle_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, _, e| ThrottleResult::Exhausted(e));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(matches!(
            policy.on_throttle(now, 1, unavailable()),
            ThrottleResult::Exhausted(_)
        ));
    }

    #[test]
    fn test_limited_attempt_count_remaining_none() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time().times(1).returning(|_, _| None);
        let policy = LimitedAttemptCount::custom(mock, 3);

        let now = std::time::Instant::now();
        assert!(policy.remaining_time(now, 0).is_none());
    }

    #[test]
    fn test_limited_attempt_count_remaining_some() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|_, _| Some(Duration::from_secs(123)));
        let policy = LimitedAttemptCount::custom(mock, 3);

        let now = std::time::Instant::now();
        assert_eq!(
            policy.remaining_time(now, 0),
            Some(Duration::from_secs(123))
        );
    }

    #[test]
    fn test_limited_attempt_count_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, _, e| RetryResult::Permanent(e));
        let policy = LimitedAttemptCount::custom(mock, 2);
        let now = std::time::Instant::now();

        let rf = policy.on_error(now, 1, false, transient_error());
        assert!(rf.is_permanent());

        let rf = policy.on_error(now, 1, false, transient_error());
        assert!(rf.is_permanent());
    }

    #[test]
    fn test_limited_attempt_count_inner_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, _, e| RetryResult::Exhausted(e));
        let policy = LimitedAttemptCount::custom(mock, 2);
        let now = std::time::Instant::now();

        let rf = policy.on_error(now, 1, false, transient_error());
        assert!(rf.is_exhausted());

        let rf = policy.on_error(now, 1, false, transient_error());
        assert!(rf.is_exhausted());
    }

    fn transient_error() -> Error {
        use crate::error::rpc::{Code, Status};
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try-again"),
        )
    }

    #[test]
    fn test_dont_retry_auth_policy() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1)
            .returning(|_, _, _, e| RetryResult::Continue(e));

        let policy = DontRetryAuthPolicy(Arc::new(mock));
        let now = std::time::Instant::now();

        // Test with an authentication error
        let auth_error = Error::authentication(CredentialsError::from_msg(true, "auth error"));
        let result = policy.on_error(now, 1, true, auth_error);
        assert!(result.is_permanent());
        if let RetryResult::Permanent(e) = result {
            assert!(e.to_string().contains("Authentication error"));
        } else {
            panic!("Expected a permanent error");
        }

        // Test with a non-authentication error
        let other_error = transient_error();
        let result = policy.on_error(now, 1, true, other_error);
        assert!(result.is_continue());
    }
}
