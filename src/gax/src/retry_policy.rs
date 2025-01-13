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
//! # Example:
//! ```
//! # use gcp_sdk_gax::retry_policy::*;
//! # use gcp_sdk_gax::options;
//! fn customize_retry_policy(config: options::ClientConfig) -> options::ClientConfig {
//!     // Retry for at most 10 seconds or at most 5 attempts: whichever limit
//!     // is reached first stops the retry loop.
//!     let d = std::time::Duration::from_secs(10);
//!     config.set_retry_policy(Aip194Strict.with_time_limit(d).with_attempt_limit(5))
//! }
//! ```
//!
//! [idempotent]: https://en.wikipedia.org/wiki/Idempotence

use crate::error::Error;
use std::sync::Arc;

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
#[derive(Debug)]
pub enum RetryFlow {
    /// Stop the retry loop because this is a permanent error.
    Permanent(Error),
    /// Stop the retry loop. The error is retryable, but the retry attempts are
    /// exhausted.
    Exhausted(Error),
    /// The error was retryable, continue the retry loop.
    Continue(Error),
}

impl RetryFlow {
    pub fn is_permanent(&self) -> bool {
        match &self {
            Self::Permanent(_) => true,
            Self::Exhausted(_) | Self::Continue(_) => false,
        }
    }
    pub fn is_exhausted(&self) -> bool {
        match &self {
            Self::Exhausted(_) => true,
            Self::Permanent(_) | Self::Continue(_) => false,
        }
    }
    pub fn is_continue(&self) -> bool {
        match &self {
            Self::Continue(_) => true,
            Self::Permanent(_) | Self::Exhausted(_) => false,
        }
    }
}

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
    fn on_error(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryFlow;

    /// Query the retry policy after a retry attempt is throttled.
    ///
    /// Retry attempts may be throttled before they are even sent out. The retry
    /// policy may choose to treat these as normal errors, consuming attempts,
    /// or may prefer to ignore them and always return [RetryFlow::Continue].
    ///
    /// # Parameters
    /// * `loop_start` - when the retry loop started.
    /// * `attempt_count` - the number of attempts. This method is never called
    ///    before the first attempt.
    fn on_throttle(&self, _loop_start: std::time::Instant, _attempt_count: u32) -> Option<Error> {
        None
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
    ///    the first attempt, so the first value is zero.
    fn remaining_time(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
    ) -> Option<std::time::Duration> {
        None
    }
}

/// A helper type to use [RetryPolicy] in client and request options.
#[derive(Clone)]
pub struct RetryPolicyArg(pub(crate) Arc<dyn RetryPolicy>);

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

/// Extension trait for [`RetryPolicy`]
pub trait RetryPolicyExt: RetryPolicy + Sized {
    /// Decorate a [`RetryPolicy`] to limit the total elapsed time in the retry loop.
    ///
    /// While the time spent in the retry loop (including time in backoff) is
    /// less than the prescribed duration the `on_error()` method returns the
    /// results of the inner policy. After that time it returns
    /// [Exhausted][RetryFlow::Exhausted] if the inner policy returns
    /// [Continue][RetryFlow::Continue].
    ///
    /// The `remaining_time()` function returns the remaining time. This is
    /// always [Duration::ZERO][std::time::Duration::ZERO] once or after the
    /// policy's expiration time is reached.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::retry_policy::*;
    /// let d = std::time::Duration::from_secs(10);
    /// let policy = Aip194Strict.with_time_limit(d);
    /// assert!(policy.remaining_time(std::time::Instant::now(), 0) <= Some(d));
    /// ```
    fn with_time_limit(self, maximum_duration: std::time::Duration) -> LimitedElapsedTime<Self> {
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
    /// is reached, the policy returns [Exhausted][RetryFlow::Exhausted] if the
    /// inner policy returns [Continue][RetryFlow::Continue].
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::error::*;
    /// use std::time::Instant;
    /// let policy = Aip194Strict.with_attempt_limit(3);
    /// assert_eq!(policy.remaining_time(Instant::now(), 0), None);
    /// assert!(policy.on_error(Instant::now(), 0, true, Error::authentication(format!("transient"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 1, true, Error::authentication(format!("transient"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, true, Error::authentication(format!("transient"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 3, true, Error::authentication(format!("transient"))).is_exhausted());
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
/// # use gcp_sdk_gax::retry_policy::*;
/// # use gcp_sdk_gax::options::RequestOptionsBuilder;
/// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
///     builder.with_retry_policy(Aip194Strict.with_attempt_limit(3))
/// }
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
    ) -> RetryFlow {
        if let Some(svc) = error.as_inner::<crate::error::ServiceError>() {
            if !idempotent {
                return RetryFlow::Permanent(error);
            }
            return if svc.status().status.as_deref() == Some("UNAVAILABLE") {
                RetryFlow::Continue(error)
            } else {
                RetryFlow::Permanent(error)
            };
        }

        if let Some(http) = error.as_inner::<crate::error::HttpError>() {
            if !idempotent {
                return RetryFlow::Permanent(error);
            }
            return if http.status_code() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                RetryFlow::Continue(error)
            } else {
                RetryFlow::Permanent(error)
            };
        }
        use crate::error::ErrorKind;
        match error.kind() {
            ErrorKind::Rpc | ErrorKind::Io => {
                if idempotent {
                    RetryFlow::Continue(error)
                } else {
                    RetryFlow::Permanent(error)
                }
            }
            ErrorKind::Authentication => {
                // This indicates the operation never left the client, so it
                // safe to retry
                RetryFlow::Continue(error)
            }
            ErrorKind::Serde => RetryFlow::Permanent(error),
            ErrorKind::Other => RetryFlow::Permanent(error),
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
/// # use std::sync::Arc;
/// # use gcp_sdk_gax::retry_policy::*;
/// # use gcp_sdk_gax::options::RequestOptionsBuilder;
/// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
///     builder.with_retry_policy(
///         AlwaysRetry.with_attempt_limit(3))
/// }
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct AlwaysRetry;

impl RetryPolicy for AlwaysRetry {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        _idempotent: bool,
        error: Error,
    ) -> RetryFlow {
        RetryFlow::Continue(error)
    }
}

/// A retry policy that never retries.
///
/// This policy is useful when the client already has (or may already have) a
/// retry policy configured, and you want to avoid retrying a particular method.
///
/// # Example
/// ```
/// # use gcp_sdk_gax::retry_policy::*;
/// # use gcp_sdk_gax::options::RequestOptionsBuilder;
/// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
///     builder.with_retry_policy(NeverRetry)
/// }
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
    ) -> RetryFlow {
        RetryFlow::Exhausted(error)
    }
}

/// A retry policy decorator that limits the total time in the retry loop.
///
/// This policy decorates an inner policy and limits the duration of retry
/// loops. While the time spent in the retry loop (including time in backoff)
/// is less than the prescribed duration the `on_error()` method returns the
/// results of the inner policy. After that time it returns
/// [Exhausted][RetryFlow::Exhausted] if the inner policy returns
/// [Continue][RetryFlow::Continue].
///
/// The `remaining_time()` function returns the remaining time. This is always
/// [Duration::ZERO][std::time::Duration::ZERO] once or after the policy's
/// deadline is reached.
///
/// # Parameters
/// * `P` - the inner retry policy, defaults to [Aip194Strict].
#[derive(Debug)]
pub struct LimitedElapsedTime<P = Aip194Strict>
where
    P: RetryPolicy,
{
    inner: P,
    maximum_duration: std::time::Duration,
}

impl LimitedElapsedTime {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    /// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
    ///     builder.with_retry_policy(LimitedElapsedTime::new(std::time::Duration::from_secs(10)))
    /// }
    /// ```
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::retry_policy::*;
    /// let d = std::time::Duration::from_secs(10);
    /// let policy = LimitedElapsedTime::new(d);
    /// assert!(policy.remaining_time(std::time::Instant::now(), 0) <= Some(d));
    /// ```
    pub fn new(maximum_duration: std::time::Duration) -> Self {
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
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    /// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
    ///     builder.with_retry_policy(LimitedElapsedTime::custom(AlwaysRetry, std::time::Duration::from_secs(10)))
    /// }
    /// ```
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::error;
    /// let d = std::time::Duration::from_secs(10);
    /// let policy = AlwaysRetry.with_time_limit(d);
    /// assert!(policy.remaining_time(std::time::Instant::now(), 0) <= Some(d));
    /// assert!(policy.on_error(std::time::Instant::now(), 1, false, error::Error::other(format!("test"))).is_continue());
    /// ```
    pub fn custom(inner: P, maximum_duration: std::time::Duration) -> Self {
        Self {
            inner,
            maximum_duration,
        }
    }

    fn error_if_expired(&self, loop_start: std::time::Instant) -> Option<Error> {
        let deadline = loop_start + self.maximum_duration;
        let now = std::time::Instant::now();
        if now < deadline {
            None
        } else {
            Some(Error::other(format!(
                "limited time retry policy exhausted {:?} ago",
                now.saturating_duration_since(deadline)
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
    ) -> RetryFlow {
        match self.inner.on_error(start, count, idempotent, error) {
            RetryFlow::Permanent(e) => RetryFlow::Permanent(e),
            RetryFlow::Exhausted(e) => RetryFlow::Exhausted(e),
            RetryFlow::Continue(e) => {
                if std::time::Instant::now() >= start + self.maximum_duration {
                    RetryFlow::Exhausted(e)
                } else {
                    RetryFlow::Continue(e)
                }
            }
        }
    }

    fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Error> {
        self.inner
            .on_throttle(loop_start, attempt_count)
            .or(self.error_if_expired(loop_start))
    }

    fn remaining_time(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> Option<std::time::Duration> {
        let deadline = loop_start + self.maximum_duration;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
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
/// `attempt_count < maximum_attempts`. Once the maximum number of attempts is
/// reached, the policy returns [Exhausted][RetryFlow::Exhausted] if the inner
/// policy returns [Continue][RetryFlow::Continue], and
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
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::*;
    /// fn customize_retry_policy() -> options::ClientConfig {
    ///     options::ClientConfig::new()
    ///         .set_retry_policy(LimitedAttemptCount::new(5))
    /// }
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
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    /// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
    ///     builder.with_retry_policy(LimitedAttemptCount::custom(AlwaysRetry, 10))
    /// }
    /// ```
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::error;
    /// use std::time::Instant;
    /// let policy = LimitedAttemptCount::custom(AlwaysRetry, 2);
    /// assert!(policy.on_error(Instant::now(), 1, false, error::Error::other(format!("test"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, false, error::Error::other(format!("test"))).is_exhausted());
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
    ) -> RetryFlow {
        match self.inner.on_error(start, count, idempotent, error) {
            RetryFlow::Permanent(e) => RetryFlow::Permanent(e),
            RetryFlow::Exhausted(e) => RetryFlow::Exhausted(e),
            RetryFlow::Continue(e) => {
                if count >= self.maximum_attempts {
                    RetryFlow::Exhausted(e)
                } else {
                    RetryFlow::Continue(e)
                }
            }
        }
    }

    fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Error> {
        if let Some(e) = self.inner.on_throttle(loop_start, attempt_count) {
            return Some(e);
        }
        if attempt_count < self.maximum_attempts {
            None
        } else {
            Some(Error::other(format!(
                "error count already reached maximum ({})",
                self.maximum_attempts
            )))
        }
    }

    fn remaining_time(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> Option<std::time::Duration> {
        self.inner.remaining_time(loop_start, attempt_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{rpc::Status, ServiceError};

    #[test]
    fn retry_flow() {
        let flow = RetryFlow::Permanent(http_unavailable());
        assert!(flow.is_permanent(), "{flow:?}");
        assert!(!flow.is_exhausted(), "{flow:?}");
        assert!(!flow.is_continue(), "{flow:?}");

        let flow = RetryFlow::Exhausted(http_unavailable());
        assert!(!flow.is_permanent(), "{flow:?}");
        assert!(flow.is_exhausted(), "{flow:?}");
        assert!(!flow.is_continue(), "{flow:?}");

        let flow = RetryFlow::Continue(http_unavailable());
        assert!(!flow.is_permanent(), "{flow:?}");
        assert!(!flow.is_exhausted(), "{flow:?}");
        assert!(flow.is_continue(), "{flow:?}");
    }

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
        assert!(p.on_throttle(now, 0).is_none());

        assert!(p.on_error(now, 0, true, permission_denied()).is_permanent());
        assert!(p
            .on_error(now, 0, false, permission_denied())
            .is_permanent());

        assert!(p.on_error(now, 0, true, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, false, http_unavailable()).is_permanent());
        assert!(p.on_throttle(now, 0).is_none());

        assert!(p
            .on_error(now, 0, true, http_permission_denied())
            .is_permanent());
        assert!(p
            .on_error(now, 0, false, http_permission_denied())
            .is_permanent());

        assert!(p
            .on_error(now, 0, true, Error::io("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(now, 0, false, Error::io("err".to_string()))
            .is_permanent());

        assert!(p
            .on_error(now, 0, true, Error::authentication("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(now, 0, false, Error::authentication("err".to_string()))
            .is_continue());

        assert!(p
            .on_error(now, 0, true, Error::serde("err".to_string()))
            .is_permanent());
        assert!(p
            .on_error(now, 0, false, Error::serde("err".to_string()))
            .is_permanent());
        assert!(p
            .on_error(now, 0, true, Error::other("err".to_string()))
            .is_permanent());
        assert!(p
            .on_error(now, 0, false, Error::other("err".to_string()))
            .is_permanent());

        assert!(p.remaining_time(now, 0).is_none());
    }

    #[test]
    fn always_retry() {
        let p = AlwaysRetry;

        let now = std::time::Instant::now();
        assert!(p.remaining_time(now, 0).is_none());
        assert!(p.on_error(now, 0, true, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, false, http_unavailable()).is_continue());
        assert!(p.on_throttle(now, 0).is_none());

        assert!(p.on_error(now, 0, true, unavailable()).is_continue());
        assert!(p.on_error(now, 0, false, unavailable()).is_continue());
    }

    #[test_case::test_case(true, Error::io("err"))]
    #[test_case::test_case(true, Error::authentication("err"))]
    #[test_case::test_case(true, Error::serde("err"))]
    #[test_case::test_case(true, Error::other("err"))]
    #[test_case::test_case(false, Error::io("err"))]
    #[test_case::test_case(false, Error::authentication("err"))]
    #[test_case::test_case(false, Error::serde("err"))]
    #[test_case::test_case(false, Error::other("err"))]
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
        assert!(p.on_throttle(now, 0).is_none());

        assert!(p.on_error(now, 0, true, unavailable()).is_exhausted());
        assert!(p.on_error(now, 0, false, unavailable()).is_exhausted());

        assert!(p
            .on_error(now, 0, true, http_permission_denied())
            .is_exhausted());
        assert!(p
            .on_error(now, 0, false, http_permission_denied())
            .is_exhausted());
    }

    #[test_case::test_case(true, Error::io("err"))]
    #[test_case::test_case(true, Error::authentication("err"))]
    #[test_case::test_case(true, Error::serde("err"))]
    #[test_case::test_case(true, Error::other("err"))]
    #[test_case::test_case(false, Error::io("err"))]
    #[test_case::test_case(false, Error::authentication("err"))]
    #[test_case::test_case(false, Error::serde("err"))]
    #[test_case::test_case(false, Error::other("err"))]
    fn never_retry_error_kind(idempotent: bool, error: Error) {
        let p = NeverRetry;
        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, idempotent, error).is_exhausted());
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

    fn http_unavailable() -> Error {
        let mut status = Status::default();
        status.code = 503;
        status.message = "SERVICE UNAVAILABLE".to_string();
        status.status = Some("UNAVAILABLE".to_string());
        from_status(status)
    }

    fn http_permission_denied() -> Error {
        let mut status = Status::default();
        status.code = 403;
        status.message = "PERMISSION DENIED".to_string();
        status.status = Some("PERMISSION_DENIED".to_string());
        from_status(status)
    }

    fn unavailable() -> Error {
        use crate::error::rpc::Code;
        let status = rpc::model::Status::default()
            .set_code(Code::Unavailable as i32)
            .set_message("UNAVAILABLE");
        Error::rpc(ServiceError::from(status))
    }

    fn permission_denied() -> Error {
        use crate::error::rpc::Code;
        let status = rpc::model::Status::default()
            .set_code(Code::PermissionDenied as i32)
            .set_message("PERMISSION_DENIED");
        Error::rpc(ServiceError::from(status))
    }

    mockall::mock! {
        #[derive(Debug)]
        Policy {}
        impl RetryPolicy for Policy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, idempotent: bool, error: Error) -> RetryFlow;
            fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Error>;
            fn remaining_time(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<std::time::Duration>;
        }
    }

    use std::time::Duration;

    #[test]
    fn test_limited_time_forwards() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, _, e| RetryFlow::Continue(e));
        mock.expect_on_throttle().times(1..).returning(|_, _| None);
        mock.expect_remaining_time().times(1).returning(|_, _| None);

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(now, 0, true, Error::other("err".to_string()));
        assert!(rf.is_continue());

        let rt = policy.remaining_time(now, 0);
        assert!(rt.is_some());

        let e = policy.on_throttle(now, 0);
        assert!(e.is_none());
    }

    #[test]
    fn test_limited_time_on_throttle_none() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle().times(1..).returning(|_, _| None);

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        // Before the policy expires the inner result is returned verbatim.
        let rf = policy.on_throttle(now - Duration::from_secs(50), 1);
        assert!(rf.is_none());

        // After the policy expires the innter result is always "exhausted".
        let rf = policy.on_throttle(now - Duration::from_secs(70), 1);
        assert!(rf.is_some());
    }

    #[test]
    fn test_limited_time_on_throttle_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, _| Some(Error::other(format!("err"))));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        // Before the policy expires the inner result is returned verbatim.
        let rf = policy.on_throttle(now - Duration::from_secs(50), 1);
        assert!(rf.is_some());

        // After the policy expires the innter result is always "exhausted".
        let rf = policy.on_throttle(now - Duration::from_secs(70), 1);
        assert!(rf.is_some());
    }

    #[test]
    fn test_limited_time_inner_continues() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, _, e| RetryFlow::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(
            now - Duration::from_secs(10),
            1,
            true,
            Error::other("err".to_string()),
        );
        assert!(rf.is_continue());

        let rf = policy.on_error(
            now - Duration::from_secs(70),
            1,
            true,
            Error::other("err".to_string()),
        );
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_time_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, _, e| RetryFlow::Permanent(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error(
            now - Duration::from_secs(10),
            1,
            false,
            Error::other("err".to_string()),
        );
        assert!(rf.is_permanent());

        let rf = policy.on_error(
            now + Duration::from_secs(10),
            1,
            false,
            Error::other("err".to_string()),
        );
        assert!(rf.is_permanent());
    }

    #[test]
    fn test_limited_time_inner_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, _, e| RetryFlow::Exhausted(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error(
            now - Duration::from_secs(10),
            1,
            false,
            Error::other("err".to_string()),
        );
        assert!(rf.is_exhausted());

        let rf = policy.on_error(
            now + Duration::from_secs(10),
            1,
            false,
            Error::other("err".to_string()),
        );
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
            .returning(|_, _, _, e| RetryFlow::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(policy
            .on_error(now, 1, true, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(now, 2, true, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(now, 3, true, Error::other("err".to_string()))
            .is_exhausted());
    }

    #[test]
    fn test_limited_attempt_count_on_throttle_none() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle().times(1..).returning(|_, _| None);

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(policy.on_throttle(now, 1).is_none());
        assert!(policy.on_throttle(now, 2).is_none());
        assert!(policy.on_throttle(now, 3).is_some());
    }

    #[test]
    fn test_limited_attempt_count_on_throttle_some() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, a| Some(Error::other(format!("err {a}"))));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(policy.on_throttle(now, 1).is_some());
        assert!(policy.on_throttle(now, 2).is_some());
        assert!(policy.on_throttle(now, 3).is_some());
    }

    #[test]
    fn test_limited_attempt_count_on_throttle_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_throttle()
            .times(1..)
            .returning(|_, _| Some(Error::other(format!("err"))));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(policy.on_throttle(now, 1).is_some());
        assert!(policy.on_throttle(now, 2).is_some());
        assert!(policy.on_throttle(now, 3).is_some());
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
            .returning(|_, _, _, e| RetryFlow::Permanent(e));
        let policy = LimitedAttemptCount::custom(mock, 2);
        let now = std::time::Instant::now();

        let rf = policy.on_error(now, 1, false, Error::other("err".to_string()));
        assert!(rf.is_permanent());

        let rf = policy.on_error(now, 1, false, Error::other("err".to_string()));
        assert!(rf.is_permanent());
    }

    #[test]
    fn test_limited_attempt_count_inner_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, _, e| RetryFlow::Exhausted(e));
        let policy = LimitedAttemptCount::custom(mock, 2);
        let now = std::time::Instant::now();

        let rf = policy.on_error(now, 1, false, Error::other("err".to_string()));
        assert!(rf.is_exhausted());

        let rf = policy.on_error(now, 1, false, Error::other("err".to_string()));
        assert!(rf.is_exhausted());
    }
}
