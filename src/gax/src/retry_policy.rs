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
//! # use std::sync::Arc;
//! # use gcp_sdk_gax::retry_policy::*;
//! # use gcp_sdk_gax::options::RequestOptionsBuilder;
//! fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
//!     builder.with_retry_policy(LimitedAttemptCount::provider(5))
//! }
//! ```
//!
//! [idempotent]: https://en.wikipedia.org/wiki/Idempotence

use crate::error::rpc::Status;
use crate::error::{Error, HttpError};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;

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
pub trait RetryPolicy: Send + Sync {
    /// Query the retry policy after an error.
    ///
    /// # Parameters
    /// * `idempotent` - if `true` assume the operation is idempotent. Many more
    ///   errors are retryable on idempotent operations.
    /// * `error` - the last error when attempting the request.
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow;

    /// The remaining time in the retry policy.
    ///
    /// For policies based on time, this returns the remaining time in the
    /// policy. The retry loop can use this value to adjust the next RPC
    /// timeout. For policies that are not time based this returns `None`.
    fn remaining_time(&self) -> Option<std::time::Duration> {
        None
    }
}

/// Creates retry policies.
///
/// Retry policies typically keep state that is specific to each retry loop. For
/// example, the number of attempts, or maybe some span to group all retry
/// attempts for tracing purposes.
///
/// The application configures clients by setting the default provider used when
/// no method provider is available. The application may also set a provider in
/// effect for a client method. Note that some methods issue multiple RPCs, for
/// example, pagination helpers and LRO helpers. The provider is used to create
/// a retry policy for each one of the RPCs.
///
/// Retry policy providers are passed between async functions, so they must be
/// `Send` and `Sync`.  They must also implement [Debug][std::fmt::Debug]
/// because they are logged as part of the overall request / response logs.
pub trait RetryPolicyProvider: Send + Sync + std::fmt::Debug {
    fn make(&self) -> Box<dyn RetryPolicy>;
}

/// A helper type to use [RetryPolicy] in client and request options.
#[derive(Clone)]
pub struct RetryPolicyArg(pub(crate) Arc<dyn RetryPolicyProvider>);

impl<T> std::convert::From<T> for RetryPolicyArg
where
    T: RetryPolicyProvider + 'static,
{
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl std::convert::From<Arc<dyn RetryPolicyProvider>> for RetryPolicyArg {
    fn from(value: Arc<dyn RetryPolicyProvider>) -> Self {
        Self(value)
    }
}

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
/// # use std::sync::Arc;
/// # use gcp_sdk_gax::retry_policy::*;
/// # use gcp_sdk_gax::options::RequestOptionsBuilder;
/// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
///     builder.with_retry_policy(
///         LimitedAttemptCount::custom_provider(Aip194Strict, 3))
/// }
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct Aip194Strict;

impl RetryPolicy for Aip194Strict {
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow {
        if let Some(http) = error.as_inner::<crate::error::HttpError>() {
            if !idempotent {
                return RetryFlow::Permanent(error);
            }
            return if match_status_code_string(http, "UNAVAILABLE") {
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

// A helper function to simplify `Api194Strict::on_error()`:
fn match_status_code_string(http: &HttpError, code: &str) -> bool {
    Status::try_from(http)
        .ok()
        .map(|v| v.status.as_deref() == Some(code))
        .unwrap_or(false)
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
///         LimitedAttemptCount::custom_provider(AlwaysRetry, 3))
/// }
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct AlwaysRetry;

impl RetryPolicy for AlwaysRetry {
    fn on_error(&self, _idempotent: bool, error: Error) -> RetryFlow {
        RetryFlow::Continue(error)
    }
}

#[derive(Clone, Debug)]
struct LimitedElapsedTimeProvider<P>
where
    P: RetryPolicy + Clone + std::fmt::Debug,
{
    inner: P,
    duration: std::time::Duration,
}

impl<P> RetryPolicyProvider for LimitedElapsedTimeProvider<P>
where
    P: RetryPolicy + Clone + std::fmt::Debug + 'static,
{
    fn make(&self) -> Box<dyn RetryPolicy> {
        Box::new(LimitedElapsedTime::custom(
            self.inner.clone(),
            self.duration,
        ))
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
pub struct LimitedElapsedTime<P = Aip194Strict>
where
    P: RetryPolicy,
{
    inner: P,
    deadline: std::time::Instant,
}

impl LimitedElapsedTime {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::retry_policy::*;
    /// let d = std::time::Duration::from_secs(10);
    /// let policy = LimitedElapsedTime::new(d.clone());
    /// assert_eq!(policy.remaining_time().map(|t| t <= d), Some(true));
    /// ```
    pub fn new(maximum_duration: std::time::Duration) -> Self {
        Self {
            inner: Aip194Strict,
            deadline: std::time::Instant::now() + maximum_duration,
        }
    }

    /// Creates a new provider, suitable to configure the request or client options.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    /// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
    ///     builder.with_retry_policy(LimitedElapsedTime::provider(std::time::Duration::from_secs(10)))
    /// }
    /// ```
    pub fn provider(duration: std::time::Duration) -> impl RetryPolicyProvider {
        LimitedElapsedTimeProvider {
            inner: Aip194Strict,
            duration,
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
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    ///
    /// let d = std::time::Duration::from_secs(10);
    /// let policy = LimitedElapsedTime::custom(AlwaysRetry, d.clone());
    /// assert!(policy.on_error(false, error::Error::other(format!("test"))).is_continue());
    /// assert_eq!(policy.remaining_time().map(|t| t < d), Some(true));
    /// ```
    pub fn custom(inner: P, maximum_duration: std::time::Duration) -> Self {
        Self {
            inner,
            deadline: std::time::Instant::now() + maximum_duration,
        }
    }

    /// Creates a new provider, suitable to configure the request or client options.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    ///
    /// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
    ///     builder.with_retry_policy(
    ///         LimitedElapsedTime::custom_provider(
    ///             AlwaysRetry, std::time::Duration::from_secs(10)))
    /// }
    /// ```
    pub fn custom_provider(inner: P, duration: std::time::Duration) -> impl RetryPolicyProvider
    where
        P: RetryPolicy + Clone + std::fmt::Debug + 'static,
    {
        LimitedElapsedTimeProvider { inner, duration }
    }

    fn on_error_now(&self, now: std::time::Instant, idempotent: bool, error: Error) -> RetryFlow {
        let exhausted = now >= self.deadline;
        match self.inner.on_error(idempotent, error) {
            RetryFlow::Permanent(e) => RetryFlow::Permanent(e),
            RetryFlow::Exhausted(e) => RetryFlow::Exhausted(e),
            RetryFlow::Continue(e) => {
                if exhausted {
                    RetryFlow::Exhausted(e)
                } else {
                    RetryFlow::Continue(e)
                }
            }
        }
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
    P: RetryPolicy + 'static,
{
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow {
        self.on_error_now(std::time::Instant::now(), idempotent, error)
    }

    fn remaining_time(&self) -> Option<std::time::Duration> {
        self.remaining_time_now(std::time::Instant::now())
    }
}

#[derive(Clone, Debug)]
struct LimitedAttemptCountProvider<P>
where
    P: RetryPolicy + Clone + std::fmt::Debug,
{
    inner: P,
    maximum_attempts: i32,
}

impl<P> RetryPolicyProvider for LimitedAttemptCountProvider<P>
where
    P: RetryPolicy + Clone + std::fmt::Debug + 'static,
{
    fn make(&self) -> Box<dyn RetryPolicy> {
        Box::new(LimitedAttemptCount::custom(
            self.inner.clone(),
            self.maximum_attempts,
        ))
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
pub struct LimitedAttemptCount<P = Aip194Strict>
where
    P: RetryPolicy,
{
    inner: P,
    maximum_attempts: i32,
    attempt_count: Mutex<Cell<i32>>,
}

impl LimitedAttemptCount {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::error::*;
    /// let policy = LimitedAttemptCount::new(3);
    /// # let transient_error = Error::authentication(format!("test only"));
    /// assert!(policy.on_error(true, transient_error).is_continue());
    /// # let transient_error = Error::authentication(format!("test only"));
    /// assert!(policy.on_error(true, transient_error).is_continue());
    /// # let transient_error = Error::authentication(format!("test only"));
    /// assert!(policy.on_error(true, transient_error).is_exhausted());
    /// assert_eq!(policy.remaining_time(), None);
    /// ```
    pub fn new(maximum_attempts: i32) -> Self {
        Self {
            inner: Aip194Strict,
            maximum_attempts,
            attempt_count: Mutex::new(Cell::new(1)),
        }
    }

    /// Creates a new provider, suitable to configure the request or client options.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::*;
    /// fn customize_retry_policy() -> options::ClientConfig {
    ///     options::ClientConfig::new()
    ///         .set_retry_policy(LimitedAttemptCount::provider(5))
    /// }
    /// ```
    pub fn provider(maximum_attempts: i32) -> impl RetryPolicyProvider {
        LimitedAttemptCountProvider {
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
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    ///
    /// let policy = LimitedAttemptCount::custom(AlwaysRetry, 2);
    /// assert!(policy.on_error(false, error::Error::other(format!("test"))).is_continue());
    /// assert!(policy.on_error(false, error::Error::other(format!("test"))).is_exhausted());
    /// ```
    pub fn custom(inner: P, maximum_attempts: i32) -> Self {
        Self {
            inner,
            maximum_attempts,
            attempt_count: Mutex::new(Cell::new(1)),
        }
    }

    /// Creates a new provider, suitable to configure the request or client options.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::retry_policy::*;
    /// # use gcp_sdk_gax::options::RequestOptionsBuilder;
    /// fn customize_retry_policy(builder: impl RequestOptionsBuilder) -> impl RequestOptionsBuilder {
    ///     builder.with_retry_policy(
    ///         LimitedAttemptCount::custom_provider(AlwaysRetry, 10))
    /// }
    /// ```
    pub fn custom_provider(inner: P, maximum_attempts: i32) -> impl RetryPolicyProvider
    where
        P: RetryPolicy + Clone + std::fmt::Debug + 'static,
    {
        LimitedAttemptCountProvider {
            inner,
            maximum_attempts,
        }
    }
}

impl<P> RetryPolicy for LimitedAttemptCount<P>
where
    P: RetryPolicy,
{
    fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow {
        let exhausted = match self.attempt_count.lock() {
            Err(_) => true,
            Ok(guard) => {
                let count = guard.get().saturating_add(1);
                guard.set(count);
                count > self.maximum_attempts
            }
        };
        match self.inner.on_error(idempotent, error) {
            RetryFlow::Permanent(e) => RetryFlow::Permanent(e),
            RetryFlow::Exhausted(e) => RetryFlow::Exhausted(e),
            RetryFlow::Continue(e) => {
                if exhausted {
                    RetryFlow::Exhausted(e)
                } else {
                    RetryFlow::Continue(e)
                }
            }
        }
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
    fn retry_flow() {
        let flow = RetryFlow::Permanent(unavailable());
        assert!(flow.is_permanent(), "{flow:?}");
        assert!(!flow.is_exhausted(), "{flow:?}");
        assert!(!flow.is_continue(), "{flow:?}");

        let flow = RetryFlow::Exhausted(unavailable());
        assert!(!flow.is_permanent(), "{flow:?}");
        assert!(flow.is_exhausted(), "{flow:?}");
        assert!(!flow.is_continue(), "{flow:?}");

        let flow = RetryFlow::Continue(unavailable());
        assert!(!flow.is_permanent(), "{flow:?}");
        assert!(!flow.is_exhausted(), "{flow:?}");
        assert!(flow.is_continue(), "{flow:?}");
    }

    // Verify `RetryPolicyArg` can be converted from the desired types.
    #[test]
    fn retry_policy_arg() {
        let provider = LimitedAttemptCount::provider(3);
        let _ = RetryPolicyArg::from(provider);

        let provider: Arc<dyn RetryPolicyProvider> = Arc::new(LimitedAttemptCount::provider(3));
        let _ = RetryPolicyArg::from(provider);
    }

    #[test]
    fn aip194_strict() {
        let p = Aip194Strict;

        assert!(p.on_error(true, unavailable()).is_continue());
        assert!(p.on_error(false, unavailable()).is_permanent());

        assert!(p.on_error(true, permission_denied()).is_permanent());
        assert!(p.on_error(false, permission_denied()).is_permanent());

        assert!(p.on_error(true, Error::io("err".to_string())).is_continue());
        assert!(p
            .on_error(false, Error::io("err".to_string()))
            .is_permanent());

        assert!(p
            .on_error(true, Error::authentication("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(false, Error::authentication("err".to_string()))
            .is_continue());

        assert!(p
            .on_error(true, Error::serde("err".to_string()))
            .is_permanent());
        assert!(p
            .on_error(false, Error::serde("err".to_string()))
            .is_permanent());
        assert!(p
            .on_error(true, Error::other("err".to_string()))
            .is_permanent());
        assert!(p
            .on_error(false, Error::other("err".to_string()))
            .is_permanent());

        assert!(p.remaining_time().is_none());
    }

    #[test]
    fn always_retry() {
        let p = AlwaysRetry;

        assert!(p.on_error(true, unavailable()).is_continue());
        assert!(p.on_error(false, unavailable()).is_continue());

        assert!(p.on_error(true, permission_denied()).is_continue());
        assert!(p.on_error(false, permission_denied()).is_continue());

        assert!(p.on_error(true, Error::io("err".to_string())).is_continue());
        assert!(p
            .on_error(false, Error::io("err".to_string()))
            .is_continue());

        assert!(p
            .on_error(true, Error::authentication("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(false, Error::authentication("err".to_string()))
            .is_continue());

        assert!(p
            .on_error(true, Error::serde("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(false, Error::serde("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(true, Error::other("err".to_string()))
            .is_continue());
        assert!(p
            .on_error(false, Error::other("err".to_string()))
            .is_continue());

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
            .returning(|_, e| RetryFlow::Continue(e));
        mock.expect_remaining_time().times(1).returning(|| None);

        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
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
            .returning(|_, e| RetryFlow::Continue(e));

        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
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
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_time_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, e| RetryFlow::Permanent(e));
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error_now(
            policy.deadline - Duration::from_secs(10),
            false,
            Error::other("err".to_string()),
        );
        assert!(rf.is_permanent());

        let rf = policy.on_error_now(
            policy.deadline + Duration::from_secs(10),
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
            .returning(|_, e| RetryFlow::Exhausted(e));
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error_now(
            policy.deadline - Duration::from_secs(10),
            false,
            Error::other("err".to_string()),
        );
        assert!(rf.is_exhausted());

        let rf = policy.on_error_now(
            policy.deadline + Duration::from_secs(10),
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
            .returning(|| Some(Duration::from_secs(50)));
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let remaining = policy.remaining_time_now(policy.deadline - Duration::from_secs(10));
        assert_eq!(remaining, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_limited_time_remaining_inner_shorter() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|| Some(Duration::from_secs(5)));
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let remaining = policy.remaining_time_now(policy.deadline - Duration::from_secs(10));
        assert_eq!(remaining, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_limited_time_remaining_inner_is_none() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time().times(1).returning(|| None);
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let remaining = policy.remaining_time_now(policy.deadline - Duration::from_secs(10));
        assert_eq!(remaining, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_limited_time_remaining_provider() {
        let provider = LimitedElapsedTime::provider(Duration::from_secs(1234));
        let policy = provider.make();
        assert!(policy.on_error(true, unavailable()).is_continue());
        let fmt = format!("{provider:?}");
        assert!(fmt.contains("1234s"), "{provider:?}");
        assert!(fmt.contains("Aip194Strict"), "{provider:?}");

        let provider = LimitedElapsedTime::custom_provider(AlwaysRetry, Duration::from_secs(1234));
        let policy = provider.make();
        assert!(policy.on_error(true, unavailable()).is_continue());
        let fmt = format!("{provider:?}");
        assert!(fmt.contains("1234s"), "{provider:?}");
        assert!(fmt.contains("AlwaysRetry"), "{provider:?}");
    }

    #[test]
    fn test_limited_attempt_count_on_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, e| RetryFlow::Continue(e));

        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(policy
            .on_error(true, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(true, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(true, Error::other("err".to_string()))
            .is_exhausted());
    }

    #[test]
    fn test_limited_attempt_count_remaining_none() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time().times(1).returning(|| None);
        let policy = LimitedAttemptCount::custom(mock, 3);

        assert!(policy.remaining_time().is_none());
    }

    #[test]
    fn test_limited_attempt_count_remaining_some() {
        let mut mock = MockPolicy::new();
        mock.expect_remaining_time()
            .times(1)
            .returning(|| Some(Duration::from_secs(123)));
        let policy = LimitedAttemptCount::custom(mock, 3);

        assert_eq!(policy.remaining_time(), Some(Duration::from_secs(123)));
    }

    #[test]
    fn test_limited_attempt_count_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, e| RetryFlow::Permanent(e));
        let policy = LimitedAttemptCount::custom(mock, 2);

        let rf = policy.on_error(false, Error::other("err".to_string()));
        assert!(rf.is_permanent());

        let rf = policy.on_error(false, Error::other("err".to_string()));
        assert!(rf.is_permanent());
    }

    #[test]
    fn test_limited_attempt_count_inner_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, e| RetryFlow::Exhausted(e));
        let policy = LimitedAttemptCount::custom(mock, 2);

        let rf = policy.on_error(false, Error::other("err".to_string()));
        assert!(rf.is_exhausted());

        let rf = policy.on_error(false, Error::other("err".to_string()));
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_attempt_count_provider() {
        let provider = LimitedAttemptCount::provider(2345);
        let policy = provider.make();
        assert!(policy.on_error(true, unavailable()).is_continue());
        let fmt = format!("{provider:?}");
        assert!(fmt.contains("2345"), "{provider:?}");
        assert!(fmt.contains("Aip194Strict"), "{provider:?}");

        let provider = LimitedAttemptCount::custom_provider(AlwaysRetry, 2345);
        let policy = provider.make();
        assert!(policy.on_error(true, unavailable()).is_continue());
        let fmt = format!("{provider:?}");
        assert!(fmt.contains("2345"), "{provider:?}");
        assert!(fmt.contains("AlwaysRetry"), "{provider:?}");
    }
}
