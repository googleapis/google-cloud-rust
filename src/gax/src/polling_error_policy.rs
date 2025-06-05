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

//! Defines the types for polling error policies.
//!
//! # Example
//! ```
//! # use google_cloud_gax::polling_error_policy::*;
//! # use google_cloud_gax::options;
//! use std::time::Duration;
//! // Poll for at most 15 minutes or at most 50 attempts: whichever limit is
//! // reached first stops the polling loop.
//! let policy = Aip194Strict
//!     .with_time_limit(Duration::from_secs(15 * 60))
//!     .with_attempt_limit(50);
//! ```
//!
//! The client libraries automatically poll long-running operations (LROs) and
//! need to (1) distinguish between transient and permanent errors, and (2)
//! provide a mechanism to limit the polling loop duration.
//!
//! We provide a trait that applications may implement to customize the behavior
//! of the polling loop, and some common implementations that should meet most
//! needs.
//!
//! To configure the default polling error policy for a client, use
//! [ClientBuilder::with_polling_error_policy]. To configure the polling error
//! policy used for a specific request, use
//! [RequestOptionsBuilder::with_polling_error_policy].
//!
//! [ClientBuilder::with_polling_error_policy]: crate::client_builder::ClientBuilder::with_polling_error_policy
//! [RequestOptionsBuilder::with_polling_error_policy]: crate::options::RequestOptionsBuilder::with_polling_error_policy

use crate::error::Error;
use crate::loop_state::LoopState;
use std::sync::Arc;

/// Determines how errors are handled in the polling loop.
///
/// Implementations of this trait determine if polling errors may resolve in
/// future attempts, and for how long the polling loop may continue.
pub trait PollingErrorPolicy: Send + Sync + std::fmt::Debug {
    /// Query the polling policy after an error.
    ///
    /// # Parameters
    /// * `loop_start` - when the polling loop started.
    /// * `attempt_count` - the number of attempts. This includes the initial
    ///   attempt. This method called after LRO successfully starts, it is
    ///   always non-zero.
    /// * `error` - the last error when attempting the request.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn on_error(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        error: Error,
    ) -> LoopState;

    /// Called when the LRO is successfully polled, but the LRO is still in
    /// progress.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn on_in_progress(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        _operation_name: &str,
    ) -> Option<Error> {
        None
    }
}

/// A helper type to use [PollingErrorPolicy] in client and request options.
#[derive(Clone)]
pub struct PollingErrorPolicyArg(pub(crate) Arc<dyn PollingErrorPolicy>);

impl<T> std::convert::From<T> for PollingErrorPolicyArg
where
    T: PollingErrorPolicy + 'static,
{
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl std::convert::From<Arc<dyn PollingErrorPolicy>> for PollingErrorPolicyArg {
    fn from(value: Arc<dyn PollingErrorPolicy>) -> Self {
        Self(value)
    }
}

/// Extension trait for [PollingErrorPolicy]
pub trait PollingErrorPolicyExt: PollingErrorPolicy + Sized {
    /// Decorate a [PollingErrorPolicy] to limit the total elapsed time in the
    /// polling loop.
    ///
    /// While the time spent in the polling loop (including time in backoff) is
    /// less than the prescribed duration the `on_error()` method returns the
    /// results of the inner policy. After that time it returns
    /// [Exhausted][LoopState::Exhausted] if the inner policy returns
    /// [Continue][LoopState::Continue].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::*;
    /// use polling_error_policy::*;
    /// use std::time::{Duration, Instant};
    /// let policy = Aip194Strict.with_time_limit(Duration::from_secs(10)).with_attempt_limit(3);
    /// let attempt_count = 4;
    /// assert!(policy.on_error(Instant::now(), attempt_count, transient_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
    /// ```
    fn with_time_limit(self, maximum_duration: std::time::Duration) -> LimitedElapsedTime<Self> {
        LimitedElapsedTime::custom(self, maximum_duration)
    }

    /// Decorate a [PollingErrorPolicy] to limit the number of poll attempts.
    ///
    /// This policy decorates an inner policy and limits the total number of
    /// attempts. Note that `on_error()` is called only after a polling attempt.
    /// Therefore, setting the maximum number of attempts to 0 or 1 results in
    /// no polling after the LRO starts.
    ///
    /// The policy passes through the results from the inner policy as long as
    /// `attempt_count < maximum_attempts`. Once the maximum number of attempts
    /// is reached, the policy returns [Exhausted][LoopState::Exhausted] if the
    /// inner policy returns [Continue][LoopState::Continue], and passes the
    /// inner policy result otherwise.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::*;
    /// use polling_error_policy::*;
    /// use std::time::Instant;
    /// let policy = Aip194Strict.with_attempt_limit(3);
    /// assert!(policy.on_error(Instant::now(), 0, transient_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 1, transient_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, transient_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 3, transient_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
    /// ```
    fn with_attempt_limit(self, maximum_attempts: u32) -> LimitedAttemptCount<Self> {
        LimitedAttemptCount::custom(self, maximum_attempts)
    }
}

impl<T: PollingErrorPolicy> PollingErrorPolicyExt for T {}

/// A polling policy that strictly follows [AIP-194].
///
/// This policy must be decorated to limit the number of polling attempts or the
/// duration of the polling loop.
///
/// The policy interprets AIP-194 **strictly**. It examines the status code to
/// determine if the polling loop may continue.
///
/// # Example
/// ```
/// # use google_cloud_gax::*;
/// # use google_cloud_gax::polling_error_policy::*;
/// use std::time::Instant;
/// let policy = Aip194Strict.with_attempt_limit(3);
/// let attempt_count = 4;
/// assert!(policy.on_error(Instant::now(), attempt_count, transient_error()).is_exhausted());
///
/// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
/// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct Aip194Strict;

impl PollingErrorPolicy for Aip194Strict {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        error: Error,
    ) -> LoopState {
        if error.is_transient_and_before_rpc() {
            return LoopState::Continue(error);
        }
        if error.is_io() {
            return LoopState::Continue(error);
        }
        if let Some(status) = error.status() {
            return if status.code == crate::error::rpc::Code::Unavailable {
                LoopState::Continue(error)
            } else {
                LoopState::Permanent(error)
            };
        }

        match error.http_status_code() {
            Some(code) if code == http::StatusCode::SERVICE_UNAVAILABLE.as_u16() => {
                LoopState::Continue(error)
            }
            _ => LoopState::Permanent(error),
        }
    }
}

/// A polling policy that continues on any error.
///
/// This policy must be decorated to limit the number of polling attempts or the
/// duration of the polling loop.
///
/// The policy continues regardless of the error type or contents.
///
/// # Example
/// ```
/// # use google_cloud_gax::*;
/// # use google_cloud_gax::polling_error_policy::*;
/// use std::time::Instant;
/// let policy = AlwaysContinue;
/// assert!(policy.on_error(Instant::now(), 1, permanent_error()).is_continue());
///
/// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
/// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::Aborted)) }
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct AlwaysContinue;

impl PollingErrorPolicy for AlwaysContinue {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        error: Error,
    ) -> LoopState {
        LoopState::Continue(error)
    }
}

/// A polling policy decorator that limits the total time in the polling loop.
///
/// This policy decorates an inner policy and limits the duration of polling
/// loops. While the time spent in the polling loop (including time in backoff)
/// is less than the prescribed duration the `on_error()` method returns the
/// results of the inner policy. After that time it returns
/// [Exhausted][LoopState::Exhausted] if the inner policy returns
/// [Continue][LoopState::Continue].
///
/// The `remaining_time()` function returns the remaining time. This is always
/// [Duration::ZERO][std::time::Duration::ZERO] once or after the policy's
/// deadline is reached.
///
/// # Parameters
/// * `P` - the inner polling policy, defaults to [Aip194Strict].
#[derive(Debug)]
pub struct LimitedElapsedTime<P = Aip194Strict>
where
    P: PollingErrorPolicy,
{
    inner: P,
    maximum_duration: std::time::Duration,
}

impl LimitedElapsedTime {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::*;
    /// # use google_cloud_gax::polling_error_policy::*;
    /// use std::time::{Duration, Instant};
    /// let policy = LimitedElapsedTime::new(Duration::from_secs(10));
    /// let start = Instant::now() - Duration::from_secs(20);
    /// assert!(policy.on_error(start, 1, transient_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
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
    P: PollingErrorPolicy,
{
    /// Creates a new instance with a custom inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::*;
    /// # use google_cloud_gax::polling_error_policy::*;
    /// use std::time::{Duration, Instant};
    /// let policy = LimitedElapsedTime::custom(AlwaysContinue, Duration::from_secs(10));
    /// let start = Instant::now() - Duration::from_secs(20);
    /// assert!(policy.on_error(start, 1, permanent_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::Aborted)) }
    /// ```
    pub fn custom(inner: P, maximum_duration: std::time::Duration) -> Self {
        Self {
            inner,
            maximum_duration,
        }
    }

    fn in_progress_impl(&self, start: std::time::Instant, operation_name: &str) -> Option<Error> {
        let now = std::time::Instant::now();
        if now < start + self.maximum_duration {
            return None;
        }
        Some(Error::other(Exhausted::new(
            operation_name,
            "elapsed time",
            format!("{:?}", now.checked_duration_since(start).unwrap()),
            format!("{:?}", self.maximum_duration),
        )))
    }
}

impl<P> PollingErrorPolicy for LimitedElapsedTime<P>
where
    P: PollingErrorPolicy + 'static,
{
    fn on_error(&self, start: std::time::Instant, count: u32, error: Error) -> LoopState {
        match self.inner.on_error(start, count, error) {
            LoopState::Permanent(e) => LoopState::Permanent(e),
            LoopState::Exhausted(e) => LoopState::Exhausted(e),
            LoopState::Continue(e) => {
                if std::time::Instant::now() >= start + self.maximum_duration {
                    LoopState::Exhausted(e)
                } else {
                    LoopState::Continue(e)
                }
            }
        }
    }

    fn on_in_progress(
        &self,
        start: std::time::Instant,
        count: u32,
        operation_name: &str,
    ) -> Option<Error> {
        self.inner
            .on_in_progress(start, count, operation_name)
            .or_else(|| self.in_progress_impl(start, operation_name))
    }
}

/// A polling policy decorator that limits the number of attempts.
///
/// This policy decorates an inner policy and limits polling total number of
/// attempts. Setting the maximum number of attempts to 0 results in no polling
/// attempts before the initial one.
///
/// The policy passes through the results from the inner policy as long as
/// `attempt_count < maximum_attempts`. However, once the maximum number of
/// attempts is reached, the policy replaces any [Continue][LoopState::Continue]
/// result with [Exhausted][LoopState::Exhausted].
///
/// # Parameters
/// * `P` - the inner polling policy.
#[derive(Debug)]
pub struct LimitedAttemptCount<P = Aip194Strict>
where
    P: PollingErrorPolicy,
{
    inner: P,
    maximum_attempts: u32,
}

impl LimitedAttemptCount {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::*;
    /// # use google_cloud_gax::polling_error_policy::*;
    /// use std::time::Instant;
    /// let policy = LimitedAttemptCount::new(5);
    /// let attempt_count = 10;
    /// assert!(policy.on_error(Instant::now(), attempt_count, transient_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error { Error::service(Status::default().set_code(Code::Unavailable)) }
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
    P: PollingErrorPolicy,
{
    /// Creates a new instance with a custom inner policy.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::polling_error_policy::*;
    /// # use google_cloud_gax::*;
    /// use std::time::Instant;
    /// let policy = LimitedAttemptCount::custom(AlwaysContinue, 2);
    /// assert!(policy.on_error(Instant::now(), 1, permanent_error()).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, permanent_error()).is_exhausted());
    ///
    /// use google_cloud_gax::error::{Error, rpc::Code, rpc::Status};
    /// fn permanent_error() -> Error { Error::service(Status::default().set_code(Code::Aborted)) }
    /// ```
    pub fn custom(inner: P, maximum_attempts: u32) -> Self {
        Self {
            inner,
            maximum_attempts,
        }
    }

    fn in_progress_impl(&self, count: u32, operation_name: &str) -> Option<Error> {
        if count < self.maximum_attempts {
            return None;
        }
        Some(Error::other(Exhausted::new(
            operation_name,
            "attempt count",
            count.to_string(),
            self.maximum_attempts.to_string(),
        )))
    }
}

impl<P> PollingErrorPolicy for LimitedAttemptCount<P>
where
    P: PollingErrorPolicy,
{
    fn on_error(&self, start: std::time::Instant, count: u32, error: Error) -> LoopState {
        match self.inner.on_error(start, count, error) {
            LoopState::Permanent(e) => LoopState::Permanent(e),
            LoopState::Exhausted(e) => LoopState::Exhausted(e),
            LoopState::Continue(e) => {
                if count >= self.maximum_attempts {
                    LoopState::Exhausted(e)
                } else {
                    LoopState::Continue(e)
                }
            }
        }
    }

    fn on_in_progress(
        &self,
        start: std::time::Instant,
        count: u32,
        operation_name: &str,
    ) -> Option<Error> {
        self.inner
            .on_in_progress(start, count, operation_name)
            .or_else(|| self.in_progress_impl(count, operation_name))
    }
}

/// Indicates that a retry or polling loop has been exhausted.
#[derive(Debug)]
pub struct Exhausted {
    operation_name: String,
    limit_name: &'static str,
    value: String,
    limit: String,
}

impl Exhausted {
    pub fn new(
        operation_name: &str,
        limit_name: &'static str,
        value: String,
        limit: String,
    ) -> Self {
        Self {
            operation_name: operation_name.to_string(),
            limit_name,
            value,
            limit,
        }
    }
}

impl std::fmt::Display for Exhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "polling loop for {} exhausted, {} value ({}) exceeds limit ({})",
            self.operation_name, self.limit_name, self.value, self.limit
        )
    }
}

impl std::error::Error for Exhausted {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{CredentialsError, Error};
    use http::HeaderMap;
    use std::error::Error as _;
    use std::time::{Duration, Instant};

    mockall::mock! {
        #[derive(Debug)]
        Policy {}
        impl PollingErrorPolicy for Policy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, error: Error) -> LoopState;
            fn on_in_progress(&self, loop_start: std::time::Instant, attempt_count: u32, operation_name: &str) -> Option<Error>;
        }
    }

    // Verify `PollingPolicyArg` can be converted from the desired types.
    #[test]
    fn polling_policy_arg() {
        let policy = LimitedAttemptCount::new(3);
        let _ = PollingErrorPolicyArg::from(policy);

        let policy: Arc<dyn PollingErrorPolicy> = Arc::new(LimitedAttemptCount::new(3));
        let _ = PollingErrorPolicyArg::from(policy);
    }

    #[test]
    fn aip194_strict() {
        let p = Aip194Strict;

        let now = std::time::Instant::now();
        assert!(p.on_in_progress(now, 0, "unused").is_none());
        assert!(p.on_error(now, 0, unavailable()).is_continue());
        assert!(p.on_error(now, 0, permission_denied()).is_permanent());
        assert!(p.on_error(now, 0, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, http_permission_denied()).is_permanent());

        assert!(
            p.on_error(now, 0, Error::io("err".to_string()))
                .is_continue()
        );

        assert!(
            p.on_error(
                now,
                0,
                Error::authentication(CredentialsError::from_msg(true, "err"))
            )
            .is_continue()
        );

        assert!(
            p.on_error(now, 0, Error::ser("err".to_string()))
                .is_permanent()
        );
    }

    #[test]
    fn always_continue() {
        let p = AlwaysContinue;

        let now = std::time::Instant::now();
        assert!(p.on_in_progress(now, 0, "unused").is_none());
        assert!(p.on_error(now, 0, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, unavailable()).is_continue());
    }

    #[test_case::test_case(Error::io("err"))]
    #[test_case::test_case(Error::authentication(CredentialsError::from_msg(true, "err")))]
    #[test_case::test_case(Error::ser("err"))]
    fn always_continue_error_kind(error: Error) {
        let p = AlwaysContinue;
        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, error).is_continue());
    }

    #[test]
    fn with_time_limit() {
        let policy = AlwaysContinue.with_time_limit(Duration::from_secs(10));
        assert!(
            policy
                .on_error(
                    Instant::now() - Duration::from_secs(1),
                    1,
                    permission_denied()
                )
                .is_continue(),
            "{policy:?}"
        );
        assert!(
            policy
                .on_error(
                    Instant::now() - Duration::from_secs(20),
                    1,
                    permission_denied()
                )
                .is_exhausted(),
            "{policy:?}"
        );
    }

    #[test]
    fn with_attempt_limit() {
        let policy = AlwaysContinue.with_attempt_limit(3);
        assert!(
            policy
                .on_error(Instant::now(), 1, permission_denied())
                .is_continue(),
            "{policy:?}"
        );
        assert!(
            policy
                .on_error(Instant::now(), 5, permission_denied())
                .is_exhausted(),
            "{policy:?}"
        );
    }

    fn http_error(code: u16, message: &str) -> Error {
        let error = serde_json::json!({"error": {
            "code": code,
            "message": message,
        }});
        let payload = bytes::Bytes::from_owner(serde_json::to_string(&error).unwrap());
        Error::http(code, HeaderMap::new(), payload)
    }

    fn http_unavailable() -> Error {
        http_error(503, "SERVICE UNAVAILABLE")
    }

    fn http_permission_denied() -> Error {
        http_error(403, "PERMISSION DENIED")
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

    #[test]
    fn test_limited_elapsed_time_on_error() {
        let policy = LimitedElapsedTime::new(Duration::from_secs(20));
        assert!(
            policy
                .on_error(Instant::now() - Duration::from_secs(10), 1, unavailable())
                .is_continue(),
            "{policy:?}"
        );
        assert!(
            policy
                .on_error(Instant::now() - Duration::from_secs(30), 1, unavailable())
                .is_exhausted(),
            "{policy:?}"
        );
    }

    #[test]
    fn test_limited_elapsed_time_in_progress() {
        let policy = LimitedElapsedTime::new(Duration::from_secs(20));
        let err = policy.on_in_progress(Instant::now() - Duration::from_secs(10), 1, "unused");
        assert!(err.is_none(), "{err:?}");
        let err = policy
            .on_in_progress(
                Instant::now() - Duration::from_secs(30),
                1,
                "test-operation-name",
            )
            .unwrap();
        let exhausted = err.source().and_then(|e| e.downcast_ref::<Exhausted>());
        assert!(exhausted.is_some());
    }

    #[test]
    fn test_limited_time_forwards_on_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, e| LoopState::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(now, 0, transient_error());
        assert!(rf.is_continue());
    }

    #[test]
    fn test_limited_time_forwards_in_progress() {
        let mut mock = MockPolicy::new();
        mock.expect_on_in_progress()
            .times(3)
            .returning(|_, _, _| None);

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        assert!(policy.on_in_progress(now, 1, "test-op-name").is_none());
        assert!(policy.on_in_progress(now, 2, "test-op-name").is_none());
        assert!(policy.on_in_progress(now, 3, "test-op-name").is_none());
    }

    #[test]
    fn test_limited_time_in_progress_returns_inner() {
        let mut mock = MockPolicy::new();
        mock.expect_on_in_progress()
            .times(1)
            .returning(|_, _, _| Some(transient_error()));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        assert!(policy.on_in_progress(now, 1, "test-op-name").is_some());
    }

    #[test]
    fn test_limited_time_inner_continues() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, e| LoopState::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(now - Duration::from_secs(10), 1, transient_error());
        assert!(rf.is_continue());

        let rf = policy.on_error(now - Duration::from_secs(70), 1, transient_error());
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_time_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, e| LoopState::Permanent(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error(now - Duration::from_secs(10), 1, transient_error());
        assert!(rf.is_permanent());

        let rf = policy.on_error(now + Duration::from_secs(10), 1, transient_error());
        assert!(rf.is_permanent());
    }

    #[test]
    fn test_limited_time_inner_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, e| LoopState::Exhausted(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));

        let rf = policy.on_error(now - Duration::from_secs(10), 1, transient_error());
        assert!(rf.is_exhausted());

        let rf = policy.on_error(now + Duration::from_secs(10), 1, transient_error());
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_attempt_count_on_error() {
        let policy = LimitedAttemptCount::new(20);
        assert!(
            policy
                .on_error(Instant::now(), 10, unavailable())
                .is_continue(),
            "{policy:?}"
        );
        assert!(
            policy
                .on_error(Instant::now(), 30, unavailable())
                .is_exhausted(),
            "{policy:?}"
        );
    }

    #[test]
    fn test_limited_attempt_count_in_progress() {
        let policy = LimitedAttemptCount::new(20);
        let err = policy.on_in_progress(Instant::now(), 10, "unused");
        assert!(err.is_none(), "{err:?}");
        let err = policy
            .on_in_progress(Instant::now(), 30, "test-operation-name")
            .unwrap();
        let exhausted = err.source().and_then(|e| e.downcast_ref::<Exhausted>());
        assert!(exhausted.is_some());
    }

    #[test]
    fn test_limited_attempt_count_forwards_on_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, e| LoopState::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(policy.on_error(now, 1, transient_error()).is_continue());
        assert!(policy.on_error(now, 2, transient_error()).is_continue());
        assert!(policy.on_error(now, 3, transient_error()).is_exhausted());
    }

    #[test]
    fn test_limited_attempt_count_forwards_in_progress() {
        let mut mock = MockPolicy::new();
        mock.expect_on_in_progress()
            .times(3)
            .returning(|_, _, _| None);

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 5);
        assert!(policy.on_in_progress(now, 1, "test-op-name").is_none());
        assert!(policy.on_in_progress(now, 2, "test-op-name").is_none());
        assert!(policy.on_in_progress(now, 3, "test-op-name").is_none());
    }

    #[test]
    fn test_limited_attempt_count_in_progress_returns_inner() {
        let mut mock = MockPolicy::new();
        mock.expect_on_in_progress()
            .times(1)
            .returning(|_, _, _| Some(unavailable()));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 5);
        assert!(policy.on_in_progress(now, 1, "test-op-name").is_some());
    }

    #[test]
    fn test_limited_attempt_count_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, e| LoopState::Permanent(e));
        let policy = LimitedAttemptCount::custom(mock, 2);
        let now = std::time::Instant::now();
        let rf = policy.on_error(now, 1, Error::ser("err"));
        assert!(rf.is_permanent());

        let rf = policy.on_error(now, 1, Error::ser("err"));
        assert!(rf.is_permanent());
    }

    #[test]
    fn test_limited_attempt_count_inner_exhausted() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, e| LoopState::Exhausted(e));
        let policy = LimitedAttemptCount::custom(mock, 2);
        let now = std::time::Instant::now();

        let rf = policy.on_error(now, 1, transient_error());
        assert!(rf.is_exhausted());

        let rf = policy.on_error(now, 1, transient_error());
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_exhausted_fmt() {
        let exhausted = Exhausted::new(
            "op-name",
            "limit-name",
            "test-value".to_string(),
            "test-limit".to_string(),
        );
        let fmt = format!("{exhausted}");
        assert!(fmt.contains("op-name"), "{fmt}");
        assert!(fmt.contains("limit-name"), "{fmt}");
        assert!(fmt.contains("test-value"), "{fmt}");
        assert!(fmt.contains("test-limit"), "{fmt}");
    }

    fn transient_error() -> Error {
        use crate::error::rpc::{Code, Status};
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try-again"),
        )
    }
}
