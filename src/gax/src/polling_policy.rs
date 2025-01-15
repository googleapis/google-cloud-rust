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

//! Defines the trait for polling policies and some common implementations.
//!
//! The client libraries automatically poll long-running operations (LROs) and
//! need to (1) distinguish between transient and permanent errors, and (2)
//! provide a mechanism to limit the polling loop duration.
//!
//! We provide a trait that applications may implement to customize the behavior
//! of the polling loop, and some common implementations that should meet most
//! needs.
//!
//! # Example:
//! ```
//! # use gcp_sdk_gax::polling_policy::*;
//! # use gcp_sdk_gax::options;
//! use std::time::Duration;
//! fn customize_polling_policy(config: options::ClientConfig) -> options::ClientConfig {
//!     // Poll for at most 15 minutes or at most 50 attempts: whichever limit
//!     // is reached first stops the polling loop.
//!     config.set_polling_policy(
//!         Aip194Strict
//!             .with_time_limit(Duration::from_secs(15 * 60))
//!             .with_attempt_limit(50))
//! }
//! ```

use crate::error::Error;
use crate::loop_state::LoopState;
use std::sync::Arc;

/// Determines how errors are handled in the polling loop.
///
/// Implementations of this trait determine if polling errors may resolve in
/// future attempts, and for how long the polling loop may continue.
pub trait PollingPolicy: Send + Sync + std::fmt::Debug {
    /// Query the polling policy after an error.
    ///
    /// # Parameters
    /// * `loop_start` - when the polling loop started.
    /// * `attempt_count` - the number of attempts. This includes the initial
    ///   attempt. This method called after LRO successfully starts, it is
    ///   always non-zero.
    /// * `error` - the last error when attempting the request.
    fn on_error(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        error: Error,
    ) -> LoopState;
}

/// A helper type to use [PollingPolicy] in client and request options.
#[derive(Clone)]
pub struct PollingPolicyArg(pub(crate) Arc<dyn PollingPolicy>);

impl<T> std::convert::From<T> for PollingPolicyArg
where
    T: PollingPolicy + 'static,
{
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl std::convert::From<Arc<dyn PollingPolicy>> for PollingPolicyArg {
    fn from(value: Arc<dyn PollingPolicy>) -> Self {
        Self(value)
    }
}

/// Extension trait for [PollingPolicy]
pub trait PollingPolicyExt: PollingPolicy + Sized {
    /// Decorate a [PollingPolicy] to limit the total elapsed time in the
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
    /// # use gcp_sdk_gax::*;
    /// use polling_policy::*;
    /// use std::time::{Duration, Instant};
    /// let policy = Aip194Strict.with_time_limit(Duration::from_secs(10)).with_attempt_limit(3);
    /// let attempt_count = 4;
    /// assert!(policy.on_error(Instant::now(), attempt_count, error::Error::authentication("transient")).is_exhausted());
    /// ```
    fn with_time_limit(self, maximum_duration: std::time::Duration) -> LimitedElapsedTime<Self> {
        LimitedElapsedTime::custom(self, maximum_duration)
    }

    /// Decorate a [PollingPolicy] to limit the number of poll attempts.
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
    /// # use gcp_sdk_gax::*;
    /// use polling_policy::*;
    /// use std::time::Instant;
    /// let policy = Aip194Strict.with_attempt_limit(3);
    /// assert!(policy.on_error(Instant::now(), 0, error::Error::authentication(format!("transient"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 1, error::Error::authentication(format!("transient"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, error::Error::authentication(format!("transient"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 3, error::Error::authentication(format!("transient"))).is_exhausted());
    /// ```
    fn with_attempt_limit(self, maximum_attempts: u32) -> LimitedAttemptCount<Self> {
        LimitedAttemptCount::custom(self, maximum_attempts)
    }
}

impl<T: PollingPolicy> PollingPolicyExt for T {}

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
/// # use gcp_sdk_gax::*;
/// # use gcp_sdk_gax::polling_policy::*;
/// use std::time::Instant;
/// let policy = Aip194Strict.with_attempt_limit(3);
/// let attempt_count = 4;
/// assert!(policy.on_error(Instant::now(), attempt_count, error::Error::authentication("transient")).is_exhausted());
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct Aip194Strict;

impl PollingPolicy for Aip194Strict {
    fn on_error(
        &self,
        _loop_start: std::time::Instant,
        _attempt_count: u32,
        error: Error,
    ) -> LoopState {
        if let Some(svc) = error.as_inner::<crate::error::ServiceError>() {
            return if svc.status().status.as_deref() == Some("UNAVAILABLE") {
                LoopState::Continue(error)
            } else {
                LoopState::Permanent(error)
            };
        }

        if let Some(http) = error.as_inner::<crate::error::HttpError>() {
            return if http.status_code() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                LoopState::Continue(error)
            } else {
                LoopState::Permanent(error)
            };
        }
        use crate::error::ErrorKind;
        match error.kind() {
            ErrorKind::Rpc | ErrorKind::Io => LoopState::Continue(error),
            ErrorKind::Authentication =>
            // This indicates the operation never left the client, so it
            // safe to poll again.
            {
                LoopState::Continue(error)
            }
            ErrorKind::Serde => LoopState::Permanent(error),
            ErrorKind::Other => LoopState::Permanent(error),
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
/// # use gcp_sdk_gax::*;
/// # use gcp_sdk_gax::polling_policy::*;
/// use std::time::Instant;
/// let policy = AlwaysContinue;
/// assert!(policy.on_error(Instant::now(), 1, error::Error::other("err")).is_continue());
/// ```
///
/// [AIP-194]: https://google.aip.dev/194
#[derive(Clone, Debug)]
pub struct AlwaysContinue;

impl PollingPolicy for AlwaysContinue {
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
    P: PollingPolicy,
{
    inner: P,
    maximum_duration: std::time::Duration,
}

impl LimitedElapsedTime {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::polling_policy::*;
    /// use std::time::{Duration, Instant};
    /// let policy = LimitedElapsedTime::new(Duration::from_secs(10));
    /// let start = Instant::now() - Duration::from_secs(20);
    /// assert!(policy.on_error(start, 1, error::Error::authentication("transient")).is_exhausted());
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
    P: PollingPolicy,
{
    /// Creates a new instance with a custom inner policy.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::polling_policy::*;
    /// use std::time::{Duration, Instant};
    /// let policy = LimitedElapsedTime::custom(AlwaysContinue, Duration::from_secs(10));
    /// let start = Instant::now() - Duration::from_secs(20);
    /// assert!(policy.on_error(start, 1, error::Error::other("err")).is_exhausted());
    /// ```
    pub fn custom(inner: P, maximum_duration: std::time::Duration) -> Self {
        Self {
            inner,
            maximum_duration,
        }
    }
}

impl<P> PollingPolicy for LimitedElapsedTime<P>
where
    P: PollingPolicy + 'static,
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
    P: PollingPolicy,
{
    inner: P,
    maximum_attempts: u32,
}

impl LimitedAttemptCount {
    /// Creates a new instance, with the default inner policy.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::polling_policy::*;
    /// use std::time::Instant;
    /// let policy = LimitedAttemptCount::new(5);
    /// let attempt_count = 10;
    /// assert!(policy.on_error(Instant::now(), attempt_count, error::Error::authentication("transient")).is_exhausted());
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
    P: PollingPolicy,
{
    /// Creates a new instance with a custom inner policy.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::polling_policy::*;
    /// # use gcp_sdk_gax::*;
    /// use std::time::Instant;
    /// let policy = LimitedAttemptCount::custom(AlwaysContinue, 2);
    /// assert!(policy.on_error(Instant::now(), 1, error::Error::other(format!("test"))).is_continue());
    /// assert!(policy.on_error(Instant::now(), 2, error::Error::other(format!("test"))).is_exhausted());
    /// ```
    pub fn custom(inner: P, maximum_attempts: u32) -> Self {
        Self {
            inner,
            maximum_attempts,
        }
    }
}

impl<P> PollingPolicy for LimitedAttemptCount<P>
where
    P: PollingPolicy,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{rpc::Status, ServiceError};
    use std::time::{Duration, Instant};

    // Verify `PollingPolicyArg` can be converted from the desired types.
    #[test]
    fn polling_policy_arg() {
        let policy = LimitedAttemptCount::new(3);
        let _ = PollingPolicyArg::from(policy);

        let policy: Arc<dyn PollingPolicy> = Arc::new(LimitedAttemptCount::new(3));
        let _ = PollingPolicyArg::from(policy);
    }

    #[test]
    fn aip194_strict() {
        let p = Aip194Strict;

        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, unavailable()).is_continue());
        assert!(p.on_error(now, 0, permission_denied()).is_permanent());
        assert!(p.on_error(now, 0, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, http_permission_denied()).is_permanent());

        assert!(p
            .on_error(now, 0, Error::io("err".to_string()))
            .is_continue());

        assert!(p
            .on_error(now, 0, Error::authentication("err".to_string()))
            .is_continue());

        assert!(p
            .on_error(now, 0, Error::serde("err".to_string()))
            .is_permanent());
        assert!(p
            .on_error(now, 0, Error::other("err".to_string()))
            .is_permanent());
    }

    #[test]
    fn always_continue() {
        let p = AlwaysContinue;

        let now = std::time::Instant::now();
        assert!(p.on_error(now, 0, http_unavailable()).is_continue());
        assert!(p.on_error(now, 0, unavailable()).is_continue());
    }

    #[test_case::test_case(Error::io("err"))]
    #[test_case::test_case(Error::authentication("err"))]
    #[test_case::test_case(Error::serde("err"))]
    #[test_case::test_case(Error::other("err"))]
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

    fn from_status(status: Status) -> Error {
        Error::rpc(crate::error::ServiceError::from(status))
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

    #[test]
    fn test_limited_elapsed_time() {
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

    mockall::mock! {
        #[derive(Debug)]
        Policy {}
        impl PollingPolicy for Policy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, error: Error) -> LoopState;
        }
    }

    #[test]
    fn test_limited_time_forwards() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, e| LoopState::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(now, 0, Error::other("err".to_string()));
        assert!(rf.is_continue());
    }

    #[test]
    fn test_limited_time_inner_continues() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, e| LoopState::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedElapsedTime::custom(mock, Duration::from_secs(60));
        let rf = policy.on_error(
            now - Duration::from_secs(10),
            1,
            Error::other("err".to_string()),
        );
        assert!(rf.is_continue());

        let rf = policy.on_error(
            now - Duration::from_secs(70),
            1,
            Error::other("err".to_string()),
        );
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

        let rf = policy.on_error(
            now - Duration::from_secs(10),
            1,
            Error::other("err".to_string()),
        );
        assert!(rf.is_permanent());

        let rf = policy.on_error(
            now + Duration::from_secs(10),
            1,
            Error::other("err".to_string()),
        );
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

        let rf = policy.on_error(
            now - Duration::from_secs(10),
            1,
            Error::other("err".to_string()),
        );
        assert!(rf.is_exhausted());

        let rf = policy.on_error(
            now + Duration::from_secs(10),
            1,
            Error::other("err".to_string()),
        );
        assert!(rf.is_exhausted());
    }

    #[test]
    fn test_limited_attempt_count_on_error() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(1..)
            .returning(|_, _, e| LoopState::Continue(e));

        let now = std::time::Instant::now();
        let policy = LimitedAttemptCount::custom(mock, 3);
        assert!(policy
            .on_error(now, 1, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(now, 2, Error::other("err".to_string()))
            .is_continue());
        assert!(policy
            .on_error(now, 3, Error::other("err".to_string()))
            .is_exhausted());
    }

    #[test]
    fn test_limited_attempt_count_inner_permanent() {
        let mut mock = MockPolicy::new();
        mock.expect_on_error()
            .times(2)
            .returning(|_, _, e| LoopState::Permanent(e));
        let policy = LimitedAttemptCount::custom(mock, 2);
        let now = std::time::Instant::now();

        let rf = policy.on_error(now, 1, Error::serde("err".to_string()));
        assert!(rf.is_permanent());

        let rf = policy.on_error(now, 1, Error::serde("err".to_string()));
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

        let rf = policy.on_error(now, 1, Error::other("err".to_string()));
        assert!(rf.is_exhausted());

        let rf = policy.on_error(now, 1, Error::other("err".to_string()));
        assert!(rf.is_exhausted());
    }
}
