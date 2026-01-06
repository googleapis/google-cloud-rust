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

//! Defines the read resume policies for Google Cloud Storage.
//!
//! Even if a read request starts successfully, it may be fail after it starts.
//! For example, the read may be interrupted or become too slow and "stall". The
//! client library can automatically recover from such errors. The application
//! may want to control what errors are treated as recoverable, and how many
//! failures are tolerated before abandoning the read request.
//!
//! The traits and types defined in this module allow for such customization.
//!
//! # Example
//! ```
//! # use google_cloud_storage::read_resume_policy::*;
//! let policy = Recommended.with_attempt_limit(3);
//! assert!(matches!(policy.on_error(&ResumeQuery::new(0), io_error()), ResumeResult::Continue(_)));
//! assert!(matches!(policy.on_error(&ResumeQuery::new(1), io_error()), ResumeResult::Continue(_)));
//! assert!(matches!(policy.on_error(&ResumeQuery::new(2), io_error()), ResumeResult::Continue(_)));
//! assert!(matches!(policy.on_error(&ResumeQuery::new(3), io_error()), ResumeResult::Exhausted(_)));
//!
//! use gax::error::{Error, rpc::Code, rpc::Status};
//! fn io_error() -> Error {
//!    // ... details omitted ...
//!    # Error::io("something failed in the read request")
//! }
//! ```

use crate::Error;
use gax::error::rpc::Code;

pub use gax::retry_result::RetryResult as ResumeResult;

/// Defines the interface to resume policies.
pub trait ReadResumePolicy: Send + Sync + std::fmt::Debug {
    /// Determines if the read should continue after an error.
    fn on_error(&self, status: &ResumeQuery, error: Error) -> ResumeResult;
}

/// Extension trait for [ReadResumePolicy].
pub trait ReadResumePolicyExt: Sized {
    /// Decorates a [ReadResumePolicy] to limit the number of resume attempts.
    ///
    /// This policy decorates an inner policy and limits the total number of
    /// attempts. Note that `on_error()` is not called before the initial
    /// (non-retry) attempt. Therefore, setting the maximum number of attempts
    /// to 0 or 1 results in no retry attempts.
    ///
    /// The policy passes through the results from the inner policy as long as
    /// `attempt_count < maximum_attempts`. Once the maximum number of attempts
    /// is reached, the policy returns [Exhausted][ResumeResult::Exhausted] if the
    /// inner policy returns [Continue][ResumeResult::Continue].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::read_resume_policy::*;
    /// let policy = Recommended.with_attempt_limit(3);
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(0), transient_error()), ResumeResult::Continue(_)));
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(1), transient_error()), ResumeResult::Continue(_)));
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(2), transient_error()), ResumeResult::Continue(_)));
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(3), transient_error()), ResumeResult::Exhausted(_)));
    ///
    /// use gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error {
    ///    // ... details omitted ...
    ///    # Error::io("something failed in the read request")
    /// }
    /// ```
    fn with_attempt_limit(self, maximum_attempts: u32) -> LimitedAttemptCount<Self> {
        LimitedAttemptCount::new(self, maximum_attempts)
    }
}
impl<T: ReadResumePolicy> ReadResumePolicyExt for T {}

/// The inputs into a resume policy query.
///
/// On an error, the client library queries the resume policy as to whether it
/// should attempt a new read request or not. The client library provides an
/// instance of this type to the resume policy.
///
/// We use a struct so we can grow the amount of information without breaking
/// existing resume policies.
#[derive(Debug)]
#[non_exhaustive]
pub struct ResumeQuery {
    /// The number of times the read request has been interrupted already.
    pub attempt_count: u32,
}

impl ResumeQuery {
    /// Create a new instance.
    pub fn new(attempt_count: u32) -> Self {
        Self { attempt_count }
    }
}

/// The recommended policy when reading objects from Cloud Storage.
///
/// This policy resumes any read that fails due to I/O errors, and stops on any
/// other error kind.
///
/// # Example
/// ```
/// # use google_cloud_storage::read_resume_policy::*;
/// let policy = Recommended;
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), io_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), other_error()), ResumeResult::Permanent(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn io_error() -> Error {
///    // ... details omitted ...
///    # Error::io("something failed in the read request")
/// }
/// fn other_error() -> Error {
///    // ... details omitted ...
///    # Error::deser("something failed in the read request")
/// }
/// ```
#[derive(Debug)]
pub struct Recommended;

impl ReadResumePolicy for Recommended {
    fn on_error(&self, _status: &ResumeQuery, error: Error) -> ResumeResult {
        match error {
            e if self::is_transient(&e) => ResumeResult::Continue(e),
            e => ResumeResult::Permanent(e),
        }
    }
}

fn is_transient(error: &Error) -> bool {
    match error {
        // When using HTTP the only error after the read starts are I/O errors.
        e if e.is_io() => true,
        // When using gRPC the errors may include more information.
        e if e.is_transport() => true,
        e if e.is_timeout() => true,
        e => e.status().is_some_and(|s| is_transient_code(s.code)),
    }
}

fn is_transient_code(code: Code) -> bool {
    // DeadlineExceeded is safe in this context because local deadline errors are not reported via e.status()
    matches!(
        code,
        Code::Unavailable | Code::ResourceExhausted | Code::Internal | Code::DeadlineExceeded
    )
}

/// A resume policy that resumes regardless of the error type.
///
/// This may be useful in tests, or if used with a very low limit on the number
/// of allowed failures.
///
/// # Example
/// ```
/// # use google_cloud_storage::read_resume_policy::*;
/// let policy = AlwaysResume.with_attempt_limit(3);
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(1), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(2), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(3), scary_error()), ResumeResult::Exhausted(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn scary_error() -> Error {
///    // ... details omitted ...
///    # Error::deser("something failed in the read request")
/// }
/// ```
#[derive(Debug)]
pub struct AlwaysResume;

impl ReadResumePolicy for AlwaysResume {
    fn on_error(&self, _status: &ResumeQuery, error: Error) -> ResumeResult {
        ResumeResult::Continue(error)
    }
}

/// A resume policy that never resumes, regardless of the error type.
///
/// This is useful to disable the default resume policy.
///
/// # Example
/// ```
/// # use google_cloud_storage::read_resume_policy::*;
/// let policy = NeverResume.with_attempt_limit(3);
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), io_error()), ResumeResult::Permanent(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(1), io_error()), ResumeResult::Permanent(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(2), io_error()), ResumeResult::Permanent(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(3), io_error()), ResumeResult::Permanent(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn io_error() -> Error {
///    // ... details omitted ...
///    # Error::io("something failed in the read request")
/// }
/// ```
#[derive(Debug)]
pub struct NeverResume;
impl ReadResumePolicy for NeverResume {
    fn on_error(&self, _status: &ResumeQuery, error: Error) -> ResumeResult {
        ResumeResult::Permanent(error)
    }
}

/// Decorates a resume policy to stop resuming after a certain number of attempts.
///
/// # Example
/// ```
/// # use google_cloud_storage::read_resume_policy::*;
/// let policy = LimitedAttemptCount::new(AlwaysResume, 3);
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(1), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(2), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(3), scary_error()), ResumeResult::Exhausted(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn scary_error() -> Error {
///    // ... details omitted ...
///    # Error::deser("something failed in the read request")
/// }
/// ```
#[derive(Debug)]
pub struct LimitedAttemptCount<P> {
    inner: P,
    maximum_attempts: u32,
}

impl<P> LimitedAttemptCount<P> {
    /// Create a new instance.
    pub fn new(inner: P, maximum_attempts: u32) -> Self {
        Self {
            inner,
            maximum_attempts,
        }
    }
}

impl<P> ReadResumePolicy for LimitedAttemptCount<P>
where
    P: ReadResumePolicy,
{
    fn on_error(&self, status: &ResumeQuery, error: Error) -> ResumeResult {
        match self.inner.on_error(status, error) {
            ResumeResult::Continue(e) if status.attempt_count >= self.maximum_attempts => {
                ResumeResult::Exhausted(e)
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommended() {
        let policy = Recommended;
        let r = policy.on_error(&ResumeQuery::new(0), common_transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), common_timeout());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), http_transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), grpc_deadline_exceeded());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), grpc_internal());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), grpc_resource_exhausted());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), grpc_unavailable());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");

        let r = policy.on_error(&ResumeQuery::new(0), http_permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), grpc_permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
    }

    #[test]
    fn always_resume() {
        let policy = AlwaysResume;
        let r = policy.on_error(&ResumeQuery::new(0), http_transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), http_permanent());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
    }

    #[test]
    fn never_resume() {
        let policy = NeverResume;
        let r = policy.on_error(&ResumeQuery::new(0), http_transient());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), http_permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
    }

    #[test]
    fn attempt_limit() {
        let policy = Recommended.with_attempt_limit(3);
        let r = policy.on_error(&ResumeQuery::new(0), http_transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(1), http_transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(2), http_transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(3), http_transient());
        assert!(matches!(r, ResumeResult::Exhausted(_)), "{r:?}");

        let r = policy.on_error(&ResumeQuery::new(0), http_permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(3), http_permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
    }

    #[test]
    fn attempt_limit_inner_exhausted() {
        let policy = AlwaysResume.with_attempt_limit(3).with_attempt_limit(5);
        let r = policy.on_error(&ResumeQuery::new(3), http_transient());
        assert!(matches!(r, ResumeResult::Exhausted(_)), "{r:?}");
    }

    fn http_transient() -> Error {
        Error::io("test only")
    }

    fn http_permanent() -> Error {
        Error::deser("bad data")
    }

    fn common_transient() -> Error {
        Error::transport(http::HeaderMap::new(), "test-only")
    }

    fn common_timeout() -> Error {
        Error::timeout("simulated timeout")
    }

    fn grpc_deadline_exceeded() -> Error {
        grpc_error(Code::DeadlineExceeded)
    }

    fn grpc_internal() -> Error {
        grpc_error(Code::Internal)
    }

    fn grpc_resource_exhausted() -> Error {
        grpc_error(Code::ResourceExhausted)
    }

    fn grpc_unavailable() -> Error {
        grpc_error(Code::Unavailable)
    }

    fn grpc_permanent() -> Error {
        grpc_error(Code::PermissionDenied)
    }

    fn grpc_error(code: Code) -> Error {
        let status = gax::error::rpc::Status::default().set_code(code);
        Error::service(status)
    }
}
