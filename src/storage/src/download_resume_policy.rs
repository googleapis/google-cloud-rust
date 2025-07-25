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

//! Defines the download resume policies for Google Cloud Storage.
//!
//! Even if a download request starts successfully, it may be fail after it
//! starts. For example, the download may be interrupted or become too slow and
//! "stall". The client library can automatically recover from such errors. The
//! application may want to control what errors are treated as recoverable, and
//! how many failures are tolerated before abandoning the download.
//!
//! The traits and types defined in this module allow for such customization.
//!
//! # Example
//! ```
//! # use google_cloud_storage::download_resume_policy::*;
//! let policy = Recommended.with_attempt_limit(3);
//! assert!(matches!(policy.on_error(&ResumeQuery::new(0), io_error()), ResumeResult::Continue(_)));
//! assert!(matches!(policy.on_error(&ResumeQuery::new(1), io_error()), ResumeResult::Continue(_)));
//! assert!(matches!(policy.on_error(&ResumeQuery::new(2), io_error()), ResumeResult::Continue(_)));
//! assert!(matches!(policy.on_error(&ResumeQuery::new(3), io_error()), ResumeResult::Exhausted(_)));
//!
//! use gax::error::{Error, rpc::Code, rpc::Status};
//! fn io_error() -> Error {
//!    // ... details omitted ...
//!    # Error::io("something failed in the download")
//! }
//! ```

use crate::Error;

pub use gax::retry_result::RetryResult as ResumeResult;

/// Defines the interface to resume policies.
pub trait DownloadResumePolicy: Send + Sync + std::fmt::Debug {
    fn on_error(&self, status: &ResumeQuery, error: Error) -> ResumeResult;
}

/// Extension trait for [DownloadResumePolicy].
pub trait DownloadResumePolicyExt: Sized {
    /// Decorates a [DownloadResumePolicy] to limit the number of resume attempts.
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
    /// # use google_cloud_storage::download_resume_policy::*;
    /// let policy = Recommended.with_attempt_limit(3);
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(0), transient_error()), ResumeResult::Continue(_)));
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(1), transient_error()), ResumeResult::Continue(_)));
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(2), transient_error()), ResumeResult::Continue(_)));
    /// assert!(matches!(policy.on_error(&ResumeQuery::new(3), transient_error()), ResumeResult::Exhausted(_)));
    ///
    /// use gax::error::{Error, rpc::Code, rpc::Status};
    /// fn transient_error() -> Error {
    ///    // ... details omitted ...
    ///    # Error::io("something failed in the download")
    /// }
    /// ```
    fn with_attempt_limit(self, maximum_attempts: u32) -> LimitedAttemptCount<Self> {
        LimitedAttemptCount::new(self, maximum_attempts)
    }
}
impl<T: DownloadResumePolicy> DownloadResumePolicyExt for T {}

/// The inputs into a resume policy query.
///
/// On an error, the client library queries the resume policy as to whether it
/// should attempt a new download or not. The client library provides
/// an instance of this type to the resume policy.
///
/// We use a struct so we can grow the amount of information without breaking
/// existing resume policies.
#[non_exhaustive]
pub struct ResumeQuery {
    /// The number of times the download has been interrupted already.
    attempt_count: u32,
}

impl ResumeQuery {
    /// Create a new instance.
    pub fn new(attempt_count: u32) -> Self {
        Self { attempt_count }
    }
}

/// The recommended policy for storage downloads.
///
/// This policy resumes any download that fails due to I/O errors, but
///
/// # Example
/// ```
/// # use google_cloud_storage::download_resume_policy::*;
/// let policy = Recommended;
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), io_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), other_error()), ResumeResult::Permanent(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn io_error() -> Error {
///    // ... details omitted ...
///    # Error::io("something failed in the download")
/// }
/// fn other_error() -> Error {
///    // ... details omitted ...
///    # Error::deser("something failed in the download")
/// }
/// ```
#[derive(Debug)]
pub struct Recommended;

impl DownloadResumePolicy for Recommended {
    fn on_error(&self, _status: &ResumeQuery, error: Error) -> ResumeResult {
        if error.is_io() {
            ResumeResult::Continue(error)
        } else {
            ResumeResult::Permanent(error)
        }
    }
}

/// A resume policy that resumes regardless of the error type.
///
/// This may be useful in tests, or if used with a very low limit on the number
/// of allowed failures.
///
/// # Example
/// ```
/// # use google_cloud_storage::download_resume_policy::*;
/// let policy = AlwaysResume.with_attempt_limit(3);
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(1), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(2), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(3), scary_error()), ResumeResult::Exhausted(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn scary_error() -> Error {
///    // ... details omitted ...
///    # Error::deser("something failed in the download")
/// }
/// ```
#[derive(Debug)]
pub struct AlwaysResume;

impl DownloadResumePolicy for AlwaysResume {
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
/// # use google_cloud_storage::download_resume_policy::*;
/// let policy = NeverResume.with_attempt_limit(3);
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), io_error()), ResumeResult::Permanent(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(1), io_error()), ResumeResult::Permanent(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(2), io_error()), ResumeResult::Permanent(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(3), io_error()), ResumeResult::Permanent(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn io_error() -> Error {
///    // ... details omitted ...
///    # Error::io("something failed in the download")
/// }
/// ```
#[derive(Debug)]
pub struct NeverResume;
impl DownloadResumePolicy for NeverResume {
    fn on_error(&self, _status: &ResumeQuery, error: Error) -> ResumeResult {
        ResumeResult::Permanent(error)
    }
}

/// Decorates a resume policy to stop resuming after a certain number of attempts.
///
/// # Example
/// ```
/// # use google_cloud_storage::download_resume_policy::*;
/// let policy = LimitedAttemptCount::new(AlwaysResume, 3);
/// assert!(matches!(policy.on_error(&ResumeQuery::new(0), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(1), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(2), scary_error()), ResumeResult::Continue(_)));
/// assert!(matches!(policy.on_error(&ResumeQuery::new(3), scary_error()), ResumeResult::Exhausted(_)));
///
/// use gax::error::{Error, rpc::Code, rpc::Status};
/// fn scary_error() -> Error {
///    // ... details omitted ...
///    # Error::deser("something failed in the download")
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

impl<P> DownloadResumePolicy for LimitedAttemptCount<P>
where
    P: DownloadResumePolicy,
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
        let r = policy.on_error(&ResumeQuery::new(0), transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
    }

    #[test]
    fn always_resume() {
        let policy = AlwaysResume;
        let r = policy.on_error(&ResumeQuery::new(0), transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), permanent());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
    }

    #[test]
    fn never_resume() {
        let policy = NeverResume;
        let r = policy.on_error(&ResumeQuery::new(0), transient());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(0), permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
    }

    #[test]
    fn attempt_limit() {
        let policy = Recommended.with_attempt_limit(3);
        let r = policy.on_error(&ResumeQuery::new(0), transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(1), transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(2), transient());
        assert!(matches!(r, ResumeResult::Continue(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(3), transient());
        assert!(matches!(r, ResumeResult::Exhausted(_)), "{r:?}");

        let r = policy.on_error(&ResumeQuery::new(0), permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
        let r = policy.on_error(&ResumeQuery::new(3), permanent());
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
    }

    #[test]
    fn attempt_limit_inner_exhausted() {
        let policy = AlwaysResume.with_attempt_limit(3).with_attempt_limit(5);
        let r = policy.on_error(&ResumeQuery::new(3), transient());
        assert!(matches!(r, ResumeResult::Exhausted(_)), "{r:?}");
    }

    fn transient() -> Error {
        Error::io("test only")
    }

    fn permanent() -> Error {
        Error::deser("bad data")
    }
}
