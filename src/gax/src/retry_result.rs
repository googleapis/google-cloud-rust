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

//! Polling and retry loop control types.
//!
//! This module contains types to control polling loops and retry loops.
//! Applications only need to use these types when implementing their own retry
//! and polling policies.

use crate::error::Error;

/// The result of a loop control decision.
///
/// # Example
///
/// ```
/// # use google_cloud_gax::{error::Error, retry_policy::RetryPolicy};
/// # use google_cloud_gax::{retry_result::RetryResult, throttle_result::ThrottleResult};
/// #[derive(Debug)]
/// struct MyRetryPolicy;
/// impl google_cloud_gax::retry_policy::RetryPolicy for MyRetryPolicy {
///     fn on_error(
///         &self,
///         _loop_start: std::time::Instant,
///         attempt_count: u32,
///         _idempotent: bool,
///         error: Error) -> RetryResult
///     {
///         if attempt_count > 42 {
///             return RetryResult::Exhausted(error);
///         }
///         RetryResult::Continue(error)
///     }
///     fn on_throttle(
///         &self,
///        loop_start: std::time::Instant,
///        attempt_count: u32,
///        error: Error,
///     ) -> ThrottleResult {
///         # panic!();
///     }
/// }
/// ```
#[derive(Debug)]
pub enum RetryResult {
    /// The error is non-retryable, stop the loop.
    Permanent(Error),

    /// The error is retryable, but the policy is stopping the loop.
    ///
    /// Loop control policies may stop the loop on retryable errors, for
    /// example, because the policy only allows a limited number of attempts.
    Exhausted(Error),

    /// The error was retryable, continue the loop.
    Continue(Error),
}

impl RetryResult {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loop_state() {
        let flow = RetryResult::Permanent(permanent_error());
        assert!(flow.is_permanent(), "{flow:?}");
        assert!(!flow.is_exhausted(), "{flow:?}");
        assert!(!flow.is_continue(), "{flow:?}");

        let flow = RetryResult::Exhausted(transient_error());
        assert!(!flow.is_permanent(), "{flow:?}");
        assert!(flow.is_exhausted(), "{flow:?}");
        assert!(!flow.is_continue(), "{flow:?}");

        let flow = RetryResult::Continue(transient_error());
        assert!(!flow.is_permanent(), "{flow:?}");
        assert!(!flow.is_exhausted(), "{flow:?}");
        assert!(flow.is_continue(), "{flow:?}");
    }

    fn permanent_error() -> Error {
        use crate::error::rpc::*;
        Error::service(Status::default().set_code(Code::PermissionDenied))
    }

    fn transient_error() -> Error {
        use crate::error::rpc::*;
        Error::service(Status::default().set_code(Code::Unavailable))
    }
}
