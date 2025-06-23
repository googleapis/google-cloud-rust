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

//! Defines types related to throttling the retry loop.

use crate::error::Error;

/// The result of a retry throttling control decision.
///
/// The client libraries retry loop may throttle some retry attempts, that is,
/// the request previously failed, it is retryable, but the policies in the
/// loop have reached some limit or budget on the number of retry attempts. In
/// such cases the retry attempt is not started, and the retry policy is
/// consulted to determine if the retry loop should continue.
///
/// The retry policy receives the previous error, and should return whether the
/// loop continues after the throttled attempt.
///
/// # Example
///
/// ```
/// # use google_cloud_gax::{error::Error, retry_policy::RetryPolicy};
/// # use google_cloud_gax::{loop_state::LoopState, throttle_result::ThrottleResult};
/// #[derive(Debug)]
/// struct MyRetryPolicy;
/// impl google_cloud_gax::retry_policy::RetryPolicy for MyRetryPolicy {
///     fn on_throttle(
///         &self,
///        _loop_start: std::time::Instant,
///        _attempt_count: u32,
///        error: Error,
///     ) -> ThrottleResult {
///         // Always stop the loop
///         ThrottleResult::Exhausted(error)
///     }
///     fn on_error(
///         &self,
///         loop_start: std::time::Instant,
///         attempt_count: u32,
///         idempotent: bool,
///         error: Error) -> LoopState {
///        # panic!();
///     }
/// }
/// ```
#[derive(Debug)]
pub enum ThrottleResult {
    /// The error is retryable, but the policy is stopping the loop.
    ///
    /// Loop control policies may stop the loop on retryable errors, for
    /// example, because the policy only allows a limited number of attempts.
    Exhausted(Error),

    /// The error was retryable, continue the loop.
    Continue(Error),
}
