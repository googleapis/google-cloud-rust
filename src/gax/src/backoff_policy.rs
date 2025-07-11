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

//! Defines traits for backoff policies and a common implementations.
//!
//! The client libraries automatically retry RPCs based on the [RetryPolicy]
//! configured for the request or client. Even when the policy determines that
//! an operation is safe to retry, the client library does not retry failed
//! requests immediately, as the service may need time to recover.
//! [Exponential backoff] is a well known algorithm to find an acceptable delay
//! between retries, but some application may need slight variations on this
//! algorithm. The client libraries use a [BackoffPolicy] to configure the
//! delays between retry attempts.
//!
//! While exponential backoff improves the system behavior when there are small
//! faults, something like a [RetryThrottler] may be needed to improve recovery
//! times in larger failures.
//!
//! To configure the default retry backoff policy for a client, use
//! [ClientBuilder::with_backoff_policy]. To configure the retry backoff policy
//! used for a specific request, use
//! [RequestOptionsBuilder::with_backoff_policy].
//!
//! [ClientBuilder::with_backoff_policy]: crate::client_builder::ClientBuilder::with_backoff_policy
//! [RequestOptionsBuilder::with_backoff_policy]: crate::options::RequestOptionsBuilder::with_backoff_policy
//! [RetryPolicy]: crate::retry_policy::RetryPolicy
//!
//! # Example
//! ```
//! # use google_cloud_gax::exponential_backoff::Error;
//! # use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
//! use std::time::Duration;
//!
//! let policy = ExponentialBackoffBuilder::new()
//!     .with_initial_delay(Duration::from_millis(100))
//!     .with_maximum_delay(Duration::from_secs(5))
//!     .with_scaling(4.0)
//!     .build()?;
//! # Ok::<(), Error>(())
//! ```
//!
//! [RetryThrottler]: crate::retry_throttler::RetryThrottler
//! [Exponential backoff]: https://en.wikipedia.org/wiki/Exponential_backoff
//! [idempotent]: https://en.wikipedia.org/wiki/Idempotence

use std::sync::Arc;

/// Defines the trait implemented by all backoff strategies.
pub trait BackoffPolicy: Send + Sync + std::fmt::Debug {
    /// Returns the backoff delay on a failure.
    ///
    /// # Parameters
    /// * `loop_start` - when the retry loop started.
    /// * `attempt_count` - the number of attempts. This method is always called
    ///   after the first attempt.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn on_failure(&self, loop_start: std::time::Instant, attempt_count: u32)
    -> std::time::Duration;
}

/// A helper type to use [BackoffPolicy] in client and request options.
#[derive(Clone)]
pub struct BackoffPolicyArg(pub(crate) Arc<dyn BackoffPolicy>);

impl<T: BackoffPolicy + 'static> From<T> for BackoffPolicyArg {
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl From<Arc<dyn BackoffPolicy>> for BackoffPolicyArg {
    fn from(value: Arc<dyn BackoffPolicy>) -> Self {
        Self(value)
    }
}

impl BackoffPolicyArg {
    pub fn into_inner(self) -> Arc<dyn BackoffPolicy> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exponential_backoff::ExponentialBackoffBuilder;

    // Verify `BackoffPolicyArg` can be converted from the desired types.
    #[test]
    fn backoff_policy_arg() {
        let policy = ExponentialBackoffBuilder::default().clamp();
        let _ = BackoffPolicyArg::from(policy);

        let policy: Arc<dyn BackoffPolicy> = Arc::new(ExponentialBackoffBuilder::default().clamp());
        let _ = BackoffPolicyArg::from(policy);
    }
}
