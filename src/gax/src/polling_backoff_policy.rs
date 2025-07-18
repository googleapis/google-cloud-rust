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

//! Defines the trait for polling backoff policies and a common implementations.
//!
//! The client libraries can automatically poll long-running operations (LROs)
//! until completion. When doing so they may backoff between polling to avoid
//! overloading the service.
//!
//! These policies should not be confused with retry backoff policies. Their
//! purpose is different, and their implementation is too. Notably, polling
//! backoff policies should not use jitter, while retry policies should.
//!
//! The most common implementation is truncated [exponential backoff]
//! **without** jitter. The backoff period grows exponentially until some limit
//! is reached. This works well when the expected execution time is not known
//! in advance.
//!
//! To configure the default polling backoff policy for a client, use
//! [ClientBuilder::with_polling_backoff_policy]. To configure the polling
//! backoff policy used for a specific request, use
//! [RequestOptionsBuilder::with_polling_backoff_policy].
//!
//! [ClientBuilder::with_polling_backoff_policy]: crate::client_builder::ClientBuilder::with_polling_backoff_policy
//! [RequestOptionsBuilder::with_polling_backoff_policy]: crate::options::RequestOptionsBuilder::with_polling_backoff_policy
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
//! // `policy` implements the `PollingBackoffPolicy` trait.
//! # Ok::<(), Error>(())
//! ```
//!
//! [Exponential backoff]: https://en.wikipedia.org/wiki/Exponential_backoff

use std::sync::Arc;

/// Defines the trait implemented by all backoff strategies.
pub trait PollingBackoffPolicy: Send + Sync + std::fmt::Debug {
    /// Returns the backoff delay on a failure.
    ///
    /// # Parameters
    /// * `loop_start` - when the polling loop started.
    /// * `attempt_count` - the number of poll queries. This method is always
    ///   called after the first attempt.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn wait_period(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> std::time::Duration;
}

/// A helper type to use [PollingBackoffPolicy] in client and request options.
#[derive(Clone)]
pub struct PollingBackoffPolicyArg(pub(crate) Arc<dyn PollingBackoffPolicy>);

impl<T: PollingBackoffPolicy + 'static> std::convert::From<T> for PollingBackoffPolicyArg {
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl std::convert::From<Arc<dyn PollingBackoffPolicy>> for PollingBackoffPolicyArg {
    fn from(value: Arc<dyn PollingBackoffPolicy>) -> Self {
        Self(value)
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
        let _ = PollingBackoffPolicyArg::from(policy);

        let policy: Arc<dyn PollingBackoffPolicy> =
            Arc::new(ExponentialBackoffBuilder::default().clamp());
        let _ = PollingBackoffPolicyArg::from(policy);
    }
}
