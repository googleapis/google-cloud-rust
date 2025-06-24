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

//! Defines traits for retry throttling and some common implementations.
//!
//! The client libraries can be configured to automatically retry RPCs. Most
//! often retries are only enabled if (1) the request was never sent and the
//! local error is recoverable, or (2) the failure is a transient errors
//! **and** the RPC is [idempotent].
//!
//! Retry strategies that do not throttle themselves can slow down recovery when
//! the service is overloaded, or when recovering from a large incident. This is
//! also known as "retry storms": the retry attempts can grow to be more than
//! the normal traffic and hinder recovery.
//!
//! Advanced applications may want to configure a retry throttler when
//! [Addressing Cascading Failures] and when [Handling Overload] conditions.
//! This module contains the traits and some implementations of retry throttling
//! strategies.
//!
//! To configure the default throttler for a client, use
//! [ClientBuilder::with_retry_throttler]. To configure the throttler used for
//! a specific request, use [RequestOptionsBuilder::with_retry_throttler].
//!
//! Typically applications should create one retry throttler and share it
//! across multiple clients.
//!
//! [ClientBuilder::with_retry_throttler]: crate::client_builder::ClientBuilder::with_retry_throttler
//! [RequestOptionsBuilder::with_retry_throttler]: crate::options::RequestOptionsBuilder::with_retry_throttler
//! [Handling Overload]: https://sre.google/sre-book/handling-overload/
//! [Addressing Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
//!
//! # Example
//!
//! Stochastically reject calls based on observed failure rates:
//! ```
//! # use google_cloud_gax::retry_throttler::*;
//! let throttler = AdaptiveThrottler::new(2.0)?;
//! # Ok::<(), Error>(())
//! ```
//!
//! Reject calls if the success rate is too low:
//! ```
//! # use google_cloud_gax::retry_throttler::*;
//! let tokens = 1000;
//! let min_tokens = 250;
//! let error_cost = 10;
//! let throttler = CircuitBreaker::new(tokens, min_tokens, error_cost)?;
//! # Ok::<(), Error>(())
//! ```
//!
//! [idempotent]: https://en.wikipedia.org/wiki/Idempotence

use crate::retry_result::RetryResult;
use std::sync::{Arc, Mutex};

/// The error type for throttler policy creation.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("the scaling factor ({0}) must be greater or equal than 0.0")]
    ScalingOutOfRange(f64),
    #[error(
        "the minimum tokens ({min}) must be less than or equal to the initial token ({initial}) count"
    )]
    TooFewMinTokens { min: u64, initial: u64 },
}

/// Implementations of this trait prevent a client from sending too many retries.
///
/// Retry throttlers are shared by all the requests in a client, and may even be
/// shared by multiple clients. The library provides a default implementation
/// (and instance) on each client. The application may choose a different
/// implementation or instance if the default is not suitable.
///
/// Implementations of this trait must also implement [Debug][std::fmt::Debug]
/// because the application may need to log the client state. The trait is
/// passed between async functions, so its implementations must be `Send`
/// and `Sync`.
pub trait RetryThrottler: Send + Sync + std::fmt::Debug {
    /// Called by the retry loop before issuing a retry attempt. Returns `true`
    /// if the request should be throttled.
    ///
    /// The retry loop would simulate a failure when this function returns
    /// `false`. Note that the retry loop may stop if too many attempts are
    /// throttled: they are treated as transient errors and may exhaust the
    /// retry policy.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn throttle_retry_attempt(&self) -> bool;

    /// Called by the retry loop after a retry failure.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn on_retry_failure(&mut self, flow: &RetryResult);

    /// Called by the retry loop when a RPC succeeds.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn on_success(&mut self);
}

/// Retry throttlers are shared by many clients, so they are wrapped in `Arc<>`.
/// Consequently, they are used from many threads at the same time, so they are
/// wrapped in `Mutex`.
pub type SharedRetryThrottler = Arc<Mutex<dyn RetryThrottler>>;

/// A helper type to use [RetryThrottler] in client and request options.
#[derive(Clone)]
pub struct RetryThrottlerArg(pub(crate) SharedRetryThrottler);

impl<T: RetryThrottler + 'static> From<T> for RetryThrottlerArg {
    fn from(value: T) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }
}

impl From<SharedRetryThrottler> for RetryThrottlerArg {
    fn from(value: SharedRetryThrottler) -> Self {
        Self(value)
    }
}

/// Implements a probabilistic throttler based on observed failure rates.
///
/// This is an implementation of the [Adaptive Throttling] strategy described
/// in [Site Reliability Engineering] book. The basic idea is to
/// *stochastically* reject some of the retry attempts, with a rejection
/// probability that increases as the number of failures increases, and
/// decreases with the number of successful requests.
///
/// The rejection rate probability is defined by:
///
/// ```norust
/// threshold = (requests - factor * accepts) / (requests + 1)
/// rejection_probability = max(0, threshold)
/// ```
///
/// Where `requests` is the number of requests completed, and `accepts` is the
/// number of requests accepted by the service, including requests that fail due
/// to parameter validation, authorization checks, or any non-transient
/// failures.
///
/// Note that `accepts <= requests` but the `threshold` value might be negative
/// as `factor` can be higher than `1.0`. In fact, the SRE book recommends using
/// `2.0` as the initial factor.
///
/// Setting `factor` to lower values makes the algorithm reject retry attempts
/// with higher probability. For example, setting it to zero would reject some
/// retry attempts even if all requests have succeeded. Setting `factor` to
/// higher values allows more retry attempts.
///
/// # Example
/// ```
/// # use google_cloud_gax::retry_throttler::*;
/// let throttler = AdaptiveThrottler::new(2.0)?;
/// # Ok::<(), Error>(())
/// ```
///
/// [Site Reliability Engineering]: https://sre.google/sre-book/table-of-contents/
/// [Adaptive Throttling]: https://sre.google/sre-book/handling-overload/
#[derive(Clone, Debug)]
pub struct AdaptiveThrottler {
    accept_count: f64,
    request_count: f64,
    factor: f64,
}

impl AdaptiveThrottler {
    /// Creates a new adaptive throttler with the given `factor`.
    ///
    /// # Parameters
    /// * `factor` - a factor to adjust the relative weight of transient
    ///   failures vs. accepted requests. See the struct definition for
    ///   details.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_throttler::*;
    /// let throttler = AdaptiveThrottler::new(2.0)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn new(factor: f64) -> Result<Self, Error> {
        if factor < 0.0 {
            return Err(Error::ScalingOutOfRange(factor));
        }
        let factor = if factor < 0.0 { 0.0 } else { factor };
        Ok(Self::clamp(factor))
    }

    /// Creates a new adaptive throttler clamping `factor` to a valid range.
    ///
    /// # Parameters
    /// * `factor` - a factor to adjust the relative weight of transient
    ///   failures vs. accepted requests. See the struct definition for
    ///   details. Clamped to zero if the value is negative.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_throttler::*;
    /// let throttler = AdaptiveThrottler::clamp(2.0);
    /// ```
    pub fn clamp(factor: f64) -> Self {
        let factor = if factor < 0.0 { 0.0 } else { factor };
        Self {
            accept_count: 0.0,
            request_count: 0.0,
            factor,
        }
    }

    // A testable version of `throttle_retry_attempt()`.
    fn throttle<R: rand::Rng>(&self, rng: &mut R) -> bool {
        let reject_probability =
            (self.request_count - self.factor * self.accept_count) / (self.request_count + 1.0);
        let reject_probability = if reject_probability < 0.0 {
            0_f64
        } else {
            reject_probability
        };
        rng.random_range(0.0..=1.0) <= reject_probability
    }
}

impl std::default::Default for AdaptiveThrottler {
    /// Returns an `AdaptiveThrottler` initialized to the recommended values.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_throttler::*;
    /// let throttler = AdaptiveThrottler::default();
    /// ```
    fn default() -> Self {
        Self::clamp(2.0)
    }
}

impl RetryThrottler for AdaptiveThrottler {
    fn throttle_retry_attempt(&self) -> bool {
        self.throttle(&mut rand::rng())
    }

    fn on_retry_failure(&mut self, flow: &RetryResult) {
        self.request_count += 1.0;
        match flow {
            RetryResult::Continue(_) | RetryResult::Exhausted(_) => {}
            RetryResult::Permanent(_) => {
                self.accept_count += 1.0;
            }
        };
    }

    fn on_success(&mut self) {
        self.request_count += 1.0;
        self.accept_count += 1.0;
    }
}

/// A `CircuitBreaker` throttler rejects retry attempts if the success rate is too low.
///
/// This struct implements the [gRPC throttler] algorithm. The throttler works
/// by tracking the number of available "tokens" for a retry attempt. If this
/// number goes below a threshold **all** retry attempts are throttled.
///
/// Retry failures decrement the number of tokens by a given cost. Completed
/// requests (successfully or not) increase the tokens by `1`.
///
/// Note: the number of tokens may go below the throttling threshold as
/// multiple concurrent requests may fail and decrease the token count.
///
/// Note: throttling only applies to retry attempts, the initial requests is
/// never throttled. This may increases the token count even if all retry
/// attempts are throttled.
///
/// # Examples
/// ```
/// # use google_cloud_gax::retry_throttler::*;
/// let tokens = 1000;
/// let min_tokens = 250;
/// let error_cost = 10;
/// let throttler = CircuitBreaker::new(tokens, min_tokens, error_cost)?;
/// # Ok::<(), Error>(())
/// ```
///
/// [ClientBuilder::with_retry_throttler]: crate::client_builder::ClientBuilder::with_retry_throttler
/// [gRPC throttler]: https://github.com/grpc/proposal/blob/master/A6-client-retries.md
#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    max_tokens: u64,
    min_tokens: u64,
    cur_tokens: u64,
    error_cost: u64,
}

impl CircuitBreaker {
    /// Creates a new instance.
    ///
    /// # Parameters
    /// * `tokens` - the initial number of tokens. This is decreased by
    ///   `error_cost` on each retry failure, and increased by `1` when a
    ///   request succeeds.
    /// * `min_tokens` - stops accepting retry attempts when the number of
    ///   tokens is at or below this value.
    /// * `error_cost` - decrease the token count by this value on failed
    ///   request attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_throttler::*;
    /// let throttler = CircuitBreaker::new(1000, 250, 10)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn new(tokens: u64, min_tokens: u64, error_cost: u64) -> Result<Self, Error> {
        if min_tokens > tokens {
            return Err(Error::TooFewMinTokens {
                min: min_tokens,
                initial: tokens,
            });
        }
        Ok(Self {
            max_tokens: tokens,
            min_tokens,
            cur_tokens: tokens,
            error_cost,
        })
    }

    /// Creates a new instance, adjusting `min_tokens` if needed.
    ///
    /// # Parameters
    /// * `tokens` - the initial number of tokens. This is decreased by
    ///   `error_cost` on each retry failure, and increased by `1` when a
    ///   request succeeds.
    /// * `min_tokens` - stops accepting retry attempts when the number of
    ///   tokens is at or below this value. Clamped to be `<= tokens`.
    /// * `error_cost` - decrease the token count by this value on failed
    ///   request attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_throttler::*;
    /// let throttler = CircuitBreaker::clamp(1000, 250, 10);
    /// ```
    pub fn clamp(tokens: u64, min_tokens: u64, error_cost: u64) -> Self {
        Self {
            max_tokens: tokens,
            min_tokens: std::cmp::min(min_tokens, tokens),
            cur_tokens: tokens,
            error_cost,
        }
    }
}

impl std::default::Default for CircuitBreaker {
    /// Initialize a `CircuitBreaker` configured with recommended values.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::retry_throttler::*;
    /// let throttler = CircuitBreaker::default();
    /// ```
    fn default() -> Self {
        CircuitBreaker::clamp(100, 50, 10)
    }
}

impl RetryThrottler for CircuitBreaker {
    fn throttle_retry_attempt(&self) -> bool {
        self.cur_tokens <= self.min_tokens
    }

    fn on_retry_failure(&mut self, flow: &RetryResult) {
        match flow {
            RetryResult::Continue(_) | RetryResult::Exhausted(_) => {
                self.cur_tokens = self.cur_tokens.saturating_sub(self.error_cost);
            }
            RetryResult::Permanent(_) => {
                self.on_success();
            }
        };
    }

    fn on_success(&mut self) {
        self.cur_tokens = std::cmp::min(self.max_tokens, self.cur_tokens.saturating_add(1));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    // Verify `RetryThrottlerArg` can be converted from the desired types.
    #[test]
    fn retry_throttler_arg() {
        let throttler = AdaptiveThrottler::default();
        let _ = RetryThrottlerArg::from(throttler);

        let throttler: Arc<Mutex<dyn RetryThrottler>> =
            Arc::new(Mutex::new(CircuitBreaker::default()));
        let _ = RetryThrottlerArg::from(throttler);
    }

    #[test]
    fn adaptive_construction() {
        let throttler = AdaptiveThrottler::new(-2.0);
        assert!(
            matches!(throttler, Err(Error::ScalingOutOfRange { .. })),
            "{throttler:?}"
        );

        let throttler = AdaptiveThrottler::new(0.0);
        assert!(throttler.is_ok(), "{throttler:?}");
    }

    fn test_error() -> crate::error::Error {
        use crate::error::{
            Error,
            rpc::{Code, Status},
        };
        Error::service(Status::default().set_code(Code::Aborted))
    }

    #[test]
    fn adaptive() -> TestResult {
        let mut throttler = AdaptiveThrottler::default();
        assert_eq!(throttler.request_count, 0.0);
        assert_eq!(throttler.accept_count, 0.0);
        assert_eq!(throttler.factor, 2.0);

        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        throttler.on_retry_failure(&RetryResult::Continue(test_error()));
        assert_eq!(throttler.request_count, 1.0);
        assert_eq!(throttler.accept_count, 0.0);

        throttler.on_retry_failure(&RetryResult::Continue(test_error()));
        assert_eq!(throttler.request_count, 2.0);
        assert_eq!(throttler.accept_count, 0.0);

        throttler.on_success();
        assert_eq!(throttler.request_count, 3.0);
        assert_eq!(throttler.accept_count, 1.0);

        throttler.on_retry_failure(&RetryResult::Permanent(test_error()));
        assert_eq!(throttler.request_count, 4.0);
        assert_eq!(throttler.accept_count, 2.0);

        let mut throttler = AdaptiveThrottler::default();
        throttler.on_retry_failure(&RetryResult::Continue(test_error()));

        // StepRng::new(x, 0) always produces the same value. We pick the values
        // to trigger the desired behavior.
        let mut rng = rand::rngs::mock::StepRng::new(0, 0);
        assert_eq!(rng.random_range(0.0..=1.0), 0.0);
        assert!(throttler.throttle(&mut rng), "{throttler:?}");

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX - u64::MAX / 4, 0);
        assert!(
            rng.random_range(0.0..=1.0) > 0.5,
            "{}",
            rng.random_range(0.0..=1.0)
        );
        assert!(!throttler.throttle(&mut rng), "{throttler:?}");

        // This creates a throttler with reject probability == 0.
        let mut throttler = AdaptiveThrottler::new(100.0)?;
        throttler.on_success();
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        Ok(())
    }

    #[test]
    fn circuit_breaker_validation() {
        let throttler = CircuitBreaker::new(100, 200, 1);
        assert!(
            matches!(throttler, Err(Error::TooFewMinTokens { .. })),
            "{throttler:?}"
        );
    }

    #[test]
    fn circuit_breaker() {
        let mut throttler = CircuitBreaker::default();
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        for _ in 0..4 {
            throttler.on_retry_failure(&RetryResult::Continue(test_error()));
            assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");
        }
        // This crosses the threshold:
        throttler.on_retry_failure(&RetryResult::Continue(test_error()));
        throttler.on_retry_failure(&RetryResult::Continue(test_error()));
        assert!(throttler.throttle_retry_attempt(), "{throttler:?}");

        // With the default settings, we will need about 10x successful calls
        // to recover.
        for _ in 0..10 {
            throttler.on_success();
            assert!(throttler.throttle_retry_attempt(), "{throttler:?}");
        }
        throttler.on_success();
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        // Permanent errors also open back the throttle.
        throttler.on_retry_failure(&RetryResult::Continue(test_error()));
        for _ in 0..9 {
            throttler.on_retry_failure(&RetryResult::Permanent(test_error()));
            assert!(throttler.throttle_retry_attempt(), "{throttler:?}");
        }
        throttler.on_retry_failure(&RetryResult::Permanent(test_error()));
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");
    }
}
