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

//! Common implements for exponential backoff.
//!
//! This module provides an implementation of truncated [exponential backoff].
//! It implements the [BackoffPolicy] and [PollingBackoffPolicy] traits.
//!
//! [BackoffPolicy]: crate::backoff_policy::BackoffPolicy
//! [PollingBackoffPolicy]: crate::polling_backoff_policy::PollingBackoffPolicy

use crate::error::Error;
use crate::Result;
use std::time::Duration;

/// Implements truncated exponential backoff with jitter.
#[derive(Clone, Debug)]
pub struct ExponentialBackoffBuilder {
    initial_delay: Duration,
    maximum_delay: Duration,
    scaling: f64,
}

impl ExponentialBackoffBuilder {
    /// Creates a builder with the default parameters.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::*;
    /// # use gcp_sdk_gax::exponential_backoff::*;
    /// use std::time::Duration;
    ///
    /// fn configure_retry(config: options::ClientConfig) -> Result<options::ClientConfig> {
    ///     let policy = ExponentialBackoffBuilder::new()
    ///         .with_initial_delay(Duration::from_millis(100))
    ///         .with_maximum_delay(Duration::from_secs(5))
    ///         .with_scaling(4.0)
    ///         .build()?;
    ///     Ok(config.set_backoff_policy(policy))
    /// }
    /// ```
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::*;
    /// # use exponential_backoff::*;
    /// use std::time::Duration;
    ///
    /// fn configure_polling(config: options::ClientConfig) -> Result<options::ClientConfig> {
    ///     let policy = ExponentialBackoffBuilder::new()
    ///         .with_initial_delay(Duration::from_millis(100))
    ///         .with_maximum_delay(Duration::from_secs(5))
    ///         .with_scaling(4.0)
    ///         .build()?;
    ///     Ok(config.set_polling_backoff_policy(policy))
    /// }
    /// ```
    pub fn new() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            maximum_delay: Duration::from_secs(60),
            scaling: 2.0,
        }
    }

    /// Change the initial delay.
    pub fn with_initial_delay<V: Into<Duration>>(mut self, v: V) -> Self {
        self.initial_delay = v.into();
        self
    }

    /// Change the initial delay.
    pub fn with_maximum_delay<V: Into<Duration>>(mut self, v: V) -> Self {
        self.maximum_delay = v.into();
        self
    }

    /// Change the scaling factor in this backoff policy.
    pub fn with_scaling<V: Into<f64>>(mut self, v: V) -> Self {
        self.scaling = v.into();
        self
    }

    /// Creates a new exponential backoff policy.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::*;
    /// # use exponential_backoff::*;
    /// # use backoff_policy::BackoffPolicy;
    /// use std::time::Duration;
    /// use std::time::Instant;
    /// let backoff = ExponentialBackoffBuilder::new()
    ///     .with_initial_delay(Duration::from_secs(5))
    ///     .with_maximum_delay(Duration::from_secs(50))
    ///     .with_scaling(2.0)
    ///     .build()?;
    /// let p = backoff.on_failure(Instant::now(), 1);
    /// assert!(p <= Duration::from_secs(5));
    /// let p = backoff.on_failure(Instant::now(), 2);
    /// assert!(p <= Duration::from_secs(10));
    /// # Ok::<(), error::Error>(())
    /// ```
    pub fn build(self) -> Result<ExponentialBackoff> {
        if let Some(error) = self.validate() {
            return Err(error);
        }
        Ok(ExponentialBackoff {
            maximum_delay: self.maximum_delay,
            scaling: self.scaling,
            initial_delay: self.initial_delay,
        })
    }

    /// Creates a new exponential backoff policy clamping the ranges to barely
    /// recommended values.
    ///
    /// The maximum delay is clamped first, to be between one second and one day
    /// (both inclusive). The upper value is hardly useful, except maybe in
    /// tests and very long running operations.
    ///
    /// Then the initial delay is clamped to be between one millisecond and the
    /// maximum delay. One millisecond is rarely useful outside of tests, but at
    /// is unlikely to cause problems.
    ///
    /// Finally, the scaling factor is clamped to the `[1.0, 32.0]` range.
    /// Neither extreme is very useful, but neither are necessarily going to
    /// cause trouble.
    ///
    /// # Example
    /// ```
    /// # use gcp_sdk_gax::*;
    /// # use exponential_backoff::*;
    /// use backoff_policy::BackoffPolicy;
    /// use std::time::Duration;
    /// use std::time::Instant;
    /// let mut backoff = ExponentialBackoffBuilder::new().clamp();
    /// assert!(backoff.on_failure(Instant::now(), 1) > Duration::ZERO);
    /// # Ok::<(), error::Error>(())
    /// ```
    pub fn clamp(self) -> ExponentialBackoff {
        let scaling = self.scaling.clamp(1.0, 32.0);
        let maximum_delay = self
            .maximum_delay
            .clamp(Duration::from_secs(1), Duration::from_secs(24 * 60 * 60));
        let current_delay = self
            .initial_delay
            .clamp(Duration::from_millis(1), maximum_delay);
        ExponentialBackoff {
            initial_delay: current_delay,
            maximum_delay,
            scaling,
        }
    }

    fn validate(&self) -> Option<crate::error::Error> {
        if self.scaling < 1.0 {
            return Some(Error::other(format!(
                "scaling ({}) must be >= 1.0",
                self.scaling
            )));
        }
        if self.initial_delay.is_zero() {
            return Some(Error::other(format!(
                "initial delay must be greater than zero, got={:?}",
                self.initial_delay
            )));
        }
        if self.maximum_delay < self.initial_delay {
            return Some(Error::other(format!(
                "maximum delay ({:?} must be greater or equal to the initial delay ({:?})",
                self.maximum_delay, self.initial_delay
            )));
        }
        None
    }
}

impl std::default::Default for ExponentialBackoffBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Implements truncated exponential backoff.
#[derive(Debug)]
pub struct ExponentialBackoff {
    initial_delay: Duration,
    maximum_delay: Duration,
    scaling: f64,
}

impl ExponentialBackoff {
    fn delay(&self, _loop_start: std::time::Instant, attempt_count: u32) -> Duration {
        let exp = std::cmp::min(i32::MAX as u32, attempt_count) as i32;
        let exp = exp.saturating_sub(1);
        let scaling = self.scaling.powi(exp);
        if scaling >= self.maximum_delay.div_duration_f64(self.initial_delay) {
            self.maximum_delay
        } else {
            // .mul_f64() cannot assert because (1) we guarantee scaling >= 1.0,
            // and (2) we just checked that
            //     self.initial_delay * scaling < maximum_delay.
            self.initial_delay.mul_f64(scaling)
        }
    }

    fn delay_with_jitter(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        rng: &mut impl rand::Rng,
    ) -> std::time::Duration {
        let delay = self.delay(loop_start, attempt_count);
        rng.gen_range(Duration::ZERO..=delay)
    }
}

impl std::default::Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            maximum_delay: Duration::from_secs(60),
            scaling: 2.0,
        }
    }
}

impl crate::polling_backoff_policy::PollingBackoffPolicy for ExponentialBackoff {
    fn wait_period(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> std::time::Duration {
        self.delay(loop_start, attempt_count)
    }
}

impl crate::backoff_policy::BackoffPolicy for ExponentialBackoff {
    fn on_failure(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> std::time::Duration {
        self.delay_with_jitter(loop_start, attempt_count, &mut rand::thread_rng())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn exponential_build_errors() -> TestResult {
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::ZERO)
            .with_maximum_delay(Duration::from_secs(5))
            .build();
        assert!(b.is_err(), "{b:?}");
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(10))
            .with_maximum_delay(Duration::from_secs(5))
            .build();
        assert!(b.is_err(), "{b:?}");

        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::from_secs(60))
            .with_scaling(-1.0)
            .build();
        assert!(b.is_err(), "{b:?}");
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::from_secs(60))
            .with_scaling(0.0)
            .build();
        assert!(b.is_err(), "{b:?}");

        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::ZERO)
            .build();
        assert!(b.is_err(), "{b:?}");
        Ok(())
    }

    #[test]
    fn exponential_build_limits() -> TestResult {
        let _ = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::MAX)
            .build()?;

        let _ = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_nanos(1))
            .with_maximum_delay(Duration::MAX)
            .build()?;

        let _ = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_nanos(1))
            .with_maximum_delay(Duration::MAX)
            .with_scaling(1.0)
            .build()?;
        Ok(())
    }

    #[test]
    fn exponential_builder_defaults() -> TestResult {
        let _ = ExponentialBackoffBuilder::new().build()?;
        let _ = ExponentialBackoffBuilder::default().build()?;
        Ok(())
    }

    #[test_case::test_case(Duration::from_secs(1), Duration::MAX, 0.5; "scaling below range")]
    #[test_case::test_case(Duration::from_secs(1), Duration::MAX, 1_000_000.0; "scaling over range")]
    #[test_case::test_case(Duration::from_secs(1), Duration::MAX, 8.0; "max over range")]
    #[test_case::test_case(Duration::from_secs(1), Duration::ZERO, 8.0; "max below range")]
    #[test_case::test_case(Duration::from_secs(10), Duration::ZERO, 8.0; "init over range")]
    #[test_case::test_case(Duration::ZERO, Duration::ZERO, 8.0; "init below range")]
    fn exponential_clamp(init: Duration, max: Duration, scaling: f64) -> TestResult {
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(init)
            .with_maximum_delay(max)
            .with_scaling(scaling)
            .clamp();
        assert_eq!(b.scaling.clamp(1.0, 32.0), b.scaling);
        assert_eq!(
            b.initial_delay
                .clamp(Duration::from_millis(1), b.maximum_delay),
            b.initial_delay
        );
        assert_eq!(
            b.maximum_delay
                .clamp(b.initial_delay, Duration::from_secs(24 * 60 * 60)),
            b.maximum_delay
        );
        Ok(())
    }

    #[test]
    fn exponential_full_jitter() -> TestResult {
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(10))
            .with_maximum_delay(Duration::from_secs(10))
            .build()?;

        let now = std::time::Instant::now();
        let mut rng = rand::rngs::mock::StepRng::new(0, 0);
        assert_eq!(b.delay_with_jitter(now, 1, &mut rng), Duration::ZERO);

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX / 2, 0);
        assert_eq!(
            b.delay_with_jitter(now, 2, &mut rng),
            Duration::from_secs(5)
        );

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        assert_eq!(
            b.delay_with_jitter(now, 3, &mut rng),
            Duration::from_secs(10)
        );
        Ok(())
    }

    #[test]
    fn exponential_scaling() -> TestResult {
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::from_secs(4))
            .with_scaling(2.0)
            .build()?;

        let now = std::time::Instant::now();
        assert_eq!(b.delay(now, 1), Duration::from_secs(1));
        assert_eq!(b.delay(now, 2), Duration::from_secs(2));
        assert_eq!(b.delay(now, 3), Duration::from_secs(4));
        assert_eq!(b.delay(now, 4), Duration::from_secs(4));

        Ok(())
    }

    #[test]
    fn wait_period() -> TestResult {
        use crate::polling_backoff_policy::PollingBackoffPolicy;
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::from_secs(4))
            .with_scaling(2.0)
            .build()?;

        let now = std::time::Instant::now();
        assert_eq!(b.wait_period(now, 1), Duration::from_secs(1));
        assert_eq!(b.wait_period(now, 2), Duration::from_secs(2));
        assert_eq!(b.wait_period(now, 3), Duration::from_secs(4));
        assert_eq!(b.wait_period(now, 4), Duration::from_secs(4));

        Ok(())
    }

    #[test]
    fn exponential_scaling_jitter() -> TestResult {
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::from_secs(4))
            .with_scaling(2.0)
            .build()?;

        let now = std::time::Instant::now();
        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        assert_eq!(
            b.delay_with_jitter(now, 1, &mut rng),
            Duration::from_secs(1)
        );

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        assert_eq!(
            b.delay_with_jitter(now, 2, &mut rng),
            Duration::from_secs(2)
        );

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        assert_eq!(
            b.delay_with_jitter(now, 3, &mut rng),
            Duration::from_secs(4)
        );

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        assert_eq!(
            b.delay_with_jitter(now, 4, &mut rng),
            Duration::from_secs(4)
        );

        Ok(())
    }

    #[test]
    fn on_failure() -> TestResult {
        use crate::backoff_policy::BackoffPolicy;
        let b = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_maximum_delay(Duration::from_secs(4))
            .with_scaling(2.0)
            .build()?;

        let now = std::time::Instant::now();
        let d = b.on_failure(now, 1);
        assert!(Duration::ZERO <= d && d <= Duration::from_secs(1), "{d:?}");
        let d = b.on_failure(now, 2);
        assert!(Duration::ZERO <= d && d <= Duration::from_secs(2), "{d:?}");
        let d = b.on_failure(now, 3);
        assert!(Duration::ZERO <= d && d <= Duration::from_secs(4), "{d:?}");
        let d = b.on_failure(now, 4);
        assert!(Duration::ZERO <= d && d <= Duration::from_secs(4), "{d:?}");
        let d = b.on_failure(now, 5);
        assert!(Duration::ZERO <= d && d <= Duration::from_secs(4), "{d:?}");

        Ok(())
    }

    #[test]
    fn default() -> TestResult {
        let b = ExponentialBackoff::default();

        let now = std::time::Instant::now();
        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        let next = 2 * b.delay_with_jitter(now, 1, &mut rng);

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        assert_eq!(b.delay_with_jitter(now, 2, &mut rng), next);
        let next = 2 * next;

        let mut rng = rand::rngs::mock::StepRng::new(u64::MAX, 0);
        assert_eq!(b.delay_with_jitter(now, 3, &mut rng), next);

        Ok(())
    }
}
