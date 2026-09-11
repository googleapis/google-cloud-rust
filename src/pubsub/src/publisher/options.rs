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

/// Configure publisher batching behavior.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchingOptions {
    pub message_count_threshold: u32,
    pub byte_threshold: u32,
    pub delay_threshold: std::time::Duration,
}

impl BatchingOptions {
    /// Create a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [BatchingOptions][Self::message_count_threshold] field.
    pub fn set_message_count_threshold<V: Into<u32>>(mut self, v: V) -> Self {
        self.message_count_threshold = v.into();
        self
    }

    /// Set the [BatchingOptions][Self::byte_threshold] field.
    pub(crate) fn set_byte_threshold<V: Into<u32>>(mut self, v: V) -> Self {
        self.byte_threshold = v.into();
        self
    }

    /// Set the [BatchingOptions][Self::delay_threshold] field.
    pub fn set_delay_threshold<V: Into<std::time::Duration>>(mut self, v: V) -> Self {
        self.delay_threshold = v.into();
        self
    }
}

impl std::default::Default for BatchingOptions {
    fn default() -> Self {
        Self {
            message_count_threshold: 100_u32,
            byte_threshold: 1_000_000_u32, // 1 MB
            delay_threshold: std::time::Duration::from_millis(10),
        }
    }
}

use super::constants::*;

/// Configure publisher request hedging behavior.
///
/// Request hedging sends a duplicate publish request when an in-flight batch publish
/// RPC exceeds a configured delay threshold, mitigating tail latency caused by slow backend
/// tasks or transient network stalls.
///
/// Hedging uses a token bucket to rate-limit hedged RPCs. Successful publish RPCs refill
/// fractional tokens, and sending a hedged RPC decrements 1 token.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) struct HedgingOptions {
    /// The delay before sending a hedged request for an outstanding batch.
    ///
    /// Clamped between 100ms and 10s by the publisher builder. Defaults to 1s.
    pub(crate) delay: std::time::Duration,
    /// The maximum number of tokens in the token bucket.
    ///
    /// Represents the maximum burst capacity of hedged requests. Clamped between 1 and 250
    /// by the publisher builder. Defaults to 50.
    pub(crate) max_tokens: u32,
    /// The fraction of a token added to the bucket for each successful publish RPC.
    ///
    /// Clamped between 0.001 and 0.2 by the publisher builder. Defaults to 0.1 (1 full token per 10 successful RPCs).
    pub(crate) refill_ratio: f32,
}

impl HedgingOptions {
    /// Set the delay before sending a hedged request.
    ///
    /// Clamped between 100ms and 10s.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn set_delay<V: Into<std::time::Duration>>(mut self, v: V) -> Self {
        self.delay = v.into().clamp(MIN_HEDGING_DELAY, MAX_HEDGING_DELAY);
        self
    }

    /// Set the maximum number of tokens in the token bucket.
    ///
    /// Clamped between 1 and 250.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn set_max_tokens<V: Into<u32>>(mut self, v: V) -> Self {
        self.max_tokens = v
            .into()
            .clamp(MIN_HEDGING_MAX_TOKENS, MAX_HEDGING_MAX_TOKENS);
        self
    }

    /// Set the fraction of a token refilled per successful publish RPC.
    ///
    /// Clamped between 0.001 and 0.2.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn set_refill_ratio<V: Into<f32>>(mut self, v: V) -> Self {
        self.refill_ratio = v
            .into()
            .clamp(MIN_HEDGING_REFILL_RATIO, MAX_HEDGING_REFILL_RATIO);
        self
    }
}

impl std::default::Default for HedgingOptions {
    fn default() -> Self {
        Self {
            delay: std::time::Duration::from_secs(1),
            max_tokens: 50_u32,
            refill_ratio: 0.1_f32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BatchingOptions, HedgingOptions, MAX_HEDGING_DELAY, MAX_HEDGING_MAX_TOKENS,
        MAX_HEDGING_REFILL_RATIO, MIN_HEDGING_DELAY, MIN_HEDGING_MAX_TOKENS,
        MIN_HEDGING_REFILL_RATIO,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn batching_options() -> anyhow::Result<()> {
        let options = BatchingOptions::new()
            .set_byte_threshold(1_234_u32)
            .set_message_count_threshold(123_u32)
            .set_delay_threshold(std::time::Duration::from_millis(12));
        assert_eq!(options.byte_threshold, 1_234_u32);
        assert_eq!(options.message_count_threshold, 123_u32);
        assert_eq!(
            options.delay_threshold,
            std::time::Duration::from_millis(12)
        );
        Ok(())
    }

    #[test]
    fn hedging_options_defaults_and_builder() {
        let default_opts = HedgingOptions::default();
        assert_eq!(default_opts.delay, Duration::from_secs(1));
        assert_eq!(default_opts.max_tokens, 50);
        assert_eq!(default_opts.refill_ratio, 0.1);

        let custom_opts = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(100_u32)
            .set_refill_ratio(0.05_f32);
        assert_eq!(custom_opts.delay, Duration::from_millis(500));
        assert_eq!(custom_opts.max_tokens, 100);
        assert_eq!(custom_opts.refill_ratio, 0.05);
    }

    #[test]
    fn hedging_options_clamps_values() {
        let under_opts = HedgingOptions::default()
            .set_delay(Duration::from_millis(10))
            .set_max_tokens(0_u32)
            .set_refill_ratio(0.0001_f32);
        assert_eq!(under_opts.delay, MIN_HEDGING_DELAY);
        assert_eq!(under_opts.max_tokens, MIN_HEDGING_MAX_TOKENS);
        assert_eq!(under_opts.refill_ratio, MIN_HEDGING_REFILL_RATIO);

        let over_opts = HedgingOptions::default()
            .set_delay(Duration::from_secs(60))
            .set_max_tokens(500_u32)
            .set_refill_ratio(0.5_f32);
        assert_eq!(over_opts.delay, MAX_HEDGING_DELAY);
        assert_eq!(over_opts.max_tokens, MAX_HEDGING_MAX_TOKENS);
        assert_eq!(over_opts.refill_ratio, MAX_HEDGING_REFILL_RATIO);
    }
}
