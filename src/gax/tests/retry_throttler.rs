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

//! Verify retry throttlers are usable from outside the crate.

#[cfg(test)]
mod tests {
    use google_cloud_gax::retry_throttler::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn adaptive() -> Result {
        // Verify the calls work from outside the crate. The functionality is
        // verified in the unit tests.
        let throttler = AdaptiveThrottler::default();
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        let throttler = AdaptiveThrottler::new(1.1)?;
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        let throttler = AdaptiveThrottler::clamp(1.1);
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        Ok(())
    }

    #[test]
    fn circuit_breaker() -> Result {
        // Verify the calls work from outside the crate. The functionality is
        // verified in the unit tests.
        let throttler = CircuitBreaker::default();
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        let throttler = CircuitBreaker::new(1000, 250, 10)?;
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        let throttler = CircuitBreaker::clamp(1000, 250, 10);
        assert!(!throttler.throttle_retry_attempt(), "{throttler:?}");

        Ok(())
    }
}
