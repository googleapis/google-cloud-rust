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

//! Verify backoff policies are usable from outside the crate.

#[cfg(test)]
mod tests {
    use google_cloud_gax::backoff_policy::*;
    use google_cloud_gax::exponential_backoff::*;
    use google_cloud_gax::retry_state::RetryState;
    use std::time::Duration;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn exponential() -> Result {
        // Verify the calls work from outside the crate. The functionality is
        // verified in the unit tests.
        let policy = ExponentialBackoff::default();
        assert!(
            policy.on_failure(&RetryState::new(true).set_attempt_count(1_u32)) > Duration::ZERO,
            "{policy:?}"
        );

        let policy = ExponentialBackoffBuilder::new().build()?;
        let _ = format!("{policy:?}");
        let policy = ExponentialBackoffBuilder::new().build()?;
        assert!(
            policy.on_failure(&RetryState::new(true).set_attempt_count(1_u32)) > Duration::ZERO,
            "{policy:?}"
        );
        let _ = BackoffPolicyArg::from(policy);

        let policy = ExponentialBackoffBuilder::new().clamp();
        let _ = BackoffPolicyArg::from(policy);

        Ok(())
    }
}
