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

//! Verify retry policies are usable from outside the crate.

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::Error;
    use google_cloud_gax::retry_policy::*;
    use google_cloud_gax::retry_result::RetryResult;
    use std::time::Duration;

    #[derive(Debug)]
    struct CustomRetryPolicy;
    impl RetryPolicy for CustomRetryPolicy {
        fn on_error(
            &self,
            state: &RetryLoopState,
            error: Error,
        ) -> RetryResult {
            if state.idempotent {
                RetryResult::Continue(error)
            } else {
                RetryResult::Permanent(error)
            }
        }

        fn remaining_time(
            &self,
            _state: &RetryLoopState,
        ) -> Option<std::time::Duration> {
            None
        }
    }

    #[test]
    fn create_limited_error_retry() {
        let _policy = LimitedAttemptCount::custom(CustomRetryPolicy, 3);
        let _policy = CustomRetryPolicy.with_attempt_limit(3);
        let _policy = LimitedAttemptCount::new(3);
    }

    #[test]
    fn create_limit_elapsed_time() {
        let _policy = LimitedElapsedTime::custom(CustomRetryPolicy, Duration::from_millis(100));
        let _policy = CustomRetryPolicy.with_time_limit(Duration::from_secs(3));
        let _policy = LimitedElapsedTime::new(Duration::from_millis(100));
    }
}
