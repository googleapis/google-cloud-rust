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
    use gcp_sdk_gax::error::Error;
    use gcp_sdk_gax::retry_policy::*;
    use std::time::Duration;

    #[derive(Clone)]
    struct CustomRetryPolicy;
    impl RetryPolicy for CustomRetryPolicy {
        fn on_error(&self, idempotent: bool, error: Error) -> RetryFlow {
            if idempotent {
                RetryFlow::Continue(())
            } else {
                RetryFlow::Break(error)
            }
        }
        fn remaining_time(&self) -> Option<std::time::Duration> {
            None
        }
    }

    #[test]
    fn create_limited_error_retry() {
        let _policy = LimitedErrorCount::new(CustomRetryPolicy, 3);
        let _policy = LimitedErrorCount::new(Aip194Strict, 3);
    }

    #[test]
    fn create_limit_elapsed_time() {
        let _policy = LimitedElapsedTime::new(CustomRetryPolicy, Duration::from_millis(100));
        let _policy = LimitedElapsedTime::new(Aip194Strict, Duration::from_millis(100));
    }
}
