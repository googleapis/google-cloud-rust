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

use google_cloud_gax::error::Error;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::time::Duration;

/// Instrument a [RetryPolicy] to log when the client needs to resume.
#[derive(Debug)]
pub struct Instrumented<T> {
    inner: T,
}

impl<T> Instrumented<T>
where
    T: std::fmt::Debug,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> RetryPolicy for Instrumented<T>
where
    T: RetryPolicy,
{
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        let result = self.inner.on_error(state, error);
        match &result {
            RetryResult::Continue(e) => {
                tracing::info!("retry policy continues, state: {state:?}, error: {e:?}")
            }
            RetryResult::Exhausted(e) => {
                tracing::info!("retry policy exhausted, state: {state:?}, error: {e:?}")
            }
            RetryResult::Permanent(e) => {
                tracing::info!("retry policy permanent error, state: {state:?}, error: {e:?}")
            }
        }
        result
    }

    fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult {
        let result = self.inner.on_throttle(state, error);
        match &result {
            ThrottleResult::Continue(e) => {
                tracing::info!("retry policy continues on throttle, state: {state:?}, error: {e:?}")
            }
            ThrottleResult::Exhausted(e) => {
                tracing::info!("retry policy exhausted on throttle, state: {state:?}, error: {e:?}")
            }
        }
        result
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        let result = self.inner.remaining_time(state);
        // This function is called on every retry attempt, reduce the noise by
        // just printing something if most (more than 75%) of the retry time
        // limit has been used.
        if result.is_some_and(|d| d < state.start.elapsed() / 4) {
            tracing::info!("retry policy remaining time: {result:?}");
        }
        result
    }
}
