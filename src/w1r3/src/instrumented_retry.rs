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

use google_cloud_gax::{
    error::Error, retry_policy::RetryPolicy, retry_result::RetryResult,
    throttle_result::ThrottleResult,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct RetryHistory(Arc<Mutex<Vec<String>>>);

impl RetryHistory {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    pub fn push<R>(&self, call: String, result: &R)
    where
        R: std::fmt::Debug,
    {
        self.0
            .lock()
            .expect("no poison")
            .push(call + &format!(" -> {result:?}"));
    }
}

#[derive(Debug)]
pub struct DebugRetry<T> {
    inner: T,
    history: RetryHistory,
}

impl<T> DebugRetry<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            history: RetryHistory::new(),
        }
    }
}

impl<T> RetryPolicy for DebugRetry<T>
where
    T: RetryPolicy,
{
    fn on_error(
        &self,
        loop_start: Instant,
        attempt_count: u32,
        idempotent: bool,
        error: Error,
    ) -> RetryResult {
        let value = format!(
            "on_error({:?}, {:?}, {attempt_count}, {idempotent}, {error:?})",
            self.inner,
            Instant::now() - loop_start
        );
        let result = self
            .inner
            .on_error(loop_start, attempt_count, idempotent, error);
        self.history.push(value, &result);
        match &result {
            RetryResult::Continue(_) => {}
            RetryResult::Exhausted(e) => {
                tracing::error!(
                    "retry policy exhausted on {e:?}, full history: {:?}",
                    self.history
                )
            }
            RetryResult::Permanent(e) => {
                tracing::error!("permanent error {e:?}, full history: {:?}", self.history)
            }
        };
        result
    }

    fn on_throttle(&self, loop_start: Instant, attempt_count: u32, error: Error) -> ThrottleResult {
        let value = format!(
            "on_throttle({:?}, {:?}, {attempt_count}, {error:?})",
            self.inner,
            Instant::now() - loop_start
        );
        let result = self.inner.on_throttle(loop_start, attempt_count, error);
        self.history.push(value, &result);
        result
    }

    fn remaining_time(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> Option<Duration> {
        self.inner.remaining_time(loop_start, attempt_count)
    }
}
