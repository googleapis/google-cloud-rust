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

use super::Result;
use super::backoff_policy::BackoffPolicy;
use super::loop_state::LoopState;
use super::retry_policy::RetryPolicy;
use super::retry_throttler::RetryThrottler;
use std::sync::{Arc, Mutex};

/// Runs the retry loop for a given function.
///
/// This functions calls an inner function as long as (1) the retry policy has
/// not expired, (2) the inner function has not returned a successful request,
/// and (3) the retry throttler allows more calls.
///
/// In between calls the function waits the amount of time prescribed by the
/// backoff policy, using `backoff` to implement any sleep.
pub async fn retry_loop<F, B, Response>(
    inner: F,
    backoff: B,
    idempotent: bool,
    retry_throttler: Arc<Mutex<dyn RetryThrottler>>,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
) -> Result<Response>
where
    F: AsyncFn(Option<std::time::Duration>) -> Result<Response> + Send,
    B: AsyncFn(std::time::Duration) -> () + Send,
{
    let loop_start = std::time::Instant::now();
    let mut attempt_count = 0;
    loop {
        let remaining_time = retry_policy.remaining_time(loop_start, attempt_count);
        let throttle = if attempt_count == 0 {
            false
        } else {
            let t = retry_throttler.lock().expect("retry throttler lock is poisoned");
            t.throttle_retry_attempt()
        };
        if throttle {
            // This counts as an error for the purposes of the retry policy.
            if let Some(error) = retry_policy.on_throttle(loop_start, attempt_count) {
                return Err(error);
            }
            let delay = backoff_policy.on_failure(loop_start, attempt_count);
            backoff(delay).await;
            continue;
        }
        attempt_count += 1;
        match inner(remaining_time).await {
            Ok(r) => {
                retry_throttler
                    .lock()
                    .expect("retry throttler lock is poisoned")
                    .on_success();
                return Ok(r);
            }
            Err(e) => {
                let flow = retry_policy.on_error(
                    loop_start,
                    attempt_count,
                    idempotent,
                    e,
                );
                let delay = backoff_policy.on_failure(loop_start, attempt_count);
                retry_throttler
                    .lock()
                    .expect("retry throttler lock is poisoned")
                    .on_retry_failure(&flow);
                on_error(&backoff, flow, delay).await?;
            }
        };
    }
}

async fn on_error<B>(
    backoff: &B,
    retry_flow: LoopState,
    backoff_delay: std::time::Duration,
) -> Result<()>
where
    B: AsyncFn(std::time::Duration) -> (),
{
    match retry_flow {
        LoopState::Permanent(e) | LoopState::Exhausted(e) => Err(e),
        LoopState::Continue(_e) => Ok(backoff(backoff_delay).await),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::error::Error;

    #[tokio::test]
    async fn immediate_success() -> anyhow::Result<()> {
        let mut throttler = MockRetryThrottler::new();
        throttler.expect_on_success().once().return_const(());
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy.expect_remaining_time().once().return_const(None);
        let backoff_policy = MockBackoffPolicy::new();
        let sleep = MockSleep::new();

        let inner = async |_| Ok("success".to_string());
        let backoff = async move |d| {
            sleep.sleep(d).await
        };
        let response = retry_loop(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await?;
        assert_eq!(response, "success");
        Ok(())
    }

    fn to_retry_throttler(mock: MockRetryThrottler) -> Arc<Mutex<dyn RetryThrottler>> {
        Arc::new(Mutex::new(mock))
    }

    fn to_retry_policy(mock: MockRetryPolicy) -> Arc<dyn RetryPolicy> {
        Arc::new(mock)
    }

    fn to_backoff_policy(mock: MockBackoffPolicy) -> Arc<dyn BackoffPolicy> {
        Arc::new(mock)
    }

    trait Sleep {
        fn sleep(&self, d: std::time::Duration) -> impl Future<Output = ()>;
    }

    mockall::mock! {
        Sleep {}
        impl Sleep for Sleep {
            fn sleep(&self, d: std::time::Duration) -> impl Future<Output = ()> + Send;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, idempotent: bool, error: Error) -> LoopState;
            fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Error>;
            fn remaining_time(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<std::time::Duration>;
        }
        impl std::clone::Clone for RetryPolicy {
            fn clone(&self) -> Self;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, loop_start: std::time::Instant, attempt_count: u32) -> std::time::Duration;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        RetryThrottler {}
        impl RetryThrottler for RetryThrottler {
            fn throttle_retry_attempt(&self) -> bool;
            fn on_retry_failure(&mut self, error: &LoopState);
            fn on_success(&mut self);
        }
    }
}
