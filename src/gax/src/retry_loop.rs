use crate::Result;
use crate::retry_throttler::SharedRetryThrottler;
use crate::exponential_backoff::ExponentialBackoff;
use crate::loop_state::LoopState;
use crate::backoff_policy::BackoffPolicy;
use crate::options::RequestOptions;
use std::sync::Arc;
use crate::error::Error;
use crate::retry_policy::RetryPolicy;
use async_trait::async_trait;


#[async_trait]
pub trait InnerRequestTrait {
    async fn make_request<O: serde::de::DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
        options: &RequestOptions,
        remaining_time: Option<std::time::Duration>,
    ) -> Result<O>;
}

#[derive(Debug, Clone)]
pub struct RetryLoop <T: InnerRequestTrait + Send + Sync> {
    pub retry_policy: Option<Arc<dyn RetryPolicy>>,
    pub backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    pub retry_throttler: SharedRetryThrottler,
    pub inner_request: T,
} 

impl<T: InnerRequestTrait + Send + Sync> RetryLoop<T>  {
    pub async fn retry_loop<O: serde::de::DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
        options: &RequestOptions,
        retry_policy: Arc<dyn RetryPolicy>,
    ) -> Result<O> {
        let loop_start = std::time::Instant::now();
        let throttler = self.get_retry_throttler(options);
        let backoff = self.get_backoff_policy(options);
        let mut attempt_count = 0;
        loop {
            let builder = builder
                .try_clone()
                .ok_or_else(|| Error::other("cannot clone builder in retry loop".to_string()))?;
            let remaining_time = retry_policy.remaining_time(loop_start, attempt_count);
            let throttle = if attempt_count == 0 {
                false
            } else {
                let t = throttler.lock().expect("retry throttler lock is poisoned");
                t.throttle_retry_attempt()
            };
            if throttle {
                // This counts as an error for the purposes of the retry policy.
                if let Some(error) = retry_policy.on_throttle(loop_start, attempt_count) {
                    return Err(error);
                }
                let delay = backoff.on_failure(loop_start, attempt_count);
                tokio::time::sleep(delay).await;
                continue;
            }
            attempt_count += 1;
            match self.inner_request.make_request::<O>(builder, options, remaining_time).await {
                Ok(r) => {
                    throttler
                        .lock()
                        .expect("retry throttler lock is poisoned")
                        .on_success();
                    return Ok(r);
                }
                Err(e) => {
                    let flow = retry_policy.on_error(
                        loop_start,
                        attempt_count,
                        options.idempotent().unwrap_or(false),
                        e,
                    );
                    let delay = backoff.on_failure(loop_start, attempt_count);
                    {
                        throttler
                            .lock()
                            .expect("retry throttler lock is poisoned")
                            .on_retry_failure(&flow);
                    };
                    self.on_error(flow, delay).await?;
                }
            };
        }
    }

    async fn on_error(
        &self,
        retry_flow: LoopState,
        backoff_delay: std::time::Duration,
    ) -> Result<()> {
        match retry_flow {
            LoopState::Permanent(e) | LoopState::Exhausted(e) => {
                return Err(e);
            }
            LoopState::Continue(_e) => {
                tokio::time::sleep(backoff_delay).await;
            }
        }
        Ok(())
    }

    pub(crate) fn get_backoff_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn BackoffPolicy> {
        options
            .backoff_policy()
            .clone()
            .or_else(|| self.backoff_policy.clone())
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
    }

    pub(crate) fn get_retry_throttler(
        &self,
        options: &RequestOptions,
    ) -> SharedRetryThrottler {
        options
            .retry_throttler()
            .clone()
            .unwrap_or_else(|| self.retry_throttler.clone())
    }
}