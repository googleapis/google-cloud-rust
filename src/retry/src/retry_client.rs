use std::sync::Arc;
use http::header::{HeaderName, HeaderValue};
use crate::backoff_policy::BackoffPolicy;
use crate::error::Error;
use crate::error::HttpError;
use crate::error::ServiceError;
use crate::exponential_backoff::ExponentialBackoff;
use crate::loop_state::LoopState;
use crate::options;
use crate::polling_backoff_policy::PollingBackoffPolicy;
use crate::polling_policy::Aip194Strict;
use crate::polling_policy::PollingPolicy;
use crate::retry_policy::RetryPolicy;
use crate::retry_throttler::RetryThrottlerWrapped;
use crate::Result;
use std::future::Future;
use std::pin::Pin;
use std::fmt;

/// A trait for getting authentication headers.
pub trait GetAuthHeaders: Send + Sync + fmt::Debug {
    /// Returns a future that resolves to a `Result` containing a vector of
    /// `(HeaderName, HeaderValue)` tuples.
    fn get_auth_headers(&self) -> Pin<Box<dyn Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send>>;
}

impl<F, Fut> GetAuthHeaders for F
where
    F: Fn() -> Fut + Send + Sync + fmt::Debug + 'static,
    Fut: Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send + 'static,
{
    fn get_auth_headers(&self) -> Pin<Box<dyn Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send>> { Box::pin(self()) }
}

#[derive(Clone, Debug)]
pub struct RetryClient{
    pub retry_policy: Option<Arc<dyn RetryPolicy>>,
    pub backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    pub retry_throttler: RetryThrottlerWrapped,
    pub polling_policy: Option<Arc<dyn PollingPolicy>>,
    pub polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
}

impl RetryClient{
    pub fn new() -> Self {
        use crate::retry_throttler::AdaptiveThrottler;
        use std::sync::{Arc, Mutex};
        Self {
            retry_policy: None,
            backoff_policy: None,
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),
            polling_policy: None,
            polling_backoff_policy: None,
        }
    }


    pub async fn request<O: serde::de::DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
        options: &options::RequestOptions,
    ) -> crate::Result<O> {
        match self.get_retry_policy(&options) {
            None => self.request_attempt::<O>(builder, &options, None).await,
            Some(policy) => self.retry_loop::<O>(builder, &options, policy).await,
        }
    }
        

    async fn retry_loop<O: serde::de::DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
        options: &options::RequestOptions,
        retry_policy: Arc<dyn RetryPolicy>,
    ) -> crate::Result<O> {
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
            match self.request_attempt(builder, options, remaining_time).await {
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
                        options.idempotent.unwrap_or(false),
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

    async fn request_attempt<O: serde::de::DeserializeOwned>(
        &self,
        mut builder: reqwest::RequestBuilder,
        options: &crate::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
    ) -> Result<O> {
        if let Some(timeout) = options
            .attempt_timeout()
            .map(|t| remaining_time.map(|r| std::cmp::min(t, r)).unwrap_or(t))
        {
            builder = builder.timeout(timeout);
        }

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return Self::to_http_error(response).await;
        }
        let response = response.json::<O>().await.map_err(Error::serde)?;
        Ok(response)
    }

    async fn to_http_error<O>(response: reqwest::Response) -> Result<O> {
        let status_code = response.status().as_u16();
        let headers = convert_headers(response.headers());
        let body = response.bytes().await.map_err(Error::io)?;
        let error = if let Ok(status) = crate::error::rpc::Status::try_from(&body) {
            Error::rpc(
                ServiceError::from(status)
                    .with_headers(headers)
                    .with_http_status_code(status_code),
            )
        } else {
            Error::rpc(HttpError::new(status_code, headers, Some(body)))
        };
        Err(error)
    }

    fn get_retry_policy(&self, options: &options::RequestOptions) -> Option<Arc<dyn RetryPolicy>> {
        options
            .retry_policy
            .clone()
            .or_else(|| self.retry_policy.clone())
    }

    pub(crate) fn get_backoff_policy(
        &self,
        options: &options::RequestOptions,
    ) -> Arc<dyn BackoffPolicy> {
        options
            .backoff_policy
            .clone()
            .or_else(|| self.backoff_policy.clone())
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
    }

    pub(crate) fn get_retry_throttler(
        &self,
        options: &options::RequestOptions,
    ) -> RetryThrottlerWrapped {
        options
            .retry_throttler
            .clone()
            .unwrap_or_else(|| self.retry_throttler.clone())
    }

    pub fn get_polling_policy(
        &self,
        options: &options::RequestOptions,
    ) -> Arc<dyn crate::polling_policy::PollingPolicy> {
        options
            .polling_policy
            .clone()
            .or_else(|| self.polling_policy.clone())
            .unwrap_or_else(|| Arc::new(Aip194Strict))
    }

    pub fn get_polling_backoff_policy(
        &self,
        options: &options::RequestOptions,
    ) -> Arc<dyn crate::polling_backoff_policy::PollingBackoffPolicy> {
        options
            .polling_backoff_policy
            .clone()
            .or_else(|| self.polling_backoff_policy.clone())
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
    }
}

pub fn convert_headers(
    header_map: &reqwest::header::HeaderMap,
) -> std::collections::HashMap<String, String> {
    let mut headers = std::collections::HashMap::new();
    for (key, value) in header_map {
        if value.is_sensitive() {
            headers.insert(key.to_string(), SENSITIVE_HEADER.to_string());
        } else if let Ok(value) = value.to_str() {
            headers.insert(key.to_string(), value.to_string());
        }
    }
    headers
}

pub const SENSITIVE_HEADER: &str = "[sensitive]";