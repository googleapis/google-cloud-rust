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

use crate::token::{Token, TokenProvider};
use crate::{errors, Result};
use gax::backoff_policy::{self, BackoffPolicy};
use gax::retry_loop_internal::retry_loop;
use gax::retry_policy::{self, RetryPolicy};
use gax::retry_throttler::{self, SharedRetryThrottler, AdaptiveThrottler};
use gax::exponential_backoff::ExponentialBackoff;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug)]
pub(crate) struct TokenProviderWithRetry<T: TokenProvider> {
    inner: T,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    retry_throttler: SharedRetryThrottler,
}

#[derive(Debug)]
pub(crate) struct Builder<T: TokenProvider> {
    inner: T,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: Option<SharedRetryThrottler>,
}

impl<T: TokenProvider> Builder<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner,
            retry_policy: None,
            backoff_policy: None,
            retry_throttler: None
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_retry_policy(mut self, retry_policy: Arc<dyn RetryPolicy>) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_backoff_policy(mut self, backoff_policy: Arc<dyn BackoffPolicy>) -> Self {
        self.backoff_policy = Some(backoff_policy);
        self
    }

    pub(crate) fn build(self) -> TokenProviderWithRetry<T> {
        let retry_throttler = self.retry_throttler.unwrap_or_else(|| {
            Arc::new(Mutex::new(AdaptiveThrottler::default()))
        });
        let backoff_policy = self.backoff_policy.unwrap_or_else(|| Arc::new(ExponentialBackoff::default()));
        TokenProviderWithRetry {
            inner: self.inner,
            retry_policy: self.retry_policy,
            backoff_policy: backoff_policy,
            retry_throttler: retry_throttler,
        }
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider> TokenProvider for TokenProviderWithRetry<T> {
    async fn token(&self) -> Result<Token> {

        match self.retry_policy.clone() {
            None => self.inner.token().await,
            Some(policy) => self.retry_loop(policy).await,
        }
    }
}

impl<T> TokenProviderWithRetry<T> where T: TokenProvider {
    async fn retry_loop(&self, retry_policy: Arc<dyn RetryPolicy>) -> Result<Token> {
        let throttler = self.retry_throttler.clone();
        let backoff = self.backoff_policy.clone();
        let this = self.clone();
        let sleep = async |d| tokio::time::sleep(d).await;
        gax::retry_loop_internal::retry_loop(self.inner.token, sleep, true, throttler, retry_policy, backoff)
            .await
    }
}