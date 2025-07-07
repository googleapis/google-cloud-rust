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

use crate::constants;
use crate::token::{Token, TokenProvider};
use gax::backoff_policy::BackoffPolicy;
use gax::error::CredentialsError;
use gax::exponential_backoff::ExponentialBackoff;
use gax::retry_loop_internal::retry_loop;
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::{AdaptiveThrottler, SharedRetryThrottler};
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::time::sleep;

type Result<T> = std::result::Result<T, CredentialsError>;

#[derive(Debug)]
pub(crate) struct TokenProviderWithRetry<T: TokenProvider> {
    inner: Arc<T>,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    retry_throttler: SharedRetryThrottler,
}

#[derive(Debug)]
pub(crate) struct Builder<T: TokenProvider> {
    inner: Arc<T>,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
}

impl<T: TokenProvider> Builder<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
            retry_policy: None,
            backoff_policy: None,
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
        let backoff_policy = self.backoff_policy.unwrap_or_else(|| {
            Arc::new(ExponentialBackoff::default())
        });
        TokenProviderWithRetry {
            inner: self.inner,
            retry_policy: self.retry_policy,
            backoff_policy,
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),
        }
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider + Send + Sync + 'static> TokenProvider for TokenProviderWithRetry<T> {
    async fn token(&self) -> Result<Token> {
        match self.retry_policy.clone() {
            None => self.inner.token().await,
            Some(policy) => self.execute_retry_loop(policy).await,
        }
    }
}

impl<T> TokenProviderWithRetry<T>
where
    T: TokenProvider,
{
    async fn execute_retry_loop(&self, retry_policy: Arc<dyn RetryPolicy>) -> Result<Token> {
        let inner = self.inner.clone();
        let sleep = async |d| tokio::time::sleep(d).await;
        retry_loop(
            move |_| {
                let inner = inner.clone();
                async move {
                    inner
                        .token()
                        .await
                        .map_err(gax::error::Error::authentication)
                }
            },
            sleep,
            true, // token fetching is idempotent
            self.retry_throttler.clone(),
            retry_policy,
            self.backoff_policy.clone(),
        )
        .await
        .map_err(|e| {
            if !e.is_authentication() {
                return CredentialsError::from_source(false, e);
            }
            let (is_transient, msg) = if e
                .source()
                .and_then(|e| e.downcast_ref::<CredentialsError>())
                .map_or(false, |ce| ce.is_transient())
            {
                (true, constants::RETRY_EXHAUSTED_ERROR)
            } else {
                (false, constants::TOKEN_FETCH_FAILED_ERROR)
            };
            CredentialsError::new(is_transient, msg, e)
        })
    }
}

