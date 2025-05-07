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

use crate::Result as CredentialResult;
use crate::errors::{self, CredentialsError, is_retryable};
use gax::Result;
use gax::backoff_policy::BackoffPolicy;
use gax::error::Error as GaxError;
use gax::exponential_backoff::ExponentialBackoff;
use gax::response::{Parts, Response};
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::AdaptiveThrottler;
use gax::retry_throttler::SharedRetryThrottler;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct ReqwestClient {
    inner: reqwest::Client,
    endpoint: String,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
}

pub(crate) struct Builder {
    inner: reqwest::Client,
    endpoint: String,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
}

impl Builder {
    pub(crate) fn new(endpoint: String) -> Self {
        let inner = reqwest::Client::new();
        Self {
            inner,
            endpoint,
            retry_policy: None,
            backoff_policy: None,
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),
        }
    }

    pub(crate) fn with_retry_policy(mut self, retry_policy: Arc<dyn RetryPolicy>) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    pub(crate) fn with_backoff_policy(mut self, backoff_policy: Arc<dyn BackoffPolicy>) -> Self {
        self.backoff_policy = Some(backoff_policy);
        self
    }

    pub(crate) fn with_retry_throttler(mut self, retry_throttler: SharedRetryThrottler) -> Self {
        self.retry_throttler = retry_throttler;
        self
    }

    pub(crate) fn build(self) -> ReqwestClient {
        ReqwestClient {
            inner: self.inner,
            endpoint: self.endpoint,
            retry_policy: self.retry_policy,
            backoff_policy: self.backoff_policy,
            retry_throttler: self.retry_throttler,
        }
    }
}

impl ReqwestClient {
    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub fn prepare_request(
        &self,
        method: reqwest::Method,
        path: String,
    ) -> reqwest::RequestBuilder {
        self.inner
            .request(method, format!("{}{path}", &self.endpoint))
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned + Default>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
    ) -> CredentialResult<Response<O>> {
        if let Some(body) = body {
            builder = builder.json(&body);
        }

        match self.retry_policy.clone() {
            None => self.request_attempt::<O>(builder, None).await,
            Some(policy) => self.retry_loop::<O>(builder, policy).await,
        }
        .map_err(|gax_error| gax_error.as_inner::<CredentialsError>().unwrap().clone())
    }

    async fn retry_loop<O: serde::de::DeserializeOwned + Default>(
        &self,
        builder: reqwest::RequestBuilder,
        retry_policy: Arc<dyn RetryPolicy>,
    ) -> Result<Response<O>> {
        let throttler = self.retry_throttler.clone();
        let backoff = self.get_backoff_policy();
        let this = self.clone();
        let inner = async move |d| {
            let builder = builder
                .try_clone()
                .ok_or_else(|| GaxError::other("cannot clone builder in retry loop".to_string()))?;
            this.request_attempt(builder, d).await
        };
        let sleep = async |d| tokio::time::sleep(d).await;
        gax::retry_loop_internal::retry_loop(inner, sleep, true, throttler, retry_policy, backoff)
            .await
    }

    async fn request_attempt<O: serde::de::DeserializeOwned + Default>(
        &self,
        mut builder: reqwest::RequestBuilder,
        remaining_time: Option<std::time::Duration>,
    ) -> Result<Response<O>> {
        if let Some(remaining_time) = remaining_time {
            builder = builder.timeout(remaining_time);
        }

        let response = builder
            .send()
            .await
            .map_err(GaxError::io)
            .map_err(errors::retryable)
            .map_err(GaxError::authentication)?;
        if !response.status().is_success() {
            return Self::to_http_error(response).await;
        }
        Self::to_http_response(response).await
    }

    async fn to_http_error<O>(response: reqwest::Response) -> Result<O> {
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(GaxError::io)
            .map_err(|e| CredentialsError::new(is_retryable(status), e))
            .map_err(GaxError::authentication)?;

        let credential_error =
            GaxError::authentication(CredentialsError::from_str(is_retryable(status), body));

        Err(credential_error)
    }

    async fn to_http_response<O: serde::de::DeserializeOwned + Default>(
        response: reqwest::Response,
    ) -> Result<Response<O>> {
        // 204 No Content has no body and throws EOF error if we try to parse with serde::json
        let no_content_status = response.status() == reqwest::StatusCode::NO_CONTENT;
        let response = http::Response::from(response);
        let (parts, body) = response.into_parts();

        let body = http_body_util::BodyExt::collect(body)
            .await
            .map_err(GaxError::io)
            .map_err(errors::retryable)
            .map_err(GaxError::authentication)?;

        let response = match body.to_bytes() {
            content if (content.is_empty() && no_content_status) => O::default(),
            content => serde_json::from_slice::<O>(&content)
                .map_err(GaxError::serde)
                .map_err(errors::non_retryable)
                .map_err(GaxError::authentication)?,
        };

        Ok(Response::from_parts(
            Parts::new().set_headers(parts.headers),
            response,
        ))
    }

    pub(crate) fn get_backoff_policy(&self) -> Arc<dyn BackoffPolicy> {
        self.backoff_policy
            .clone()
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
    }
}

#[doc(hidden)]
#[derive(serde::Serialize, Default)]
pub(crate) struct NoBody {}
