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

use gax::Result;
use gax::backoff_policy::BackoffPolicy;
use gax::error::Error;
use gax::error::HttpError;
use gax::error::ServiceError;
use gax::exponential_backoff::ExponentialBackoff;
use gax::loop_state::LoopState;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::Aip194Strict;
use gax::polling_error_policy::PollingErrorPolicy;
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::SharedRetryThrottler;
use std::sync::Arc;
use gax::retry_loop::RetryLoop;
use gax::retry_loop::InnerRequestTrait;

#[derive(Debug, Clone)]
pub struct AuthInnerRequest;

impl AuthInnerRequest {
    // These helper methods could be put in a common place as well in gax
    async fn to_http_error<O>(response: reqwest::Response) -> Result<O> {
        let status_code = response.status().as_u16();
        let headers = Self::convert_headers(response.headers());
        let body = response.bytes().await.map_err(Error::io)?;
        let error = if let Ok(status) = gax::error::rpc::Status::try_from(&body) {
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

    fn convert_headers(
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

    fn effective_timeout(
        options: &gax::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
    ) -> Option<std::time::Duration> {
        match (options.attempt_timeout(), remaining_time) {
            (None, None) => None,
            (None, Some(t)) => Some(t),
            (Some(t), None) => Some(*t),
            (Some(a), Some(r)) => Some(*std::cmp::min(a, &r)),
        }
    }
}

#[async_trait::async_trait]
impl InnerRequestTrait for AuthInnerRequest {
    async fn make_request<O: serde::de::DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
        options: &gax::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
    ) -> Result<O> {
        let mut builder = builder;
        builder = Self::effective_timeout(options, remaining_time)
            .into_iter()
            .fold(builder, |b, t| b.timeout(t));
        
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return Self::to_http_error(response).await;
        }
        let response = response.json::<O>().await.map_err(Error::serde)?;
        Ok(response)
    }
}

pub struct AuthClientConfig {
    endpoint: Option<String>,
    tracing: bool,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
    polling_error_policy: Option<Arc<dyn PollingErrorPolicy>>,
    polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
}


#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ReqwestClient{
    inner: reqwest::Client,
    retry_loop: RetryLoop<AuthInnerRequest>,
    inner_request: AuthInnerRequest,
    endpoint: String,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
    polling_error_policy: Option<Arc<dyn PollingErrorPolicy>>,
    polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
}

impl ReqwestClient {
    pub async fn new(config: AuthClientConfig, default_endpoint: &str) -> Result<Self> {
        let inner = reqwest::Client::new();
        
        let endpoint = config
            .endpoint()
            .clone()
            .unwrap_or_else(|| default_endpoint.to_string());

        let inner_request = AuthInnerRequest { };

        let retry_loop = RetryLoop {
            retry_policy: config.retry_policy().clone(),
            backoff_policy: config.backoff_policy().clone(),
            retry_throttler: config.retry_throttler(),
            inner_request: inner_request.clone(),
        };

        Ok(Self {
            inner,
            retry_loop,
            inner_request,
            endpoint,
            retry_policy: config.retry_policy().clone(),
            backoff_policy: config.backoff_policy().clone(),
            retry_throttler: config.retry_throttler(),
            polling_error_policy: config.polling_error_policy().clone(),
            polling_backoff_policy: config.polling_backoff_policy().clone(),

        })
    }

    pub fn builder(&self, method: reqwest::Method, path: String) -> reqwest::RequestBuilder {
        self.inner
            .request(method, format!("{}{path}", &self.endpoint))
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
    ) -> Result<O> {
        if let Some(user_agent) = options.user_agent() {
            builder = builder.header(
                reqwest::header::USER_AGENT,
                reqwest::header::HeaderValue::from_str(user_agent).map_err(Error::other)?,
            );
        }
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        match self.get_retry_policy(&options) {
            None => self.inner_request.make_request::<O>(builder, &options, None).await,
            Some(policy) => self.retry_loop.retry_loop::<O>(builder, &options, policy).await,
        }
    }

    fn get_retry_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> Option<Arc<dyn RetryPolicy>> {
        options
            .retry_policy()
            .clone()
            .or_else(|| self.retry_policy.clone())
    }
}

#[doc(hidden)]
#[derive(serde::Serialize)]
pub struct NoBody {}

const SENSITIVE_HEADER: &str = "[sensitive]";