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

#[cfg(test)]
mod test {
    use super::*;
    use std::error::Error;
    use test_case::test_case;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Empty {}

    #[tokio::test]
    #[test_case(reqwest::StatusCode::BAD_REQUEST, r#"{"error": "bad request"}"#.to_string(); "non_retryable")]
    #[test_case(reqwest::StatusCode::INTERNAL_SERVER_ERROR, r#"{"error": "internal error"}"#.to_string(); "retryable")]
    async fn client_credential_error(code: reqwest::StatusCode, content: String) -> TestResult {
        let http_resp = http::Response::builder()
            .header("Content-Type", "application/json")
            .status(code)
            .body(content.clone())?;
        let response: reqwest::Response = http_resp.into();
        let response = ReqwestClient::to_http_error::<()>(response).await;
        assert!(response.is_err(), "{response:?}");
        let err = response.err().unwrap();
        let err = err.as_inner::<CredentialsError>().unwrap();
        assert_eq!(err.is_retryable(), errors::is_retryable(code));

        assert_eq!(err.source().unwrap().to_string(), content);
        Ok(())
    }

    #[tokio::test]
    #[test_case(reqwest::StatusCode::OK, "{}"; "200 with empty object")]
    #[test_case(reqwest::StatusCode::NO_CONTENT, "{}"; "204 with empty object")]
    #[test_case(reqwest::StatusCode::NO_CONTENT, ""; "204 with empty content")]
    async fn client_empty_content(code: reqwest::StatusCode, content: &str) -> TestResult {
        let response = resp_from_code_content(code, content)?;
        assert!(response.status().is_success());

        let response = ReqwestClient::to_http_response::<Empty>(response).await;
        assert!(response.is_ok());

        let response = response.unwrap();
        let body = response.into_body();
        assert_eq!(body, Empty::default());
        Ok(())
    }

    #[tokio::test]
    #[test_case(reqwest::StatusCode::OK, ""; "200 with empty content")]
    async fn client_error_with_empty_content(
        code: reqwest::StatusCode,
        content: &str,
    ) -> TestResult {
        let response = resp_from_code_content(code, content)?;
        assert!(response.status().is_success());

        let response = ReqwestClient::to_http_response::<Empty>(response).await;
        assert!(response.is_err());
        Ok(())
    }

    fn resp_from_code_content(
        code: reqwest::StatusCode,
        content: &str,
    ) -> http::Result<reqwest::Response> {
        let http_resp = http::Response::builder()
            .header("Content-Type", "application/json")
            .status(code)
            .body(content.to_string())?;

        let response: reqwest::Response = http_resp.into();
        Ok(response)
    }
}
