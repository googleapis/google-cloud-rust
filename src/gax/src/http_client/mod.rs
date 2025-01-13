// Copyright 2024 Google LLC
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

use crate::backoff_policy::BackoffPolicy;
use crate::backoff_policy::ExponentialBackoff;
use crate::error::Error;
use crate::error::HttpError;
use crate::error::ServiceError;
use crate::options;
use crate::retry_policy::{RetryFlow, RetryPolicy};
use crate::retry_throttler::RetryThrottlerWrapped;
use crate::Result;
use auth::credentials::{create_access_token_credential, Credential};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ReqwestClient {
    inner: reqwest::Client,
    cred: Credential,
    endpoint: String,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: RetryThrottlerWrapped,
}

impl ReqwestClient {
    pub async fn new(config: ClientConfig, default_endpoint: &str) -> Result<Self> {
        let inner = reqwest::Client::new();
        let cred = if let Some(c) = config.cred {
            c
        } else {
            create_access_token_credential()
                .await
                .map_err(Error::authentication)?
        };
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| default_endpoint.to_string());
        Ok(Self {
            inner,
            cred,
            endpoint,
            retry_policy: config.retry_policy,
            backoff_policy: config.backoff_policy,
            retry_throttler: config.retry_throttler,
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
        options: crate::options::RequestOptions,
    ) -> Result<O> {
        let auth_headers = self
            .cred
            .get_headers()
            .await
            .map_err(Error::authentication)?;
        for header in auth_headers.into_iter() {
            builder = builder.header(header.0, header.1);
        }
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
            None => self.request_attempt::<O>(builder, &options, None).await,
            Some(policy) => self.retry_loop::<O>(builder, &options, policy).await,
        }
    }

    async fn retry_loop<O: serde::de::DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
        options: &crate::options::RequestOptions,
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
        retry_flow: crate::retry_policy::RetryFlow,
        backoff_delay: std::time::Duration,
    ) -> Result<()> {
        match retry_flow {
            RetryFlow::Permanent(e) | RetryFlow::Exhausted(e) => {
                return Err(e);
            }
            RetryFlow::Continue(_e) => {
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
        let headers = Self::convert_headers(response.headers());
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
}

#[derive(serde::Serialize)]
pub struct NoBody {}

const SENSITIVE_HEADER: &str = "[sensitive]";

pub type ClientConfig = crate::options::ClientConfig;

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn headers_empty() -> TestResult {
        let http_resp = http::Response::builder()
            .status(reqwest::StatusCode::OK)
            .body("")?;
        let response: reqwest::Response = http_resp.into();
        let got = ReqwestClient::convert_headers(response.headers());
        assert!(got.is_empty(), "{got:?}");
        Ok(())
    }

    #[test]
    fn headers_basic() -> TestResult {
        let http_resp = http::Response::builder()
            .header("content-type", "application/json")
            .header("x-test-k1", "v1")
            .status(reqwest::StatusCode::OK)
            .body("")?;
        let response: reqwest::Response = http_resp.into();
        let got = ReqwestClient::convert_headers(response.headers());
        let want = HashMap::from(
            [("content-type", "application/json"), ("x-test-k1", "v1")]
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn headers_sensitive() -> TestResult {
        let sensitive = {
            let mut h = reqwest::header::HeaderValue::from_static("abc123");
            h.set_sensitive(true);
            h
        };
        let http_resp = http::Response::builder()
            .header("content-type", "application/json")
            .header("x-test-k1", "v1")
            .header("x-sensitive", sensitive)
            .status(reqwest::StatusCode::OK)
            .body("")?;
        let response: reqwest::Response = http_resp.into();
        let got = ReqwestClient::convert_headers(response.headers());
        let want = HashMap::from(
            [
                ("content-type", "application/json"),
                ("x-test-k1", "v1"),
                ("x-sensitive", SENSITIVE_HEADER),
            ]
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(got, want);
        Ok(())
    }

    #[tokio::test]
    async fn client_http_error_bytes() -> TestResult {
        let http_resp = http::Response::builder()
            .header("Content-Type", "application/json")
            .status(400)
            .body(r#"{"error": "bad request"}"#)?;
        let response: reqwest::Response = http_resp.into();
        assert!(response.status().is_client_error());
        let response = ReqwestClient::to_http_error::<()>(response).await;
        assert!(response.is_err(), "{response:?}");
        let err = response.err().unwrap();
        let err = err.as_inner::<HttpError>().unwrap();
        assert_eq!(err.status_code(), 400);
        let want = HashMap::from(
            [("content-type", "application/json")].map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(err.headers(), &want);
        assert_eq!(
            err.payload(),
            Some(bytes::Bytes::from(r#"{"error": "bad request"}"#)).as_ref()
        );
        Ok(())
    }

    #[tokio::test]
    async fn client_error_with_status() -> TestResult {
        use crate::error::rpc::*;
        use crate::error::ServiceError;
        let status = Status {
            code: 404,
            message: "The thing is not there, oh noes!".to_string(),
            status: Some("NOT_FOUND".to_string()),
            details: vec![StatusDetails::LocalizedMessage(
                rpc::model::LocalizedMessage::default()
                    .set_locale("en-US")
                    .set_message("we searched everywhere, honest"),
            )],
        };
        let body = serde_json::json!({"error": serde_json::to_value(&status)?});
        let http_resp = http::Response::builder()
            .header("Content-Type", "application/json")
            .status(404)
            .body(body.to_string())?;
        let response: reqwest::Response = http_resp.into();
        assert!(response.status().is_client_error());
        let response = ReqwestClient::to_http_error::<()>(response).await;
        assert!(response.is_err(), "{response:?}");
        let err = response.err().unwrap();
        let err = err.as_inner::<ServiceError>().unwrap();
        assert_eq!(err.status(), &status);
        assert_eq!(err.http_status_code(), &Some(404 as u16));
        let want = HashMap::from(
            [("content-type", "application/json")].map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(err.headers(), &Some(want));
        Ok(())
    }
}
