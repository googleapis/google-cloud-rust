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

use auth::credentials::{Builder as AccessTokenCredentialBuilder, Credentials};
use gax::Result;
use gax::backoff_policy::BackoffPolicy;
use gax::error::Error;
use gax::error::HttpError;
use gax::error::ServiceError;
use gax::exponential_backoff::ExponentialBackoff;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::Aip194Strict;
use gax::polling_error_policy::PollingErrorPolicy;
use gax::response::{Parts, Response};
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::SharedRetryThrottler;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ReqwestClient {
    inner: reqwest::Client,
    cred: Credentials,
    endpoint: String,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
    polling_error_policy: Option<Arc<dyn PollingErrorPolicy>>,
    polling_backoff_policy: Option<Arc<dyn PollingBackoffPolicy>>,
}

impl ReqwestClient {
    pub async fn new(config: crate::options::ClientConfig, default_endpoint: &str) -> Result<Self> {
        let inner = reqwest::Client::new();
        let cred = if let Some(c) = config.cred.clone() {
            c
        } else {
            AccessTokenCredentialBuilder::default()
                .build()
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
            polling_error_policy: config.polling_error_policy,
            polling_backoff_policy: config.polling_backoff_policy,
        })
    }

    pub fn builder(&self, method: reqwest::Method, path: String) -> reqwest::RequestBuilder {
        self.inner
            .request(method, format!("{}{path}", &self.endpoint))
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned + Default>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
        options: gax::options::RequestOptions,
    ) -> Result<Response<O>> {
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
            Some(policy) => self.retry_loop::<O>(builder, options, policy).await,
        }
    }

    async fn retry_loop<O: serde::de::DeserializeOwned + Default>(
        &self,
        builder: reqwest::RequestBuilder,
        options: gax::options::RequestOptions,
        retry_policy: Arc<dyn RetryPolicy>,
    ) -> Result<Response<O>> {
        let idempotent = options.idempotent().unwrap_or(false);
        let throttler = self.get_retry_throttler(&options);
        let backoff = self.get_backoff_policy(&options);
        let this = self.clone();
        let inner = async move |d| {
            let builder = builder
                .try_clone()
                .ok_or_else(|| Error::other("cannot clone builder in retry loop".to_string()))?;
            this.request_attempt(builder, &options, d).await
        };
        let sleep = async |d| tokio::time::sleep(d).await;
        gax::retry_loop_internal::retry_loop(
            inner,
            sleep,
            idempotent,
            throttler,
            retry_policy,
            backoff,
        )
        .await
    }

    async fn request_attempt<O: serde::de::DeserializeOwned + Default>(
        &self,
        mut builder: reqwest::RequestBuilder,
        options: &gax::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
    ) -> Result<Response<O>> {
        builder = gax::retry_loop_internal::effective_timeout(options, remaining_time)
            .into_iter()
            .fold(builder, |b, t| b.timeout(t));
        let auth_headers = self.cred.headers().await.map_err(Error::authentication)?;
        for (key, value) in auth_headers.into_iter() {
            builder = builder.header(key, value);
        }
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return Self::to_http_error(response).await;
        }

        Self::to_http_response(response).await
    }

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

    async fn to_http_response<O: serde::de::DeserializeOwned + Default>(
        response: reqwest::Response,
    ) -> Result<Response<O>> {
        // 204 No Content has no body and throws EOF error if we try to parse with serde::json
        let no_content_status = response.status() == reqwest::StatusCode::NO_CONTENT;
        let response = http::Response::from(response);
        let (parts, body) = response.into_parts();

        let body = http_body_util::BodyExt::collect(body)
            .await
            .map_err(Error::io)?;

        let response = match body.to_bytes() {
            content if (content.is_empty() && no_content_status) => O::default(),
            content => serde_json::from_slice::<O>(&content).map_err(Error::serde)?,
        };

        Ok(Response::from_parts(
            Parts::new().set_headers(parts.headers),
            response,
        ))
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

    fn get_retry_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> Option<Arc<dyn RetryPolicy>> {
        options
            .retry_policy()
            .clone()
            .or_else(|| self.retry_policy.clone())
    }

    pub(crate) fn get_backoff_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> Arc<dyn BackoffPolicy> {
        options
            .backoff_policy()
            .clone()
            .or_else(|| self.backoff_policy.clone())
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
    }

    pub(crate) fn get_retry_throttler(
        &self,
        options: &gax::options::RequestOptions,
    ) -> SharedRetryThrottler {
        options
            .retry_throttler()
            .clone()
            .unwrap_or_else(|| self.retry_throttler.clone())
    }

    pub fn get_polling_error_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> Arc<dyn PollingErrorPolicy> {
        options
            .polling_error_policy()
            .clone()
            .or_else(|| self.polling_error_policy.clone())
            .unwrap_or_else(|| Arc::new(Aip194Strict))
    }

    pub fn get_polling_backoff_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> Arc<dyn PollingBackoffPolicy> {
        options
            .polling_backoff_policy()
            .clone()
            .or_else(|| self.polling_backoff_policy.clone())
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
    }
}

#[derive(serde::Serialize)]
pub struct NoBody;

const SENSITIVE_HEADER: &str = "[sensitive]";

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;
    use test_case::test_case;
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
        use gax::error::ServiceError;
        use gax::error::rpc::{Code, Status, StatusDetails::LocalizedMessage};
        let body = serde_json::json!({"error": {
            "code": 404,
            "message": "The thing is not there, oh noes!",
            "status": "NOT_FOUND",
            "details": [{
                    "@type": "type.googleapis.com/google.rpc.LocalizedMessage",
                    "locale": "en-US",
                    "message": "we searched everywhere, honest",
                }]
        }});
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
        let want_status = Status::default()
            .set_code(Code::NotFound)
            .set_message("The thing is not there, oh noes!")
            .set_details([LocalizedMessage(
                rpc::model::LocalizedMessage::new()
                    .set_locale("en-US")
                    .set_message("we searched everywhere, honest"),
            )]);
        assert_eq!(err.status(), &want_status);
        assert_eq!(err.http_status_code(), &Some(404_u16));
        let want = HashMap::from(
            [("content-type", "application/json")].map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(err.headers(), &Some(want));
        Ok(())
    }

    #[tokio::test]
    #[test_case(reqwest::StatusCode::OK, "{}"; "200 with empty object")]
    #[test_case(reqwest::StatusCode::NO_CONTENT, "{}"; "204 with empty object")]
    #[test_case(reqwest::StatusCode::NO_CONTENT, ""; "204 with empty content")]
    async fn client_empty_content(code: reqwest::StatusCode, content: &str) -> TestResult {
        let response = resp_from_code_content(code, content)?;
        assert!(response.status().is_success());

        let response = ReqwestClient::to_http_response::<wkt::Empty>(response).await;
        assert!(response.is_ok());

        let response = response.unwrap();
        let body = response.into_body();
        assert_eq!(body, wkt::Empty::default());
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

        let response = ReqwestClient::to_http_response::<wkt::Empty>(response).await;
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
