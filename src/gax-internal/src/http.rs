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

use auth::credentials::CacheableResource;
use auth::credentials::Credentials;
use gax::Result;
use gax::backoff_policy::BackoffPolicy;
use gax::client_builder::Error as BuilderError;
use gax::error::Error;
use gax::exponential_backoff::ExponentialBackoff;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::Aip194Strict;
use gax::polling_error_policy::PollingErrorPolicy;
use gax::response::{Parts, Response};
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::SharedRetryThrottler;
use http::Extensions;
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
    pub async fn new(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
    ) -> gax::client_builder::Result<Self> {
        let cred = Self::make_credentials(&config).await?;
        let inner = reqwest::Client::new();
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
                reqwest::header::HeaderValue::from_str(user_agent).map_err(Error::ser)?,
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

    async fn make_credentials(
        config: &crate::options::ClientConfig,
    ) -> gax::client_builder::Result<auth::credentials::Credentials> {
        if let Some(c) = config.cred.clone() {
            return Ok(c);
        }
        auth::credentials::Builder::default()
            .build()
            .map_err(BuilderError::cred)
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
                .expect("client libraries only create builders where `try_clone()` succeeds");
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
        let cached_auth_headers = self
            .cred
            .headers(Extensions::new())
            .await
            .map_err(Error::authentication)?;

        let auth_headers = match cached_auth_headers {
            CacheableResource::New { data, .. } => Ok(data),
            CacheableResource::NotModified => {
                unreachable!("headers are not cached");
            }
        };

        let auth_headers = auth_headers?;
        for (key, value) in auth_headers.iter() {
            builder = builder.header(key, value);
        }
        let response = builder.send().await.map_err(Self::map_send_error)?;
        if !response.status().is_success() {
            return self::to_http_error(response).await;
        }

        self::to_http_response(response).await
    }

    fn map_send_error(err: reqwest::Error) -> Error {
        match err {
            e if e.is_timeout() => Error::timeout(e),
            e => Error::io(e),
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

pub async fn to_http_error<O>(response: reqwest::Response) -> Result<O> {
    let status_code = response.status().as_u16();
    let response = http::Response::from(response);
    let (parts, body) = response.into_parts();

    let body = http_body_util::BodyExt::collect(body)
        .await
        .map_err(Error::io)?
        .to_bytes();

    let error = match gax::error::rpc::Status::try_from(&body) {
        Ok(status) => {
            Error::service_with_http_metadata(status, Some(status_code), Some(parts.headers))
        }
        Err(_) => Error::http(status_code, parts.headers, body),
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
        content => serde_json::from_slice::<O>(&content).map_err(Error::deser)?,
    };

    Ok(Response::from_parts(
        Parts::new().set_headers(parts.headers),
        response,
    ))
}

#[cfg(test)]
mod test {
    use http::{HeaderMap, HeaderValue};
    use test_case::test_case;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn client_http_error_bytes() -> TestResult {
        let http_resp = http::Response::builder()
            .header("Content-Type", "application/json")
            .status(400)
            .body(r#"{"error": "bad request"}"#)?;
        let response: reqwest::Response = http_resp.into();
        assert!(response.status().is_client_error());
        let response = super::to_http_error::<()>(response).await;
        assert!(response.is_err(), "{response:?}");
        let err = response.err().unwrap();
        assert_eq!(err.http_status_code(), Some(400));
        let mut want = HeaderMap::new();
        want.insert("content-type", HeaderValue::from_static("application/json"));
        assert_eq!(err.http_headers(), Some(&want));
        assert_eq!(
            err.http_payload(),
            Some(bytes::Bytes::from(r#"{"error": "bad request"}"#)).as_ref()
        );
        Ok(())
    }

    #[tokio::test]
    async fn client_error_with_status() -> TestResult {
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
        let response = super::to_http_error::<()>(response).await;
        assert!(response.is_err(), "{response:?}");
        let err = response.err().unwrap();
        let want_status = Status::default()
            .set_code(Code::NotFound)
            .set_message("The thing is not there, oh noes!")
            .set_details([LocalizedMessage(
                rpc::model::LocalizedMessage::new()
                    .set_locale("en-US")
                    .set_message("we searched everywhere, honest"),
            )]);
        assert_eq!(err.status(), Some(&want_status));
        assert_eq!(err.http_status_code(), Some(404_u16));
        let mut want = HeaderMap::new();
        want.insert("content-type", HeaderValue::from_static("application/json"));
        assert_eq!(err.http_headers(), Some(&want));
        Ok(())
    }

    #[tokio::test]
    #[test_case(reqwest::StatusCode::OK, "{}"; "200 with empty object")]
    #[test_case(reqwest::StatusCode::NO_CONTENT, "{}"; "204 with empty object")]
    #[test_case(reqwest::StatusCode::NO_CONTENT, ""; "204 with empty content")]
    async fn client_empty_content(code: reqwest::StatusCode, content: &str) -> TestResult {
        let response = resp_from_code_content(code, content)?;
        assert!(response.status().is_success());

        let response = super::to_http_response::<wkt::Empty>(response).await;
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

        let response = super::to_http_response::<wkt::Empty>(response).await;
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
