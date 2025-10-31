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

#[cfg(google_cloud_unstable_tracing)]
use crate::observability::{create_http_attempt_span, record_http_response_attributes};
use auth::credentials::{CacheableResource, Credentials};
use gax::Result;
use gax::backoff_policy::BackoffPolicy;
use gax::client_builder::Error as BuilderError;
use gax::error::Error;
use gax::exponential_backoff::ExponentialBackoff;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::{Aip194Strict as PollingAip194Strict, PollingErrorPolicy};
use gax::response::{Parts, Response};
use gax::retry_policy::{Aip194Strict as RetryAip194Strict, RetryPolicy, RetryPolicyExt as _};
use gax::retry_throttler::SharedRetryThrottler;
use http::{Extensions, Method};
use std::sync::Arc;
use std::time::Duration;
#[cfg(google_cloud_unstable_tracing)]
use tracing::Instrument;

#[derive(Clone, Debug)]
pub struct ReqwestClient {
    inner: reqwest::Client,
    cred: Credentials,
    endpoint: String,
    host: String,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    retry_throttler: SharedRetryThrottler,
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
    instrumentation: Option<&'static crate::options::InstrumentationClientInfo>,
    _tracing_enabled: bool,
}

impl ReqwestClient {
    pub async fn new(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
    ) -> gax::client_builder::Result<Self> {
        let cred = Self::make_credentials(&config).await?;
        let inner = reqwest::Client::new();
        let host = crate::host::from_endpoint(
            config.endpoint.as_deref(),
            default_endpoint,
            |_origin, host| host,
        )?;
        let tracing_enabled = crate::options::tracing_enabled(&config);
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| default_endpoint.to_string());
        Ok(Self {
            inner,
            cred,
            endpoint,
            host,
            retry_policy: config.retry_policy.unwrap_or_else(|| {
                Arc::new(
                    RetryAip194Strict
                        .with_attempt_limit(10)
                        .with_time_limit(Duration::from_secs(60)),
                )
            }),
            backoff_policy: config
                .backoff_policy
                .unwrap_or_else(|| Arc::new(ExponentialBackoff::default())),
            retry_throttler: config.retry_throttler,
            polling_error_policy: config
                .polling_error_policy
                .unwrap_or_else(|| Arc::new(PollingAip194Strict)),
            polling_backoff_policy: config
                .polling_backoff_policy
                .unwrap_or_else(|| Arc::new(ExponentialBackoff::default())),
            instrumentation: None,
            _tracing_enabled: tracing_enabled,
        })
    }

    pub fn with_instrumentation(
        mut self,
        instrumentation: &'static crate::options::InstrumentationClientInfo,
    ) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    pub fn builder(&self, method: Method, path: String) -> reqwest::RequestBuilder {
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
        builder = builder.header(reqwest::header::HOST, &self.host);
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        self.retry_loop::<O>(builder, options).await
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
    ) -> Result<Response<O>> {
        let idempotent = options.idempotent().unwrap_or(false);
        let throttler = self.get_retry_throttler(&options);
        let retry = self.get_retry_policy(&options);
        let backoff = self.get_backoff_policy(&options);
        let this = self.clone();

        let mut attempt_count = 0u32;

        let inner = async move |d| {
            let builder = builder
                .try_clone()
                .expect("client libraries only create builders where `try_clone()` succeeds");

            let current_attempt = attempt_count;
            attempt_count += 1;

            this.request_attempt(builder, &options, d, current_attempt)
                .await
        };
        let sleep = async |d| tokio::time::sleep(d).await;
        gax::retry_loop_internal::retry_loop(inner, sleep, idempotent, throttler, retry, backoff)
            .await
    }

    async fn request_attempt<O: serde::de::DeserializeOwned + Default>(
        &self,
        mut builder: reqwest::RequestBuilder,
        options: &gax::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
        _attempt_count: u32,
    ) -> Result<Response<O>> {
        builder = gax::retry_loop_internal::effective_timeout(options, remaining_time)
            .into_iter()
            .fold(builder, |b, t| b.timeout(t));
        builder = match self.cred.headers(Extensions::new()).await {
            Err(e) => return Err(Error::authentication(e)),
            Ok(CacheableResource::New { data, .. }) => builder.headers(data),
            Ok(CacheableResource::NotModified) => unreachable!("headers are not cached"),
        };

        let request = builder.build().map_err(map_send_error)?;

        #[cfg(google_cloud_unstable_tracing)]
        let (reqwest_result, span) = if self._tracing_enabled {
            let span =
                create_http_attempt_span(&request, options, self.instrumentation, _attempt_count);
            // The instrument call ensures the span is entered/exited as the execute future is polled.
            let result = self.inner.execute(request).instrument(span.clone()).await;
            (result, Some(span))
        } else {
            (self.inner.execute(request).await, None)
        };
        #[cfg(not(google_cloud_unstable_tracing))]
        let reqwest_result = self.inner.execute(request).await;

        let intermediate_result = reqwest_result.map_err(map_send_error);

        #[cfg(google_cloud_unstable_tracing)]
        if let Some(s) = span {
            record_http_response_attributes(&s, &intermediate_result);
            // Span 's' is dropped here, ending it before decoding starts.
        }

        match intermediate_result {
            Ok(response) => {
                if !response.status().is_success() {
                    self::to_http_error(response).await
                } else {
                    self::to_http_response(response).await
                }
            }
            Err(err) => Err(err),
        }
    }

    fn get_retry_policy(&self, options: &gax::options::RequestOptions) -> Arc<dyn RetryPolicy> {
        options
            .retry_policy()
            .clone()
            .unwrap_or_else(|| self.retry_policy.clone())
    }

    pub(crate) fn get_backoff_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> Arc<dyn BackoffPolicy> {
        options
            .backoff_policy()
            .clone()
            .unwrap_or_else(|| self.backoff_policy.clone())
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
            .unwrap_or_else(|| self.polling_error_policy.clone())
    }

    pub fn get_polling_backoff_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> Arc<dyn PollingBackoffPolicy> {
        options
            .polling_backoff_policy()
            .clone()
            .unwrap_or_else(|| self.polling_backoff_policy.clone())
    }
}

fn as_inner<E>(error: &reqwest::Error) -> Option<&E>
where
    E: std::error::Error + 'static,
{
    use std::error::Error as _;
    let mut e = error.source()?;
    // Prevent infinite loops due to cycles in the `source()` errors. This seems
    // unlikely, and it would require effort to create, but it is easy to
    // prevent.
    for _ in 0..32 {
        if let Some(value) = e.downcast_ref::<E>() {
            return Some(value);
        }
        e = e.source()?;
    }
    None
}

pub fn map_send_error(err: reqwest::Error) -> Error {
    if let Some(e) = as_inner::<hyper::Error>(&err) {
        if e.is_user() {
            return Error::ser(err);
        }
    }
    match err {
        e if e.is_connect() => Error::connect(e),
        e if e.is_timeout() => Error::timeout(e),
        e => Error::io(e),
    }
}

#[derive(Default, serde::Serialize)]
pub struct NoBody;

pub fn handle_empty<T: Default>(body: Option<T>, method: &Method) -> Option<T> {
    body.or_else(|| {
        if method == Method::PUT || method == Method::POST {
            Some(T::default())
        } else {
            None
        }
    })
}

// Returns `true` if the method is idempotent by default, and `false`, if not.
pub fn default_idempotency(m: &Method) -> bool {
    m == Method::GET || m == Method::PUT || m == Method::DELETE
}

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
mod tests {
    use http::{HeaderMap, HeaderValue, Method};
    use test_case::test_case;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
    use super::*;
    use crate::options::ClientConfig;
    use crate::options::InstrumentationClientInfo;

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

    #[test_case(Method::GET, false)]
    #[test_case(Method::POST, true)]
    #[test_case(Method::PUT, true)]
    #[test_case(Method::DELETE, false)]
    #[test_case(Method::PATCH, false)]
    fn handle_empty(input: Method, expected: bool) {
        assert!(super::handle_empty(None::<super::NoBody>, &input).is_some() == expected);

        let s = Some(wkt::Empty {});
        assert_eq!(s, super::handle_empty(s.clone(), &input));
    }

    #[test_case(Method::GET, true)]
    #[test_case(Method::POST, false)]
    #[test_case(Method::PUT, true)]
    #[test_case(Method::DELETE, true)]
    #[test_case(Method::PATCH, false)]
    fn default_idempotency(input: Method, expected: bool) {
        assert!(super::default_idempotency(&input) == expected);
    }

    static TEST_INSTRUMENTATION_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
        service_name: "test-service",
        client_version: "1.2.3",
        client_artifact: "test-artifact",
        default_host: "test.googleapis.com",
    };

    #[tokio::test]
    async fn reqwest_client_new() {
        let config = ClientConfig::default();
        let client = ReqwestClient::new(config, "https://test.googleapis.com")
            .await
            .unwrap();
        assert!(client.instrumentation.is_none());
    }

    #[tokio::test]
    async fn reqwest_client_with_instrumentation() {
        let config = ClientConfig::default();
        let client = ReqwestClient::new(config, "https://test.googleapis.com")
            .await
            .unwrap();
        assert!(client.instrumentation.is_none());

        let client = client.with_instrumentation(&TEST_INSTRUMENTATION_INFO);
        assert!(client.instrumentation.is_some());
        let info = client.instrumentation.unwrap();
        assert_eq!(info.service_name, "test-service");
        assert_eq!(info.client_version, "1.2.3");
        assert_eq!(info.client_artifact, "test-artifact");
        assert_eq!(info.default_host, "test.googleapis.com");
    }

    #[test_case(None, "test.googleapis.com"; "default")]
    #[test_case(Some("http://www.googleapis.com"), "test.googleapis.com"; "global")]
    #[test_case(Some("http://private.googleapis.com"), "test.googleapis.com"; "VPC-SC private")]
    #[test_case(Some("http://restricted.googleapis.com"), "test.googleapis.com"; "VPC-SC restricted")]
    #[test_case(Some("http://test-my-private-ep.p.googleapis.com"), "test.googleapis.com"; "PSC custom endpoint")]
    #[test_case(Some("https://us-central1-test.googleapis.com"), "us-central1-test.googleapis.com"; "locational endpoint")]
    #[test_case(Some("https://test.us-central1.rep.googleapis.com"), "test.us-central1.rep.googleapis.com"; "regional endpoint")]
    #[test_case(Some("https://test.my-universe-domain.com"), "test.googleapis.com"; "universe domain")]
    #[test_case(Some("localhost:5678"), "test.googleapis.com"; "emulator")]
    #[tokio::test]
    async fn host_from_endpoint(
        custom_endpoint: Option<&str>,
        expected_host: &str,
    ) -> anyhow::Result<()> {
        let mut config = ClientConfig::default();
        config.endpoint = custom_endpoint.map(String::from);
        config.cred = Some(auth::credentials::anonymous::Builder::new().build());
        let client = ReqwestClient::new(config.clone(), "https://test.googleapis.com/").await?;
        assert_eq!(client.host, expected_host);

        // Rarely, (I think only in GCS), does the default endpoint end without
        // a `/`. Make sure everything still works.
        let client = ReqwestClient::new(config, "https://test.googleapis.com").await?;
        assert_eq!(client.host, expected_host);

        Ok(())
    }

    #[test_case(None; "default")]
    #[test_case(Some("localhost:5678"); "custom")]
    #[tokio::test]
    async fn host_from_endpoint_showcase(custom_endpoint: Option<&str>) -> anyhow::Result<()> {
        let mut config = ClientConfig::default();
        config.endpoint = custom_endpoint.map(String::from);
        config.cred = Some(auth::credentials::anonymous::Builder::new().build());
        let client = ReqwestClient::new(config.clone(), "https://localhost:7469/").await?;
        assert_eq!(client.host, "localhost");

        Ok(())
    }
}
