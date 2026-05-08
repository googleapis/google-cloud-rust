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

//! Implements the HTTP client for the client libraries.
//!
//! This is a wrapper around `reqwest::Client` with some helpers to simplify
//! authentication, the retry loops, the telemetry headers, default endpoints,
//! etc.  We are not trying to make it a secret that we use `reqwest`. We
//! must be careful to export enough symbols so the callers can use this crate
//! without having to link `reqwest` directly. That leads to unexpected breaking
//! changes.

pub mod http_request_builder;
pub mod reqwest;

use crate::as_inner::as_inner;
use crate::attempt_info::AttemptInfo;
use crate::observability::{HttpResultExt, RequestRecorder, create_http_attempt_span};
use crate::universe_domain::DEFAULT_UNIVERSE_DOMAIN;
use google_cloud_auth::credentials::{
    Builder as CredentialsBuilder, CacheableResource, Credentials,
};
use google_cloud_gax::Result;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::client_builder::Error as BuilderError;
use google_cloud_gax::client_builder::Result as ClientBuilderResult;
use google_cloud_gax::error::{Error, rpc::Status};
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_error_policy::{
    Aip194Strict as PollingAip194Strict, PollingErrorPolicy,
};
use google_cloud_gax::response::{Parts, Response};
use google_cloud_gax::retry_loop_internal::{effective_timeout, retry_loop};
use google_cloud_gax::retry_policy::{
    Aip194Strict as RetryAip194Strict, RetryPolicy, RetryPolicyExt as _,
};
use google_cloud_gax::retry_throttler::SharedRetryThrottler;
use http::Extensions;
pub use http_request_builder::HttpRequestBuilder;
use reqwest::Method;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;

const X_GOOG_USER_PROJECT: &str = "x-goog-user-project";

#[derive(Clone, Debug)]
pub struct ReqwestClient {
    inner: ::reqwest::Client,
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
    universe_domain: String,
    transport_metric: Option<crate::observability::TransportMetric>,
}

impl ReqwestClient {
    pub async fn new(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
    ) -> ClientBuilderResult<Self> {
        let cred = Self::make_credentials(&config).await?;
        let mut builder = ::reqwest::Client::builder();
        // Force http1 as http2 with not currently supported.
        // TODO(#4298): Remove after adding HTTP2 support.
        builder = builder.http1_only();
        if config.disable_automatic_decompression {
            builder = builder.no_gzip().no_brotli().no_deflate().no_zstd();
        }
        if config.disable_follow_redirects {
            builder = builder.redirect(::reqwest::redirect::Policy::none());
        }
        let inner = builder.build().map_err(BuilderError::transport)?;
        let universe_domain =
            crate::universe_domain::resolve(config.universe_domain.as_deref(), &cred).await?;
        let host = crate::host::header(
            config.endpoint.as_deref(),
            default_endpoint,
            &universe_domain,
        )
        .map_err(|e| e.client_builder())?;
        let service_endpoint = default_endpoint.replace(DEFAULT_UNIVERSE_DOMAIN, &universe_domain);
        let tracing_enabled = crate::options::tracing_enabled(&config);
        let endpoint = config.endpoint.unwrap_or(service_endpoint);
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
            universe_domain,
            transport_metric: None,
        })
    }

    pub fn with_instrumentation(
        mut self,
        instrumentation: &'static crate::options::InstrumentationClientInfo,
    ) -> Self {
        self.instrumentation = Some(instrumentation);
        if self._tracing_enabled {
            self.transport_metric = Some(crate::observability::TransportMetric::new(Some(
                instrumentation,
            )));
        }
        self
    }

    pub fn builder(&self, method: Method, path: String) -> reqwest::RequestBuilder {
        self.inner
            .request(method, format!("{}{path}", &self.endpoint))
            .header(::reqwest::header::HOST, &self.host)
    }

    /// Creates a builder for a complete URL.
    ///
    /// Most clients use a single endpoint for all requests. Therefore, the
    /// [builder()][Self::builder()] prepends the endpoint to a request path.
    /// The most notable exception is the storage client, which receives the URL
    /// for uploads dynamically, and needs to make requests to arbitrary URLs.
    pub fn builder_with_url(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        self.inner.request(method, url)
    }

    /// Creates a builder for a plain HTTP request.
    ///
    /// Most crates in google-cloud-rust use this struct to make RPCs over HTTP,
    /// with a JSON payload and response. The Storage client (and maybe others)
    /// use plain HTTP requests, with streaming requests and responses,
    /// alternative endpoints and a number of other features.
    ///
    /// This function is used to make such requests. It returns a builder that
    /// is more constrained than a `reqwest::RequestBuilder`, but also harder to
    /// use incorrectly.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax_internal::http::ReqwestClient;
    /// use google_cloud_gax_internal::http::reqwest::Method;
    /// use google_cloud_gax::options::RequestOptions;
    /// use google_cloud_gax_internal::attempt_info::AttemptInfo;
    /// async fn sample(client: &ReqwestClient, options: RequestOptions) -> anyhow::Result<()> {
    ///     let builder = client.http_builder(Method::GET, "storage/v1/b/my-bucket/o/my-object")
    ///         .query("alt", "media")
    ///         .header("x-goog-api-client", "client/1.2.3");
    ///     let response = builder.send(options, AttemptInfo::new(0)).await?;
    ///     println!("status={:?}", response.status());
    ///     Ok(())
    /// }
    /// ```
    pub fn http_builder(&self, method: Method, path: &str) -> HttpRequestBuilder {
        let builder = self
            .inner
            .request(method, format!("{}{path}", &self.endpoint))
            .header(::reqwest::header::HOST, &self.host);
        HttpRequestBuilder::new(self.clone(), builder)
    }

    /// Creates a builder for a plain HTTP request.
    ///
    /// Most crates in google-cloud-rust use this struct to make RPCs over HTTP,
    /// with a JSON payload and response. The Storage client (and maybe others)
    /// use plain HTTP requests, with streaming requests and responses,
    /// alternative endpoints and a number of other features.
    ///
    /// This function is used to make such requests. It returns a builder that
    /// is more constrained than a `reqwest::RequestBuilder`, but also harder to
    /// use incorrectly.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax_internal::http::ReqwestClient;
    /// use google_cloud_gax_internal::http::reqwest::Method;
    /// use google_cloud_gax::options::RequestOptions;
    /// use google_cloud_gax_internal::attempt_info::AttemptInfo;
    /// async fn sample(client: &ReqwestClient, options: RequestOptions) -> anyhow::Result<()> {
    ///     let builder = client.http_builder_with_url(
    ///         Method::GET,
    ///         "https://storage.googleapis.com/storage/v1/b/my-bucket/o/my-object",
    ///         "https://storage.googleapis.com",
    ///     )?
    ///     .query("alt", "media")
    ///     .header("x-goog-api-client", "client/1.2.3");
    ///     let response = builder.send(options, AttemptInfo::new(0)).await?;
    ///     println!("status={:?}", response.status());
    ///     Ok(())
    /// }
    /// ```
    pub fn http_builder_with_url(
        &self,
        method: Method,
        url: &str,
        default_endpoint: &str,
    ) -> Result<HttpRequestBuilder> {
        let host = crate::host::header(Some(url), default_endpoint, &self.universe_domain)
            .map_err(|e| e.gax())?;
        let builder = self
            .inner
            .request(method, url)
            .header(::reqwest::header::HOST, &host);

        Ok(HttpRequestBuilder::new(self.clone(), builder))
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned + Default>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
        options: RequestOptions,
    ) -> Result<Response<O>> {
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        self.retry_loop::<O>(builder, options).await
    }

    pub(crate) async fn execute_http(
        &self,
        builder: reqwest::RequestBuilder,
        options: RequestOptions,
        attempt_info: AttemptInfo,
    ) -> Result<reqwest::Response> {
        let request = self
            .request(builder, &options, attempt_info.remaining_time)
            .await?;
        if self._tracing_enabled {
            return self
                .execute_http_traced(request, options, attempt_info)
                .await;
        }
        self.execute_http_inner(request).await
    }

    async fn execute_http_traced(
        &self,
        request: reqwest::Request,
        options: RequestOptions,
        attempt_info: AttemptInfo,
    ) -> Result<reqwest::Response> {
        let span = create_http_attempt_span(
            &request,
            &options,
            self.instrumentation,
            attempt_info.attempt_count,
        );
        if let Some(recorder) = RequestRecorder::current() {
            recorder.on_http_request(&request);
        }
        let result = self
            .execute_http_inner(request)
            .instrument(span.clone())
            .await;
        if let Some(recorder) = RequestRecorder::current() {
            match &result {
                Ok(r) => recorder.on_http_response(r),
                Err(e) => recorder.on_http_error(e),
            }
        }
        result.record_http(&span)
    }

    async fn execute_http_inner(&self, mut request: reqwest::Request) -> Result<reqwest::Response> {
        // We want to send the tracing propagation headers even if tracing is disabled in the
        // client. A global trace (say from the incoming HTTP request to Cloud Run) could be
        // propagated.
        crate::observability::propagation::inject_context(
            &tracing::Span::current(),
            request.headers_mut(),
        );
        self.inner.execute(request).await.map_err(map_send_error)
    }

    #[deprecated]
    /// Executes a streaming request.
    ///
    /// The `builder` should be configured with the HTTP method, URL, and any
    /// request body.
    ///
    /// This method does *not* handle retries. The caller is responsible for
    /// handling retries if necessary.
    pub async fn execute_streaming_once(
        &self,
        builder: reqwest::RequestBuilder,
        options: RequestOptions,
        remaining_time: Option<std::time::Duration>,
        attempt_count: u32,
    ) -> Result<Response<impl futures::Stream<Item = Result<bytes::Bytes>>>> {
        use futures::TryStreamExt;

        let response = self
            .request_attempt(builder, &options, remaining_time, attempt_count)
            .await?;

        let response = http::Response::from(response);
        let (parts, body) = response.into_parts();
        let stream = http_body_util::BodyStream::new(body)
            .map_ok(|frame| frame.into_data().unwrap_or_default())
            .map_err(Error::io);

        Ok(Response::from_parts(
            Parts::new().set_headers(parts.headers),
            stream,
        ))
    }

    async fn make_credentials(
        config: &crate::options::ClientConfig,
    ) -> ClientBuilderResult<Credentials> {
        if let Some(c) = config.cred.clone() {
            return Ok(c);
        }
        CredentialsBuilder::default()
            .build()
            .map_err(BuilderError::cred)
    }

    async fn retry_loop<O: serde::de::DeserializeOwned + Default>(
        &self,
        builder: reqwest::RequestBuilder,
        options: RequestOptions,
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

            let response = this
                .request_attempt(builder, &options, d, current_attempt)
                .await?;
            self::to_http_response(response).await
        };
        let sleep = async |d| tokio::time::sleep(d).await;
        retry_loop(inner, sleep, idempotent, throttler, retry, backoff).await
    }

    async fn request(
        &self,
        mut builder: reqwest::RequestBuilder,
        options: &RequestOptions,
        remaining_time: Option<std::time::Duration>,
    ) -> Result<reqwest::Request> {
        builder = effective_timeout(options, remaining_time)
            .into_iter()
            .fold(builder, |b, t| b.timeout(t));

        let mut headers = match self.cred.headers(Extensions::new()).await {
            Err(e) => return Err(Error::authentication(e)),
            Ok(CacheableResource::New { data, .. }) => data,
            Ok(CacheableResource::NotModified) => unreachable!("headers are not cached"),
        };

        if let Some(user_agent) = options.user_agent() {
            headers.insert(
                http::header::USER_AGENT,
                http::header::HeaderValue::from_str(user_agent).map_err(Error::ser)?,
            );
        }

        if let Some(quota_project) = options.quota_project() {
            headers.insert(
                http::header::HeaderName::from_static(X_GOOG_USER_PROJECT),
                http::header::HeaderValue::from_str(quota_project).map_err(Error::ser)?,
            );
        }

        builder = builder.headers(headers);

        builder.build().map_err(map_send_error)
    }

    async fn request_attempt(
        &self,
        builder: reqwest::RequestBuilder,
        options: &RequestOptions,
        remaining_time: Option<std::time::Duration>,
        _attempt_count: u32,
    ) -> Result<reqwest::Response> {
        let request = self.request(builder, options, remaining_time).await?;
        if self._tracing_enabled {
            return self
                .request_attempt_traced(request, options, _attempt_count)
                .await;
        }
        self.request_attempt_inner(request).await
    }

    async fn request_attempt_traced(
        &self,
        request: reqwest::Request,
        options: &RequestOptions,
        attempt_count: u32,
    ) -> Result<reqwest::Response> {
        let span = create_http_attempt_span(&request, options, self.instrumentation, attempt_count);
        if let Some(recorder) = RequestRecorder::current() {
            recorder.on_http_request(&request);
        }
        let pending = self.request_attempt_inner(request);
        let pending = crate::observability::WithTransportLogging::new(pending);
        let metric = self
            .transport_metric
            .clone()
            .unwrap_or_else(|| crate::observability::TransportMetric::new(None));
        let pending =
            crate::observability::WithTransportMetric::new(metric, pending, attempt_count);
        crate::observability::WithTransportSpan::new(span, pending).await
    }

    async fn request_attempt_inner(
        &self,
        mut request: reqwest::Request,
    ) -> Result<reqwest::Response> {
        // We want to send the tracing propagation headers even if tracing is disabled in the
        // client. A global trace (say from the incoming HTTP request to Cloud Run) could be
        // propagated.
        crate::observability::propagation::inject_context(
            &tracing::Span::current(),
            request.headers_mut(),
        );
        let result = self.inner.execute(request).await.map_err(map_send_error);
        if let Some(recorder) = RequestRecorder::current() {
            match &result {
                Ok(r) => recorder.on_http_response(r),
                Err(e) => recorder.on_http_error(e),
            }
        }
        let response = result?;
        if !response.status().is_success() {
            return self::to_http_error(response).await;
        }
        Ok(response)
    }

    fn get_retry_policy(&self, options: &RequestOptions) -> Arc<dyn RetryPolicy> {
        options
            .retry_policy()
            .clone()
            .unwrap_or_else(|| self.retry_policy.clone())
    }

    pub(crate) fn get_backoff_policy(&self, options: &RequestOptions) -> Arc<dyn BackoffPolicy> {
        options
            .backoff_policy()
            .clone()
            .unwrap_or_else(|| self.backoff_policy.clone())
    }

    pub(crate) fn get_retry_throttler(&self, options: &RequestOptions) -> SharedRetryThrottler {
        options
            .retry_throttler()
            .clone()
            .unwrap_or_else(|| self.retry_throttler.clone())
    }

    pub fn get_polling_error_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn PollingErrorPolicy> {
        options
            .polling_error_policy()
            .clone()
            .unwrap_or_else(|| self.polling_error_policy.clone())
    }

    pub fn get_polling_backoff_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn PollingBackoffPolicy> {
        options
            .polling_backoff_policy()
            .clone()
            .unwrap_or_else(|| self.polling_backoff_policy.clone())
    }
}

pub fn map_send_error(err: ::reqwest::Error) -> Error {
    if let Some(e) = as_inner::<hyper::Error, _>(&err) {
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

    let error = match Status::try_from(&body) {
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
    use super::*;
    use crate::options::ClientConfig;
    use crate::options::InstrumentationClientInfo;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider};
    use google_cloud_auth::errors::CredentialsError;
    use http::{HeaderMap, HeaderValue, Method};
    use scoped_env::ScopedEnv;
    use serial_test::serial;
    use test_case::test_case;

    type AuthResult<T> = std::result::Result<T, CredentialsError>;
    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> AuthResult<CacheableResource<HeaderMap>>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test]
    async fn client_http_error_bytes() -> anyhow::Result<()> {
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
    async fn client_error_with_status() -> anyhow::Result<()> {
        use google_cloud_gax::error::rpc::{Code, Status, StatusDetails::LocalizedMessage};
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
                google_cloud_rpc::model::LocalizedMessage::new()
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
    async fn client_empty_content(code: reqwest::StatusCode, content: &str) -> anyhow::Result<()> {
        let response = resp_from_code_content(code, content)?;
        assert!(response.status().is_success());

        let response = super::to_http_response::<wkt::Empty>(response).await?;
        let body = response.into_body();
        assert_eq!(body, wkt::Empty::default());
        Ok(())
    }

    #[tokio::test]
    #[test_case(reqwest::StatusCode::OK, ""; "200 with empty content")]
    async fn client_error_with_empty_content(
        code: reqwest::StatusCode,
        content: &str,
    ) -> anyhow::Result<()> {
        let response = resp_from_code_content(code, content)?;
        assert!(response.status().is_success());

        let response = super::to_http_response::<wkt::Empty>(response).await;
        assert!(response.is_err(), "{response:?}");
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
        assert!(client.instrumentation.is_none(), "{client:?}");
    }

    #[tokio::test]
    async fn reqwest_client_with_instrumentation() {
        let config = ClientConfig::default();
        let client = ReqwestClient::new(config, "https://test.googleapis.com")
            .await
            .unwrap();
        assert!(client.instrumentation.is_none(), "{client:?}");

        let client = client.with_instrumentation(&TEST_INSTRUMENTATION_INFO);
        assert!(client.instrumentation.is_some(), "{client:?}");
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
    #[test_case(Some("localhost:5678"), "localhost"; "emulator")]
    #[tokio::test]
    async fn host_from_endpoint(
        custom_endpoint: Option<&str>,
        expected_host: &str,
    ) -> anyhow::Result<()> {
        let mut config = ClientConfig::default();
        config.endpoint = custom_endpoint.map(String::from);
        config.cred = Some(Anonymous::new().build());
        let client = ReqwestClient::new(config.clone(), "https://test.googleapis.com/").await?;
        assert_eq!(client.host, expected_host);

        // Rarely, (I think only in GCS), does the default endpoint end without
        // a `/`. Make sure everything still works.
        let client = ReqwestClient::new(config, "https://test.googleapis.com").await?;
        assert_eq!(client.host, expected_host);

        Ok(())
    }

    #[tokio::test]
    #[test_case(None, "test.my-custom-universe.com"; "default")]
    #[test_case(Some("http://www.my-custom-universe.com"), "test.my-custom-universe.com"; "global")]
    #[test_case(Some("http://private.my-custom-universe.com"), "test.my-custom-universe.com"; "VPC-SC private")]
    #[test_case(Some("http://restricted.my-custom-universe.com"), "test.my-custom-universe.com"; "VPC-SC restricted")]
    #[test_case(Some("http://test-my-private-ep.p.my-custom-universe.com"), "test.my-custom-universe.com"; "PSC custom endpoint")]
    #[test_case(Some("https://us-central1-test.my-custom-universe.com"), "us-central1-test.my-custom-universe.com"; "locational endpoint")]
    #[test_case(Some("https://test.us-central1.rep.my-custom-universe.com"), "test.us-central1.rep.my-custom-universe.com"; "regional endpoint")]
    #[serial]
    async fn host_from_endpoint_with_universe_domain_success(
        endpoint_override: Option<&str>,
        expected_host: &str,
    ) -> TestResult {
        let _env = ScopedEnv::remove("GOOGLE_CLOUD_UNIVERSE_DOMAIN");
        let universe_domain = "my-custom-universe.com";
        let mut config = ClientConfig::default();
        config.universe_domain = Some(universe_domain.to_string());
        config.endpoint = endpoint_override.map(String::from);

        let mut cred = MockCredentials::new();
        cred.expect_universe_domain()
            .returning(move || Some(universe_domain.to_string()));
        config.cred = Some(cred.into());

        let client = ReqwestClient::new(config, "https://test.googleapis.com").await?;
        assert_eq!(client.universe_domain, universe_domain);
        assert_eq!(client.host, expected_host);

        Ok(())
    }

    #[tokio::test]
    async fn host_from_endpoint_with_universe_domain_mismatch_fails() -> TestResult {
        let mut config = ClientConfig::default();
        config.universe_domain = Some("custom.com".to_string());
        config.cred = Some(Anonymous::new().build());

        let err = ReqwestClient::new(config, "https://language.googleapis.com")
            .await
            .unwrap_err();

        assert!(err.is_universe_domain_mismatch(), "{err:?}");

        Ok(())
    }

    #[test_case(None; "default")]
    #[test_case(Some("localhost:5678"); "custom")]
    #[tokio::test]
    async fn host_from_endpoint_showcase(custom_endpoint: Option<&str>) -> anyhow::Result<()> {
        let mut config = ClientConfig::default();
        config.endpoint = custom_endpoint.map(String::from);
        config.cred = Some(Anonymous::new().build());
        let client = ReqwestClient::new(config.clone(), "https://localhost:7469/").await?;
        assert_eq!(client.host, "localhost");

        Ok(())
    }

    #[tokio::test]
    async fn reqwest_client_decompression_config() -> anyhow::Result<()> {
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        config.disable_automatic_decompression = true;
        let _client = ReqwestClient::new(config.clone(), "https://test.googleapis.com").await?;

        config.disable_automatic_decompression = false;
        let _client = ReqwestClient::new(config, "https://test.googleapis.com").await?;
        Ok(())
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn execute_streaming_success() -> anyhow::Result<()> {
        let server = httptest::Server::run();
        server.expect(
            httptest::Expectation::matching(httptest::matchers::request::method_path(
                "GET", "/foo",
            ))
            .respond_with(httptest::responders::status_code(200).body("hello world")),
        );

        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        let client = ReqwestClient::new(config, &server.url_str("/")).await?;
        let builder = client.builder(Method::GET, "foo".to_string());
        let options = RequestOptions::default();

        let response = client
            .execute_streaming_once(builder, options, None, 0)
            .await?;

        use futures::TryStreamExt;
        let body_bytes = response
            .into_body()
            .map_ok(|b| b.to_vec())
            .try_concat()
            .await?;
        assert_eq!(body_bytes, b"hello world");
        Ok(())
    }

    /// 308 (Permanent Redirect) is used in Resumable Uploads to indicate "Resume Incomplete".
    /// We need to ensure that `execute_streaming_once` treats this as an error (and not a success)
    /// so that the caller can handle the 308 status code appropriately (e.g., to query the upload status).
    #[tokio::test]
    #[allow(deprecated)]
    async fn execute_streaming_308() -> anyhow::Result<()> {
        let server = httptest::Server::run();
        server.expect(
            httptest::Expectation::matching(httptest::matchers::request::method_path(
                "PUT", "/upload",
            ))
            .respond_with(
                httptest::responders::status_code(308)
                    .append_header("Location", "/new-location")
                    .body("Resume Incomplete"),
            ),
        );

        let mut config = ClientConfig::default();
        config.disable_follow_redirects = true;
        config.cred = Some(Anonymous::new().build());
        let client = ReqwestClient::new(config, &server.url_str("/")).await?;

        let builder = client.builder(Method::PUT, "upload".to_string());
        let options = RequestOptions::default();

        let result = client
            .execute_streaming_once(builder, options, None, 0)
            .await;
        assert!(
            result.is_err(),
            "expected error, got successful stream: {:?}",
            {
                let (parts, _body) = result.unwrap().into_parts();
                parts
            }
        );
        assert_eq!(result.err().unwrap().http_status_code(), Some(308));
        Ok(())
    }
}
