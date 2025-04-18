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

//! Implements the common features of all gRPC-based client.

use auth::credentials::Credentials;
use gax::Result;
use gax::backoff_policy::BackoffPolicy;
use gax::error::Error;
mod from_status;
use from_status::to_gax_error;
use gax::exponential_backoff::ExponentialBackoff;
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::SharedRetryThrottler;
use http::HeaderMap;
use std::sync::Arc;
use std::time::Duration;

#[doc(hidden)]
pub type InnerClient = tonic::client::Grpc<tonic::transport::Channel>;

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct Client {
    inner: InnerClient,
    credentials: Credentials,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
}

impl Client {
    /// Create a new client.
    pub async fn new(config: crate::options::ClientConfig, default_endpoint: &str) -> Result<Self> {
        let credentials = Self::make_credentials(&config).await?;
        let inner = Self::make_inner(config.endpoint, default_endpoint).await?;
        Ok(Self {
            inner,
            credentials,
            retry_policy: config.retry_policy.clone(),
            backoff_policy: config.backoff_policy.clone(),
            retry_throttler: config.retry_throttler,
        })
    }

    /// Sends a request.
    pub async fn execute<Request, Response>(
        &self,
        method: tonic::GrpcMethod<'static>,
        path: http::uri::PathAndQuery,
        request: Request,
        options: gax::options::RequestOptions,
        api_client_header: &'static str,
        request_params: &'static str,
    ) -> Result<Response>
    where
        Request: prost::Message + 'static + Clone,
        Response: prost::Message + Default + 'static,
    {
        let headers = Self::make_headers(api_client_header, request_params).await?;
        match self.get_retry_policy(&options) {
            None => {
                let mut inner = self.inner.clone();
                Self::request_attempt::<Request, Response>(
                    &mut inner,
                    &self.credentials,
                    method,
                    path,
                    request,
                    &options,
                    None,
                    headers,
                )
                .await
            }
            Some(policy) => {
                self.retry_loop::<Request, Response>(
                    policy, method, path, request, options, headers,
                )
                .await
            }
        }
    }

    /// Runs the retry loop.
    async fn retry_loop<Request, Response>(
        &self,
        retry_policy: Arc<dyn RetryPolicy>,
        method: tonic::GrpcMethod<'static>,
        path: http::uri::PathAndQuery,
        request: Request,
        options: gax::options::RequestOptions,
        headers: HeaderMap,
    ) -> Result<Response>
    where
        Request: prost::Message + 'static + Clone,
        Response: prost::Message + Default + 'static,
    {
        let idempotent = options.idempotent().unwrap_or(false);
        let retry_throttler = self.get_retry_throttler(&options);
        let backoff_policy = self.get_backoff_policy(&options);
        let inner = async move |remaining_time: Option<Duration>| {
            Self::request_attempt::<Request, Response>(
                &mut self.inner.clone(),
                &self.credentials,
                method.clone(),
                path.clone(),
                request.clone(),
                &options,
                remaining_time,
                headers.clone(),
            )
            .await
        };
        let sleep = async |d| tokio::time::sleep(d).await;
        gax::retry_loop_internal::retry_loop(
            inner,
            sleep,
            idempotent,
            retry_throttler,
            retry_policy,
            backoff_policy,
        )
        .await
    }

    /// Makes a single request attempt.
    #[allow(clippy::too_many_arguments)]
    async fn request_attempt<Request, Response>(
        inner: &mut InnerClient,
        credentials: &Credentials,
        method: tonic::GrpcMethod<'static>,
        path: http::uri::PathAndQuery,
        request: Request,
        options: &gax::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
        headers: HeaderMap,
    ) -> Result<Response>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + std::default::Default + 'static,
    {
        let mut headers = headers;
        let auth_headers = credentials.headers().await.map_err(Error::authentication)?;
        for (key, value) in auth_headers.into_iter() {
            headers.append(key, value);
        }
        let mut extensions = tonic::Extensions::new();
        extensions.insert(method);
        let metadata = tonic::metadata::MetadataMap::from_headers(headers);
        let mut request = tonic::Request::from_parts(metadata, extensions, request);
        if let Some(timeout) = gax::retry_loop_internal::effective_timeout(options, remaining_time)
        {
            request.set_timeout(timeout);
        }
        let codec = tonic::codec::ProstCodec::default();
        inner.ready().await.map_err(Error::rpc)?;
        let response: tonic::Response<Response> = inner
            .unary(request, path, codec)
            .await
            .map_err(to_gax_error)?;
        let response = response.into_inner();
        Ok(response)
    }

    async fn make_inner(endpoint: Option<String>, default_endpoint: &str) -> Result<InnerClient> {
        use tonic::transport::{ClientTlsConfig, Endpoint};
        let endpoint =
            Endpoint::from_shared(endpoint.unwrap_or_else(|| default_endpoint.to_string()))
                .map_err(Error::other)?
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .map_err(Error::other)?;
        let conn = endpoint.connect().await.map_err(Error::io)?;
        Ok(tonic::client::Grpc::new(conn))
    }

    async fn make_credentials(
        config: &crate::options::ClientConfig,
    ) -> Result<auth::credentials::Credentials> {
        if let Some(c) = config.cred.clone() {
            return Ok(c);
        }
        auth::credentials::create_access_token_credentials()
            .await
            .map_err(Error::authentication)
    }

    async fn make_headers(
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<http::header::HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.append(
            http::header::HeaderName::from_static("x-goog-api-client"),
            http::header::HeaderValue::from_static(api_client_header),
        );
        headers.append(
            http::header::HeaderName::from_static("x-goog-request-params"),
            http::header::HeaderValue::from_str(request_params).map_err(Error::other)?,
        );
        Ok(headers)
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
}
