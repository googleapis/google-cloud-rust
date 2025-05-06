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

pub type InnerClient = tonic::client::Grpc<tonic::transport::Channel>;

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
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: Request,
        options: gax::options::RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Response<Response>>
    where
        Request: prost::Message + 'static + Clone,
        Response: prost::Message + Default + 'static,
    {
        let headers = Self::make_headers(api_client_header, request_params).await?;
        match self.get_retry_policy(&options) {
            None => {
                self.request_attempt::<Request, Response>(
                    extensions, path, request, &options, None, headers,
                )
                .await
            }
            Some(policy) => {
                self.retry_loop::<Request, Response>(
                    policy, extensions, path, request, options, headers,
                )
                .await
            }
        }
    }

    /// Runs the retry loop.
    async fn retry_loop<Request, Response>(
        &self,
        retry_policy: Arc<dyn RetryPolicy>,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: Request,
        options: gax::options::RequestOptions,
        headers: HeaderMap,
    ) -> Result<tonic::Response<Response>>
    where
        Request: prost::Message + 'static + Clone,
        Response: prost::Message + Default + 'static,
    {
        let idempotent = options.idempotent().unwrap_or(false);
        let retry_throttler = self.get_retry_throttler(&options);
        let backoff_policy = self.get_backoff_policy(&options);
        let this = self.clone();
        let inner = async move |remaining_time: Option<Duration>| {
            this.clone()
                .request_attempt::<Request, Response>(
                    extensions.clone(),
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
    async fn request_attempt<Request, Response>(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: Request,
        options: &gax::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
        headers: HeaderMap,
    ) -> Result<tonic::Response<Response>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + std::default::Default + 'static,
    {
        let mut headers = headers;
        let auth_headers = self
            .credentials
            .headers()
            .await
            .map_err(Error::authentication)?;
        for (key, value) in auth_headers.into_iter() {
            headers.append(key, value);
        }
        let metadata = tonic::metadata::MetadataMap::from_headers(headers);
        let mut request = tonic::Request::from_parts(metadata, extensions, request);
        if let Some(timeout) = gax::retry_loop_internal::effective_timeout(options, remaining_time)
        {
            request.set_timeout(timeout);
        }
        let codec = tonic::codec::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::rpc)?;
        inner
            .unary(request, path, codec)
            .await
            .map_err(to_gax_error)
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
        auth::credentials::Builder::default()
            .build()
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
        if !request_params.is_empty() {
            // When using routing info to populate the request parameters it is
            // possible that none of the path template matches. AIP-4222 says:
            //
            //     If none of the routing parameters matched their respective
            //     fields, the routing header **must not** be sent.
            //
            headers.append(
                http::header::HeaderName::from_static("x-goog-request-params"),
                http::header::HeaderValue::from_str(request_params).map_err(Error::other)?,
            );
        }
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

/// Convert a `tonic::Response` wrapping a prost message into a
/// `gax::response::Response` wrapping our equivalent message
pub fn to_gax_response<T, G>(response: tonic::Response<T>) -> gax::response::Response<G>
where
    T: crate::prost::FromProto<G>,
{
    let (metadata, body, _extensions) = response.into_parts();
    gax::response::Response::from_parts(
        gax::response::Parts::new().set_headers(metadata.into_headers()),
        body.cnv(),
    )
}
