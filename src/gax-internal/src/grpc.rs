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

pub mod from_status;
pub mod status;

use auth::credentials::{CacheableResource, Credentials};
use from_status::to_gax_error;
use gax::Result;
use gax::backoff_policy::BackoffPolicy;
use gax::client_builder::Error as BuilderError;
use gax::error::Error;
use gax::exponential_backoff::ExponentialBackoff;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::{Aip194Strict as PollingAip194Strict, PollingErrorPolicy};
use gax::retry_policy::{Aip194Strict as RetryAip194Strict, RetryPolicy, RetryPolicyExt as _};
use gax::retry_throttler::SharedRetryThrottler;
use http::HeaderMap;
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(google_cloud_unstable_tracing))]
pub type GrpcService = tonic::transport::Channel;

#[cfg(google_cloud_unstable_tracing)]
pub type GrpcService = tower::util::Either<
    crate::observability::grpc_tracing::TracingTowerService<tonic::transport::Channel>,
    crate::observability::grpc_tracing::NoTracingTowerService<tonic::transport::Channel>,
>;

/// The inner gRPC client type.
pub type InnerClient = tonic::client::Grpc<GrpcService>;

#[derive(Clone, Debug)]
pub struct Client {
    inner: InnerClient,
    credentials: Credentials,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    retry_throttler: SharedRetryThrottler,
    polling_error_policy: Arc<dyn PollingErrorPolicy>,
    polling_backoff_policy: Arc<dyn PollingBackoffPolicy>,
}

impl Client {
    /// Create a new client.
    pub async fn new(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
    ) -> gax::client_builder::Result<Self> {
        Self::build(
            config,
            default_endpoint,
            #[cfg(google_cloud_unstable_tracing)]
            None,
        )
        .await
    }

    /// Create a new client with instrumentation info.
    #[cfg(google_cloud_unstable_tracing)]
    pub async fn new_with_instrumentation(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
        instrumentation: &'static crate::options::InstrumentationClientInfo,
    ) -> gax::client_builder::Result<Self> {
        Self::build(config, default_endpoint, Some(instrumentation)).await
    }

    async fn build(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
        #[cfg(google_cloud_unstable_tracing)] instrumentation: Option<
            &'static crate::options::InstrumentationClientInfo,
        >,
    ) -> gax::client_builder::Result<Self> {
        let credentials = Self::make_credentials(&config).await?;
        let tracing_enabled = crate::options::tracing_enabled(&config);

        let inner = Self::make_inner(
            config.endpoint,
            default_endpoint,
            tracing_enabled,
            #[cfg(google_cloud_unstable_tracing)]
            instrumentation,
        )
        .await?;

        Ok(Self {
            inner,
            credentials,
            retry_policy: config.retry_policy.clone().unwrap_or_else(|| {
                Arc::new(
                    RetryAip194Strict
                        .with_attempt_limit(10)
                        .with_time_limit(Duration::from_secs(60)),
                )
            }),
            backoff_policy: config
                .backoff_policy
                .clone()
                .unwrap_or_else(|| Arc::new(ExponentialBackoff::default())),
            retry_throttler: config.retry_throttler,
            polling_error_policy: config
                .polling_error_policy
                .unwrap_or_else(|| Arc::new(PollingAip194Strict)),
            polling_backoff_policy: config
                .polling_backoff_policy
                .unwrap_or_else(|| Arc::new(ExponentialBackoff::default())),
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
        Request: prost::Message + Clone + 'static,
        Response: prost::Message + Default + 'static,
    {
        let headers = Self::make_headers(api_client_header, request_params, &options).await?;
        self.retry_loop::<Request, Response>(extensions, path, request, options, headers)
            .await
    }

    #[cfg(google_cloud_unstable_storage_bidi)]
    /// Opens a bidirectional stream.
    pub async fn bidi_stream<Request, Response>(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
        options: gax::options::RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Response<tonic::codec::Streaming<Response>>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        self.bidi_stream_with_status(
            extensions,
            path,
            request,
            options,
            api_client_header,
            request_params,
        )
        .await?
        .map_err(to_gax_error)
    }

    #[cfg(google_cloud_unstable_storage_bidi)]
    /// Opens a bidirectional stream.
    ///
    /// Some services (notably Storage) need to examine the `tonic::Status` to
    /// extract data from the error details. Typically this data is encoded
    /// using protobuf messages unavailable in this library.
    pub async fn bidi_stream_with_status<Request, Response>(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
        options: gax::options::RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Result<tonic::Response<tonic::codec::Streaming<Response>>>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        use tonic::IntoStreamingRequest;
        let headers = Self::make_headers(api_client_header, request_params, &options).await?;
        let headers = self.add_auth_headers(headers).await?;
        let metadata = tonic::metadata::MetadataMap::from_headers(headers);
        let request = tonic::Request::from_parts(metadata, extensions, request);
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;
        Ok(inner
            .streaming(request.into_streaming_request(), path, codec)
            .await)
    }

    /// Runs the retry loop.
    async fn retry_loop<Request, Response>(
        &self,
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
        let retry_policy = self.get_retry_policy(&options);
        let backoff_policy = self.get_backoff_policy(&options);
        let this = self.clone();
        let mut prior_attempt_count: i64 = 0;
        let inner = async move |remaining_time: Option<Duration>| {
            let current_attempt = prior_attempt_count;
            prior_attempt_count += 1;
            this.clone()
                .request_attempt::<Request, Response>(
                    extensions.clone(),
                    path.clone(),
                    request.clone(),
                    &options,
                    remaining_time,
                    headers.clone(),
                    current_attempt,
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
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: Request,
        options: &gax::options::RequestOptions,
        remaining_time: Option<std::time::Duration>,
        headers: HeaderMap,
        prior_attempt_count: i64,
    ) -> Result<tonic::Response<Response>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + std::default::Default + 'static,
    {
        let headers = self.add_auth_headers(headers).await?;
        let metadata = tonic::metadata::MetadataMap::from_headers(headers);
        let mut request = tonic::Request::from_parts(metadata, extensions, request);

        #[cfg(google_cloud_unstable_tracing)]
        {
            use crate::observability::grpc_tracing::AttemptCount;
            request
                .extensions_mut()
                .insert(AttemptCount(prior_attempt_count));
        }
        #[cfg(not(google_cloud_unstable_tracing))]
        let _ = prior_attempt_count;

        if let Some(timeout) = gax::retry_loop_internal::effective_timeout(options, remaining_time)
        {
            request.set_timeout(timeout);
        }
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;
        inner
            .unary(request, path, codec)
            .await
            .map_err(to_gax_error)
    }

    async fn make_inner(
        endpoint: Option<String>,
        default_endpoint: &str,
        tracing_enabled: bool,
        #[cfg(google_cloud_unstable_tracing)] instrumentation: Option<
            &'static crate::options::InstrumentationClientInfo,
        >,
    ) -> gax::client_builder::Result<InnerClient> {
        use tonic::transport::{ClientTlsConfig, Endpoint};

        let origin =
            crate::host::from_endpoint(endpoint.as_deref(), default_endpoint, |origin, _host| {
                origin
            });
        let origin = origin?;
        let endpoint =
            Endpoint::from_shared(endpoint.unwrap_or_else(|| default_endpoint.to_string()))
                .map_err(BuilderError::transport)?;
        let endpoint = if endpoint
            .uri()
            .scheme()
            .is_some_and(|s| s == &http::uri::Scheme::HTTPS)
        {
            endpoint
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .map_err(BuilderError::transport)?
        } else {
            endpoint
        };
        let endpoint = endpoint.origin(origin);
        let channel = endpoint.connect_lazy();

        #[cfg(not(google_cloud_unstable_tracing))]
        {
            let _ = tracing_enabled;
            Ok(InnerClient::new(channel))
        }

        #[cfg(google_cloud_unstable_tracing)]
        {
            use crate::observability::grpc_tracing::NoTracingTowerLayer;
            use crate::observability::grpc_tracing::TracingTowerLayer;
            use tower::ServiceBuilder;
            use tower::util::Either;

            if tracing_enabled {
                let default_uri = default_endpoint
                    .parse::<tonic::transport::Uri>()
                    .map_err(BuilderError::transport)?;
                let default_host = default_uri.host().unwrap_or("").to_string();
                let layer = TracingTowerLayer::new(endpoint.uri(), default_host, instrumentation);
                let service = ServiceBuilder::new().layer(layer).service(channel);
                Ok(InnerClient::new(Either::Left(service)))
            } else {
                let layer = NoTracingTowerLayer;
                let service = ServiceBuilder::new().layer(layer).service(channel);
                Ok(InnerClient::new(Either::Right(service)))
            }
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

    async fn add_auth_headers(&self, mut headers: http::HeaderMap) -> Result<http::HeaderMap> {
        let h = self
            .credentials
            .headers(http::Extensions::new())
            .await
            .map_err(Error::authentication)?;

        let CacheableResource::New { data, .. } = h else {
            unreachable!("headers are not cached");
        };

        headers.extend(data);
        Ok(headers)
    }

    async fn make_headers(
        api_client_header: &'static str,
        request_params: &str,
        options: &gax::options::RequestOptions,
    ) -> Result<http::header::HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(user_agent) = options.user_agent() {
            headers.append(
                http::header::USER_AGENT,
                http::header::HeaderValue::from_str(user_agent).map_err(Error::ser)?,
            );
        }
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
                http::header::HeaderValue::from_str(request_params).map_err(Error::ser)?,
            );
        }
        Ok(headers)
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

/// Convert a `tonic::Response` wrapping a prost message into a
/// `gax::response::Response` wrapping our equivalent message
pub fn to_gax_response<T, G>(response: tonic::Response<T>) -> Result<gax::response::Response<G>>
where
    T: crate::prost::FromProto<G>,
{
    let (metadata, body, _extensions) = response.into_parts();
    Ok(gax::response::Response::from_parts(
        gax::response::Parts::new().set_headers(metadata.into_headers()),
        body.cnv().map_err(Error::deser)?,
    ))
}

#[cfg(test)]
#[cfg(google_cloud_unstable_tracing)]
mod tests {
    use super::Client;
    use crate::options::InstrumentationClientInfo;

    #[tokio::test]
    async fn test_new_with_instrumentation() {
        let config = crate::options::ClientConfig::default();
        static TEST_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
            service_name: "test-service",
            client_version: "1.0.0",
            client_artifact: "test-artifact",
            default_host: "example.com",
        };
        let _client = Client::new_with_instrumentation(config, "http://example.com", &TEST_INFO)
            .await
            .unwrap();
        // We can't easily assert the internal state without exposing more internals,
        // but this verifies the method exists and runs.
    }
}
