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
pub mod tonic;

use ::tonic::client::Grpc;
use ::tonic::transport::Channel;
use from_status::to_gax_error;
use futures::TryFutureExt;
use google_cloud_auth::credentials::{
    Builder as CredentialsBuilder, CacheableResource, Credentials,
};
use google_cloud_gax::Result;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::client_builder::Error as BuilderError;
use google_cloud_gax::client_builder::Result as ClientBuilderResult;
use google_cloud_gax::error::Error;
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
use http::HeaderMap;
use std::sync::Arc;
use std::time::Duration;

// A tonic::transport::Channel always has a Buffer layer.
const DEFAULT_REQUEST_BUFFER_CAPACITY: usize = 1024;

pub type GrpcService = Channel;

/// The inner gRPC client type.
pub type InnerClient = Grpc<GrpcService>;

#[cfg(google_cloud_unstable_tracing)]
#[derive(Clone, Debug)]
pub struct TracingAttributes {
    pub server_address: String,
    pub server_port: Option<i64>,
    pub url_domain: String,
    pub instrumentation: Option<&'static crate::options::InstrumentationClientInfo>,
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: InnerClient,
    #[cfg(google_cloud_unstable_tracing)]
    metric: crate::observability::TransportMetric,
    #[cfg(google_cloud_unstable_tracing)]
    tracing_attributes: Option<TracingAttributes>,
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
    ) -> ClientBuilderResult<Self> {
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
    ) -> ClientBuilderResult<Self> {
        Self::build(config, default_endpoint, Some(instrumentation)).await
    }

    async fn build(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
        #[cfg(google_cloud_unstable_tracing)] instrumentation: Option<
            &'static crate::options::InstrumentationClientInfo,
        >,
    ) -> ClientBuilderResult<Self> {
        let credentials = Self::make_credentials(&config).await?;
        let tracing_enabled = crate::options::tracing_enabled(&config);

        #[cfg(not(google_cloud_unstable_tracing))]
        let inner = Self::make_inner(&config, default_endpoint, tracing_enabled).await?;

        #[cfg(google_cloud_unstable_tracing)]
        let (inner, tracing_attributes) =
            Self::make_inner(&config, default_endpoint, tracing_enabled, instrumentation).await?;

        Ok(Self {
            inner,
            #[cfg(google_cloud_unstable_tracing)]
            metric: crate::observability::TransportMetric::new(instrumentation),
            #[cfg(google_cloud_unstable_tracing)]
            tracing_attributes,
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
        options: RequestOptions,
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

    /// Opens a bidirectional stream.
    pub async fn bidi_stream<Request, Response>(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
        options: RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Response<tonic::Streaming<Response>>>
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
        options: RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Result<tonic::Response<tonic::Streaming<Response>>>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        use ::tonic::IntoStreamingRequest;
        let headers = Self::make_headers(api_client_header, request_params, &options).await?;
        let headers = self.add_auth_headers(headers).await?;
        let metadata = tonic::MetadataMap::from_headers(headers);
        let request = ::tonic::Request::from_parts(metadata, extensions, request);
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;
        #[cfg(google_cloud_unstable_tracing)]
        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            recorder.on_grpc_request(&path);
        }
        let result = inner
            .streaming(request.into_streaming_request(), path, codec)
            .await;
        #[cfg(google_cloud_unstable_tracing)]
        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            match &result {
                Ok(_) => recorder.on_grpc_response(),
                Err(e) => recorder.on_grpc_error(&to_gax_error(e.clone())),
            }
        }
        Ok(result)
    }

    /// Opens a server stream.
    #[cfg(feature = "_internal-grpc-server-streaming")]
    pub async fn server_streaming<Request, Response>(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: Request,
        options: RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Response<tonic::Streaming<Response>>>
    where
        Request: prost::Message + Clone + 'static,
        Response: prost::Message + Default + 'static,
    {
        self.server_streaming_with_status(
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

    /// Opens a server stream with detailed status.
    #[allow(dead_code)]
    async fn server_streaming_with_status<Request, Response>(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: Request,
        options: RequestOptions,
        api_client_header: &'static str,
        request_params: &str,
    ) -> Result<tonic::Result<tonic::Response<tonic::Streaming<Response>>>>
    where
        Request: prost::Message + Clone + 'static,
        Response: prost::Message + Default + 'static,
    {
        let headers = Self::make_headers(api_client_header, request_params, &options).await?;
        let headers = self.add_auth_headers(headers).await?;
        let metadata = tonic::MetadataMap::from_headers(headers);
        let mut request = ::tonic::Request::from_parts(metadata, extensions, request);
        if let Some(attempt_timeout) = options.attempt_timeout() {
            request.set_timeout(*attempt_timeout);
        }
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;
        #[cfg(google_cloud_unstable_tracing)]
        let span = self.create_grpc_span(&path, None);

        #[cfg(google_cloud_unstable_tracing)]
        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            recorder.on_grpc_request(&path);
        }

        let pending = inner
            .server_streaming(request, path, codec)
            .map_err(to_gax_error);

        #[cfg(not(google_cloud_unstable_tracing))]
        let result = pending.await;

        // TODO(#5372): The span created by `WithTransportSpan` only covers stream initiation.
        // Consider instrumenting the returned stream to capture errors during the stream's lifetime.

        #[cfg(google_cloud_unstable_tracing)]
        let result = {
            use crate::observability::{
                WithTransportLogging, WithTransportMetric, WithTransportSpan,
            };

            let pending = WithTransportMetric::new(self.metric.clone(), pending, 0);
            let pending = WithTransportLogging::new(pending);
            let pending = WithTransportSpan::new(span, pending);

            if let Some(recorder) = crate::observability::RequestRecorder::current() {
                recorder.scope(pending).await
            } else {
                pending.await
            }
        };

        match result {
            Ok(response) => Ok(Ok(response)),
            Err(err) => {
                use std::error::Error as _;
                if let Some(status) = err.source().and_then(|e| e.downcast_ref::<tonic::Status>()) {
                    Ok(Err(status.clone()))
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Runs the retry loop.
    async fn retry_loop<Request, Response>(
        &self,
        extensions: tonic::Extensions,
        path: http::uri::PathAndQuery,
        request: Request,
        options: RequestOptions,
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
        retry_loop(
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
        options: &RequestOptions,
        remaining_time: Option<std::time::Duration>,
        headers: HeaderMap,
        _prior_attempt_count: i64,
    ) -> Result<tonic::Response<Response>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + std::default::Default + 'static,
    {
        #[cfg(google_cloud_unstable_tracing)]
        let span = self.create_grpc_span(
            &path,
            if _prior_attempt_count > 0 {
                Some(_prior_attempt_count)
            } else {
                None
            },
        );

        #[allow(unused_mut)]
        let mut headers = self.add_auth_headers(headers).await?;

        #[cfg(google_cloud_unstable_tracing)]
        crate::observability::propagation::inject_context(&span, &mut headers);

        let metadata = tonic::MetadataMap::from_headers(headers);
        let mut request = ::tonic::Request::from_parts(metadata, extensions, request);

        if let Some(timeout) = effective_timeout(options, remaining_time) {
            request.set_timeout(timeout);
        }
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;

        #[cfg(google_cloud_unstable_tracing)]
        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            recorder.on_grpc_request(&path);
        }

        let pending = inner.unary(request, path, codec).map_err(to_gax_error);

        #[cfg(not(google_cloud_unstable_tracing))]
        let result = pending.await;
        #[cfg(google_cloud_unstable_tracing)]
        let result = {
            use crate::observability::{
                WithTransportLogging, WithTransportMetric, WithTransportSpan,
            };

            let pending =
                WithTransportMetric::new(self.metric.clone(), pending, _prior_attempt_count as u32);
            let pending = WithTransportLogging::new(pending);
            let pending = WithTransportSpan::new(span, pending);

            if let Some(recorder) = crate::observability::RequestRecorder::current() {
                recorder.scope(pending).await
            } else {
                pending.await
            }
        };

        result
    }

    #[cfg(not(google_cloud_unstable_tracing))]
    async fn make_inner(
        config: &crate::options::ClientConfig,
        default_endpoint: &str,
        tracing_enabled: bool,
    ) -> ClientBuilderResult<InnerClient> {
        use ::tonic::transport::{Channel, channel::Change};
        let endpoint = Self::make_endpoint(
            config.endpoint.clone(),
            default_endpoint,
            config.grpc_max_header_list_size,
        )
        .await?;
        let (channel, tx) = Channel::balance_channel(
            config
                .grpc_request_buffer_capacity
                .unwrap_or(DEFAULT_REQUEST_BUFFER_CAPACITY),
        );
        let count = std::cmp::max(1, config.grpc_subchannel_count.unwrap_or_default());
        for i in 0..count {
            let _ = tx.send(Change::Insert(i, endpoint.clone())).await;
        }

        let _ = tracing_enabled;
        Ok(InnerClient::new(channel))
    }

    #[cfg(google_cloud_unstable_tracing)]
    async fn make_inner(
        config: &crate::options::ClientConfig,
        default_endpoint: &str,
        tracing_enabled: bool,
        instrumentation: Option<&'static crate::options::InstrumentationClientInfo>,
    ) -> ClientBuilderResult<(InnerClient, Option<TracingAttributes>)> {
        use ::tonic::transport::{Channel, channel::Change};
        let endpoint = Self::make_endpoint(
            config.endpoint.clone(),
            default_endpoint,
            config.grpc_max_header_list_size,
        )
        .await?;
        let (channel, tx) = Channel::balance_channel(
            config
                .grpc_request_buffer_capacity
                .unwrap_or(DEFAULT_REQUEST_BUFFER_CAPACITY),
        );
        let count = std::cmp::max(1, config.grpc_subchannel_count.unwrap_or_default());
        for i in 0..count {
            let _ = tx.send(Change::Insert(i, endpoint.clone())).await;
        }

        let default_uri = default_endpoint
            .parse::<::tonic::transport::Uri>()
            .map_err(BuilderError::transport)?;
        let default_host = default_uri.host().unwrap_or("").to_string();

        let uri = endpoint.uri();
        let host = uri.host().unwrap_or("").to_string();
        let port = uri.port_u16().or_else(|| match uri.scheme_str() {
            Some("https") => Some(443),
            Some("http") => Some(80),
            _ => None,
        });

        let attrs = TracingAttributes {
            server_address: host,
            server_port: port.map(|p| p as i64),
            url_domain: default_host.clone(),
            instrumentation,
        };

        let inner_client = InnerClient::new(channel);
        if tracing_enabled {
            Ok((inner_client, Some(attrs)))
        } else {
            Ok((inner_client, None))
        }
    }

    async fn make_endpoint(
        endpoint: Option<String>,
        default_endpoint: &str,
        grpc_max_header_list_size: Option<u32>,
    ) -> ClientBuilderResult<::tonic::transport::Endpoint> {
        use ::tonic::transport::{ClientTlsConfig, Endpoint};

        let origin = crate::host::origin(endpoint.as_deref(), default_endpoint)
            .map_err(|e| e.client_builder())?;
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
        let mut endpoint = endpoint.origin(origin).concurrency_limit(100);
        if let Some(limit) = grpc_max_header_list_size {
            endpoint = endpoint.http2_max_header_list_size(limit);
        }
        Ok(endpoint)
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
        options: &RequestOptions,
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

    #[cfg(google_cloud_unstable_tracing)]
    fn create_grpc_span(
        &self,
        path: &http::uri::PathAndQuery,
        resend_count: Option<i64>,
    ) -> tracing::Span {
        use crate::observability::attributes::{self, keys, otel_status_codes};
        use opentelemetry_semantic_conventions::attribute as otel_attr;

        if let Some(attrs) = &self.tracing_attributes {
            let rpc_method = path.path().trim_start_matches('/');
            let (service, version, repo, artifact) = if let Some(info) = attrs.instrumentation {
                (
                    Some(info.service_name),
                    Some(info.client_version),
                    Some("googleapis/google-cloud-rust"),
                    Some(info.client_artifact),
                )
            } else {
                (None, None, None, None)
            };

            tracing::info_span!(
                "grpc.request",
                { keys::OTEL_NAME } = rpc_method,
                { keys::RPC_SYSTEM_NAME } = attributes::RPC_SYSTEM_GRPC,
                { keys::OTEL_KIND } = attributes::OTEL_KIND_CLIENT,
                { keys::RPC_METHOD } = rpc_method,
                { keys::SERVER_ADDRESS } = attrs.server_address,
                { keys::SERVER_PORT } = attrs.server_port,
                { otel_attr::URL_DOMAIN } = attrs.url_domain,
                { keys::RPC_RESPONSE_STATUS_CODE } = tracing::field::Empty,
                { keys::OTEL_STATUS_CODE } = otel_status_codes::UNSET,
                { keys::ERROR_TYPE } = tracing::field::Empty,
                { keys::GCP_CLIENT_SERVICE } = service,
                { keys::GCP_CLIENT_VERSION } = version,
                { keys::GCP_CLIENT_REPO } = repo,
                { keys::GCP_CLIENT_ARTIFACT } = artifact,
                { keys::GCP_GRPC_RESEND_COUNT } = resend_count,
                { keys::GCP_RESOURCE_DESTINATION_ID } = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        }
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

/// Convert a `tonic::Response` wrapping a prost message into a
/// `google_cloud_gax::response::Response` wrapping our equivalent message
pub fn to_gax_response<T, G>(response: tonic::Response<T>) -> Result<Response<G>>
where
    T: crate::prost::FromProto<G>,
{
    let (metadata, body, _extensions) = response.into_parts();
    Ok(Response::from_parts(
        Parts::new().set_headers(metadata.into_headers()),
        body.cnv().map_err(Error::deser)?,
    ))
}

#[cfg(test)]
#[cfg(google_cloud_unstable_tracing)]
mod tests {
    use super::Client;
    use crate::options::InstrumentationClientInfo;

    #[tokio::test(flavor = "multi_thread")]
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
