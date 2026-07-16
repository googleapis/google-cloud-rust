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
mod grpc_helpers;
#[cfg(google_cloud_unstable_grpc_rust)]
mod grpc_rust;
pub mod status;
pub mod tonic;
mod tracing_attributes;
mod transport_policies;
#[cfg(google_cloud_unstable_grpc_rust)]
pub use grpc_rust::{GrpcRustClient, GrpcRustStreaming};
pub use tracing_attributes::TracingAttributes;

use crate::attempt_interceptor::AttemptInterceptor;
use crate::observability::attributes::{self, keys::*, otel_status_codes};
use crate::universe_domain::DEFAULT_UNIVERSE_DOMAIN;
use ::tonic::client::Grpc;
use ::tonic::transport::Channel;
use from_status::to_gax_error;
use futures::TryFutureExt;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::Result;
use google_cloud_gax::client_builder::Error as BuilderError;
use google_cloud_gax::client_builder::Result as ClientBuilderResult;
use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_error_policy::PollingErrorPolicy;
use google_cloud_gax::response::{Parts, Response};
use google_cloud_gax::retry_loop_internal::retry_loop;
use grpc_helpers::{add_auth_headers, make_credentials, make_headers};
use http::HeaderMap;
use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
use std::sync::Arc;
use std::time::Duration;
use transport_policies::TransportPolicies;

// A tonic::transport::Channel always has a Buffer layer.
const DEFAULT_REQUEST_BUFFER_CAPACITY: usize = 1024;

pub type GrpcService = Channel;

/// The inner gRPC client type.
pub type InnerClient = Grpc<GrpcService>;

#[derive(Clone, Debug)]
pub struct Client {
    inner: InnerClient,
    metric: crate::observability::TransportMetric,
    tracing_attributes: Option<TracingAttributes>,
    credentials: Credentials,
    transport_policies: TransportPolicies,
    attempt_interceptor: Option<Arc<dyn AttemptInterceptor>>,
}

impl Client {
    /// Create a new client.
    pub async fn new(
        config: crate::options::ClientConfig,
        default_endpoint: &str,
    ) -> ClientBuilderResult<Self> {
        Self::build(config, default_endpoint, None).await
    }

    /// Create a new client with instrumentation info.
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
        instrumentation: Option<&'static crate::options::InstrumentationClientInfo>,
    ) -> ClientBuilderResult<Self> {
        let credentials = make_credentials(&config)?;
        let tracing_enabled = crate::options::tracing_enabled(&config);
        let universe_domain =
            crate::universe_domain::resolve(config.universe_domain.as_deref(), &credentials)
                .await?;

        let (inner, tracing_attributes) = Self::make_inner(
            &config,
            default_endpoint,
            tracing_enabled,
            &universe_domain,
            instrumentation,
        )
        .await?;

        Ok(Self {
            inner,
            metric: crate::observability::TransportMetric::new(instrumentation),
            tracing_attributes,
            credentials,
            transport_policies: TransportPolicies::from_config(&config),
            attempt_interceptor: None,
        })
    }

    /// Sets the attempt interceptor for the client.
    pub fn set_attempt_interceptor(&mut self, interceptor: Arc<dyn AttemptInterceptor>) {
        self.attempt_interceptor = Some(interceptor);
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
        let headers = make_headers(api_client_header, request_params, &options)?;
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
        let headers = make_headers(api_client_header, request_params, &options)?;
        let mut headers = add_auth_headers(headers, &self.credentials).await?;
        self.attempt_interceptor.intercept(&mut headers, 1);
        let metadata = tonic::MetadataMap::from_headers(headers);
        let request = ::tonic::Request::from_parts(metadata, extensions, request);
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;
        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            recorder.on_grpc_request(&path);
        }
        let result = inner
            .streaming(request.into_streaming_request(), path, codec)
            .await;
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
        use ::tonic::IntoRequest;
        let headers = make_headers(api_client_header, request_params, &options)?;
        let mut headers = add_auth_headers(headers, &self.credentials).await?;
        self.attempt_interceptor.intercept(&mut headers, 1);
        let metadata = tonic::MetadataMap::from_headers(headers);
        let mut request = ::tonic::Request::from_parts(metadata, extensions, request);
        if let Some(timeout) = crate::options::resolve_effective_timeout(
            &options,
            self.transport_policies.attempt_timeout(),
            None,
        ) {
            request.set_timeout(timeout);
        }
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;
        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            recorder.on_grpc_request(&path);
        }
        let result = inner
            .server_streaming(request.into_request(), path, codec)
            .await;
        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            match &result {
                Ok(_) => recorder.on_grpc_response(),
                Err(e) => recorder.on_grpc_error(&to_gax_error(e.clone())),
            }
        }
        Ok(result)
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
        let retry_throttler = self.transport_policies.get_retry_throttler(&options);
        let retry_policy = self.transport_policies.get_retry_policy(&options);
        let backoff_policy = self.transport_policies.get_backoff_policy(&options);
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
        remaining_time: Option<Duration>,
        headers: HeaderMap,
        prior_attempt_count: i64,
    ) -> Result<tonic::Response<Response>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        let span = if let Some(attrs) = &self.tracing_attributes {
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
            let resend_count = if prior_attempt_count > 0 {
                Some(prior_attempt_count)
            } else {
                None
            };

            tracing::info_span!(
                "grpc.request",
                { OTEL_NAME } = rpc_method,
                { RPC_SYSTEM_NAME } = attributes::RPC_SYSTEM_GRPC,
                { OTEL_KIND } = attributes::OTEL_KIND_CLIENT,
                { otel_trace::RPC_METHOD } = rpc_method,
                { otel_trace::SERVER_ADDRESS } = attrs.server_address,
                { otel_trace::SERVER_PORT } = attrs.server_port,
                { otel_attr::URL_DOMAIN } = attrs.url_domain,
                { RPC_RESPONSE_STATUS_CODE } = tracing::field::Empty,
                { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
                { otel_trace::ERROR_TYPE } = tracing::field::Empty,
                { GCP_CLIENT_SERVICE } = service,
                { GCP_CLIENT_VERSION } = version,
                { GCP_CLIENT_REPO } = repo,
                { GCP_CLIENT_ARTIFACT } = artifact,
                { GCP_GRPC_RESEND_COUNT } = resend_count,
                { GCP_RESOURCE_DESTINATION_ID } = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        };

        #[allow(unused_mut)]
        let mut headers = add_auth_headers(headers, &self.credentials).await?;

        crate::observability::propagation::inject_context(&span, &mut headers);
        self.attempt_interceptor
            .intercept(&mut headers, prior_attempt_count as u32 + 1);

        let metadata = tonic::MetadataMap::from_headers(headers);
        let mut request = ::tonic::Request::from_parts(metadata, extensions, request);

        if let Some(timeout) = crate::options::resolve_effective_timeout(
            options,
            self.transport_policies.attempt_timeout(),
            remaining_time,
        ) {
            request.set_timeout(timeout);
        }
        let codec = tonic_prost::ProstCodec::<Request, Response>::default();
        let mut inner = self.inner.clone();
        inner.ready().await.map_err(Error::io)?;

        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            recorder.on_grpc_request(&path);
        }

        let pending = inner.unary(request, path, codec).map_err(to_gax_error);

        use crate::observability::{WithTransportLogging, WithTransportMetric, WithTransportSpan};

        let pending =
            WithTransportMetric::new(self.metric.clone(), pending, prior_attempt_count as u32);
        let pending = WithTransportLogging::new(pending);
        let pending = WithTransportSpan::new(span, pending);

        if let Some(recorder) = crate::observability::RequestRecorder::current() {
            recorder.scope(pending).await
        } else {
            pending.await
        }
    }

    async fn make_inner(
        config: &crate::options::ClientConfig,
        default_endpoint: &str,
        tracing_enabled: bool,
        universe_domain: &str,
        instrumentation: Option<&'static crate::options::InstrumentationClientInfo>,
    ) -> ClientBuilderResult<(InnerClient, Option<TracingAttributes>)> {
        use ::tonic::transport::{Channel, channel::Change};
        let endpoint = Self::make_endpoint(
            config.endpoint.clone(),
            default_endpoint,
            universe_domain,
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
        universe_domain: &str,
        grpc_max_header_list_size: Option<u32>,
    ) -> ClientBuilderResult<::tonic::transport::Endpoint> {
        use ::tonic::transport::{ClientTlsConfig, Endpoint};

        let service_endpoint = default_endpoint.replace(DEFAULT_UNIVERSE_DOMAIN, universe_domain);
        let origin = crate::host::origin(endpoint.as_deref(), default_endpoint, universe_domain)
            .map_err(|e| e.client_builder())?;
        let target_endpoint = endpoint.unwrap_or(service_endpoint);
        let endpoint = Endpoint::from_shared(target_endpoint).map_err(BuilderError::transport)?;
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

    pub fn get_polling_error_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn PollingErrorPolicy> {
        self.transport_policies.get_polling_error_policy(options)
    }

    pub fn get_polling_backoff_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn PollingBackoffPolicy> {
        self.transport_policies.get_polling_backoff_policy(options)
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
mod tests {
    use super::*;
    use crate::options::InstrumentationClientInfo;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    #[test_case(None, DEFAULT_UNIVERSE_DOMAIN, "https://test.googleapis.com/"; "default GDU")]
    #[test_case(None, "my-custom-universe.com", "https://test.my-custom-universe.com/"; "default custom universe domain")]
    #[test_case(Some("https://test.googleapis.com/"), DEFAULT_UNIVERSE_DOMAIN, "https://test.googleapis.com/"; "GDU override")]
    #[test_case(Some("https://another-custom-universe.com/"), "my-custom-universe.com", "https://another-custom-universe.com/"; "custom endpoint override with universe domain")]
    #[test_case(Some("https://test.us-central1.rep.googleapis.com/"), "my-custom-universe.com", "https://test.us-central1.rep.googleapis.com/"; "regional endpoint with universe domain")]
    #[test_case(Some("http://www.my-custom-universe.com/"), "my-custom-universe.com", "http://www.my-custom-universe.com/"; "global custom universe")]
    #[test_case(Some("http://private.my-custom-universe.com/"), "my-custom-universe.com", "http://private.my-custom-universe.com/"; "VPC-SC private custom universe")]
    #[test_case(Some("http://restricted.my-custom-universe.com/"), "my-custom-universe.com", "http://restricted.my-custom-universe.com/"; "VPC-SC restricted custom universe")]
    #[test_case(Some("http://test-my-private-ep.p.my-custom-universe.com/"), "my-custom-universe.com", "http://test-my-private-ep.p.my-custom-universe.com/"; "PSC custom endpoint custom universe")]
    #[test_case(Some("https://us-central1-test.my-custom-universe.com/"), "my-custom-universe.com", "https://us-central1-test.my-custom-universe.com/"; "locational custom universe")]
    #[test_case(Some("https://us-central1-test.googleapis.com/"), "my-custom-universe.com", "https://us-central1-test.googleapis.com/"; "locational endpoint with universe domain")]
    #[test_case(Some("https://us-central1-test.googleapis.com/"), DEFAULT_UNIVERSE_DOMAIN, "https://us-central1-test.googleapis.com/"; "locational GDU")]
    #[test_case(Some("https://test.us-central1.rep.my-custom-universe.com/"), "my-custom-universe.com", "https://test.us-central1.rep.my-custom-universe.com/"; "regional custom universe")]
    #[test_case(Some("https://test.us-central1.rep.googleapis.com/"), DEFAULT_UNIVERSE_DOMAIN, "https://test.us-central1.rep.googleapis.com/"; "regional GDU")]

    async fn make_endpoint_with_universe_domain(
        endpoint_override: Option<&str>,
        universe_domain: &str,
        expected_uri: &str,
    ) -> TestResult {
        let default_endpoint = "https://test.googleapis.com";
        let endpoint = Client::make_endpoint(
            endpoint_override.map(String::from),
            default_endpoint,
            universe_domain,
            None,
        )
        .await?;

        assert_eq!(endpoint.uri().to_string(), expected_uri);

        Ok(())
    }

    #[tokio::test]
    async fn make_endpoint_with_universe_domain_mismatch() -> TestResult {
        let mut config = crate::options::ClientConfig::default();
        config.universe_domain = Some("my-custom-universe.com".to_string());
        config.cred = Some(google_cloud_auth::credentials::anonymous::Builder::new().build());

        let err = Client::new(config, "https://language.googleapis.com")
            .await
            .unwrap_err();

        assert!(err.is_universe_domain_mismatch(), "{err:?}");

        Ok(())
    }

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
