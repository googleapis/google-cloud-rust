// Copyright 2026 Google LLC
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

use super::grpc_helpers;
use crate::grpc::tonic::{Extensions, Response as TonicResponse, Result as TonicResult};
use crate::options::{ClientConfig, InstrumentationClientInfo};
use crate::universe_domain::DEFAULT_UNIVERSE_DOMAIN;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::client_builder::{Error as BuilderError, Result as ClientBuilderResult};
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
use google_cloud_gax::polling_error_policy::PollingErrorPolicy;
use grpc::client::{Channel, ChannelOptions};
use grpc::credentials::LocalChannelCredentials;
// TODO(#5991): remove once grpc-rust corrects the typo.
use grpc::credentials::rustls::client::{
    ClientTlsConfig, RustlsChannelCredendials as RustlsChannelCredentials,
};
use http::Uri;
use std::sync::Arc;

mod bidi;

/// A gRPC client backed by the [grpc-rust][grpc] crate.
#[derive(Clone)]
pub struct GrpcRustClient {
    inner: Arc<GrpcRustClientInner>,
}

// TODO(#5991): Will be used by `bidi_stream_with_status` in an upcoming commit.
#[allow(dead_code)]
struct GrpcRustClientInner {
    credentials: Credentials,
    transport_metric: crate::observability::TransportMetric,
    tracing_attributes: Option<super::TracingAttributes>,
    endpoint: ResolvedGrpcEndpoint,
    invoker: Channel,
}

/// A gRPC endpoint resolved from the client configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedGrpcEndpoint {
    target: String,
    authority: String,
    tls: bool,
    server_address: String,
    server_port: Option<u16>,
}

impl GrpcRustClient {
    pub async fn new(config: ClientConfig, default_endpoint: &str) -> ClientBuilderResult<Self> {
        Self::build(config, default_endpoint, None).await
    }

    pub async fn new_with_instrumentation(
        config: ClientConfig,
        default_endpoint: &str,
        instrumentation: &'static InstrumentationClientInfo,
    ) -> ClientBuilderResult<Self> {
        Self::build(config, default_endpoint, Some(instrumentation)).await
    }

    pub async fn execute<Request, Response>(
        &self,
        _extensions: Extensions,
        _path: http::uri::PathAndQuery,
        _request: Request,
        _options: RequestOptions,
        _api_client_header: &'static str,
        _request_params: &str,
    ) -> GaxResult<TonicResponse<Response>>
    where
        Request: prost::Message + Clone + 'static,
        Response: prost::Message + Default + 'static,
    {
        unimplemented!("not implemented yet")
    }

    pub async fn bidi_stream<Request, Response>(
        &self,
        _extensions: Extensions,
        _path: http::uri::PathAndQuery,
        _request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
        _options: RequestOptions,
        _api_client_header: &'static str,
        _request_params: &str,
    ) -> GaxResult<TonicResponse<GrpcRustStreaming<Response>>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        unimplemented!("not implemented yet")
    }

    pub async fn bidi_stream_with_status<Request, Response>(
        &self,
        _extensions: Extensions,
        _path: http::uri::PathAndQuery,
        _request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
        _options: RequestOptions,
        _api_client_header: &'static str,
        _request_params: &str,
    ) -> GaxResult<TonicResult<TonicResponse<GrpcRustStreaming<Response>>>>
    where
        Request: prost::Message + 'static,
        Response: prost::Message + Default + 'static,
    {
        unimplemented!("not implemented yet")
    }

    pub fn get_polling_error_policy(
        &self,
        _options: &RequestOptions,
    ) -> Arc<dyn PollingErrorPolicy> {
        unimplemented!("not implemented yet")
    }

    pub fn get_polling_backoff_policy(
        &self,
        _options: &RequestOptions,
    ) -> Arc<dyn PollingBackoffPolicy> {
        unimplemented!("not implemented yet")
    }

    async fn build(
        config: ClientConfig,
        default_endpoint: &str,
        instrumentation: Option<&'static InstrumentationClientInfo>,
    ) -> ClientBuilderResult<Self> {
        Self::check_unused_config_options(&config);

        let credentials = grpc_helpers::make_credentials(&config)?;
        let tracing_enabled = crate::options::tracing_enabled(&config);
        let universe_domain =
            crate::universe_domain::resolve(config.universe_domain.as_deref(), &credentials)
                .await?;
        let endpoint = resolve_endpoint(&config, default_endpoint, &universe_domain)?;
        let tracing_attributes = tracing_enabled
            .then(|| make_tracing_attributes(default_endpoint, &endpoint, instrumentation))
            .transpose()?;

        // NOTE: The existing Tonic-based `Client` uses
        // `Channel::balance_channel()` and inserts
        // max(1, grpc_subchannel_count) copies of the endpoint. This creates
        // independently connected subchannels behind Tonic's load balancer.
        // While grpc-rust internally supports a `round_robin` policy, it is not
        // exposed for use.
        //
        // For now, ignore `grpc_subchannel_count` and use grpc-rust's default
        // `pick_first`. If measurements show that this isn't sufficient, we
        // can consider implementing a connection pool.
        let options = ChannelOptions::default().override_authority(endpoint.authority.clone());

        let invoker = if endpoint.tls {
            Channel::new(endpoint.target.clone(), make_tls_credentials()?, options)
        } else {
            Channel::new(
                endpoint.target.clone(),
                LocalChannelCredentials::new_arc(),
                options,
            )
        };

        Ok(Self {
            inner: Arc::new(GrpcRustClientInner {
                credentials,
                transport_metric: crate::observability::TransportMetric::new(instrumentation),
                tracing_attributes,
                endpoint,
                invoker,
            }),
        })
    }

    fn check_unused_config_options(config: &ClientConfig) {
        // These are present in `ClientConfig` but are not supported by
        // grpc-rust.
        debug_assert!(
            config.grpc_request_buffer_capacity.is_none(),
            "grpc_request_buffer_capacity is not supported by grpc-rust"
        );
        debug_assert!(
            config.grpc_max_header_list_size.is_none(),
            "grpc_max_header_list_size is not supported by grpc-rust"
        );
    }
}

impl std::fmt::Debug for GrpcRustClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRustClient")
            .field("transport_metric", &self.inner.transport_metric)
            .field("endpoint", &self.inner.endpoint)
            .field("tracing_attributes", &self.inner.tracing_attributes)
            .finish()
    }
}

#[derive(Debug)]
pub struct GrpcRustStreaming<Response> {
    // TODO(#5991): not implemented yet
    _phantom: std::marker::PhantomData<Response>,
}

impl<Response> GrpcRustStreaming<Response>
where
    Response: prost::Message + Default,
{
    pub async fn message(&mut self) -> TonicResult<Option<Response>> {
        unimplemented!("not implemented yet")
    }
}

/// Resolves endpoint settings from [`ClientConfig`] and a default service
/// endpoint into grpc-rust channel settings.
///
/// The connection target and request authority are resolved independently.
/// VIP and private-network overrides are used as connection targets, while
/// requests use the default service authority.
///
/// When no endpoint override is configured, `universe_domain` is substituted
/// into the default endpoint. It is also used to select the request authority
/// for Google Cloud endpoints.
// TODO(#5991): Consider refactoring endpoint resolution with `Client`.
fn resolve_endpoint(
    config: &ClientConfig,
    default_endpoint: &str,
    universe_domain: &str,
) -> ClientBuilderResult<ResolvedGrpcEndpoint> {
    let endpoint_override = config.endpoint.as_deref();
    let target_endpoint =
        resolve_target_endpoint(endpoint_override, default_endpoint, universe_domain);
    let authority =
        resolve_request_authority(endpoint_override, default_endpoint, universe_domain)?;

    resolve_transport_endpoint(target_endpoint, authority)
}

/// Resolves the endpoint used to derive the connection target.
fn resolve_target_endpoint(
    endpoint: Option<&str>,
    default_endpoint: &str,
    universe_domain: &str,
) -> String {
    endpoint
        .map(str::to_string)
        .unwrap_or_else(|| default_endpoint.replace(DEFAULT_UNIVERSE_DOMAIN, universe_domain))
}

/// Resolves the HTTP/2 request authority for an endpoint.
fn resolve_request_authority(
    endpoint: Option<&str>,
    default_endpoint: &str,
    universe_domain: &str,
) -> ClientBuilderResult<String> {
    let origin = crate::host::origin(endpoint, default_endpoint, universe_domain)
        .map_err(|e| e.client_builder())?;

    origin
        .authority()
        .ok_or_else(|| BuilderError::transport(format!("missing authority in endpoint: {origin}")))
        .map(|authority| authority.as_str().to_string())
}

/// Resolves a connection endpoint into grpc-rust's channel settings.
fn resolve_transport_endpoint(
    target_endpoint: String,
    authority: String,
) -> ClientBuilderResult<ResolvedGrpcEndpoint> {
    let target_uri = target_endpoint
        .parse::<http::Uri>()
        .map_err(BuilderError::transport)?;
    let target_authority = target_uri.authority().ok_or_else(|| {
        BuilderError::transport(format!("missing authority in endpoint: {target_endpoint}"))
    })?;

    let tls = target_uri.scheme() == Some(&http::uri::Scheme::HTTPS);
    let port = resolve_port(&target_uri, target_authority);
    let host = target_authority.host();
    let target = resolve_dns_target(host, port);

    Ok(ResolvedGrpcEndpoint {
        target,
        authority,
        tls,
        server_address: host.to_string(),
        server_port: port,
    })
}

/// Gets the specified port. If there is none, infers it from the URI schema.
fn resolve_port(uri: &Uri, authority: &http::uri::Authority) -> Option<u16> {
    authority.port_u16().or_else(|| match uri.scheme_str() {
        Some("https") => Some(443),
        Some("http") => Some(80),
        _ => None,
    })
}

/// Resolves a host and port into a DNS target.
///
/// IPv6 literals are bracketed when needed.
fn resolve_dns_target(host: &str, port: Option<u16>) -> String {
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };

    match port {
        Some(port) => format!("dns:///{host}:{port}"),
        None => format!("dns:///{host}"),
    }
}

fn make_tracing_attributes(
    default_endpoint: &str,
    endpoint: &ResolvedGrpcEndpoint,
    instrumentation: Option<&'static InstrumentationClientInfo>,
) -> ClientBuilderResult<super::TracingAttributes> {
    let default_uri = default_endpoint
        .parse::<Uri>()
        .map_err(BuilderError::transport)?;
    Ok(super::TracingAttributes {
        server_address: endpoint.server_address.clone(),
        server_port: endpoint.server_port.map(i64::from),
        url_domain: default_uri.host().unwrap_or_default().to_string(),
        instrumentation,
    })
}

// NOTE: Tonic first uses an installed process-default CryptoProvider;
// otherwise, it selects the provider enabled through its Cargo features. In
// contrast, grpc-rust's RustlsChannelCredentials::new requires a
// process-wide rustls CryptoProvider to be installed.
//
// For now, we install aws-lc-rs if the _default-rustls-provider feature
// is enabled; otherwise, we raise an error if the user hasn't installed
// a process-wide one.
fn make_tls_credentials() -> ClientBuilderResult<Arc<RustlsChannelCredentials>> {
    #[cfg(feature = "_default-rustls-provider")]
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    RustlsChannelCredentials::new(ClientTlsConfig::new())
        .map(Arc::new)
        .map_err(|source| {
            BuilderError::transport(format!(
                "failed to create grpc-rust TLS credentials: {source}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use test_case::test_case;

    static TEST_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
        service_name: "test-service",
        client_version: "1.0.0",
        client_artifact: "test-artifact",
        default_host: "example.com",
    };

    // TODO(#5991): Considering refactoring these endpoint tests and the Tonic
    // endpoint tests.
    #[test_case(
        None,
        "googleapis.com",
        "dns:///storage.googleapis.com:443",
        "storage.googleapis.com",
        "storage.googleapis.com",
        Some(443),
        true;
        "default"
    )]
    #[test_case(
        Some("https://storage.googleapis.com"),
        "googleapis.com",
        "dns:///storage.googleapis.com:443",
        "storage.googleapis.com",
        "storage.googleapis.com",
        Some(443),
        true;
        "explicit https"
    )]
    #[test_case(
        Some("http://localhost:8080"),
        "googleapis.com",
        "dns:///localhost:8080",
        "localhost:8080",
        "localhost",
        Some(8080),
        false;
        "emulator"
    )]
    #[test_case(
        Some("http://[::1]:8080"),
        "googleapis.com",
        "dns:///[::1]:8080",
        "[::1]:8080",
        "[::1]",
        Some(8080),
        false;
        "ipv6 emulator"
    )]
    #[test_case(
        Some("https://private.googleapis.com"),
        "googleapis.com",
        "dns:///private.googleapis.com:443",
        "storage.googleapis.com",
        "private.googleapis.com",
        Some(443),
        true;
        "private endpoint"
    )]
    #[test_case(
        Some("https://us-central1-storage.googleapis.com"),
        "googleapis.com",
        "dns:///us-central1-storage.googleapis.com:443",
        "us-central1-storage.googleapis.com",
        "us-central1-storage.googleapis.com",
        Some(443),
        true;
        "locational endpoint"
    )]
    #[test_case(
        None,
        "example.com",
        "dns:///storage.example.com:443",
        "storage.example.com",
        "storage.example.com",
        Some(443),
        true;
        "universe domain"
    )]
    fn resolve_endpoint_cases(
        endpoint: Option<&str>,
        universe_domain: &str,
        want_target: &str,
        want_authority: &str,
        want_server_address: &str,
        want_server_port: Option<u16>,
        want_tls: bool,
    ) -> anyhow::Result<()> {
        // Arrange
        let mut config = ClientConfig::default();
        config.endpoint = endpoint.map(str::to_string);

        // Act
        let got = resolve_endpoint(&config, "https://storage.googleapis.com", universe_domain)?;

        // Assert
        assert_eq!(got.target, want_target);
        assert_eq!(got.authority, want_authority);
        assert_eq!(got.server_address, want_server_address);
        assert_eq!(got.server_port, want_server_port);
        assert_eq!(got.tls, want_tls);
        Ok(())
    }

    #[tokio::test]
    async fn new_does_not_construct_tracing_attributes_when_tracing_is_disabled()
    -> anyhow::Result<()> {
        // Arrange
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        config.tracing = false;

        // Act
        let client = GrpcRustClient::new(config, "http://example.com:8080").await?;

        // Assert
        assert!(client.inner.tracing_attributes.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn new_constructs_tracing_attributes_without_instrumentation() -> anyhow::Result<()> {
        // Arrange
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        config.tracing = true;

        // Act
        let client = GrpcRustClient::new(config, "http://example.com:8080").await?;

        // Assert
        let attributes = client
            .inner
            .tracing_attributes
            .as_ref()
            .expect("tracing should be configured");
        assert_eq!(attributes.server_address, "example.com");
        assert_eq!(attributes.server_port, Some(8080));
        assert_eq!(attributes.url_domain, "example.com");
        assert_eq!(attributes.instrumentation, None);
        Ok(())
    }

    #[tokio::test]
    async fn new_with_instrumentation_constructs_tracing_attributes_with_instrumentation()
    -> anyhow::Result<()> {
        // Arrange
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        config.tracing = true;

        // Act
        let client =
            GrpcRustClient::new_with_instrumentation(config, "http://example.com:8080", &TEST_INFO)
                .await?;

        // Assert
        let attributes = client
            .inner
            .tracing_attributes
            .as_ref()
            .expect("tracing should be configured");
        assert_eq!(attributes.server_address, "example.com");
        assert_eq!(attributes.server_port, Some(8080));
        assert_eq!(attributes.url_domain, "example.com");
        assert_eq!(attributes.instrumentation, Some(&TEST_INFO));
        Ok(())
    }
}
