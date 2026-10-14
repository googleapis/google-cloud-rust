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

//! Helpers to run the same integration test against each gRPC transport.
//!
//! [`EchoTransport`] wraps the calls the tests make, and
//! [`both_transports!`](crate::both_transports) defines one test per
//! transport for a generic test function.

use crate::google::test::v1::{EchoRequest, EchoResponse};
use gaxi::options::ClientConfig;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::client_builder::ClientBuilder;
use google_cloud_gax::client_builder::Result as ClientBuilderResult;
use google_cloud_gax::client_builder::internal::{ClientFactory, new_builder};
use google_cloud_gax::options::RequestOptions;
use std::future::Future;
use std::marker::PhantomData;

pub use gaxi::grpc::Client;
#[cfg(google_cloud_unstable_grpc_rust)]
pub use gaxi::grpc::GrpcRustClient;

/// The echo service calls made by the integration tests.
///
/// Every transport implements this trait the same way, so a generic test
/// sends the same requests through each of them.
pub trait EchoTransport: Clone + Send + Sync + Sized + 'static {
    /// Creates a client that sends requests to `endpoint`.
    fn connect(
        config: ClientConfig,
        endpoint: &str,
    ) -> impl Future<Output = ClientBuilderResult<Self>> + Send;

    /// Calls `google.test.v1.EchoService/Echo`.
    fn echo(
        &self,
        request: EchoRequest,
        options: RequestOptions,
        request_params: &str,
    ) -> impl Future<Output = google_cloud_gax::Result<tonic::Response<EchoResponse>>> + Send;
}

/// Implements [`EchoTransport`] with the inherent methods that every client
/// provides.
macro_rules! impl_echo_transport {
    ($client:ty) => {
        impl EchoTransport for $client {
            async fn connect(config: ClientConfig, endpoint: &str) -> ClientBuilderResult<Self> {
                Self::new(config, endpoint).await
            }

            async fn echo(
                &self,
                request: EchoRequest,
                options: RequestOptions,
                request_params: &str,
            ) -> google_cloud_gax::Result<tonic::Response<EchoResponse>> {
                let mut extensions = tonic::Extensions::new();
                extensions.insert(tonic::GrpcMethod::new(
                    "google.test.v1.EchoServices",
                    "Echo",
                ));
                self.execute(
                    extensions,
                    http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                    request,
                    options,
                    "test-only-api-client/1.0",
                    request_params,
                )
                .await
            }
        }
    };
}

impl_echo_transport!(Client);
#[cfg(google_cloud_unstable_grpc_rust)]
impl_echo_transport!(GrpcRustClient);

/// A [`ClientFactory`] for `C` clients, see [`transport_builder`].
pub struct TransportFactory<C> {
    endpoint: String,
    client: PhantomData<fn() -> C>,
}

impl<C: EchoTransport> ClientFactory for TransportFactory<C> {
    type Client = C;
    type Credentials = Credentials;
    async fn build(self, config: ClientConfig) -> ClientBuilderResult<C> {
        C::connect(config, &self.endpoint).await
    }
}

/// Returns a [`ClientBuilder`] for `C` clients that send requests to
/// `endpoint`.
pub fn transport_builder<C: EchoTransport>(
    endpoint: impl Into<String>,
) -> ClientBuilder<TransportFactory<C>, Credentials> {
    new_builder(TransportFactory {
        endpoint: endpoint.into(),
        client: PhantomData,
    })
}

/// Defines `mod $name` with a `tonic` test and, when built with
/// `--cfg google_cloud_unstable_grpc_rust`, a `grpc_rust` test.
///
/// Both tests get the attributes passed to the macro (`#[tokio::test]` if
/// there are none), and call the generic
/// `async fn $name<C: EchoTransport>() -> anyhow::Result<()>` defined next
/// to the macro call.
///
/// ```ignore
/// both_transports!(echo_works);
/// async fn echo_works<C: EchoTransport>() -> anyhow::Result<()> { ... }
/// ```
#[macro_export]
macro_rules! both_transports {
    ($name:ident) => {
        $crate::both_transports!(#[tokio::test] $name);
    };
    ($(#[$attr:meta])+ $name:ident) => {
        mod $name {
            $(#[$attr])+
            async fn tonic() -> ::anyhow::Result<()> {
                super::$name::<$crate::transport::Client>().await
            }

            #[cfg(google_cloud_unstable_grpc_rust)]
            $(#[$attr])+
            async fn grpc_rust() -> ::anyhow::Result<()> {
                super::$name::<$crate::transport::GrpcRustClient>().await
            }
        }
    };
}
