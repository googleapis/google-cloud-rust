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

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "_internal-grpc-client", feature = "_internal-http-client"))]
    const DEFAULT_ENDPOINT: &str = "https://kms.googleapis.com";

    #[cfg(feature = "_internal-grpc-client")]
    mod grpc {
        use auth::credentials::Credentials;
        use google_cloud_gax_internal as gaxi;

        #[tokio::test]
        async fn test_build_default() -> anyhow::Result<()> {
            let (endpoint, _server) = grpc_server::start_echo_server().await?;
            let _client = FakeClient::builder()
                .with_endpoint(endpoint)
                .build()
                .await?;
            Ok(())
        }

        #[allow(dead_code)]
        pub struct FakeClient {
            inner: gaxi::grpc::Client,
        }
        impl FakeClient {
            pub fn builder() -> ClientBuilder {
                gax::client_builder::internal::new_builder(fake_client::Factory)
            }

            async fn new(config: gaxi::options::ClientConfig) -> gax::client_builder::Result<Self> {
                let inner = gaxi::grpc::Client::new(config, super::DEFAULT_ENDPOINT).await?;
                Ok(Self { inner })
            }
        }
        /// Make this visible for documentation purposes.
        pub type ClientBuilder =
            gax::client_builder::ClientBuilder<fake_client::Factory, Credentials>;
        // Note the pub(self), the types in this module are not accessible to
        // application developers.
        mod fake_client {
            use super::gaxi;
            pub struct Factory;
            impl gax::client_builder::internal::ClientFactory for Factory {
                type Client = super::FakeClient;
                type Credentials = super::Credentials;
                async fn build(
                    self,
                    config: gaxi::options::ClientConfig,
                ) -> gax::client_builder::Result<Self::Client> {
                    Self::Client::new(config).await
                }
            }
        }
    }

    #[cfg(feature = "_internal-http-client")]
    mod http {
        use auth::credentials::Credentials;
        use google_cloud_gax_internal as gaxi;

        #[tokio::test]
        async fn test_build_default() -> Result<(), Box<dyn std::error::Error>> {
            let (endpoint, _server) = echo_server::start().await?;
            let _client = FakeClient::builder()
                .with_endpoint(endpoint)
                .build()
                .await?;
            Ok(())
        }

        #[allow(dead_code)]
        pub struct FakeClient {
            inner: gaxi::http::ReqwestClient,
        }
        impl FakeClient {
            pub fn builder() -> ClientBuilder {
                gax::client_builder::internal::new_builder(fake_client::Factory)
            }

            async fn new(config: gaxi::options::ClientConfig) -> gax::client_builder::Result<Self> {
                let inner = gaxi::http::ReqwestClient::new(config, super::DEFAULT_ENDPOINT).await?;
                Ok(Self { inner })
            }
        }
        /// Make this visible for documentation purposes.
        pub type ClientBuilder =
            gax::client_builder::ClientBuilder<fake_client::Factory, Credentials>;
        // Note the pub(self), the types in this module are not accessible to
        // application developers.
        mod fake_client {
            use super::gaxi;
            pub struct Factory;
            impl gax::client_builder::internal::ClientFactory for Factory {
                type Client = super::FakeClient;
                type Credentials = super::Credentials;
                async fn build(
                    self,
                    config: gaxi::options::ClientConfig,
                ) -> gax::client_builder::Result<Self::Client> {
                    Self::Client::new(config).await
                }
            }
        }
    }
}
