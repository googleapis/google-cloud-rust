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
mod test {
    #[cfg(feature = "_internal_grpc_client")]
    mod grpc {
        use auth::credentials::Credential;
        use gax::Result;
        use google_cloud_gax_internal as gaxi;

        #[tokio::test]
        async fn test_build_default() -> Result<()> {
            let (endpoint, _server) = grpc_server::start_echo_server().await?;
            let client = FakeClient::builder().with_endpoint(endpont).build().await?;
            Ok(())
        }

        struct FakeClient {
            inner: gaxi::grpc::Client,
        }
        impl FakeClient {
            pub fn builder() -> fake_client::Builder {
                gax::client_builder::internal::new_builder(Self::new)
            }

            async fn new(config: ClientConfig<Credential>) -> Result<Self> {
                let inner =
                    gaxi::http::ReqwestClient::new(config, "https://secretmanager.googleapis.com")
                        .await?;
                Self { inner }
            }
        }
        mod fake_client {
            use super::gax;
            struct Factory;
            impl gax::client_builder::internal::ClientFactory for Factory {
                type Client = super::FakeClient;
                type Credentials = super::Credential;
                async fn build(
                    self,
                    config: gax::client_builder::internal::ClientConfig<Self::Credential>,
                ) -> Result<Self::Client> {
                    Self::Client::new(config).await
                }
            }
        }
    }

    #[cfg(feature = "_internal_http_client")]
    mod http {
        use auth::credentials::Credential;
        use gax::Result;
        use google_cloud_gax_internal as gaxi;

        #[tokio::test]
        async fn test_build_default() -> Result<()> {
            let (endpoint, _server) = echo_server::start().await?;
            let client = FakeClient::builder().with_endpoint(endpont).build().await?;
            Ok(())
        }

        pub struct FakeClient {
            inner: gaxi::http::ReqwestClient,
        }
        impl FakeClient {
            pub fn builder() -> fake_client::Builder {
                gax::client_builder::internal::new_builder(Self::new)
            }

            async fn new(config: ClientConfig<Credential>) -> Result<Self> {
                let inner =
                    gaxi::http::ReqwestClient::new(config, "https://secretmanager.googleapis.com")
                        .await?;
                Self { inner }
            }
        }
        pub mod fake_client {
            use super::gax;
            pub type Builder = gax::client_builder::ClientBuilder<Factory, super::Credential>;
            struct Factory;
            impl gax::client_builder::internal::ClientFactory for Factory {
                type Client = super::FakeClient;
                type Credentials = super::Credential;
                async fn build(
                    self,
                    config: gax::client_builder::internal::ClientConfig<Self::Credential>,
                ) -> Result<Self::Client> {
                    Self::Client::new(config).await
                }
            }
        }
    }
}
