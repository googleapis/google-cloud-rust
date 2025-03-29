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

// Verify `ClientBuilder` can be used outside the crate.
#[cfg(test)]
mod test {
    use gax::client_builder::internal::ClientConfig;
    use google_cloud_gax as gax;

    #[tokio::test]
    async fn test_default() -> anyhow::Result<()> {
        let client = MyClient::builder().build().await?;
        assert_eq!(client.endpoint, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_endpoint() -> anyhow::Result<()> {
        let client = MyClient::builder().with_endpoint("abc123").build().await?;
        assert_eq!(client.endpoint.as_deref(), Some("abc123"));
        Ok(())
    }

    pub struct Credential;

    pub struct MyClient {
        endpoint: Option<String>,
    }
    impl MyClient {
        pub fn builder() -> my_client::Builder {
            gax::client_builder::internal::new_builder(my_client::Factory)
        }

        async fn new(config: ClientConfig<Credential>) -> gax::Result<Self> {
            let endpoint = config.endpoint;
            Ok(Self { endpoint })
        }
    }
    mod my_client {
        use super::gax;
        pub type Builder = gax::client_builder::ClientBuilder<Factory, super::Credential>;
        pub struct Factory;
        impl gax::client_builder::internal::ClientFactory for Factory {
            type Client = super::MyClient;
            type Credentials = super::Credential;
            async fn build(
                self,
                config: gax::client_builder::internal::ClientConfig<Self::Credentials>,
            ) -> gax::Result<Self::Client> {
                Self::Client::new(config).await
            }
        }
    }
}
