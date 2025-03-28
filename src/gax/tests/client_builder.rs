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

#[cfg(all(test, feature = "unstable-sdk-client"))]
mod test {
    use gax::client_builder::internal::ClientConfig;
    use google_cloud_gax as gax;

    #[test]
    fn test_default() {
        let client = MyClient::builder().build();
        assert_eq!(client.endpoint, None);
    }

    #[test]
    fn test_with_endpoint() {
        let client = MyClient::builder().with_endpoint("abc123").build();
        assert_eq!(client.endpoint.as_deref(), Some("abc123"));
    }

    struct Credential;

    struct MyClient {
        endpoint: Option<String>,
    }
    impl MyClient {
        pub fn builder() -> my_client::Builder {
            gax::client_builder::internal::new_builder(Self::new)
        }

        fn new(config: ClientConfig<Credential>) -> Self {
            Self {
                endpoint: config.endpoint,
            }
        }
    }
    pub mod my_client {
        use super::Credential;
        use super::gax;
        pub(super) type Builder = gax::client_builder::ClientBuilder<
            fn(super::ClientConfig<Credential>) -> super::MyClient,
            Credential,
        >;
    }
}
