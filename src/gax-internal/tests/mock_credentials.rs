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

use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use google_cloud_gax::error::CredentialsError;
use http::{
    Extensions, HeaderMap,
    header::{HeaderName, HeaderValue},
};

type AuthResult<T> = std::result::Result<T, CredentialsError>;

mockall::mock! {
    #[derive(Debug)]
    pub Credentials {}

    impl CredentialsProvider for Credentials {
        async fn headers(&self, extensions: Extensions) -> AuthResult<CacheableResource<HeaderMap>>;
        async fn universe_domain(&self) -> Option<String>;
    }
}

pub fn mock_credentials() -> MockCredentials {
    // We use mock credentials instead of fake credentials, because
    // 1. we can test that multiple headers are included in the request
    // 2. it gives us extra confidence that our interfaces are called
    let mut mock = MockCredentials::new();
    let header = HeaderMap::from_iter([
        (
            HeaderName::from_static("auth-key-1"),
            HeaderValue::from_static("auth-value-1"),
        ),
        (
            HeaderName::from_static("auth-key-2"),
            HeaderValue::from_static("auth-value-2"),
        ),
    ]);
    mock.expect_headers().return_once(|_extensions| {
        Ok(CacheableResource::New {
            entity_tag: EntityTag::default(),
            data: header,
        })
    });
    mock
}
