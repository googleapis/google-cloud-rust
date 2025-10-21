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

//! [API Key] Credentials type.
//!
//! An API key is a simple encrypted string that you can use when calling
//! Google Cloud APIs. When you use API keys in your applications, ensure that
//! they are kept secure during both storage and transmission.
//!
//! [API Key]: https://cloud.google.com/api-keys/docs/overview

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{CacheableResource, Credentials, Result};
use crate::headers_util::build_cacheable_api_key_headers;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use http::{Extensions, HeaderMap};
use std::sync::Arc;

struct ApiKeyTokenProvider {
    api_key: String,
}

impl std::fmt::Debug for ApiKeyTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyCredentials")
            .field("api_key", &"[censored]")
            .finish()
    }
}

#[async_trait::async_trait]
impl TokenProvider for ApiKeyTokenProvider {
    async fn token(&self) -> Result<Arc<Token>> {
        Ok(Arc::new(Token {
            token: self.api_key.clone(),
            token_type: String::new(),
            expires_at: None,
            metadata: None,
        }))
    }
}

#[derive(Debug)]
struct ApiKeyCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
}

/// A builder for creating credentials that authenticate using an [API key].
///
/// API keys are convenient because no [principal] is needed. The API key
/// associates the request with a Google Cloud project for billing and quota
/// purposes.
///
/// Note that only some Cloud APIs support API keys. The rest require full
/// credentials.
///
/// [API key]: https://cloud.google.com/docs/authentication/api-keys-use
/// [principal]: https://cloud.google.com/docs/authentication#principal
#[derive(Debug)]
pub struct Builder {
    api_key: String,
}

impl Builder {
    /// Creates a new builder with given API key.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::api_key_credentials::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::new("my-api-key")
    ///     .build();
    /// # });
    /// ```
    pub fn new<T: Into<String>>(api_key: T) -> Self {
        Self {
            api_key: api_key.into(),
        }
    }

    fn build_token_provider(self) -> ApiKeyTokenProvider {
        ApiKeyTokenProvider {
            api_key: self.api_key,
        }
    }

    /// Returns a [Credentials] instance with the configured settings.
    pub fn build(self) -> Credentials {
        Credentials {
            inner: Arc::new(ApiKeyCredentials {
                token_provider: TokenCache::new(self.build_token_provider()),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for ApiKeyCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let cached_token = self.token_provider.token(extensions).await?;
        build_cacheable_api_key_headers(&cached_token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::get_headers_from_cache;
    use http::HeaderValue;
    use scoped_env::ScopedEnv;

    const API_KEY_HEADER_KEY: &str = "x-goog-api-key";
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn debug_token_provider() {
        let expected = Builder::new("test-api-key").build_token_provider();

        let fmt = format!("{expected:?}");
        assert!(!fmt.contains("super-secret-api-key"), "{fmt}");
    }

    #[tokio::test]
    async fn api_key_credentials_token_provider() {
        let tp = Builder::new("test-api-key").build_token_provider();
        assert_eq!(
            *tp.token().await.unwrap(),
            Token {
                token: "test-api-key".to_string(),
                token_type: String::new(),
                expires_at: None,
                metadata: None,
            }
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_basic() -> TestResult {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let creds = Builder::new("test-api-key").build();
        let headers = get_headers_from_cache(creds.headers(Extensions::new()).await.unwrap())?;
        let value = headers.get(API_KEY_HEADER_KEY).unwrap();

        assert_eq!(headers.len(), 1, "{headers:?}");
        assert_eq!(value, HeaderValue::from_static("test-api-key"));
        assert!(value.is_sensitive());
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_basic_with_extensions() -> TestResult {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let creds = Builder::new("test-api-key").build();
        let mut extensions = Extensions::new();
        let cached_headers = creds.headers(extensions.clone()).await?;
        let (headers, entity_tag) = match cached_headers {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };
        let value = headers.get(API_KEY_HEADER_KEY).unwrap();

        assert_eq!(headers.len(), 1, "{headers:?}");
        assert_eq!(value, HeaderValue::from_static("test-api-key"));
        assert!(value.is_sensitive());
        extensions.insert(entity_tag);

        let cached_headers = creds.headers(extensions).await?;

        match cached_headers {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<HeaderMap>::NotModified,
        };

        Ok(())
    }
}
