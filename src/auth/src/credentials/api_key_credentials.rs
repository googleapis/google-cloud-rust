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
    async fn token(&self) -> Result<Token> {
        Ok(Token {
            token: self.api_key.clone(),
            token_type: String::new(),
            expires_at: None,
            metadata: None,
        })
    }
}

#[derive(Debug)]
struct ApiKeyCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
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
    quota_project_id: Option<String>,
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
            quota_project_id: None,
        }
    }

    /// Sets the [quota project] for these credentials.
    ///
    /// In some services, you can use an account in one project for authentication
    /// and authorization, and charge the usage to a different project. This requires
    /// that the user has `serviceusage.services.use` permissions on the quota project.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::api_key_credentials::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::new("my-api-key")
    ///     .with_quota_project_id("my-project")
    ///     .build();
    /// # });
    /// ```
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<T: Into<String>>(mut self, quota_project_id: T) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    fn build_token_provider(self) -> ApiKeyTokenProvider {
        ApiKeyTokenProvider {
            api_key: self.api_key,
        }
    }

    /// Returns a [Credentials] instance with the configured settings.
    pub fn build(self) -> Credentials {
        let quota_project_id = self.quota_project_id.clone();

        Credentials {
            inner: Arc::new(ApiKeyCredentials {
                token_provider: TokenCache::new(self.build_token_provider()),
                quota_project_id,
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
        build_cacheable_api_key_headers(&cached_token, &self.quota_project_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
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
            tp.token().await.unwrap(),
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
    async fn create_api_key_credentials_with_options() -> TestResult {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let creds = Builder::new("test-api-key")
            .with_quota_project_id("qp-option")
            .build();
        let headers = get_headers_from_cache(creds.headers(Extensions::new()).await.unwrap())?;
        let api_key = headers.get(API_KEY_HEADER_KEY).unwrap();
        let quota_project = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2, "{headers:?}");
        assert_eq!(api_key, HeaderValue::from_static("test-api-key"));
        assert!(api_key.is_sensitive());
        assert_eq!(quota_project, HeaderValue::from_static("qp-option"));
        assert!(!quota_project.is_sensitive());
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
