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

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{Credentials, Result};
use crate::headers_util::build_api_key_headers;
use crate::token::{Token, TokenProvider};
use http::{Extensions, HeaderMap};
use std::sync::Arc;

/// Configuration options for API key credentials.
#[derive(Default)]
pub struct ApiKeyOptions {
    quota_project: Option<String>,
}

impl ApiKeyOptions {
    /// Set the [quota project].
    ///
    /// If unset, the project associated with the API key will be used as the
    /// quota project.
    ///
    /// You can also configure the quota project by setting the
    /// `GOOGLE_CLOUD_QUOTA_PROJECT` environment variable. The environment
    /// variable takes precedence over this option's value.
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn set_quota_project<T: Into<String>>(mut self, v: T) -> Self {
        self.quota_project = Some(v.into());
        self
    }
}

/// Create credentials that authenticate using an [API key].
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
pub async fn create_api_key_credentials<T: Into<String>>(
    api_key: T,
    o: ApiKeyOptions,
) -> Result<Credentials> {
    let token_provider = ApiKeyTokenProvider {
        api_key: api_key.into(),
    };

    let quota_project_id = std::env::var("GOOGLE_CLOUD_QUOTA_PROJECT")
        .ok()
        .or(o.quota_project);

    Ok(Credentials {
        inner: Arc::new(ApiKeyCredentials {
            token_provider,
            quota_project_id,
        }),
    })
}

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
    async fn token(&self, _extensions: Option<Extensions>) -> Result<Token> {
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
    T: TokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for ApiKeyCredentials<T>
where
    T: TokenProvider,
{
    async fn token(&self, extensions: Option<Extensions>) -> Result<Token> {
        self.token_provider.token(extensions).await
    }

    async fn headers(&self, extensions: Option<Extensions>) -> Result<HeaderMap> {
        let token = self.token(extensions).await?;
        build_api_key_headers(&token, &self.quota_project_id)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use http::HeaderValue;
    use scoped_env::ScopedEnv;

    const API_KEY_HEADER_KEY: &str = "x-goog-api-key";

    #[test]
    fn debug_token_provider() {
        let expected = ApiKeyTokenProvider {
            api_key: "super-secret-api-key".to_string(),
        };
        let fmt = format!("{expected:?}");
        assert!(!fmt.contains("super-secret-api-key"), "{fmt}");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_basic() {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let creds = create_api_key_credentials("test-api-key", ApiKeyOptions::default())
            .await
            .unwrap();
        let token = creds.token(None).await.unwrap();
        assert_eq!(
            token,
            Token {
                token: "test-api-key".to_string(),
                token_type: String::new(),
                expires_at: None,
                metadata: None,
            }
        );
        let headers = creds.headers(None).await.unwrap();
        let value = headers.get(API_KEY_HEADER_KEY).unwrap();

        assert_eq!(headers.len(), 1);
        assert_eq!(value, HeaderValue::from_str("test-api-key").unwrap());
        assert!(value.is_sensitive());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_with_options() {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let options = ApiKeyOptions::default().set_quota_project("qp-option");
        let creds = create_api_key_credentials("test-api-key", options)
            .await
            .unwrap();
        let headers = creds.headers(None).await.unwrap();
        let api_key = headers.get(API_KEY_HEADER_KEY).unwrap();
        let quota_project = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2);
        assert_eq!(api_key, HeaderValue::from_str("test-api-key").unwrap());
        assert!(api_key.is_sensitive());
        assert_eq!(quota_project, HeaderValue::from_str("qp-option").unwrap());
        assert!(!quota_project.is_sensitive());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_with_env() {
        let _e = ScopedEnv::set("GOOGLE_CLOUD_QUOTA_PROJECT", "qp-env");
        let options = ApiKeyOptions::default().set_quota_project("qp-option");
        let creds = create_api_key_credentials("test-api-key", options)
            .await
            .unwrap();
        let headers = creds.headers(None).await.unwrap();
        let api_key = headers.get(API_KEY_HEADER_KEY).unwrap();
        let quota_project = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2);
        assert_eq!(api_key, HeaderValue::from_str("test-api-key").unwrap());
        assert!(api_key.is_sensitive());
        assert_eq!(quota_project, HeaderValue::from_str("qp-env").unwrap());
        assert!(!quota_project.is_sensitive());
    }
}
