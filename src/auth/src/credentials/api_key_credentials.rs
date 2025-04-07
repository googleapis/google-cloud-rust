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

use crate::credentials::dynamic::CredentialsTrait;
use crate::credentials::{Credentials, QUOTA_PROJECT_KEY, Result};
use crate::errors;
use crate::token::{Token, TokenProvider};
use http::header::{HeaderName, HeaderValue};
use std::sync::Arc;

const API_KEY_HEADER_KEY: &str = "x-goog-api-key";

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
    async fn get_token(&self) -> Result<Token> {
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
impl<T> CredentialsTrait for ApiKeyCredentials<T>
where
    T: TokenProvider,
{
    async fn get_token(&self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.get_token().await?;
        let mut value = HeaderValue::from_str(&token.token).map_err(errors::non_retryable)?;
        value.set_sensitive(true);
        let mut headers = vec![(HeaderName::from_static(API_KEY_HEADER_KEY), value)];
        if let Some(project) = &self.quota_project_id {
            headers.push((
                HeaderName::from_static(QUOTA_PROJECT_KEY),
                HeaderValue::from_str(project).map_err(errors::non_retryable)?,
            ));
        }
        Ok(headers)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::test::HV;
    use scoped_env::ScopedEnv;

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
        let token = creds.get_token().await.unwrap();
        assert_eq!(
            token,
            Token {
                token: "test-api-key".to_string(),
                token_type: String::new(),
                expires_at: None,
                metadata: None,
            }
        );
        let headers: Vec<HV> = HV::from(creds.headers().await.unwrap());

        assert_eq!(
            headers,
            vec![HV {
                header: API_KEY_HEADER_KEY.to_string(),
                value: "test-api-key".to_string(),
                is_sensitive: true,
            }]
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_with_options() {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let options = ApiKeyOptions::default().set_quota_project("qp-option");
        let creds = create_api_key_credentials("test-api-key", options)
            .await
            .unwrap();
        let headers: Vec<HV> = HV::from(creds.headers().await.unwrap());

        assert_eq!(
            headers,
            vec![
                HV {
                    header: API_KEY_HEADER_KEY.to_string(),
                    value: "test-api-key".to_string(),
                    is_sensitive: true,
                },
                HV {
                    header: QUOTA_PROJECT_KEY.to_string(),
                    value: "qp-option".to_string(),
                    is_sensitive: false,
                }
            ]
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_with_env() {
        let _e = ScopedEnv::set("GOOGLE_CLOUD_QUOTA_PROJECT", "qp-env");
        let options = ApiKeyOptions::default().set_quota_project("qp-option");
        let creds = create_api_key_credentials("test-api-key", options)
            .await
            .unwrap();
        let headers: Vec<HV> = HV::from(creds.headers().await.unwrap());

        assert_eq!(
            headers,
            vec![
                HV {
                    header: API_KEY_HEADER_KEY.to_string(),
                    value: "test-api-key".to_string(),
                    is_sensitive: true,
                },
                HV {
                    header: QUOTA_PROJECT_KEY.to_string(),
                    value: "qp-env".to_string(),
                    is_sensitive: false,
                }
            ]
        );
    }
}
