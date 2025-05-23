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

use crate::credentials::Result;
use crate::credentials::internal::sts_exchange::ClientAuthentication;
use crate::errors;
use crate::headers_util::build_cacheable_headers;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;

use gax::error::CredentialsError;
use http::{Extensions, HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

use super::dynamic::CredentialsProvider;
use super::external_account_sources::url_sourced_account::UrlSourcedSubjectTokenProvider;
use super::internal::sts_exchange::{ExchangeTokenRequest, STSHandler};
use super::{CacheableResource, Credentials};

const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

#[async_trait::async_trait]
pub(crate) trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
    /// Generate subject token that will be used on STS exchange.
    async fn subject_token(&self) -> Result<String>;
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CredentialSourceFormat {
    #[serde(rename = "type")]
    pub format_type: String,
    pub subject_token_field_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CredentialSourceHeaders {
    #[serde(flatten)]
    pub headers: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ExecutableConfig {
    pub command: String,
    pub timeout_millis: Option<u32>,
    pub output_file: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum CredentialSource {
    UrlSourced {
        url: String,
        headers: Option<CredentialSourceHeaders>,
        format: Option<CredentialSourceFormat>,
    },
    File {
        file: String,
        format: Option<CredentialSourceFormat>,
    },
    Executable {
        executable: ExecutableConfig,
    },
    Aws {
        environment_id: String,
        region_url: Option<String>,
        regional_cred_verification_url: Option<String>,
        cred_verification_url: Option<String>,
        imdsv2_session_token_url: Option<String>,
    },
}

#[async_trait::async_trait]
impl SubjectTokenProvider for CredentialSource {
    async fn subject_token(&self) -> Result<String> {
        match self.clone() {
            CredentialSource::UrlSourced {
                url,
                headers,
                format,
            } => {
                let source = UrlSourcedSubjectTokenProvider {
                    url,
                    headers,
                    format,
                };
                source.subject_token().await
            }
            CredentialSource::Executable { .. } => Err(CredentialsError::from_str(
                false,
                "executable sourced credential not supported yet",
            )),
            CredentialSource::File { .. } => Err(CredentialsError::from_str(
                false,
                "file sourced credential not supported yet",
            )),
            CredentialSource::Aws { .. } => Err(CredentialsError::from_str(
                false,
                "AWS sourced credential not supported yet",
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExternalAccountConfig {
    pub audience: String,
    pub subject_token_type: String,
    pub token_url: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    // TODO(#2261): set up impersonation token provider when this attribute is used.
    pub service_account_impersonation_url: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub credential_source: CredentialSource,
}

#[derive(Debug)]
struct ExternalAccountTokenProvider<T>
where
    T: SubjectTokenProvider,
{
    subject_token_provider: T,
    config: ExternalAccountConfig,
}

#[async_trait::async_trait]
impl<T> TokenProvider for ExternalAccountTokenProvider<T>
where
    T: SubjectTokenProvider,
{
    async fn token(&self) -> Result<Token> {
        let subject_token = self.subject_token_provider.subject_token().await?;

        let audience = self.config.audience.clone();
        let subject_token_type = self.config.subject_token_type.clone();
        let url = self.config.token_url.clone();
        let mut scope = vec![];
        if let Some(scopes) = self.config.scopes.clone() {
            scopes.into_iter().for_each(|v| scope.push(v));
        }
        if scope.is_empty() {
            scope.push(CLOUD_PLATFORM_SCOPE.to_string());
        }
        let req = ExchangeTokenRequest {
            url,
            audience: Some(audience),
            subject_token,
            subject_token_type,
            scope,
            authentication: ClientAuthentication {
                client_id: self.config.client_id.clone(),
                client_secret: self.config.client_secret.clone(),
            },
            ..ExchangeTokenRequest::default()
        };

        let token_res = STSHandler::exchange_token(req).await?;

        let token = Token {
            token: token_res.access_token,
            token_type: token_res.token_type,
            expires_at: Some(Instant::now() + Duration::from_secs(token_res.expires_in)),
            metadata: None,
        };
        Ok(token)
    }
}

#[derive(Debug)]
pub(crate) struct ExternalAccountCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

/// A builder for constructing external account [Credentials] instances.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::external_account::{Builder};
/// # tokio_test::block_on(async {
/// let project_id = project_id();
/// let workload_identity_pool_id = workload_identity_pool();
/// let provider_id = workload_identity_provider();
/// let provider_name = format!(
///     "//iam.googleapis.com/projects/{project_id}/locations/global/workloadIdentityPools/{workload_identity_pool_id}/providers/{provider_id}"
/// );
/// let config = serde_json::json!({
///     "type": "external_account",
///     "audience": provider_name,
///     "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
///     "token_url": "https://sts.googleapis.com/v1beta/token",
///     "credential_source": {
///         "url": format!("http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource={provider_name}"),
///         "headers": {
///           "Metadata": "True"
///         },
///         "format": {
///           "type": "json",
///           "subject_token_field_name": "access_token"
///         }
///     }
/// });
/// let credentials = Builder::new(config)
///     .with_quota_project_id("quota_project")
///     .build();
/// });
///
/// fn project_id() -> String {
///     "test-only".to_string()
/// }
/// fn workload_identity_pool() -> String {
///     "test-only".to_string()
/// }
/// fn workload_identity_provider() -> String {
///     "test-only".to_string()
/// }
/// ```
pub struct Builder {
    external_account_config: Value,
    quota_project_id: Option<String>,
}

impl Builder {
    /// Creates a new builder using [external_account_config] JSON value.    
    ///
    /// [external_account_config]: https://cloud.google.com/iam/docs/workload-download-cred-and-grant-access#download-configuration
    pub fn new(external_account_config: Value) -> Self {
        Self {
            external_account_config,
            quota_project_id: None,
        }
    }

    /// Sets the [quota project] for this credentials.
    ///
    /// In some services, you can use a service account in
    /// one project for authentication and authorization, and charge
    /// the usage to a different project. This requires that the
    /// service account has `serviceusage.services.use` permissions on the quota project.
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Returns a [Credentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns a [CredentialsError] if the `external_account_config`
    /// provided to [`Builder::new`] cannot be successfully deserialized into the
    /// expected format for an external account configuration. This typically happens if the
    /// JSON value is malformed or missing required fields. For more information,
    /// on the expected format, consult the relevant section in
    /// the [external account config] guide.
    ///
    /// [external account config]: https://cloud.google.com/iam/docs/workload-download-cred-and-grant-access#download-configuration
    pub fn build(self) -> Result<Credentials> {
        let external_account_config: ExternalAccountConfig =
            serde_json::from_value(self.external_account_config).map_err(errors::non_retryable)?;

        let config = external_account_config.clone();

        let token_provider = ExternalAccountTokenProvider {
            subject_token_provider: external_account_config.credential_source,
            config,
        };

        Ok(Credentials {
            inner: Arc::new(ExternalAccountCredentials {
                quota_project_id: self.quota_project_id.clone(),
                token_provider: TokenCache::new(token_provider),
            }),
        })
    }
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for ExternalAccountCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let token = self.token_provider.token(extensions).await?;
        build_cacheable_headers(&token, &self.quota_project_id)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::*;

    #[tokio::test]
    async fn create_external_account_builder() {
        let contents = json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/<PROJECT_ID>/locations/global/workloadIdentityPools/<WORKLOAD_IDENTITY_POOL>/providers/<WORKLOAD_IDENTITY_PROVIDER_ID>",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1beta/token",
            "credential_source": {
                "url": "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://iam.googleapis.com/projects/<PROJECT_ID>/locations/global/workloadIdentityPools/<WORKLOAD_IDENTITY_POOL>/providers/<WORKLOAD_IDENTITY_PROVIDER_ID>",
                "headers": {
                  "Metadata": "True"
                },
                "format": {
                  "type": "json",
                  "subject_token_field_name": "access_token"
                }
            }
        });

        let creds = Builder::new(contents)
            .with_quota_project_id("test_project")
            .build()
            .unwrap();

        let fmt = format!("{:?}", creds);
        print!("{:?}", creds);
        assert!(fmt.contains("ExternalAccountCredentials"));
    }

    #[tokio::test]
    async fn create_external_account_detect_url_sourced() {
        let contents = json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/<PROJECT_ID>/locations/global/workloadIdentityPools/<WORKLOAD_IDENTITY_POOL>/providers/<WORKLOAD_IDENTITY_PROVIDER_ID>",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1beta/token",
            "credential_source": {
                "url": "http://169.254.169.254/metadata/identity/oauth2/token",
                "headers": {
                  "Metadata": "True"
                },
                "format": {
                  "type": "json",
                  "subject_token_field_name": "access_token"
                }
            }
        });

        let config: ExternalAccountConfig =
            serde_json::from_value(contents).expect("failed to parse external account config");
        let source = config.credential_source;

        match source {
            CredentialSource::UrlSourced {
                url,
                headers,
                format,
            } => {
                assert_eq!(
                    url,
                    "http://169.254.169.254/metadata/identity/oauth2/token".to_string()
                );
                assert_eq!(
                    headers,
                    Some(CredentialSourceHeaders {
                        headers: HashMap::from([("Metadata".to_string(), "True".to_string()),]),
                    })
                );
                assert_eq!(
                    format,
                    Some(CredentialSourceFormat {
                        format_type: "json".to_string(),
                        subject_token_field_name: "access_token".to_string(),
                    })
                )
            }
            _ => {
                unreachable!("expected Url Sourced credential")
            }
        }
    }
}
