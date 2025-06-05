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

use super::dynamic::CredentialsProvider;
use super::external_account_sources::programmatic_sourced::ProgrammaticSourcedCredentials;
use super::external_account_sources::url_sourced::UrlSourcedCredentials;
use super::internal::sts_exchange::{ClientAuthentication, ExchangeTokenRequest, STSHandler};
use super::{CacheableResource, Credentials};
use crate::build_errors::Error as BuilderError;
use crate::constants::DEFAULT_SCOPE;
use crate::headers_util::build_cacheable_headers;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use http::{Extensions, HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

pub trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
    fn subject_token(&self) -> impl Future<Output = Result<String>> + Send;
}

pub(crate) mod dynamic {
    use super::Result;
    #[async_trait::async_trait]
    pub trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
        /// Generate subject token that will be used on STS exchange.
        async fn subject_token(&self) -> Result<String>;
    }

    #[async_trait::async_trait]
    impl<T> SubjectTokenProvider for T
    where
        T: super::SubjectTokenProvider,
    {
        async fn subject_token(&self) -> Result<String> {
            T::subject_token(self).await
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct CredentialSourceFormat {
    #[serde(rename = "type")]
    pub format_type: String,
    pub subject_token_field_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
enum CredentialSource {
    Url(UrlSourcedCredentials),
    File {},
    Aws {},
    Executable {},
}

impl CredentialSource {
    fn make_credentials(
        self,
        config: ExternalAccountConfig,
        quota_project_id: Option<String>,
    ) -> Credentials {
        match self {
            Self::Url(source) => make_credentials_from_provider(source, config, quota_project_id),
            Self::Executable { .. } => {
                unimplemented!("executable sourced credential not supported yet")
            }
            Self::File { .. } => {
                unimplemented!("file sourced credential not supported yet")
            }
            Self::Aws { .. } => {
                unimplemented!("AWS sourced credential not supported yet")
            }
        }
    }
}

fn make_credentials_from_provider<T: dynamic::SubjectTokenProvider + 'static>(
    subject_token_provider: T,
    config: ExternalAccountConfig,
    quota_project_id: Option<String>,
) -> Credentials {
    let token_provider = ExternalAccountTokenProvider {
        subject_token_provider,
        config,
    };
    let cache = TokenCache::new(token_provider);
    Credentials {
        inner: Arc::new(ExternalAccountCredentials {
            token_provider: cache,
            quota_project_id,
        }),
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ExternalAccountConfig {
    audience: String,
    subject_token_type: String,
    token_url: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    scopes: Option<Vec<String>>,
    credential_source: CredentialSource,
}

#[derive(Debug)]
struct ExternalAccountTokenProvider<T>
where
    T: dynamic::SubjectTokenProvider,
{
    subject_token_provider: T,
    config: ExternalAccountConfig,
}

#[async_trait::async_trait]
impl<T> TokenProvider for ExternalAccountTokenProvider<T>
where
    T: dynamic::SubjectTokenProvider,
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
            scope.push(DEFAULT_SCOPE.to_string());
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

/// A builder for external account [Credentials] instances.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::external_account::Builder;
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
/// # fn project_id() -> String {
/// #     "test-only".to_string()
/// # }
/// # fn workload_identity_pool() -> String {
/// #     "test-only".to_string()
/// # }
/// # fn workload_identity_provider() -> String {
/// #     "test-only".to_string()
/// # }
/// ```
pub struct Builder {
    external_account_config: Value,
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
    subject_token_provider: Option<Box<dyn dynamic::SubjectTokenProvider>>,
}

impl Builder {
    /// Creates a new builder using [external_account_credentials] JSON value.
    ///
    /// [external_account_credentials]: https://google.aip.dev/auth/4117#configuration-file-generation-and-usage
    pub fn new(external_account_config: Value) -> Self {
        Self {
            external_account_config,
            quota_project_id: None,
            scopes: None,
            subject_token_provider: None,
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

    /// Overrides the [scopes] for this credentials.
    ///
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = Some(scopes.into_iter().map(|s| s.into()).collect());
        self
    }

    /// bring your own custom implementation of
    /// SubjectTokenProvider for OIDC/SAML credentials.
    pub fn with_subject_token_provider<T: SubjectTokenProvider + 'static>(
        mut self,
        subject_token_provider: T,
    ) -> Self {
        self.subject_token_provider = Some(Box::new(subject_token_provider));
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
    /// on the expected format, consult the relevant section in the
    /// [external_account_credentials] guide.
    ///
    /// [external_account_credentials]: https://google.aip.dev/auth/4117#configuration-file-generation-and-usage
    pub fn build(self) -> BuildResult<Credentials> {
        let external_account_config: ExternalAccountConfig =
            serde_json::from_value(self.external_account_config).map_err(BuilderError::parsing)?;

        let mut config = external_account_config.clone();
        if let Some(scopes) = self.scopes {
            config.scopes = Some(scopes);
        }

        if let Some(subject_token_provider) = self.subject_token_provider {
            let source = ProgrammaticSourcedCredentials {
                subject_token_provider,
            };
            return Ok(make_credentials_from_provider(
                source,
                config,
                self.quota_project_id,
            ));
        }

        Ok(external_account_config
            .credential_source
            .make_credentials(config, self.quota_project_id))
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
    use std::collections::HashMap;

    #[tokio::test]
    async fn create_external_account_builder() {
        let contents = json!({
            "type": "external_account",
            "audience": "audience",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1beta/token",
            "credential_source": {
                "url": "https://example.com/token",
                "format": {
                  "type": "json",
                  "subject_token_field_name": "access_token"
                }
            }
        });

        let creds = Builder::new(contents)
            .with_quota_project_id("test_project")
            .with_scopes(["a", "b"])
            .build()
            .unwrap();

        let fmt = format!("{:?}", creds);
        // Use the debug output to verify the right kind of credentials are created.
        print!("{:?}", creds);
        assert!(fmt.contains("ExternalAccountCredentials"));
    }

    #[tokio::test]
    async fn create_external_account_detect_url_sourced() {
        let contents = json!({
            "type": "external_account",
            "audience": "audience",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1beta/token",
            "credential_source": {
                "url": "https://example.com/token",
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
            CredentialSource::Url(source) => {
                assert_eq!(source.url, "https://example.com/token");
                assert_eq!(
                    source.headers,
                    Some(HashMap::from([(
                        "Metadata".to_string(),
                        "True".to_string()
                    ),])),
                );
                assert_eq!(
                    source.format,
                    Some(CredentialSourceFormat {
                        format_type: "json".into(),
                        subject_token_field_name: "access_token".into(),
                    })
                )
            }
            _ => {
                unreachable!("expected Url Sourced credential")
            }
        }
    }
}
