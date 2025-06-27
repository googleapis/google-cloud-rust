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
use super::external_account_sources::executable_sourced::ExecutableSourcedCredentials;
use super::external_account_sources::url_sourced::UrlSourcedCredentials;
use super::internal::sts_exchange::{ClientAuthentication, ExchangeTokenRequest, STSHandler};
use super::{CacheableResource, Credentials};
use crate::build_errors::Error as BuilderError;
use crate::constants::DEFAULT_SCOPE;
use crate::credentials::external_account_sources::programmatic_sourced::ProgrammaticSourcedCredentials;
use crate::credentials::subject_token::dynamic;
use crate::headers_util::build_cacheable_headers;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use derive_builder::Builder;
use http::{Extensions, HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct CredentialSourceFormat {
    #[serde(rename = "type")]
    pub format_type: String,
    pub subject_token_field_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub(crate) struct ExecutableConfig {
    pub command: String,
    pub timeout_millis: Option<u32>,
    pub output_file: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum CredentialSourceFile {
    Url {
        url: String,
        headers: Option<HashMap<String, String>>,
        format: Option<CredentialSourceFormat>,
    },
    Executable {
        executable: ExecutableConfig,
    },
    File {},
    Aws {},
}

/// A representation of a [external account config file].
///
/// [external account config file]: https://google.aip.dev/auth/4117#configuration-file-generation-and-usage
#[derive(Serialize, Deserialize, Debug)]
struct ExternalAccountFile {
    audience: String,
    subject_token_type: String,
    token_url: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    scopes: Option<Vec<String>>,
    credential_source: CredentialSourceFile,
}

impl From<ExternalAccountFile> for ExternalAccountConfig {
    fn from(config: ExternalAccountFile) -> Self {
        let mut scope = config.scopes.unwrap_or_default();
        if scope.is_empty() {
            scope.push(DEFAULT_SCOPE.to_string());
        }
        Self {
            audience: config.audience,
            client_id: config.client_id,
            client_secret: config.client_secret,
            subject_token_type: config.subject_token_type,
            token_url: config.token_url,
            credential_source: config.credential_source.into(),
            scopes: scope,
        }
    }
}

impl From<CredentialSourceFile> for CredentialSource {
    fn from(source: CredentialSourceFile) -> Self {
        match source {
            CredentialSourceFile::Url {
                url,
                headers,
                format,
            } => Self::Url(UrlSourcedCredentials::new(url, headers, format)),
            CredentialSourceFile::Executable { executable } => {
                Self::Executable(ExecutableSourcedCredentials::new(executable))
            }
            CredentialSourceFile::File { .. } => {
                unimplemented!("file sourced credential not supported yet")
            }
            CredentialSourceFile::Aws { .. } => {
                unimplemented!("AWS sourced credential not supported yet")
            }
        }
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
struct ExternalAccountConfig {
    audience: String,
    subject_token_type: String,
    token_url: String,
    #[builder(default)]
    client_id: Option<String>,
    #[builder(default)]
    client_secret: Option<String>,
    scopes: Vec<String>,
    credential_source: CredentialSource,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum CredentialSource {
    Url(UrlSourcedCredentials),
    Executable(ExecutableSourcedCredentials),
    File {},
    Aws {},
    Programmatic(ProgrammaticSourcedCredentials),
}

impl ExternalAccountConfig {
    fn make_credentials(self, quota_project_id: Option<String>) -> Credentials {
        let config = self.clone();
        match self.credential_source {
            CredentialSource::Url(source) => {
                Self::make_credentials_from_source(source, config, quota_project_id)
            }
            CredentialSource::Executable(source) => {
                Self::make_credentials_from_source(source, config, quota_project_id)
            }
            CredentialSource::Programmatic(source) => {
                Self::make_credentials_from_source(source, config, quota_project_id)
            }
            CredentialSource::File { .. } => {
                unimplemented!("file sourced credential not supported yet")
            }
            CredentialSource::Aws { .. } => {
                unimplemented!("AWS sourced credential not supported yet")
            }
        }
    }

    fn make_credentials_from_source<T>(
        subject_token_provider: T,
        config: ExternalAccountConfig,
        quota_project_id: Option<String>,
    ) -> Credentials
    where
        T: dynamic::SubjectTokenProvider + 'static,
    {
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
        let scope = self.config.scopes.clone();
        let url = self.config.token_url.clone();
        let req = ExchangeTokenRequest {
            url,
            audience: Some(audience),
            subject_token: subject_token.token,
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
        let mut file: ExternalAccountFile =
            serde_json::from_value(self.external_account_config).map_err(BuilderError::parsing)?;

        if let Some(scopes) = self.scopes {
            file.scopes = Some(scopes);
        }

        let config: ExternalAccountConfig = file.into();

        Ok(config.make_credentials(self.quota_project_id))
    }
}

/// A builder for external account [Credentials] that uses a user provided subject
/// token provider.
///
/// This builder is designed for advanced use cases where the subject token is
/// provided directly by the application through a custom implementation of the
/// [SubjectTokenProvider] trait.
///
/// # Example
///
/// ```
/// # use google_cloud_auth::credentials::external_account::ProgrammaticBuilder;
/// # use google_cloud_auth::credentials::subject_token::{SubjectTokenProvider, SubjectToken, Builder as SubjectTokenBuilder};
/// # use google_cloud_auth::errors::SubjectTokenProviderError;
/// # use std::error::Error;
/// # use std::fmt;
/// # use std::sync::Arc;
/// #
/// # #[derive(Debug)]
/// # struct MyTokenProvider;
/// #
/// # #[derive(Debug)]
/// # struct MyProviderError;
/// # impl fmt::Display for MyProviderError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "MyProviderError") } }
/// # impl Error for MyProviderError {}
/// # impl SubjectTokenProviderError for MyProviderError { fn is_transient(&self) -> bool { false } }
/// #
/// # #[async_trait::async_trait]
/// # impl SubjectTokenProvider for MyTokenProvider {
/// #     type Error = MyProviderError;
/// #     async fn subject_token(&self) -> Result<SubjectToken, Self::Error> {
/// #         Ok(SubjectTokenBuilder::new("my-programmatic-token".to_string()).build())
/// #     }
/// # }
/// #
/// # tokio_test::block_on(async {
/// let provider = Arc::new(MyTokenProvider);
///
/// let credentials = ProgrammaticBuilder::new(provider)
///     .with_audience("//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/my-pool/providers/my-provider".to_string())
///     .with_subject_token_type("urn:ietf:params:oauth:token-type:jwt".to_string())
///     .with_token_url("https://sts.googleapis.com/v1beta/token".to_string())
///     .with_quota_project_id("my-quota-project")
///     .with_scopes(vec!["https://www.googleapis.com/auth/devstorage.read_only".to_string()])
///     .build()
///     .unwrap();
/// # });
/// ```
/// [SubjectTokenProvider]: crate::credentials::subject_token::SubjectTokenProvider
pub struct ProgrammaticBuilder {
    quota_project_id: Option<String>,
    subject_token_provider: Arc<dyn dynamic::SubjectTokenProvider>,
    config: ExternalAccountConfigBuilder,
}

impl ProgrammaticBuilder {
    /// Creates a new builder that uses the provided [`SubjectTokenProvider`] to
    /// fetch the third-party subject token.
    pub fn new(subject_token_provider: Arc<dyn dynamic::SubjectTokenProvider>) -> Self {
        Self {
            subject_token_provider,
            quota_project_id: None,
            config: ExternalAccountConfigBuilder::default(),
        }
    }

    /// Sets the optional [quota project] for this credentials.
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

    /// Sets the [scopes] for these credentials.
    /// By default `https://www.googleapis.com/auth/cloud-platform` scope is used.
    ///
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.scopes(
            scopes
                .into_iter()
                .map(|s| s.into())
                .collect::<Vec<String>>(),
        );
        self
    }

    /// Sets the required audience for the token exchange.
    ///
    /// This is the resource name for the workload identity pool and the provider
    /// identifier in that pool.
    pub fn with_audience(mut self, audience: String) -> Self {
        self.config.audience(audience);
        self
    }

    /// Sets the required subject token type.
    ///
    /// This is the STS subject token type based on the OAuth 2.0 token exchange spec.
    pub fn with_subject_token_type(mut self, subject_token_type: String) -> Self {
        self.config.subject_token_type(subject_token_type);
        self
    }

    /// Sets the required token URL for the STS token exchange.
    pub fn with_token_url(mut self, token_url: String) -> Self {
        self.config.token_url(token_url);
        self
    }

    /// Sets the optional client ID for client authentication.
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.config.client_id(client_id);
        self
    }

    /// Sets the optional client secret for client authentication.
    pub fn with_client_secret(mut self, client_secret: String) -> Self {
        self.config.client_secret(client_secret);
        self
    }

    /// Returns a [Credentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns a [CredentialsError] if any of the required fields (such as
    /// `audience`, `subject_token_type`, or `token_url`) have not been set.
    pub fn build(self) -> BuildResult<Credentials> {
        let mut config_builder = self.config;
        if config_builder.scopes.is_none() {
            config_builder.scopes(vec![DEFAULT_SCOPE.to_string()]);
        }
        let config = config_builder
            .credential_source(CredentialSource::Programmatic(
                ProgrammaticSourcedCredentials::new(self.subject_token_provider),
            ))
            .build()
            .map_err(BuilderError::parsing)?;

        Ok(config.make_credentials(self.quota_project_id))
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
    use crate::credentials::subject_token::{
        Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
    };
    use crate::errors::SubjectTokenProviderError;
    use serde_json::*;
    use std::collections::HashMap;
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct TestProviderError;
    impl fmt::Display for TestProviderError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestProviderError")
        }
    }
    impl Error for TestProviderError {}
    impl SubjectTokenProviderError for TestProviderError {
        fn is_transient(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct TestSubjectTokenProvider;
    impl SubjectTokenProvider for TestSubjectTokenProvider {
        type Error = TestProviderError;
        async fn subject_token(&self) -> std::result::Result<SubjectToken, Self::Error> {
            Ok(SubjectTokenBuilder::new("test-subject-token".to_string()).build())
        }
    }

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

        let fmt = format!("{creds:?}");
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

        let file: ExternalAccountFile =
            serde_json::from_value(contents).expect("failed to parse external account config");
        let config: ExternalAccountConfig = file.into();
        let source = config.credential_source;

        match source {
            CredentialSource::Url(source) => {
                assert_eq!(source.url, "https://example.com/token");
                assert_eq!(
                    source.headers,
                    HashMap::from([("Metadata".to_string(), "True".to_string()),]),
                );
                assert_eq!(source.format, "json");
                assert_eq!(source.subject_token_field_name, "access_token");
            }
            _ => {
                unreachable!("expected Url Sourced credential")
            }
        }
    }

    #[tokio::test]
    async fn create_external_account_detect_executable_sourced() {
        let contents = json!({
            "type": "external_account",
            "audience": "audience",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1beta/token",
            "credential_source": {
                "executable": {
                    "command": "cat /some/file",
                    "output_file": "/some/file",
                    "timeout_millis": 5000
                }
            }
        });

        let file: ExternalAccountFile =
            serde_json::from_value(contents).expect("failed to parse external account config");
        let config: ExternalAccountConfig = file.into();
        let source = config.credential_source;

        match source {
            CredentialSource::Executable(source) => {
                assert_eq!(source.command, "cat");
                assert_eq!(source.args, vec!["/some/file"]);
                assert_eq!(source.output_file.as_deref(), Some("/some/file"));
                assert_eq!(source.timeout, Duration::from_secs(5));
            }
            _ => {
                unreachable!("expected Executable Sourced credential")
            }
        }
    }

    #[tokio::test]
    async fn create_programmatic_builder() {
        let provider = Arc::new(TestSubjectTokenProvider);
        let creds = ProgrammaticBuilder::new(provider)
            .with_audience("test-audience".to_string())
            .with_subject_token_type("test-token-type".to_string())
            .with_token_url("http://test.com/token".to_string())
            .with_quota_project_id("test-quota-project")
            .with_scopes(vec!["scope1".to_string(), "scope2".to_string()])
            .build()
            .unwrap();

        let fmt = format!("{creds:?}");
        assert!(
            fmt.contains("ExternalAccountCredentials"),
            "Expected 'ExternalAccountCredentials', got: {fmt}"
        );
        assert!(
            fmt.contains("test-quota-project"),
            "Expected 'test-quota-project', got: {fmt}"
        );
    }

    #[tokio::test]
    async fn create_programmatic_builder_fails_on_missing_required_field() {
        let provider = Arc::new(TestSubjectTokenProvider);
        let result = ProgrammaticBuilder::new(provider)
            .with_subject_token_type("test-token-type".to_string())
            // Missing .with_audience(...)
            .with_token_url("http://test.com/token".to_string())
            .build();

        assert!(result.is_err());
        let error_string = result.unwrap_err().to_string();
        assert!(
            error_string.contains("`audience` must be initialized"),
            "Expected error about missing 'audience', got: {error_string}"
        );
    }
}
