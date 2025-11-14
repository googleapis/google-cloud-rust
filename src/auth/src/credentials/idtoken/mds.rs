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

//! Obtain [OIDC ID tokens] using [Metadata Service].
//!
//! Google Cloud environments such as [Google Compute Engine (GCE)][gce-link],
//! [Google Kubernetes Engine (GKE)][gke-link], or [Cloud Run] provide a metadata service.
//! This is a local service to the VM (or pod) which (as the name implies) provides
//! metadata information about the environment. The service also provides access
//! tokens associated with the [default service account] for the corresponding
//! VM. This module provides a builder for `IDTokenCredentials`
//! from such metadata service.
//!
//! The default host name of the metadata service is `metadata.google.internal`.
//! If you would like to use a different hostname, you can set it using the
//! `GCE_METADATA_HOST` environment variable.
//!
//! `IDTokenCredentials` obtain OIDC ID tokens, which are commonly
//! used for [service to service authentication]. For example, when the
//! target service is hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
//!
//! Unlike access tokens, ID tokens are not used to authorize access to
//! Google Cloud APIs but to verify the identity of a principal.
//!
//! ## Example: Creating MDS sourced credentials with target audience and sending ID Tokens.
//!
//! ```
//! # use google_cloud_auth::credentials::idtoken;
//! # use reqwest;
//! # tokio_test::block_on(async {
//! let audience = "https://example.com";
//! let credentials = idtoken::mds::Builder::new(audience)
//!     .build()?;
//! let id_token = credentials.id_token().await?;
//!
//! // Make request with ID Token as Bearer Token.
//! let client = reqwest::Client::new();
//! let target_url = format!("{audience}/api/method");
//! client.get(target_url)
//!     .bearer_auth(id_token)
//!     .send()
//!     .await?;
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```    
//!
//! [Application Default Credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
//! [ID tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
//! [Cloud Run]: https://cloud.google.com/run
//! [default service account]: https://cloud.google.com/iam/docs/service-account-types#default
//! [gce-link]: https://cloud.google.com/products/compute
//! [gke-link]: https://cloud.google.com/kubernetes-engine
//! [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview

use crate::Result;
use crate::credentials::CacheableResource;
use crate::credentials::mds::{
    GCE_METADATA_HOST_ENV_VAR, MDS_DEFAULT_URI, METADATA_FLAVOR, METADATA_FLAVOR_VALUE,
    METADATA_ROOT,
};
use crate::errors::CredentialsError;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{
    BuildResult,
    credentials::idtoken::dynamic::IDTokenCredentialsProvider,
    credentials::idtoken::{IDTokenCredentials, parse_id_token_from_str},
};
use async_trait::async_trait;
use http::{Extensions, HeaderValue};
use reqwest::Client;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MDSCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
}

#[async_trait]
impl<T> IDTokenCredentialsProvider for MDSCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn id_token(&self) -> Result<String> {
        let cached_token = self.token_provider.token(Extensions::new()).await?;
        match cached_token {
            CacheableResource::New { data, .. } => Ok(data.token),
            CacheableResource::NotModified => {
                Err(CredentialsError::from_msg(false, "failed to fetch token"))
            }
        }
    }
}

/// Specifies what assertions are included in ID Tokens fetched from the Metadata Service.
#[derive(Debug, Clone)]
pub enum Format {
    /// Omit project and instance details from the payload. It's the default value.
    Standard,
    /// Include project and instance details in the payload.
    Full,
    /// Use this variant to handle new values that are not yet known to this library.
    UnknownValue(String),
}

impl Format {
    fn as_str(&self) -> &str {
        match self {
            Format::Standard => "standard",
            Format::Full => "full",
            Format::UnknownValue(value) => value.as_str(),
        }
    }
}

/// Creates [`IDTokenCredentials`] instances that fetch ID tokens from the
/// metadata service.
pub struct Builder {
    endpoint: Option<String>,
    format: Option<Format>,
    licenses: Option<String>,
    target_audience: String,
}

impl Builder {
    /// Creates a new `Builder`.
    ///
    /// The `target_audience` is a required parameter that specifies the
    /// intended audience of the ID token. This is typically the URL of the
    /// service that will be receiving the token.
    pub fn new<S: Into<String>>(target_audience: S) -> Self {
        Builder {
            format: None,
            endpoint: None,
            licenses: None,
            target_audience: target_audience.into(),
        }
    }

    /// Sets the endpoint for this credentials.
    ///
    /// A trailing slash is significant, so specify the base URL without a trailing  
    /// slash. If not set, the credentials use `http://metadata.google.internal`.
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the [format] of the token.
    ///
    /// Specifies whether or not the project and instance details are included in the payload.
    /// Specify `full` to include this information in the payload or `standard` to omit the information
    /// from the payload. The default value is `standard`.
    ///
    /// [format]: https://cloud.google.com/compute/docs/instances/verifying-instance-identity#token_format
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    /// Whether to include the [license codes] of the instance in the token.
    ///
    /// Specify `true` to include this information or `false` to omit this information from the payload.
    /// The default value is `false`. Has no effect unless format is `full`.
    ///
    /// [license codes]: https://cloud.google.com/compute/docs/reference/rest/v1/images/get#body.Image.FIELDS.license_code
    pub fn with_licenses(mut self, licenses: bool) -> Self {
        self.licenses = if licenses {
            Some("TRUE".to_string())
        } else {
            Some("FALSE".to_string())
        };
        self
    }

    fn build_token_provider(self) -> MDSTokenProvider {
        let final_endpoint: String;

        // Determine the endpoint and whether it was overridden
        if let Ok(host_from_env) = std::env::var(GCE_METADATA_HOST_ENV_VAR) {
            // Check GCE_METADATA_HOST environment variable first
            final_endpoint = format!("http://{host_from_env}");
        } else if let Some(builder_endpoint) = self.endpoint {
            // Else, check if an endpoint was provided to the mds::Builder
            final_endpoint = builder_endpoint;
        } else {
            // Else, use the default metadata root
            final_endpoint = METADATA_ROOT.to_string();
        };

        MDSTokenProvider {
            format: self.format,
            licenses: self.licenses,
            endpoint: final_endpoint,
            target_audience: self.target_audience,
        }
    }

    /// Returns an [`IDTokenCredentials`] instance with the configured
    /// settings.
    pub fn build(self) -> BuildResult<IDTokenCredentials> {
        let creds = MDSCredentials {
            token_provider: TokenCache::new(self.build_token_provider()),
        };
        Ok(IDTokenCredentials {
            inner: Arc::new(creds),
        })
    }
}

#[derive(Debug, Clone, Default)]
struct MDSTokenProvider {
    endpoint: String,
    format: Option<Format>,
    licenses: Option<String>,
    target_audience: String,
}

#[async_trait]
impl TokenProvider for MDSTokenProvider {
    async fn token(&self) -> Result<Token> {
        let client = Client::new();
        let audience = self.target_audience.clone();
        let request = client
            .get(format!("{}{}/identity", self.endpoint, MDS_DEFAULT_URI))
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            )
            .query(&[("audience", audience)]);

        let request = self.format.iter().fold(request, |builder, format| {
            builder.query(&[("format", format.as_str())])
        });
        let request = self.licenses.iter().fold(request, |builder, licenses| {
            builder.query(&[("licenses", licenses)])
        });

        let response = request
            .send()
            .await
            .map_err(|e| crate::errors::from_http_error(e, "failed to fetch token"))?;

        if !response.status().is_success() {
            let err = crate::errors::from_http_response(response, "failed to fetch token").await;
            return Err(err);
        }

        let token = response
            .text()
            .await
            .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

        parse_id_token_from_str(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::idtoken::tests::generate_test_id_token;
    use crate::credentials::tests::find_source_error;
    use httptest::matchers::{all_of, contains, request, url_decoded};
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use reqwest::StatusCode;
    use scoped_env::ScopedEnv;
    use serial_test::{parallel, serial};
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    #[test_case(Format::Standard)]
    #[test_case(Format::Full)]
    #[test_case(Format::UnknownValue("minimal".to_string()))]
    #[parallel]
    async fn test_idtoken_builder_build(format: Format) -> TestResult {
        let server = Server::run();
        let audience = "test-audience";
        let token_string = generate_test_id_token(audience);
        let format_str = format.as_str().to_string();
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/identity")),
                request::query(url_decoded(contains(("audience", audience)))),
                request::query(url_decoded(contains(("format", format_str)))),
                request::query(url_decoded(contains(("licenses", "TRUE"))))
            ])
            .respond_with(status_code(200).body(token_string.clone())),
        );

        let creds = Builder::new(audience)
            .with_endpoint(format!("http://{}", server.addr()))
            .with_format(format)
            .with_licenses(true)
            .build()?;

        let id_token = creds.id_token().await?;
        assert_eq!(id_token, token_string);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_idtoken_builder_build_with_env_var() -> TestResult {
        let server = Server::run();
        let audience = "test-audience";
        let token_string = generate_test_id_token(audience);
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/identity")),
                request::query(url_decoded(contains(("audience", audience))))
            ])
            .respond_with(status_code(200).body(token_string.clone())),
        );

        let addr = server.addr().to_string();
        let _e = ScopedEnv::set(super::GCE_METADATA_HOST_ENV_VAR, &addr);

        let creds = Builder::new(audience).build()?;

        let id_token = creds.id_token().await?;
        assert_eq!(id_token, token_string);

        let _e = ScopedEnv::remove(super::GCE_METADATA_HOST_ENV_VAR);
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_idtoken_provider_http_error() -> TestResult {
        let server = Server::run();
        let audience = "test-audience";
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/identity")),
                request::query(url_decoded(contains(("audience", audience))))
            ])
            .respond_with(status_code(503)),
        );

        let creds = Builder::new(audience)
            .with_endpoint(format!("http://{}", server.addr()))
            .build()?;

        let err = creds.id_token().await.unwrap_err();
        let source = find_source_error::<reqwest::Error>(&err);
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::SERVICE_UNAVAILABLE)),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_idtoken_caching() -> TestResult {
        let server = Server::run();
        let audience = "test-audience";
        let token_string = generate_test_id_token(audience);
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/identity")),
                request::query(url_decoded(contains(("audience", audience))))
            ])
            .times(1)
            .respond_with(status_code(200).body(token_string.clone())),
        );

        let creds = Builder::new(audience)
            .with_endpoint(format!("http://{}", server.addr()))
            .build()?;

        let id_token = creds.id_token().await?;
        assert_eq!(id_token, token_string);

        let id_token = creds.id_token().await?;
        assert_eq!(id_token, token_string);

        Ok(())
    }
}
