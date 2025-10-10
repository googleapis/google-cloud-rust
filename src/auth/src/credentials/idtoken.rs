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

use serde_json::Value;
use crate::build_errors::Error as BuilderError;
use crate::credentials::{AdcContents, extract_credential_type, load_adc, mds, service_account};
use crate::{BuildResult, Result};
use std::future::Future;
use std::sync::Arc;

/// Obtain [OIDC ID Tokens].
///
/// `IDTokenCredentials` provide a way to obtain OIDC ID tokens, which are
/// commonly used for [service to service authentication], like when services are
/// hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
/// Unlike access tokens, ID tokens are not used to authorize access to
/// Google Cloud APIs but to verify the identity of a principal.
///
/// This struct serves as a wrapper around different credential types that can
/// produce ID tokens, such as service accounts or metadata server credentials.
///
/// [OIDC ID Tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
/// [Service to Service Authentication]: https://cloud.google.com/run/docs/authenticating/service-to-service
#[derive(Clone, Debug)]
pub struct IDTokenCredentials {
    pub(crate) inner: Arc<dyn dynamic::IDTokenCredentialsProvider>,
}

impl<T> From<T> for IDTokenCredentials
where
    T: IDTokenCredentialsProvider + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

impl IDTokenCredentials {
    /// Asynchronously retrieves an ID token.
    ///
    /// Obtains an ID token. If one is cached, returns the cached value.
    pub async fn id_token(&self) -> Result<String> {
        self.inner.id_token().await
    }
}

/// A trait for credential types that can provide OIDC ID tokens.
///
/// Implement this trait to create custom ID token providers.
/// For example, if you are working with an authentication system not
/// supported by this crate. Or if you are trying to write a test and need
/// to mock the existing `IDTokenCredentialsProvider` implementations.
pub trait IDTokenCredentialsProvider: std::fmt::Debug {
    /// Asynchronously retrieves an ID token.
    fn id_token(&self) -> impl Future<Output = Result<String>> + Send;
}

/// A module containing the dynamically-typed, dyn-compatible version of the
/// `IDTokenCredentialsProvider` trait. This is an internal implementation detail.
pub(crate) mod dynamic {
    use crate::Result;

    /// A dyn-compatible, crate-private version of `IDTokenCredentialsProvider`.
    #[async_trait::async_trait]
    pub trait IDTokenCredentialsProvider: Send + Sync + std::fmt::Debug {
        /// Asynchronously retrieves an ID token.
        async fn id_token(&self) -> Result<String>;
    }

    /// The public `IDTokenCredentialsProvider` implements the dyn-compatible `IDTokenCredentialsProvider`.
    #[async_trait::async_trait]
    impl<T> IDTokenCredentialsProvider for T
    where
        T: super::IDTokenCredentialsProvider + Send + Sync,
    {
        async fn id_token(&self) -> Result<String> {
            T::id_token(self).await
        }
    }
}

pub(crate) struct Builder {
    target_audience: Option<String>,
}

impl Default for Builder {
    /// Creates a new builder where id tokens will be obtained via [application-default login].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::idtoken::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::default().build();
    /// # });
    /// ```
    ///
    /// [application-default login]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
    fn default() -> Self {
        Self {
            target_audience: None,
        }
    }
}

impl Builder {
    /// Returns a [IDTokenCredentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns a [CredentialsError] if a unsupported credential type is provided
    /// or if the JSON value is either malformed
    /// or missing required fields. For more information, on how to generate
    /// json, consult the relevant section in the [application-default credentials] guide.
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build(self) -> BuildResult<IDTokenCredentials> {
        let json_data = match load_adc()? {
            AdcContents::Contents(contents) => {
                Some(serde_json::from_str(&contents).map_err(BuilderError::parsing)?)
            }
            AdcContents::FallbackToMds => None,
        };

        // TODO: accept scopes and quota project id on builder
        build_id_token_credentials(json_data, self.target_audience)
    }
}

fn build_id_token_credentials(
    json: Option<Value>,
    audience: Option<String>,
) -> BuildResult<IDTokenCredentials> {
    match json {
        None => {
            let audience = audience.ok_or_else(|| BuilderError::missing_field("audience"))?;
            // TODO(#3449): pass context that is being built from ADC flow.
            mds::idtoken::Builder::new(audience).build()
        }
        Some(json) => {
            let cred_type = extract_credential_type(&json)?;
            match cred_type {
                "authorized_user" => {
                    // TODO(#3449): need to guide user to use user_account::idtoken::Builder directly
                    Err(BuilderError::not_supported(cred_type))
                }
                "service_account" => {
                    let builder = service_account::idtoken::Builder::new(json);
                    let builder = audience
                        .into_iter()
                        .fold(builder, |b, audience| b.with_target_audience(audience));

                    builder.build()
                }
                "impersonated_service_account" => {
                    // TODO(#3449): to be implemented
                    Err(BuilderError::not_supported(cred_type))
                }
                "external_account" =>
                // never gonna be supported for id tokens
                {
                    Err(BuilderError::not_supported(cred_type))
                }
                _ => Err(BuilderError::unknown_type(cred_type)),
            }
        }
    }
}
