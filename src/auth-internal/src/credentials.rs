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

use crate::Result;
use crate::token::AccessToken;
use http::{Extensions, HeaderMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Represents an Entity Tag for a [CacheableResource].
///
/// An `EntityTag` is an opaque token that can be used to determine if a
/// cached resource has changed. The specific format of this tag is an
/// implementation detail.
///
/// As the name indicates, these are inspired by the ETags prevalent in HTTP
/// caching protocols. Their implementation is very different, and are only
/// intended for use within a single program.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct EntityTag(u64);

static ENTITY_TAG_GENERATOR: AtomicU64 = AtomicU64::new(0);
impl EntityTag {
    pub fn new() -> Self {
        let value = ENTITY_TAG_GENERATOR.fetch_add(1, Ordering::SeqCst);
        Self(value)
    }
}

/// Represents a resource that can be cached, along with its [EntityTag].
///
/// This enum is used to provide cacheable data to the consumers of the credential provider.
/// It allows a data provider to return either new data (with an [EntityTag]) or
/// indicate that the caller's cached version (identified by a previously provided [EntityTag])
/// is still valid.
#[derive(Clone, PartialEq, Debug)]
pub enum CacheableResource<T> {
    NotModified,
    New { entity_tag: EntityTag, data: T },
}

/// An implementation of [crate::credentials::CredentialsProvider].
///
/// Represents a [Credentials] used to obtain the auth request headers.
///
/// In general, [Credentials][credentials-link] are "digital object that provide
/// proof of identity", the archetype may be a username and password
/// combination, but a private RSA key may be a better example.
///
/// Modern authentication protocols do not send the credentials to authenticate
/// with a service. Even when sent over encrypted transports, the credentials
/// may be accidentally exposed via logging or may be captured if there are
/// errors in the transport encryption. Because the credentials are often
/// long-lived, that risk of exposure is also long-lived.
///
/// Instead, modern authentication protocols exchange the credentials for a
/// time-limited [Token][token-link], a digital object that shows the caller was
/// in possession of the credentials. Because tokens are time limited, risk of
/// misuse is also time limited. Tokens may be further restricted to only a
/// certain subset of the RPCs in the service, or even to specific resources, or
/// only when used from a given machine (virtual or not). Further limiting the
/// risks associated with any leaks of these tokens.
///
/// This struct also abstracts token sources that are not backed by a specific
/// digital object. The canonical example is the [Metadata Service]. This
/// service is available in many Google Cloud environments, including
/// [Google Compute Engine], and [Google Kubernetes Engine].
///
/// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
/// [token-link]: https://cloud.google.com/docs/authentication#token
/// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
/// [Google Compute Engine]: https://cloud.google.com/products/compute
/// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
#[derive(Clone, Debug)]
pub struct Credentials {
    // We use an `Arc` to hold the inner implementation.
    //
    // Credentials may be shared across threads (`Send + Sync`), so an `Rc`
    // will not do.
    //
    // They also need to derive `Clone`, as the
    // `google_cloud_gax::http_client::ReqwestClient`s which hold them derive `Clone`. So a
    // `Box` will not do.
    inner: Arc<dyn dynamic::CredentialsProvider>,
}

impl<T> std::convert::From<T> for Credentials
where
    T: CredentialsProvider + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

pub fn new_credentials<T>(inner: T) -> Credentials
where
    T: dynamic::CredentialsProvider + Send + Sync + 'static,
{
    Credentials { inner: Arc::new(inner) }
}

impl Credentials {
    pub async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        self.inner.headers(extensions).await
    }

    pub async fn universe_domain(&self) -> Option<String> {
        self.inner.universe_domain().await
    }
}

/// An implementation of [crate::credentials::CredentialsProvider] that can also
/// provide direct access to the underlying access token.
///
/// This struct is returned by the `build_access_token_credentials()` method on
/// the various credential builders. It can be used to obtain an access token
/// directly via the `access_token()` method, or it can be converted into a `Credentials`
/// object to be used with the Google Cloud client libraries.
#[derive(Clone, Debug)]
pub struct AccessTokenCredentials {
    // We use an `Arc` to hold the inner implementation.
    //
    // AccessTokenCredentials may be shared across threads (`Send + Sync`), so an `Rc`
    // will not do.
    //
    // They also need to derive `Clone`, as the
    // `google_cloud_gax::http_client::ReqwestClient`s which hold them derive `Clone`. So a
    // `Box` will not do.
    inner: Arc<dyn dynamic::AccessTokenCredentialsProvider>,
}

impl<T> std::convert::From<T> for AccessTokenCredentials
where
    T: AccessTokenCredentialsProvider + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

pub fn new_access_token_credentials<T>(inner: T) -> AccessTokenCredentials
where
    T: dynamic::AccessTokenCredentialsProvider + Send + Sync + 'static,
{
    AccessTokenCredentials { inner: Arc::new(inner) }
}

impl AccessTokenCredentials {

    pub async fn access_token(&self) -> Result<AccessToken> {
        self.inner.access_token().await
    }
}

/// Makes [AccessTokenCredentials] compatible with clients that expect
/// a [Credentials] and/or a [CredentialsProvider].
impl CredentialsProvider for AccessTokenCredentials {
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        self.inner.headers(extensions).await
    }

    async fn universe_domain(&self) -> Option<String> {
        self.inner.universe_domain().await
    }
}

/// A trait for credential types that can provide direct access to an access token.
///
/// This trait is primarily intended for interoperability with other libraries that
/// require a raw access token, or for calling Google Cloud APIs that are not yet
/// supported by the SDK.
pub trait AccessTokenCredentialsProvider: CredentialsProvider + std::fmt::Debug {
    /// Asynchronously retrieves an access token.
    fn access_token(&self) -> impl Future<Output = Result<AccessToken>> + Send;
}

/// Represents a [Credentials] used to obtain auth request headers.
///
/// In general, [Credentials][credentials-link] are "digital object that
/// provide proof of identity", the archetype may be a username and password
/// combination, but a private RSA key may be a better example.
///
/// Modern authentication protocols do not send the credentials to
/// authenticate with a service. Even when sent over encrypted transports,
/// the credentials may be accidentally exposed via logging or may be
/// captured if there are errors in the transport encryption. Because the
/// credentials are often long-lived, that risk of exposure is also
/// long-lived.
///
/// Instead, modern authentication protocols exchange the credentials for a
/// time-limited [Token][token-link], a digital object that shows the caller
/// was in possession of the credentials. Because tokens are time limited,
/// risk of misuse is also time limited. Tokens may be further restricted to
/// only a certain subset of the RPCs in the service, or even to specific
/// resources, or only when used from a given machine (virtual or not).
/// Further limiting the risks associated with any leaks of these tokens.
///
/// This struct also abstracts token sources that are not backed by a
/// specific digital object. The canonical example is the
/// [Metadata Service]. This service is available in many Google Cloud
/// environments, including [Google Compute Engine], and
/// [Google Kubernetes Engine].
///
/// # Notes
///
/// Application developers who directly use the Auth SDK can use this trait,
/// along with [crate::credentials::Credentials::from()] to mock the credentials.
/// Application developers who use the Google Cloud Rust SDK directly should not
/// need this functionality.
///
/// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
/// [token-link]: https://cloud.google.com/docs/authentication#token
/// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
/// [Google Compute Engine]: https://cloud.google.com/products/compute
/// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
pub trait CredentialsProvider: std::fmt::Debug {
    /// Asynchronously constructs the auth headers.
    ///
    /// Different auth tokens are sent via different headers. The
    /// [Credentials] constructs the headers (and header values) that should be
    /// sent with a request.
    ///
    /// # Parameters
    /// * `extensions` - An `http::Extensions` map that can be used to pass additional
    ///   context to the credential provider. For caching purposes, this can include
    ///   an [EntityTag] from a previously returned [`CacheableResource<HeaderMap>`].
    ///   If a valid `EntityTag` is provided and the underlying authentication data
    ///   has not changed, this method returns `Ok(CacheableResource::NotModified)`.
    ///
    /// # Returns
    /// A `Future` that resolves to a `Result` containing:
    /// * `Ok(CacheableResource::New { entity_tag, data })`: If new or updated headers
    ///   are available.
    /// * `Ok(CacheableResource::NotModified)`: If the headers have not changed since
    ///   the ETag provided via `extensions` was issued.
    /// * `Err(CredentialsError)`: If an error occurs while trying to fetch or
    ///   generating the headers.
    fn headers(
        &self,
        extensions: Extensions,
    ) -> impl Future<Output = Result<CacheableResource<HeaderMap>>> + Send;

    /// Retrieves the universe domain associated with the credentials, if any.
    fn universe_domain(&self) -> impl Future<Output = Option<String>> + Send;
}

pub mod dynamic {
    use super::Result;
    use super::{CacheableResource, Extensions, HeaderMap};

    /// A dyn-compatible, crate-private version of `CredentialsProvider`.
    #[async_trait::async_trait]
    pub trait CredentialsProvider: Send + Sync + std::fmt::Debug {
        /// Asynchronously constructs the auth headers.
        ///
        /// Different auth tokens are sent via different headers. The
        /// [Credentials] constructs the headers (and header values) that should be
        /// sent with a request.
        ///
        /// # Parameters
        /// * `extensions` - An `http::Extensions` map that can be used to pass additional
        ///   context to the credential provider. For caching purposes, this can include
        ///   an [EntityTag] from a previously returned [CacheableResource<HeaderMap>].
        ///   If a valid `EntityTag` is provided and the underlying authentication data
        ///   has not changed, this method returns `Ok(CacheableResource::NotModified)`.
        ///
        /// # Returns
        /// A `Future` that resolves to a `Result` containing:
        /// * `Ok(CacheableResource::New { entity_tag, data })`: If new or updated headers
        ///   are available.
        /// * `Ok(CacheableResource::NotModified)`: If the headers have not changed since
        ///   the ETag provided via `extensions` was issued.
        /// * `Err(CredentialsError)`: If an error occurs while trying to fetch or
        ///   generating the headers.
        async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>>;

        /// Retrieves the universe domain associated with the credentials, if any.
        async fn universe_domain(&self) -> Option<String> {
            Some("googleapis.com".to_string())
        }
    }

    /// The public CredentialsProvider implements the dyn-compatible CredentialsProvider.
    #[async_trait::async_trait]
    impl<T> CredentialsProvider for T
    where
        T: super::CredentialsProvider + Send + Sync,
    {
        async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
            T::headers(self, extensions).await
        }
        async fn universe_domain(&self) -> Option<String> {
            T::universe_domain(self).await
        }
    }

    /// A dyn-compatible, crate-private version of `AccessTokenCredentialsProvider`.
    #[async_trait::async_trait]
    pub trait AccessTokenCredentialsProvider:
        CredentialsProvider + Send + Sync + std::fmt::Debug
    {
        async fn access_token(&self) -> Result<super::AccessToken>;
    }

    #[async_trait::async_trait]
    impl<T> AccessTokenCredentialsProvider for T
    where
        T: super::AccessTokenCredentialsProvider + Send + Sync,
    {
        async fn access_token(&self) -> Result<super::AccessToken> {
            T::access_token(self).await
        }
    }
}

pub use factory::build_default_credentials;

/// Factory for creating default credentials.
pub mod factory {
    use super::Credentials;
    use crate::Result;
    use std::sync::OnceLock;

    /// A function type that returns default credentials.
    pub type BuilderFn = Box<dyn Fn() -> Result<Credentials> + Send + Sync>;
    static BUILDER: OnceLock<BuilderFn> = OnceLock::new();

    /// Registers a builder for default credentials.
    pub fn set_default_credentials_builder(f: BuilderFn) {
        let _ = BUILDER.set(f);
    }

    /// Builds the default credentials.
    pub fn build_default_credentials() -> Result<Credentials> {
        if let Some(f) = BUILDER.get() {
            f()
        } else {
            Err(crate::errors::non_retryable_from_str(
                "No default credentials builder registered",
            ))
        }
    }
}

pub mod anonymous {
    use crate::credentials::dynamic::CredentialsProvider;
    use crate::credentials::{CacheableResource, Credentials, EntityTag, Result};
    use http::{Extensions, HeaderMap};
    use std::sync::Arc;

    #[derive(Debug)]
    struct AnonymousCredentials {
        entity_tag: EntityTag,
    }

    /// A builder for creating anonymous credentials.
    #[derive(Debug, Default)]
    pub struct Builder {}

    impl Builder {
        /// Creates a new builder.
        pub fn new() -> Self {
            Self::default()
        }

        /// Returns a [Credentials] instance.
        pub fn build(self) -> Credentials {
            Credentials {
                inner: Arc::new(AnonymousCredentials {
                    entity_tag: EntityTag::new(),
                }),
            }
        }
    }

    #[async_trait::async_trait]
    impl CredentialsProvider for AnonymousCredentials {
        async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
            match extensions.get::<EntityTag>() {
                Some(tag) if self.entity_tag.eq(tag) => Ok(CacheableResource::NotModified),
                _ => Ok(CacheableResource::New {
                    data: HeaderMap::new(),
                    entity_tag: self.entity_tag.clone(),
                }),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

        #[tokio::test]
        async fn create_anonymous_credentials() -> TestResult {
            let creds = Builder::new().build();
            let mut extensions = Extensions::new();
            let cached_headers = creds.headers(extensions.clone()).await.unwrap();
            let (headers, entity_tag) = match cached_headers {
                CacheableResource::New { entity_tag, data } => (data, entity_tag),
                CacheableResource::NotModified => unreachable!("expecting new headers"),
            };
            assert!(headers.is_empty(), "{headers:?}");

            extensions.insert(entity_tag);
            let cached_headers = creds.headers(extensions).await.unwrap();
            match cached_headers {
                CacheableResource::New { .. } => unreachable!("expecting cached headers"),
                CacheableResource::NotModified => {}
            }
            Ok(())
        }
    }
}
