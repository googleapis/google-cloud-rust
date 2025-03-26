use std::future::Future;
use std::sync::Arc;
use crate::Result;
use http::header::{HeaderName, HeaderValue};

/// An implementation of [crate::credentials::CredentialTrait].
///
/// Represents a [Credential] used to obtain auth [Token][crate::token::Token]s
/// and the corresponding request headers.
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
pub struct Credential {
    // We use an `Arc` to hold the inner implementation.
    //
    // Credentials may be shared across threads (`Send + Sync`), so an `Rc`
    // will not do.
    //
    // They also need to derive `Clone`, as the
    // `gax::http_client::ReqwestClient`s which hold them derive `Clone`. So a
    // `Box` will not do.
    pub inner: Arc<dyn dynamic::CredentialTrait>,
}

impl<T> std::convert::From<T> for Credential
where
    T: crate::credentials::CredentialTrait + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

impl Credential {
    pub async fn get_token(&self) -> Result<crate::token::Token> {
        self.inner.get_token().await
    }

    pub async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        self.inner.get_headers().await
    }

    pub async fn get_universe_domain(&self) -> Option<String> {
        self.inner.get_universe_domain().await
    }
}

/// Represents a [Credential] used to obtain auth
/// [Token][crate::token::Token]s and the corresponding request headers.
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
/// along with [crate::credentials::Credential::from()] to mock the credentials.
/// Application developers who use the Google Cloud Rust SDK directly should not
/// need this functionality.
///
/// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
/// [token-link]: https://cloud.google.com/docs/authentication#token
/// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
/// [Google Compute Engine]: https://cloud.google.com/products/compute
/// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
pub trait CredentialTrait: std::fmt::Debug {
    /// Asynchronously retrieves a token.
    ///
    /// Returns a [Token][crate::token::Token] for the current credentials.
    /// The underlying implementation refreshes the token as needed.
    fn get_token(&self) -> impl Future<Output = Result<crate::token::Token>> + Send;

    /// Asynchronously constructs the auth headers.
    ///
    /// Different auth tokens are sent via different headers. The
    /// [Credential] constructs the headers (and header values) that should be
    /// sent with a request.
    ///
    /// The underlying implementation refreshes the token as needed.
    fn get_headers(&self) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send;

    /// Retrieves the universe domain associated with the credential, if any.
    fn get_universe_domain(&self) -> impl Future<Output = Option<String>> + Send;
}

pub mod dynamic {
    use super::Result;
    use super::{HeaderName, HeaderValue};

    /// A dyn-compatible, crate-private version of `CredentialTrait`.
    #[async_trait::async_trait]
    pub trait CredentialTrait: Send + Sync + std::fmt::Debug {
        /// Asynchronously retrieves a token.
        ///
        /// Returns a [Token][crate::token::Token] for the current credentials.
        /// The underlying implementation refreshes the token as needed.
        async fn get_token(&self) -> Result<crate::token::Token>;

        /// Asynchronously constructs the auth headers.
        ///
        /// Different auth tokens are sent via different headers. The
        /// [Credential] constructs the headers (and header values) that should be
        /// sent with a request.
        ///
        /// The underlying implementation refreshes the token as needed.
        async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>>;

        /// Retrieves the universe domain associated with the credential, if any.
        async fn get_universe_domain(&self) -> Option<String> {
            Some("googleapis.com".to_string())
        }
    }

    /// The public CredentialTrait implements the dyn-compatible CredentialTrait.
    #[async_trait::async_trait]
    impl<T> CredentialTrait for T
    where
        T: crate::credentials::CredentialTrait + Send + Sync,
    {
        async fn get_token(&self) -> Result<crate::token::Token> {
            T::get_token(self).await
        }
        async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
            T::get_headers(self).await
        }
        async fn get_universe_domain(&self) -> Option<String> {
            T::get_universe_domain(self).await
        }
    }
}