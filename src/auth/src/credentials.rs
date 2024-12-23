// Copyright 2024 Google LLC
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

pub(crate) mod mds_credential;
pub(crate) mod user_credential;

use crate::Result;
use http::header::{HeaderName, HeaderValue};
use std::future::Future;

/// An implementation of [crate::credentials::traits::Credential].
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
pub struct Credential {
    inner: Box<dyn traits::dynamic::Credential>,
}

impl traits::Credential for Credential {
    fn get_token(&mut self) -> impl Future<Output = Result<crate::token::Token>> + Send {
        self.inner.get_token()
    }

    fn get_headers(
        &mut self,
    ) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send {
        self.inner.get_headers()
    }

    fn get_universe_domain(&mut self) -> impl Future<Output = Option<String>> + Send {
        self.inner.get_universe_domain()
    }
}

pub mod traits {
    use super::Future;
    use super::Result;
    use super::{HeaderName, HeaderValue};

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
    /// Application developers who directly use the Auth SDK can use this trait
    /// to mock the credentials. Application developers who use the Google Cloud
    /// Rust SDK directly should not need this functionality.
    ///
    /// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
    /// [token-link]: https://cloud.google.com/docs/authentication#token
    /// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
    /// [Google Compute Engine]: https://cloud.google.com/products/compute
    /// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
    pub trait Credential {
        /// Asynchronously retrieves a token.
        ///
        /// Returns a [Token][crate::token::Token] for the current credentials.
        /// The underlying implementation refreshes the token as needed.
        fn get_token(&mut self) -> impl Future<Output = Result<crate::token::Token>> + Send;

        /// Asynchronously constructs the auth headers.
        ///
        /// Different auth tokens are sent via different headers. The
        /// [Credential] constructs the headers (and header values) that should be
        /// sent with a request.
        ///
        /// The underlying implementation refreshes the token as needed.
        fn get_headers(
            &mut self,
        ) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send;

        /// Retrieves the universe domain associated with the credential, if any.
        fn get_universe_domain(&mut self) -> impl Future<Output = Option<String>> + Send;
    }

    pub(crate) mod dynamic {
        use super::Result;
        use super::{HeaderName, HeaderValue};

        /// A dyn-compatible, crate-private version of `Credential`.
        #[async_trait::async_trait]
        pub trait Credential: Send + Sync {
            /// Asynchronously retrieves a token.
            ///
            /// Returns a [Token][crate::token::Token] for the current credentials.
            /// The underlying implementation refreshes the token as needed.
            async fn get_token(&mut self) -> Result<crate::token::Token>;

            /// Asynchronously constructs the auth headers.
            ///
            /// Different auth tokens are sent via different headers. The
            /// [Credential] constructs the headers (and header values) that should be
            /// sent with a request.
            ///
            /// The underlying implementation refreshes the token as needed.
            async fn get_headers(&mut self) -> Result<Vec<(HeaderName, HeaderValue)>>;

            /// Retrieves the universe domain associated with the credential, if any.
            async fn get_universe_domain(&mut self) -> Option<String>;
        }
    }
}
