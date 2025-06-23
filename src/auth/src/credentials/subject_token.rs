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

//! Subject Token Credential Type.
//!
//! A **subject token** is a credential that asserts the identity of a workload,
//! application, or a user. In the case of the [Workload Identity Federation] flow,
//! this allows applications to authenticate to Google Cloud, instead of using
//! long-lived service account keys. The process involves exchanging this subject
//! token for a short-lived Google Cloud access token via the
//! [Security Token Service (STS)].
//!
//! This module provides the [`SubjectTokenProvider`] trait, which is used to
//! fetch subject tokens. The Google Cloud client libraries for Rust will typically
//! use the [`SubjectTokenProvider`] automatically for external account credentials.
//! You might need to implement this trait for advanced authentication scenarios where
//! you want to integrate a custom subject token fetching mechanism.
//!
//! # Example
//!
//! ```
//! # use std::error::Error;
//! # use std::fmt;
//! # use std::future::Future;
//! # use google_cloud_auth::credentials::subject_token::{
//! #     Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
//! # };
//! # use google_cloud_auth::errors::SubjectTokenProviderError;
//! #[derive(Debug)]
//! struct CustomProviderError {
//!     message: String,
//!     is_transient: bool,
//! }
//!
//! impl fmt::Display for CustomProviderError {
//!     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//!         write!(f, "CustomProviderError: {}", self.message)
//!     }
//! }
//!
//! impl Error for CustomProviderError {}
//!
//! impl SubjectTokenProviderError for CustomProviderError {
//!     fn is_transient(&self) -> bool {
//!         self.is_transient
//!     }
//! }
//!
//! #[derive(Debug)]
//! struct MyCustomProvider {
//!     api_key: String,
//! }
//!
//! impl SubjectTokenProvider for MyCustomProvider {
//!     type Error = CustomProviderError;
//!
//!     async fn subject_token(&self) -> Result<SubjectToken, Self::Error> {
//!             let token_from_idp = "a-very-secret-token-from-your-idp";
//!             Ok(SubjectTokenBuilder::new(token_from_idp.to_string()).build())
//!     }
//! }
//! ```
//!
//! [Workload Identity Federation]: https://cloud.google.com/iam/docs/workload-identity-federation
//! [Security Token Service (STS)]: https://cloud.google.com/iam/docs/reference/sts/rest
use crate::credentials::errors::SubjectTokenProviderError;

/// A builder for [`SubjectToken`] instances.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::subject_token::Builder;
/// let subject_token = Builder::new("test-token")
///     .build();
///
pub(crate) struct Builder {
    token: String,
}

impl Builder {
    /// Creates a new builder using the string token.
    pub fn new<S: Into<String>>(token: S) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// Returns a [`SubjectToken`] instance.
    pub fn build(self) -> SubjectToken {
        SubjectToken { token: self.token }
    }
}

/// Represents a third-party subject token used for authentication.
///
/// A `SubjectToken` should be constructed using its corresponding [`Builder`].
///
/// # Example
///
/// ```
/// # use google_cloud_auth::credentials::subject_token::Builder;
/// let token_value = "my-secret-token".to_string();
/// let subject_token = Builder::new(token_value).build();
///
/// ```
#[derive(Debug)]
pub(crate) struct SubjectToken {
    pub(crate) token: String,
}

/// Trait for providing a third-party subject token.
///
/// The Google Cloud client libraries for Rust will automatically implement this
/// trait for external account credentials. You might need to implement this trait
/// for advanced authentication scenarios where you want to integrate a custom
/// subject token fetching mechanism.
///
/// # Example
///
/// ```
/// # use std::error::Error;
/// # use std::fmt;
/// # use std::future::Future;
/// # use google_cloud_auth::credentials::subject_token::{
/// #     Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
/// # };
/// # use google_cloud_auth::errors::SubjectTokenProviderError;
/// #[derive(Debug)]
/// struct CustomProviderError {
///     message: String,
///     is_transient: bool,
/// }
///
/// impl fmt::Display for CustomProviderError {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "CustomProviderError: {}", self.message)
///     }
/// }
///
/// impl Error for CustomProviderError {}
///
/// impl SubjectTokenProviderError for CustomProviderError {
///     fn is_transient(&self) -> bool {
///         self.is_transient
///     }
/// }
///
/// #[derive(Debug)]
/// struct MyCustomProvider {
///     api_key: String,
/// }
///
/// impl SubjectTokenProvider for MyCustomProvider {
///     type Error = CustomProviderError;
///
///     async fn subject_token(&self) -> Result<SubjectToken, Self::Error> {
///             let token_from_idp = "a-very-secret-token-from-your-idp";
///             Ok(SubjectTokenBuilder::new(token_from_idp.to_string()).build())
///     }
/// }
/// ```
pub(crate) trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
    /// The error type that can be returned by this provider.
    ///
    /// The error must implement the [`SubjectTokenProviderError`] trait to allow the
    /// authentication client to know whether the error is transient and can be retried.
    type Error: SubjectTokenProviderError;
    /// Asynchronously fetches the third-party subject token.
    fn subject_token(&self) -> impl Future<Output = Result<SubjectToken, Self::Error>> + Send;
}
