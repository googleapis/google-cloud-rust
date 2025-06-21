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

use crate::credentials::errors::SubjectTokenProviderError;

/// A builder for [SubjectToken] instances.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::subjet_token::Builder;
/// let subject_token = Builder::new("test-token")
///     .build()?;
///
pub struct Builder {
    token: String,
}

impl Builder {
    /// Creates a new Builder using the string token.
    pub fn new(token: String) -> Self {
        Self { token }
    }

    /// Returns a [SubjectToken] instance with the configured token.
    pub fn build(self) -> SubjectToken {
        SubjectToken { token: self.token }
    }
}

/// Represents a third-party subject token used for authentication.
///
/// This token is typically obtained from an external identity provider and is
/// exchanged for a Google Cloud access token via the Security Token Service (STS)
///
/// [SubjectToken] should be constructed using it's corresponding [Builder].
///
/// # Example
///
/// ```
/// # # use google_cloud_auth::credentials::subjet_token::Builder;
///
/// let token_value = "my-secret-token".to_string();
/// let subject_token = Builder::new(token_value).build();
///
/// ```
#[derive(Debug)]
pub struct SubjectToken {
    pub(crate) token: String,
}

/// A trait for providing a third-party subject token.
/// TODO(#2254): Add documentation and example after implementation.
pub trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
    /// The error type that can be returned by this provider.
    ///
    /// The error must implement the [`SubjectTokenProviderError`] trait to allow the
    /// authentication client to know whether the error is transient and can be retried.
    type Error: SubjectTokenProviderError;
    /// Asynchronously fetches the third-party subject token.
    ///
    /// # Returns
    ///
    /// - `Ok(SubjectToken)`: On a successful token fetch.
    /// - `Err(Self::Error)`: On a failure, returning the custom error type.
    fn subject_token(&self) -> impl Future<Output = Result<SubjectToken, Self::Error>> + Send;
}
