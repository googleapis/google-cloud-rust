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

use http::header::{HeaderName, HeaderValue};
use std::future::Future;

pub type Result<T> = std::result::Result<T, crate::errors::CredentialError>;

/// Represents an auth credential used to obtain auth tokens.
/// Implementors of this trait provide a way to asynchronously retrieve tokens
/// and construct auth headers.
pub trait Credential: Send + Sync {
    /// Asynchronously retrieves a token.
    ///
    /// This function returns a `Future` that resolves to a `Result` containing
    /// either the `Token` or an `AuthError` if an error occurred during
    /// token retrieval.
    fn get_token(&mut self) -> impl Future<Output = Result<crate::token::Token>> + Send;

    /// Asynchronously retrieves auth headers.
    ///
    /// This function returns a `Future` that resolves to a `Result` containing
    /// either a vector of key-value pairs representing the headers or an
    /// `AuthError` if an error occurred during header construction.
    fn get_headers(
        &mut self,
    ) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send;

    /// Retrieves the universe domain associated with the credential, if any.
    fn get_universe_domain(&self) -> impl Future<Output = Option<String>> + Send;
}
