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

use crate::Result;

/// Represents an auth token.
#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    /// The actual token string.
    ///
    /// This is the value used in `Authorization:` header.
    pub token: String,

    /// The type of the token.
    ///
    /// The most common type is `"Bearer"` but other types may appear in the
    /// future.
    pub token_type: String,

    /// The instant at which the token expires.
    ///
    /// If `None`, the token does not expire or its expiration is unknown.
    ///
    /// Note that the `Instant` is not valid across processes. It is
    /// recommended to let the authentication library refresh tokens within a
    /// process instead of handling expirations yourself. If you do need to
    /// copy an expiration across processes, consider converting it to a
    /// `time::OffsetDateTime` first:
    ///
    /// ```
    /// # let expires_at = Some(std::time::Instant::now());
    /// expires_at.map(|i| time::OffsetDateTime::now_utc() + (i - std::time::Instant::now()));
    /// ```
    pub expires_at: Option<std::time::Instant>,

    /// Optional metadata associated with the token.
    ///
    /// This might include information like granted scopes or other claims.
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

#[async_trait::async_trait]
pub(crate) trait TokenProvider: std::fmt::Debug + Send + Sync + Default {
    async fn get_token(&self) -> Result<Token>;
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    mockall::mock! {
        #[derive(Debug)]
        pub TokenProvider { }

        #[async_trait::async_trait]
        impl TokenProvider for TokenProvider {
            async fn get_token(&self) -> Result<Token>;
        }
    }
}
