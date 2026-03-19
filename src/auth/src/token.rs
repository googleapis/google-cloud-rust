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

//! Types and functions to work with auth [Tokens].
//!
//! [Tokens]: https://cloud.google.com/docs/authentication#token

use crate::Result;
use crate::credentials::CacheableResource;
use http::Extensions;
use std::collections::HashMap;
use tokio::time::Instant;

/// Represents an auth token.
#[derive(Clone, PartialEq)]
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
    /// If `None`, the token does not expire.
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
    pub expires_at: Option<Instant>,

    /// Optional metadata associated with the token.
    ///
    /// This might include information like granted scopes or other claims.
    pub metadata: Option<HashMap<String, String>>,
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Token")
            .field("token", &"[censored]")
            .field("token_type", &self.token_type)
            .field("expires_at", &self.expires_at)
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[async_trait::async_trait]
pub(crate) trait TokenProvider: std::fmt::Debug + Send + Sync {
    async fn token(&self) -> Result<Token>;
}

#[async_trait::async_trait]
pub(crate) trait CachedTokenProvider: std::fmt::Debug + Send + Sync {
    async fn token(&self, extensions: Extensions) -> Result<CacheableResource<Token>>;
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::time::Duration;

    // Used by tests in other modules.
    mockall::mock! {
        #[derive(Debug)]
        pub TokenProvider { }

        #[async_trait::async_trait]
        impl TokenProvider for TokenProvider {
            async fn token(&self) -> Result<Token>;
        }
    }

    #[test]
    fn debug() {
        let expires_at = Instant::now() + Duration::from_secs(3600);
        let metadata =
            HashMap::from([("a", "test-only")].map(|(k, v)| (k.to_string(), v.to_string())));

        let token = Token {
            token: "token-test-only".into(),
            token_type: "token-type-test-only".into(),
            expires_at: Some(expires_at),
            metadata: Some(metadata.clone()),
        };
        let got = format!("{token:?}");
        assert!(!got.contains("token-test-only"), "{got}");
        assert!(got.contains("token: \"[censored]\""), "{got}");
        assert!(got.contains("token_type: \"token-type-test-only"), "{got}");
        assert!(
            got.contains(&format!("expires_at: Some({expires_at:?}")),
            "{got}"
        );
        assert!(
            got.contains(&format!("metadata: Some({metadata:?}")),
            "{got}"
        );
    }
}
