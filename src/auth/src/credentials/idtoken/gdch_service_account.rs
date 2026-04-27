// Copyright 2026 Google LLC
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

//! Obtain GDCH bearer tokens using GDCH service accounts.

use crate::credentials::CacheableResource;
use crate::credentials::gdch_service_account::GdchServiceAccountTokenProvider;
use crate::credentials::idtoken::IDTokenCredentials;
use crate::credentials::idtoken::dynamic::IDTokenCredentialsProvider;
use crate::token::CachedTokenProvider;
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use google_cloud_gax::error::CredentialsError;
use http::Extensions;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug)]
struct GdchServiceAccountCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
}

#[async_trait::async_trait]
impl<T> IDTokenCredentialsProvider for GdchServiceAccountCredentials<T>
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

/// Creates [`IDTokenCredentials`] instances that fetch GDCH bearer tokens using
/// GDCH service accounts.
pub struct Builder {
    service_account_key: Value,
    audience: String,
}

impl Builder {
    /// Creates a new builder using a `gdch_service_account` JSON value.
    pub fn new<S: Into<String>>(audience: S, service_account_key: Value) -> Self {
        Self {
            service_account_key,
            audience: audience.into(),
        }
    }

    /// Returns an [`IDTokenCredentials`] instance with the configured settings.
    pub fn build(self) -> BuildResult<IDTokenCredentials> {
        let token_provider =
            GdchServiceAccountTokenProvider::from_json(self.audience, self.service_account_key)?;
        let creds = GdchServiceAccountCredentials {
            token_provider: TokenCache::new(token_provider),
        };
        Ok(IDTokenCredentials {
            inner: Arc::new(creds),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;
    use std::error::Error;

    type TestResult = std::result::Result<(), Box<dyn Error>>;

    const ES256_PRIVATE_KEY: &str = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIEUByN/Cd73iTqf85VeQ74wWaZr6sMnkMY25RvOIUJ94oAoGCCqGSM49\nAwEHoUQDQgAEHf1LlK7P4qdsjslUqKVx5AlEBXN9VLzYYhC700o2DOthBjBFU7Yu\nmohy0DCDBPJ9pfiCPe/lZSFlvdl8Xyz9Lg==\n-----END EC PRIVATE KEY-----\n";

    #[derive(Debug, serde::Deserialize)]
    struct TestTokenRequest {
        audience: String,
        subject_token: String,
    }

    #[tokio::test]
    async fn id_token_returns_gdch_bearer_token() -> TestResult {
        let audience = "https://example.com/test-audience";
        let expected_audience = audience.to_string();
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/authenticate"),
                request::body(json_decoded(move |req: &TestTokenRequest| {
                    req.audience == expected_audience && !req.subject_token.is_empty()
                })),
            ])
            .respond_with(json_encoded(json!({
                "access_token": "test-gdch-token",
                "token_type": "Bearer",
                "expires_in": 3600_u64,
            }))),
        );

        let service_account_key = json!({
            "type": "gdch_service_account",
            "format_version": "1",
            "project": "test-project",
            "private_key_id": "test-private-key-id",
            "private_key": ES256_PRIVATE_KEY,
            "name": "test-name",
            "token_uri": server.url("/authenticate").to_string(),
        });
        let credentials = Builder::new(audience, service_account_key).build()?;

        assert_eq!(credentials.id_token().await?, "test-gdch-token");
        Ok(())
    }
}
