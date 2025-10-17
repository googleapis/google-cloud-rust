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
use crate::errors::CredentialsError;
use jsonwebtoken::{Algorithm, DecodingKey, jwk::JwkSet};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

const IAP_JWK_URL: &str = "https://www.gstatic.com/iap/verify/public_key-jwk";
const OAUTH2_JWK_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const CACHE_TTL: Duration = Duration::from_secs(3600);

#[derive(Clone, Debug)]
struct CacheEntry {
    key: DecodingKey,
    expires_at: Instant,
}

#[derive(Clone, Debug)]
pub struct JwkClient {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>, // KeyID -> Certificate
    ttl: Duration,
}

impl JwkClient {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl: CACHE_TTL,
        }
    }

    #[cfg(test)]
    fn with_ttl(ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    pub async fn get_or_load_cert(
        &self,
        key_id: String,
        alg: Algorithm,
        jwks_url: Option<String>,
    ) -> Result<DecodingKey> {
        let key_id_str = key_id.as_str();
        let mut cache = self.cache.try_write().map_err(|_e| {
            CredentialsError::from_msg(false, "failed to obtain lock to read certificate cache")
        })?;
        if let Some(entry) = cache.get(key_id_str) {
            if entry.expires_at > Instant::now() {
                return Ok(entry.key.clone());
            }
        }

        let jwks_url = self.resolve_jwks_url(alg, jwks_url)?;
        let jwk_set: JwkSet = self.fetch_certs(jwks_url).await?;
        let jwk = jwk_set.find(key_id_str).ok_or_else(|| {
            CredentialsError::from_msg(false, "JWKS did not contain a matching `kid`")
        })?;

        let key = DecodingKey::from_jwk(jwk)
            .map_err(|e| CredentialsError::new(false, "failed to parse JWK", e))?;

        let entry = CacheEntry {
            key: key.clone(),
            expires_at: Instant::now() + self.ttl,
        };
        cache.insert(key_id_str.to_string(), entry);

        Ok(key)
    }

    fn resolve_jwks_url(&self, alg: Algorithm, jwks_url: Option<String>) -> Result<String> {
        if let Some(jwks_url) = jwks_url {
            return Ok(jwks_url);
        }
        match alg {
            Algorithm::RS256 => Ok(OAUTH2_JWK_URL.to_string()),
            Algorithm::ES256 => Ok(IAP_JWK_URL.to_string()),
            _ => Err(CredentialsError::from_msg(
                false,
                format!(
                    "unexpected signing algorithm: expected either RS256 or ES256: found {alg:?}"
                ),
            )),
        }
    }

    async fn fetch_certs(&self, jwks_url: String) -> Result<JwkSet> {
        let client = reqwest::Client::new();
        let response = client
            .get(jwks_url)
            .send()
            .await
            .map_err(|e| crate::errors::from_http_error(e, "failed to fetch JWK set"))?;

        if !response.status().is_success() {
            let err = crate::errors::from_http_response(response, "failed to fetch JWK set").await;
            return Err(err);
        }

        let jwk_set: JwkSet = response
            .json()
            .await
            .map_err(|e| CredentialsError::new(!e.is_decode(), "failed to parse JWK set", e))?;

        Ok(jwk_set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use httptest::matchers::{all_of, request};
    use httptest::responders::json_encoded;
    use httptest::{Expectation, Server};
    use jsonwebtoken::Algorithm;
    use rsa::traits::PublicKeyParts;
    use serial_test::parallel;

    type TestResult = anyhow::Result<()>;

    const TEST_KEY_ID: &str = "test-key-id";

    fn create_jwk_set_response() -> serde_json::Value {
        let pub_cert = crate::credentials::tests::RSA_PRIVATE_KEY.to_public_key();
        serde_json::json!({
            "keys": [
                {
                    "e": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(pub_cert.e().to_bytes_be()),
                    "kid": TEST_KEY_ID,
                    "use": "sig",
                    "kty": "RSA",
                    "n": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(pub_cert.n().to_bytes_be()),
                    "alg": "RS256"
                }
            ]
        })
    }

    #[tokio::test]
    #[parallel]
    async fn test_get_or_load_cert_success() -> TestResult {
        let server = Server::run();
        let response = create_jwk_set_response();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(response.clone())),
        );

        let client = JwkClient::new();
        let jwks_url = format!("http://{}/certs", server.addr());

        // First call, should fetch from URL
        let _key = client
            .get_or_load_cert(
                TEST_KEY_ID.to_string(),
                Algorithm::RS256,
                Some(jwks_url.clone()),
            )
            .await?;

        // Second call, should use cache
        let _key = client
            .get_or_load_cert(TEST_KEY_ID.to_string(), Algorithm::RS256, Some(jwks_url))
            .await?;

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_get_or_load_cert_kid_not_found() -> TestResult {
        let server = Server::run();
        let response = create_jwk_set_response();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(response.clone())),
        );

        let client = JwkClient::new();
        let jwks_url = format!("http://{}/certs", server.addr());

        let result = client
            .get_or_load_cert("unknown-kid".to_string(), Algorithm::RS256, Some(jwks_url))
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("JWKS did not contain a matching `kid`")
        );

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_get_or_load_cert_fetch_error() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(httptest::responders::status_code(500)),
        );

        let client = JwkClient::new();
        let jwks_url = format!("http://{}/certs", server.addr());

        let result = client
            .get_or_load_cert(TEST_KEY_ID.to_string(), Algorithm::RS256, Some(jwks_url))
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("failed to fetch JWK set"));

        Ok(())
    }

    #[test]
    #[parallel]
    fn test_resolve_jwks_url() -> TestResult {
        let client = JwkClient::new();

        // Custom URL
        let url = "https://example.com/jwks".to_string();
        assert_eq!(
            client
                .resolve_jwks_url(Algorithm::RS256, Some(url.clone()))
                .unwrap(),
            url
        );

        // Default for RS256
        assert_eq!(
            client.resolve_jwks_url(Algorithm::RS256, None).unwrap(),
            OAUTH2_JWK_URL
        );

        // Default for ES256
        assert_eq!(
            client.resolve_jwks_url(Algorithm::ES256, None).unwrap(),
            IAP_JWK_URL
        );

        // Unsupported algorithm
        let result = client.resolve_jwks_url(Algorithm::HS256, None);
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_get_or_load_cert_cache_expiration() -> TestResult {
        let server = Server::run();
        let response = create_jwk_set_response();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(2)
                .respond_with(json_encoded(response.clone())),
        );

        let client = JwkClient::with_ttl(Duration::from_secs(1));
        let jwks_url = format!("http://{}/certs", server.addr());

        // First call, should fetch from URL and cache it.
        let _key = client
            .get_or_load_cert(
                TEST_KEY_ID.to_string(),
                Algorithm::RS256,
                Some(jwks_url.clone()),
            )
            .await?;

        // Second call, should still be cached.
        let _key = client
            .get_or_load_cert(
                TEST_KEY_ID.to_string(),
                Algorithm::RS256,
                Some(jwks_url.clone()),
            )
            .await?;

        // Wait for the cache to expire.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // This call should fetch from URL again.
        let _key = client
            .get_or_load_cert(TEST_KEY_ID.to_string(), Algorithm::RS256, Some(jwks_url))
            .await?;

        Ok(())
    }
}
