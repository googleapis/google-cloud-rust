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
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

const IAP_JWK_URL: &str = "https://www.gstatic.com/iap/verify/public_key-jwk";
const OAUTH2_JWK_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";

#[derive(Clone, Debug)]
pub struct JwkClient {
    cache: Arc<RwLock<HashMap<String, DecodingKey>>>, // KeyID -> Certificate
}

impl JwkClient {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_or_load_cert(
        &self,
        key_id: String,
        alg: Algorithm,
        jwks_url: Option<String>,
    ) -> Result<DecodingKey> {
        let key_id_str = key_id.as_str();
        let cache = self.cache.try_read().map_err(|_e| {
            CredentialsError::from_msg(false, "failed to obtain lock to read certificate cache")
        })?;
        if let Some(cert) = cache.get(key_id_str) {
            return Ok(cert.clone());
        }
        drop(cache);

        let jwks_url = self.resolve_jwks_url(alg, jwks_url)?;
        let jwk_set: JwkSet = self.fetch_certs(jwks_url).await?;
        let jwk = jwk_set.find(key_id_str).ok_or_else(|| {
            CredentialsError::from_msg(false, "JWKS did not contain a matching `kid`")
        })?;

        let key = DecodingKey::from_jwk(jwk)
            .map_err(|e| CredentialsError::new(false, "failed to parse JWK", e))?;

        let mut cache = self.cache.try_write().map_err(|_e| {
            CredentialsError::from_msg(false, "failed to obtain lock to update certificate cache")
        })?;
        // TODO: cache certs and expires after 1h
        cache.insert(key_id_str.to_string(), key.clone());

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
