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

use crate::{Error, Result};
use auth::signer::Signer;
use chrono::Utc;
use hex;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use url::form_urlencoded;

/// A builder for creating signed URLs.
pub struct SignObject {
    signer: Signer,
    bucket: String,
    object: String,
    method: String,
    expiration: std::time::Duration,
    headers: BTreeMap<&'static str, String>,
    query_parameters: BTreeMap<&'static str, String>,
}

impl SignObject {
    pub(crate) fn new(signer: Signer, bucket: String, object: String) -> Self {
        Self {
            signer,
            bucket,
            object,
            method: "GET".to_string(),
            expiration: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            headers: BTreeMap::new(),
            query_parameters: BTreeMap::new(),
        }
    }

    /// Sets the HTTP method for the signed URL. Default is "GET".
    pub fn with_method<S: Into<String>>(mut self, method: S) -> Self {
        self.method = method.into();
        self
    }

    /// Sets the expiration time for the signed URL. Default is 7 days.
    pub fn with_expiration(mut self, expiration: std::time::Duration) -> Self {
        self.expiration = expiration;
        self
    }

    /// Adds a header to the signed URL.
    /// Note: These headers must be present in the request when using the signed URL.
    pub fn with_header<S: Into<String>>(mut self, key: &'static str, value: S) -> Self {
        self.headers.insert(key, value.into());
        self
    }

    /// Adds a query parameter to the signed URL.
    pub fn with_query_param<S: Into<String>>(mut self, key: &'static str, value: S) -> Self {
        self.query_parameters.insert(key, value.into());
        self
    }

    /// Generates the signed URL.
    pub async fn send(self) -> Result<String> {
        let canonical_uri = format!("/{}", self.object); // TODO: escape object name

        let now = Utc::now();
        let request_timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = now.format("%Y%m%d");
        let credential_scope = format!("{datestamp}/auto/storage/goog4_request");
        let client_email = self.signer.client_email().await.map_err(Error::io)?; // TODO map to proper error
        let credential = format!("{client_email}/{credential_scope}");

        let bucket_name = self.bucket.trim_start_matches("projects/_/buckets/");
        let host = format!("{}.storage.googleapis.com", bucket_name);

        let mut headers = self.headers;
        headers.insert("host", host.clone());

        let canonical_headers = headers
            .iter()
            .fold("".to_string(), |acc, (k, v)| format!("{acc}{k}:{v}\n"));

        let signed_headers = headers
            .iter()
            .fold("".to_string(), |acc, (k, _)| format!("{acc}{k};"));
        let signed_headers = signed_headers.trim_end_matches(';').to_string();

        let mut query_parameters = self.query_parameters;
        query_parameters.insert("X-Goog-Algorithm", "GOOG4-RSA-SHA256".to_string());
        query_parameters.insert("X-Goog-Credential", credential);
        query_parameters.insert("X-Goog-Date", request_timestamp.clone());
        query_parameters.insert("X-Goog-Expires", self.expiration.as_secs().to_string());
        query_parameters.insert("X-Goog-SignedHeaders", signed_headers.clone());

        let mut canonical_query = form_urlencoded::Serializer::new("".to_string());
        query_parameters.iter().for_each(|(k, v)| {
            canonical_query.append_pair(k, v);
        });
        let canonical_query_string = canonical_query.finish();

        let canonical_request = [
            self.method,
            canonical_uri.clone(),
            canonical_query_string.clone(),
            canonical_headers,
            signed_headers,
            "UNSIGNED-PAYLOAD".to_string(),
        ]
        .join("\n");

        let canonical_request_hash = Sha256::digest(canonical_request.as_bytes());
        let canonical_request_hash = hex::encode(canonical_request_hash);

        let string_to_sign = [
            "GOOG4-RSA-SHA256".to_string(),
            request_timestamp,
            credential_scope,
            canonical_request_hash,
        ]
        .join("\n");

        let signature = self
            .signer
            .sign(string_to_sign.as_str())
            .await
            .map_err(Error::io)?; // TODO map to proper error

        let scheme_and_host = format!("https://{}", host);
        let signed_url = format!(
            "{}{}?{}&x-goog-signature={}",
            scheme_and_host, canonical_uri, canonical_query_string, signature
        );

        Ok(signed_url)
    }
}
