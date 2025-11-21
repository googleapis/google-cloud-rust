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

use crate::error::SigningError;
use auth::signer::Signer;
use chrono::Utc;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use url::form_urlencoded;

/// https://cloud.google.com/storage/docs/request-endpoints#encoding
const PATH_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

/// A builder for creating signed URLs.
pub struct SignedUrlBuilder {
    bucket: String,
    object: String,
    method: String,
    expiration: std::time::Duration,
    headers: BTreeMap<&'static str, String>,
    query_parameters: BTreeMap<&'static str, String>,
    endpoint: String,
    client_email: Option<String>,
}

impl SignedUrlBuilder {
    pub fn new<B, O>(bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            method: "GET".to_string(),
            expiration: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            headers: BTreeMap::new(),
            query_parameters: BTreeMap::new(),
            endpoint: "https://storage.googleapis.com".to_string(),
            client_email: None,
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

    /// Sets the endpoint for the signed URL. Default is "https://storage.googleapis.com".
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Sets the client email for the signed URL.
    /// If not set, the email will be fetched from the signer.
    pub fn with_client_email<S: Into<String>>(mut self, client_email: S) -> Self {
        self.client_email = Some(client_email.into());
        self
    }

    /// Generates the signed URL using the provided signer.
    pub async fn sign_with(self, signer: &Signer) -> std::result::Result<String, SigningError> {
        let encoded_object = utf8_percent_encode(&self.object, PATH_ENCODE_SET).to_string();
        let canonical_uri = format!("/{}", encoded_object);

        let now = Utc::now();
        let request_timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = now.format("%Y%m%d");
        let credential_scope = format!("{datestamp}/auto/storage/goog4_request");
        let client_email = if let Some(email) = self.client_email {
            email
        } else {
            signer.client_email().await.map_err(SigningError::Signing)?
        };
        let credential = format!("{client_email}/{credential_scope}");

        let endpoint_url =
            url::Url::parse(&self.endpoint).map_err(|e| SigningError::InvalidEndpoint(e.into()))?;
        let endpoint_host = endpoint_url
            .host_str()
            .ok_or_else(|| SigningError::InvalidEndpoint("invalid endpoint host".into()))?;
        let bucket_name = self.bucket.trim_start_matches("projects/_/buckets/");
        let host = format!("{}.{}", bucket_name, endpoint_host);

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

        let signature = signer
            .sign(string_to_sign.as_str())
            .await
            .map_err(SigningError::Signing)?;

        let scheme_and_host = format!("{}://{}", endpoint_url.scheme(), host);

        let signed_url = format!(
            "{}{}?{}&x-goog-signature={}",
            scheme_and_host, canonical_uri, canonical_query_string, signature
        );

        Ok(signed_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::signer::{Signer, SigningProvider};

    type TestResult = anyhow::Result<()>;

    #[derive(Debug)]
    struct MockSigner;

    #[async_trait::async_trait]
    impl SigningProvider for MockSigner {
        async fn client_email(&self) -> auth::signer::Result<String> {
            Ok("test@example.com".to_string())
        }

        async fn sign(&self, _content: &[u8]) -> auth::signer::Result<String> {
            Ok("test-signature".to_string())
        }
    }

    #[tokio::test]
    async fn test_signed_url_generation() -> TestResult {
        let signer = Signer::from(MockSigner);
        let url = SignedUrlBuilder::new("test-bucket", "test-object")
            .with_method("PUT")
            .with_expiration(std::time::Duration::from_secs(3600))
            .with_header("x-goog-meta-test", "value")
            .sign_with(&signer)
            .await
            .unwrap();

        assert!(url.starts_with("https://test-bucket.storage.googleapis.com/test-object"));
        assert!(url.contains("x-goog-signature=test-signature"));
        assert!(url.contains("X-Goog-Algorithm=GOOG4-RSA-SHA256"));
        assert!(url.contains("X-Goog-Credential=test%40example.com"));

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_generation_escaping() -> TestResult {
        let signer: Signer = Signer::from(MockSigner);
        let url = SignedUrlBuilder::new("test-bucket", "folder/test object.txt")
            .with_method("PUT")
            .with_header("content-type", "text/plain")
            .sign_with(&signer)
            .await
            .unwrap();

        assert!(
            url.starts_with("https://test-bucket.storage.googleapis.com/folder/test%20object.txt?")
        );
        assert!(url.contains("x-goog-signature="));

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_error_signing() -> TestResult {
        #[derive(Debug)]
        struct FailSigner;
        #[async_trait::async_trait]
        impl SigningProvider for FailSigner {
            async fn client_email(&self) -> auth::signer::Result<String> {
                Ok("test@example.com".to_string())
            }
            async fn sign(&self, _content: &[u8]) -> auth::signer::Result<String> {
                Err(auth::signer::SigningError::from_msg("test".to_string()))
            }
        }
        let signer = Signer::from(FailSigner);
        let err = SignedUrlBuilder::new("b", "o")
            .sign_with(&signer)
            .await
            .unwrap_err();

        match err {
            SigningError::Signing(e) => assert!(e.is_sign()),
            _ => panic!("unexpected error type: {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_error_endpoint() -> TestResult {
        let signer: Signer = Signer::from(MockSigner);
        let err = SignedUrlBuilder::new("b", "o")
            .with_endpoint("invalid-url")
            .sign_with(&signer)
            .await
            .unwrap_err();

        match err {
            SigningError::InvalidEndpoint(_) => {}
            _ => panic!("unexpected error type: {:?}", err),
        }

        Ok(())
    }
}
