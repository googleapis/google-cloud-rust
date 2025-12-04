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
use std::collections::BTreeMap;
use tokio::time::Duration;

/// A builder for creating signed URLs.
#[derive(Debug)]
#[allow(dead_code)]
pub struct SignedUrlBuilder {
    scope: SigningScope,
    method: http::Method,
    expiration: std::time::Duration,
    headers: BTreeMap<String, String>,
    query_parameters: BTreeMap<String, String>,
    endpoint: Option<String>,
    universe_domain: String,
    client_email: Option<String>,
    url_style: UrlStyle,
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub enum UrlStyle {
    #[default]
    PathStyle,
    BucketBoundHostname,
    VirtualHostedStyle,
}

#[derive(Debug)]
#[allow(dead_code)]
enum SigningScope {
    Bucket(String),
    Object(String, String),
}

impl SignedUrlBuilder {
    fn new(scope: SigningScope) -> Self {
        Self {
            scope,
            method: http::Method::GET,
            expiration: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            headers: BTreeMap::new(),
            query_parameters: BTreeMap::new(),
            endpoint: None,
            universe_domain: "googleapis.com".to_string(),
            client_email: None,
            url_style: UrlStyle::PathStyle,
        }
    }

    pub fn for_object<B, O>(bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        Self::new(SigningScope::Object(bucket.into(), object.into()))
    }

    pub fn for_bucket<B>(bucket: B) -> Self
    where
        B: Into<String>,
    {
        Self::new(SigningScope::Bucket(bucket.into()))
    }

    /// Sets the HTTP method for the signed URL. Default is "GET".
    pub fn with_method(mut self, method: http::Method) -> Self {
        self.method = method;
        self
    }

    /// Sets the expiration time for the signed URL. Default is 7 days.
    pub fn with_expiration(mut self, expiration: Duration) -> Self {
        self.expiration = expiration;
        self
    }

    /// Sets the URL style for the signed URL. Default is `UrlStyle::PathStyle`.
    pub fn with_url_style(mut self, url_style: UrlStyle) -> Self {
        self.url_style = url_style;
        self
    }

    /// Adds a header to the signed URL.
    /// Note: These headers must be present in the request when using the signed URL.
    pub fn with_header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Adds a query parameter to the signed URL.
    pub fn with_query_param<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.query_parameters.insert(key.into(), value.into());
        self
    }

    /// Sets the endpoint for the signed URL. Default is "https://storage.googleapis.com".
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the universe domain for the signed URL. Default is "googleapis.com".
    pub fn with_universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = universe_domain.into();
        self
    }

    /// Sets the client email for the signed URL.
    /// If not set, the email will be fetched from the signer.
    pub fn with_client_email<S: Into<String>>(mut self, client_email: S) -> Self {
        self.client_email = Some(client_email.into());
        self
    }

    /// Generates the signed URL using the provided signer.
    pub async fn sign_with(self, _signer: &Signer) -> std::result::Result<String, SigningError> {
        // TODO(#3645): implement gcs logic for signed url generation.
        Err(SigningError::Signing(auth::signer::SigningError::from_msg(
            "not implemented",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::signer::{Signer, SigningProvider};
    use tokio::time::Duration;

    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Signer {}

        impl SigningProvider for Signer {
            async fn client_email(&self) -> auth::signer::Result<String>;
            async fn sign(&self, _content: &[u8]) -> auth::signer::Result<String>;
        }
    }

    #[tokio::test]
    async fn test_signed_url_builder() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_client_email()
            .return_once(|| Ok("test@example.com".to_string()));
        mock.expect_sign()
            .return_once(|_content| Ok("test-signature".to_string()));

        let signer = Signer::from(mock);
        let err = SignedUrlBuilder::for_object("test-bucket", "test-object")
            .with_method(http::Method::PUT)
            .with_expiration(Duration::from_secs(3600))
            .with_header("x-goog-meta-test", "value")
            .with_query_param("test", "value")
            .with_endpoint("https://storage.googleapis.com")
            .with_universe_domain("googleapis.com")
            .with_client_email("test@example.com")
            .with_url_style(UrlStyle::PathStyle)
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(matches!(err, SigningError::Signing(_)));

        Ok(())
    }
}
