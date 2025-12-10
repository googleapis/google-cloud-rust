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

use crate::credentials::Credentials;
use crate::credentials::mds::{MDS_DEFAULT_URI, METADATA_FLAVOR, METADATA_FLAVOR_VALUE};
use crate::signer::{Result, SigningError, dynamic::SigningProvider};
use http::HeaderValue;
use reqwest::Client;
use tokio::sync::SetOnce;

// Implements Signer for MDS that extends the existing IamSigner by fetching
// email via MDS email endpoint.
#[derive(Clone, Debug)]
pub(crate) struct MDSSigner {
    endpoint: String,
    iam_endpoint_override: Option<String>,
    client_email: SetOnce<String>,
    inner: Credentials,
}

impl MDSSigner {
    pub(crate) fn new(endpoint: String, inner: Credentials) -> Self {
        Self {
            endpoint,
            client_email: SetOnce::new(),
            inner,
            iam_endpoint_override: None,
        }
    }

    // only used for testing
    pub(crate) fn with_iam_endpoint_override(mut self, endpoint: &str) -> Self {
        self.iam_endpoint_override = Some(endpoint.to_string());
        self
    }
}

#[async_trait::async_trait]
impl SigningProvider for MDSSigner {
    async fn client_email(&self) -> Result<String> {
        if self.client_email.get().is_none() {
            let email = self.fetch_client_email().await?;
            // Ignore error if we can't set the client email.
            // Might be due to multiple tasks trying to set value
            let _ = self.client_email.set(email.clone());
            return Ok(email);
        }

        Ok(self.client_email.get().unwrap().to_string())
    }

    async fn sign(&self, content: &[u8]) -> Result<bytes::Bytes> {
        let client_email = self.client_email().await?;

        let signer = crate::signer::iam::IamSigner::new(
            client_email,
            self.inner.clone(),
            self.iam_endpoint_override.clone(),
        );

        signer.sign(content).await
    }
}

impl MDSSigner {
    async fn fetch_client_email(&self) -> Result<String> {
        let client = Client::new();

        let request = client
            .get(format!("{}{}/email", self.endpoint, MDS_DEFAULT_URI))
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            );

        let response = request.send().await.map_err(SigningError::transport)?;
        let email = response.text().await.map_err(SigningError::transport)?;

        Ok(email)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{CacheableResource, Credentials, CredentialsProvider, EntityTag};
    use crate::errors::CredentialsError;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use http::header::{HeaderName, HeaderValue};
    use http::{Extensions, HeaderMap};
    use httptest::matchers::{all_of, contains, request};
    use httptest::responders::{json_encoded, status_code};
    use httptest::{Expectation, Server};
    use serde_json::json;

    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> std::result::Result<CacheableResource<HeaderMap>, CredentialsError>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test]
    async fn test_fetch_client_email_and_cache() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path(format!("{MDS_DEFAULT_URI}/email")),])
                .times(1)
                .respond_with(status_code(200).body("test-client-email")),
        );
        let mock = MockCredentials::new();
        let creds = Credentials::from(mock);
        let signer = MDSSigner::new(format!("http://{}", server.addr()), creds);

        let client_email = signer.client_email().await?;
        assert_eq!(client_email, "test-client-email");

        let client_email = signer.client_email().await?;
        assert_eq!(client_email, "test-client-email");

        Ok(())
    }

    #[tokio::test]
    async fn test_sign() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path(format!("{MDS_DEFAULT_URI}/email")),])
                .times(1)
                .respond_with(status_code(200).body("test-client-email")),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-client-email:signBlob"
                ),
                request::headers(contains(("authorization", "Bearer test-value"))),
            ])
            .respond_with(json_encoded(json!({
                "signedBlob": BASE64_STANDARD.encode("signed_blob"),
            }))),
        );
        let mut mock = MockCredentials::new();
        mock.expect_headers().return_once(|_extensions| {
            let headers = HeaderMap::from_iter([(
                HeaderName::from_static("authorization"),
                HeaderValue::from_static("Bearer test-value"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });

        let creds = Credentials::from(mock);
        let endpoint = server.url("").to_string().trim_end_matches('/').to_string();
        let mut signer = MDSSigner::new(endpoint.clone(), creds);
        signer.iam_endpoint_override = Some(endpoint);

        let client_email = signer.client_email().await?;
        assert_eq!(client_email, "test-client-email");

        let signature = signer.sign(b"test").await?;
        assert_eq!(signature.as_ref(), b"signed_blob");

        Ok(())
    }
}
