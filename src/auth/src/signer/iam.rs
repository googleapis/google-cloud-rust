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

use crate::credentials::{CacheableResource, Credentials};
use crate::signer::{Result, SigningError, dynamic::SigningProvider};
use http::Extensions;
use reqwest::Client;

// Implements Signer using IAM signBlob API and reusing using existing [Credentials] to
// authenticate to it.
#[derive(Clone, Debug)]
pub(crate) struct IamSigner {
    client_email: String,
    inner: Credentials,
    endpoint: String,
    client: Client,
}

#[derive(Debug, serde::Serialize)]
struct SignBlobRequest {
    payload: String,
}

#[derive(Debug, serde::Deserialize)]
struct SignBlobResponse {
    #[serde(rename = "signedBlob")]
    signed_blob: String,
}

impl IamSigner {
    pub(crate) fn new(client_email: String, inner: Credentials) -> Self {
        Self {
            client_email,
            inner,
            endpoint: "https://iamcredentials.googleapis.com".to_string(),
            client: Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl SigningProvider for IamSigner {
    async fn client_email(&self) -> Result<String> {
        Ok(self.client_email.clone())
    }

    async fn sign(&self, content: &[u8]) -> Result<String> {
        use base64::{Engine, prelude::BASE64_STANDARD};

        let source_headers = self
            .inner
            .headers(Extensions::new())
            .await
            .map_err(SigningError::transport)?;
        let source_headers = match source_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => {
                unreachable!("requested source credentials without a caching etag")
            }
        };

        let client_email = self.client_email.clone();
        let url = format!(
            "{}/v1/projects/-/serviceAccounts/{}:signBlob",
            self.endpoint, client_email
        );

        let payload = BASE64_STANDARD.encode(content);
        let body = SignBlobRequest { payload };

        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .headers(source_headers)
            .json(&body)
            .send()
            .await
            .map_err(SigningError::transport)?;

        if !response.status().is_success() {
            let err_text = response.text().await.map_err(SigningError::transport)?;
            return Err(SigningError::transport(format!("err status: {err_text:?}")));
        }

        let res = response
            .json::<SignBlobResponse>()
            .await
            .map_err(SigningError::transport)?;

        let signature = BASE64_STANDARD
            .decode(res.signed_blob)
            .map_err(SigningError::transport)?;

        let signature = hex::encode(signature);

        Ok(signature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{Credentials, CredentialsProvider, EntityTag};
    use crate::errors::CredentialsError;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use http::header::{HeaderName, HeaderValue};
    use http::{Extensions, HeaderMap};
    use httptest::matchers::{all_of, contains, eq, json_decoded, request};
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
    async fn test_iam_sign() -> TestResult {
        let server = Server::run();
        let payload = BASE64_STANDARD.encode("test");
        let signed_blob = BASE64_STANDARD.encode("signed_blob");
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test@example.com:signBlob"
                ),
                request::headers(contains(("authorization", "Bearer test-value"))),
                request::body(json_decoded(eq(json!({
                    "payload": payload,
                }))))
            ])
            .respond_with(json_encoded(json!({
                "signedBlob": signed_blob,
            }))),
        );
        let endpoint = server.url("").to_string().trim_end_matches('/').to_string();

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

        let mut signer = IamSigner::new("test@example.com".to_string(), creds);
        signer.endpoint = endpoint;
        let signature = signer.sign(b"test").await.unwrap();

        assert_eq!(signature, hex::encode("signed_blob"));

        Ok(())
    }

    #[tokio::test]
    async fn test_iam_client_email() -> TestResult {
        let mock = MockCredentials::new();
        let creds = Credentials::from(mock);

        let signer = IamSigner::new("test@example.com".to_string(), creds);
        let client_email = signer.client_email().await.unwrap();
        assert_eq!(client_email, "test@example.com");

        Ok(())
    }

    #[tokio::test]
    async fn test_iam_sign_api_error() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::method_path(
                "POST",
                "/v1/projects/-/serviceAccounts/test@example.com:signBlob"
            ),])
            .respond_with(status_code(500)),
        );
        let endpoint = server.url("").to_string().trim_end_matches('/').to_string();

        let mut mock = MockCredentials::new();
        mock.expect_headers().return_once(|_extensions| {
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: HeaderMap::new(),
            })
        });
        let creds = Credentials::from(mock);

        let mut signer = IamSigner::new("test@example.com".to_string(), creds);
        signer.endpoint = endpoint;
        let err = signer.sign(b"test").await.unwrap_err();

        assert!(err.is_transport());

        Ok(())
    }
}
