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
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy, RetryPolicyExt};
use google_cloud_gax::retry_throttler::{
    AdaptiveThrottler, RetryThrottlerArg, SharedRetryThrottler,
};
use http::{Extensions, HeaderMap};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

// Implements Signer using [IAM signBlob API] and reusing using existing [Credentials] to
// authenticate to it.
//
// [IAM signBlob API]: https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/signBlob
#[derive(Debug)]
pub(crate) struct IamSigner {
    client_email: String,
    inner: Credentials,
    iam_endpoint_override: Option<String>,
    client: Client,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct SignBlobRequest {
    payload: String,
}

#[derive(Debug, serde::Deserialize)]
struct SignBlobResponse {
    #[serde(rename = "signedBlob")]
    signed_blob: String,
}

impl IamSigner {
    pub(crate) fn new(
        client_email: String,
        inner: Credentials,
        iam_endpoint_override: Option<String>,
    ) -> Self {
        let retry_policy = Aip194Strict.with_time_limit(Duration::from_secs(60));
        let backoff_policy = ExponentialBackoff::default();
        Self {
            client_email,
            inner,
            iam_endpoint_override,
            client: Client::new(),
            retry_policy: Arc::new(retry_policy),
            backoff_policy: Arc::new(backoff_policy),
        }
    }

    async fn sign_blob_url(&self) -> String {
        let endpoint = match self.iam_endpoint_override.as_ref() {
            Some(endpoint) => endpoint.clone(),
            None => {
                let universe_domain = crate::universe_domain::resolve(&self.inner).await;
                format!("https://iamcredentials.{universe_domain}")
            }
        };
        format!(
            "{}/v1/projects/-/serviceAccounts/{}:signBlob",
            endpoint, self.client_email
        )
    }
}

#[async_trait::async_trait]
impl SigningProvider for IamSigner {
    async fn client_email(&self) -> Result<String> {
        Ok(self.client_email.clone())
    }

    async fn sign(&self, content: &[u8]) -> Result<bytes::Bytes> {
        use base64::{Engine, prelude::BASE64_STANDARD};

        let payload = BASE64_STANDARD.encode(content);
        let body = SignBlobRequest { payload };

        let url = self.sign_blob_url().await;
        let response = sign_blob_call_with_retry(
            self.inner.clone(),
            self.client.clone(),
            url,
            body,
            self.retry_policy.clone(),
            self.backoff_policy.clone(),
        )
        .await?;

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

        Ok(bytes::Bytes::from(signature))
    }
}

async fn sign_blob_call_with_retry(
    credentials: Credentials,
    client: Client,
    url: String,
    body: SignBlobRequest,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
) -> Result<reqwest::Response> {
    let sleep = async |d| tokio::time::sleep(d).await;

    let retry_throttler: RetryThrottlerArg = AdaptiveThrottler::default().into();
    let retry_throttler: SharedRetryThrottler = retry_throttler.into();

    retry_loop(
        async move |_| {
            let source_headers = credentials
                .headers(Extensions::new())
                .await
                .map_err(google_cloud_gax::error::Error::authentication)?;

            sign_blob_call(&client, &url, source_headers, body.clone()).await
        },
        sleep,
        true, // signBlob is idempotent
        retry_throttler,
        retry_policy,
        backoff_policy,
    )
    .await
    .map_err(SigningError::transport)
}

async fn sign_blob_call(
    client: &Client,
    url: &str,
    source_headers: CacheableResource<HeaderMap>,
    body: SignBlobRequest,
) -> google_cloud_gax::Result<reqwest::Response> {
    let source_headers = match source_headers {
        CacheableResource::New { data, .. } => data,
        CacheableResource::NotModified => {
            unreachable!("requested source credentials without a caching etag")
        }
    };

    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .headers(source_headers.clone())
        .json(&body)
        .send()
        .await
        .map_err(google_cloud_gax::error::Error::io)?;

    let status = response.status();
    if !status.is_success() {
        let err_headers = response.headers().clone();
        let err_payload = response
            .bytes()
            .await
            .map_err(|e| google_cloud_gax::error::Error::transport(err_headers.clone(), e))?;
        return Err(google_cloud_gax::error::Error::http(
            status.as_u16(),
            err_headers,
            err_payload,
        ));
    }

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::MockCredentials;
    use crate::credentials::{Credentials, EntityTag};
    use base64::{Engine, prelude::BASE64_STANDARD};
    use http::HeaderMap;
    use http::header::{HeaderName, HeaderValue};
    use httptest::cycle;
    use httptest::matchers::{all_of, contains, eq, json_decoded, request};
    use httptest::responders::{json_encoded, status_code};
    use httptest::{Expectation, Server};
    use serde_json::json;
    use test_case::test_case;
    use tokio::time::Duration;

    type TestResult = anyhow::Result<()>;

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

        let signer = IamSigner::new("test@example.com".to_string(), creds, Some(endpoint));
        let signature = signer.sign(b"test").await.unwrap();

        assert_eq!(signature.as_ref(), b"signed_blob");

        Ok(())
    }

    #[tokio::test]
    async fn test_iam_client_email() -> TestResult {
        let mock = MockCredentials::new();
        let creds = Credentials::from(mock);

        let signer = IamSigner::new("test@example.com".to_string(), creds, None);
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

        let signer = IamSigner::new("test@example.com".to_string(), creds, Some(endpoint));
        let err = signer.sign(b"test").await.unwrap_err();

        assert!(err.is_transport());

        Ok(())
    }

    #[tokio::test]
    async fn test_iam_sign_retry() -> TestResult {
        let server = Server::run();
        let signed_blob = BASE64_STANDARD.encode("signed_blob");
        let invalid_res = http::Response::builder()
            .version(http::Version::HTTP_3) // unsupported version
            .status(204)
            .body(Vec::new())
            .unwrap();
        server.expect(
            Expectation::matching(all_of![request::method_path(
                "POST",
                "/v1/projects/-/serviceAccounts/test@example.com:signBlob"
            ),])
            .times(3)
            .respond_with(cycle![
                invalid_res, // forces i/o error
                status_code(503).body("try-again"),
                json_encoded(json!({
                    "signedBlob": signed_blob,
                }))
            ]),
        );
        let endpoint = server.url("").to_string().trim_end_matches('/').to_string();

        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_extensions| {
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: HeaderMap::new(),
            })
        });
        let creds = Credentials::from(mock);

        let mut signer = IamSigner::new("test@example.com".to_string(), creds, Some(endpoint));
        signer.backoff_policy = Arc::new(test_backoff_policy());
        let signature = signer.sign(b"test").await.unwrap();

        assert_eq!(signature.as_ref(), b"signed_blob");

        Ok(())
    }

    #[test_case(None ; "no custom universe domain")]
    #[test_case(Some("my-custom-universe.com".to_string()) ; "with custom universe domain")]
    #[tokio::test]
    async fn test_sign_blob_url_with_override(universe_domain: Option<String>) -> TestResult {
        let mut mock = MockCredentials::new();
        mock.expect_universe_domain()
            .returning(move || universe_domain.clone());
        let creds = Credentials::from(mock);
        let signer = IamSigner::new(
            "test@example.com".to_string(),
            creds,
            Some("http://example.com".to_string()),
        );
        let url = signer.sign_blob_url().await;
        assert_eq!(
            url,
            "http://example.com/v1/projects/-/serviceAccounts/test@example.com:signBlob"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_sign_blob_url_default_universe() -> TestResult {
        let mut mock = MockCredentials::new();
        mock.expect_universe_domain().returning(|| None);
        let creds = Credentials::from(mock);
        let signer = IamSigner::new("test@example.com".to_string(), creds, None);
        let url = signer.sign_blob_url().await;
        assert_eq!(
            url,
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test@example.com:signBlob"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_sign_blob_url_custom_universe() -> TestResult {
        let mut mock = MockCredentials::new();
        mock.expect_universe_domain()
            .returning(|| Some("my-custom-universe.com".to_string()));
        let creds = Credentials::from(mock);
        let signer = IamSigner::new("test@example.com".to_string(), creds, None);
        let url = signer.sign_blob_url().await;
        assert_eq!(
            url,
            "https://iamcredentials.my-custom-universe.com/v1/projects/-/serviceAccounts/test@example.com:signBlob"
        );
        Ok(())
    }

    fn test_backoff_policy() -> ExponentialBackoff {
        use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(1))
            .with_maximum_delay(Duration::from_millis(1))
            .build()
            .expect("hard-coded policy succeeds")
    }
}
