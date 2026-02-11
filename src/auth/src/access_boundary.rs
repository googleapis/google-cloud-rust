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

use crate::credentials::CacheableResource;
use crate::errors::CredentialsError;
use crate::headers_util::AuthHeadersBuilder;
use crate::mds::client::Client as MDSClient;
use crate::token::CachedTokenProvider;
use http::Extensions;
use reqwest::Client;
use std::clone::Clone;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};

const REGIONAL_ACCESS_BOUNDARIES_ENV_VAR: &str = "GOOGLE_AUTH_ENABLE_TRUST_BOUNDARIES";
const NO_OP_ENCODED_LOCATIONS: &str = "0x0";

// TTL: 6 hours
const DEFAULT_TTL: Duration = Duration::from_secs(6 * 60 * 60);
// Refresh interval: every hour
const REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);
// Period to wait after an error: 15 minutes
const COOLDOWN_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub(crate) struct AccessBoundary {
    rx_header: watch::Receiver<Option<String>>,
}

impl AccessBoundary {
    pub(crate) fn new<T>(token_provider: T, url: String) -> Self
    where
        T: CachedTokenProvider + 'static,
    {
        let (tx_header, rx_header) = watch::channel(None);

        if Self::is_enabled() {
            let provider = IAMAccessBoundaryProvider {
                token_provider,
                url,
            };
            tokio::spawn(refresh_task(Arc::new(provider), tx_header));
        }

        Self { rx_header }
    }

    pub(crate) fn new_for_mds<T>(token_provider: T, mds_client: MDSClient) -> Self
    where
        T: CachedTokenProvider + 'static,
    {
        let (tx_header, rx_header) = watch::channel(None);

        if Self::is_enabled() {
            tokio::spawn(refresh_task_mds(token_provider, mds_client, tx_header));
        }

        Self { rx_header }
    }

    #[allow(dead_code)]
    pub(crate) fn new_with_override(val: String) -> Self {
        let (_tx, rx_header) = watch::channel(Some(val));
        Self { rx_header }
    }

    fn is_enabled() -> bool {
        std::env::var(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR)
            .map(|v| v.to_lowercase())
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
    }

    pub(crate) fn header_value(&self) -> Option<String> {
        // TODO(#4186): handle expiration. Each new entry should have TTL per design doc.
        let val = self.rx_header.borrow().clone();
        if val.as_ref().is_some_and(|v| v == NO_OP_ENCODED_LOCATIONS) {
            return None;
        }
        val
    }
}

// internal trait for testability and avoid dependency on reqwest
// which causes issues with tokio::time::advance and tokio::task::yield_now
#[async_trait::async_trait]
trait AccessBoundaryProvider: std::fmt::Debug + Send + Sync {
    async fn fetch_access_boundary(&self) -> Result<Option<String>, CredentialsError>;
}

// default implementation that uses IAM Access Boundaries API
#[derive(Debug)]
struct IAMAccessBoundaryProvider<T>
where
    T: CachedTokenProvider + 'static,
{
    token_provider: T,
    url: String,
}

#[async_trait::async_trait]
impl<T> AccessBoundaryProvider for IAMAccessBoundaryProvider<T>
where
    T: CachedTokenProvider + 'static,
{
    async fn fetch_access_boundary(&self) -> Result<Option<String>, CredentialsError> {
        fetch_access_boundary(&self.token_provider, &self.url).await
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AllowedLocationsResponse {
    #[allow(dead_code)]
    locations: Vec<String>,
    #[serde(rename = "encodedLocations")]
    encoded_locations: String,
}

async fn fetch_access_boundary<T>(
    token_provider: &T,
    url: &str,
) -> Result<Option<String>, CredentialsError>
where
    T: CachedTokenProvider + 'static,
{
    let token = token_provider.token(Extensions::new()).await?;
    let headers = AuthHeadersBuilder::new(&token).build()?;
    let headers = match headers {
        CacheableResource::New { data, .. } => data,
        CacheableResource::NotModified => {
            unreachable!("requested access boundary without a caching etag")
        }
    };

    let client = Client::new();

    // TODO(#4186): add retries
    let resp = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(|e| CredentialsError::from_msg(true, e.to_string()))?;

    if !resp.status().is_success() {
        return Err(CredentialsError::from_msg(
            true,
            format!("Failed to fetch access boundary: {}", resp.status()),
        ));
    }

    let response: AllowedLocationsResponse = resp
        .json()
        .await
        .map_err(|e| CredentialsError::from_msg(true, e.to_string()))?;

    if !response.encoded_locations.is_empty() {
        return Ok(Some(response.encoded_locations));
    }

    Ok(None)
}

async fn refresh_task<T>(provider: Arc<T>, tx_header: watch::Sender<Option<String>>)
where
    T: AccessBoundaryProvider,
{
    loop {
        fetch_and_update(provider.as_ref(), &tx_header).await;
    }
}

async fn refresh_task_mds<T>(
    token_provider: T,
    mds_client: MDSClient,
    tx_header: watch::Sender<Option<String>>,
) where
    T: CachedTokenProvider + 'static,
{
    let mut provider = IAMAccessBoundaryProvider {
        token_provider,
        url: String::new(),
    };

    loop {
        if provider.url.is_empty() {
            let res = mds_client.email().await;
            match res {
                Ok(email) => {
                    provider.url = service_account_lookup_url(&email);
                }
                Err(_e) => {
                    sleep(COOLDOWN_INTERVAL).await;
                    continue;
                }
            }
        }

        fetch_and_update(&provider, &tx_header).await;
    }
}

async fn fetch_and_update<T>(provider: &T, tx_header: &watch::Sender<Option<String>>)
where
    T: AccessBoundaryProvider,
{
    match provider.fetch_access_boundary().await {
        Ok(val) => {
            let _ = tx_header.send(val);
            sleep(REFRESH_INTERVAL).await;
        }
        Err(_e) => {
            sleep(COOLDOWN_INTERVAL).await;
        }
    }
}

pub(crate) fn service_account_lookup_url(email: &str) -> String {
    format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{email}/allowedLocations"
    )
}

pub(crate) fn external_account_lookup_url(audience: &str) -> Option<String> {
    let path = audience
        .trim_start_matches("//iam.googleapis.com/")
        .trim_start_matches("https://iam.googleapis.com/")
        .trim_start_matches('/');

    let parts: Vec<&str> = path.split('/').collect();

    // Workload: projects/{project}/locations/global/workloadIdentityPools/{pool}/providers/{provider} (6 parts)
    if parts.len() >= 6
        && parts[0] == "projects"
        && parts[2] == "locations"
        && parts[4] == "workloadIdentityPools"
    {
        let project = parts[1];
        let pool = parts[5];
        return Some(format!(
            "https://iamcredentials.googleapis.com/v1/projects/{}/locations/global/workloadIdentityPools/{}/allowedLocations",
            project, pool
        ));
    }

    // Workforce: locations/global/workforcePools/{pool}/providers/{provider} (4 parts)
    if parts.len() >= 4 && parts[0] == "locations" && parts[2] == "workforcePools" {
        let pool = parts[3];
        return Some(format!(
            "https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/{}/allowedLocations",
            pool
        ));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Token;
    use crate::token::tests::MockTokenProvider;
    use crate::token_cache::TokenCache;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use scoped_env::ScopedEnv;
    use serde_json::json;
    use serial_test::{parallel, serial};
    use std::sync::Arc;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        pub AccessBoundaryProvider { }

        #[async_trait::async_trait]
        impl AccessBoundaryProvider for AccessBoundaryProvider {
            async fn fetch_access_boundary(&self) -> Result<Option<String>, CredentialsError>;
        }
    }

    #[test]
    #[parallel]
    fn test_service_account_url() {
        assert_eq!(
            service_account_lookup_url("sa@project.iam.gserviceaccount.com"),
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@project.iam.gserviceaccount.com/allowedLocations"
        );
    }

    #[test]
    #[parallel]
    fn test_external_account_url_workload() {
        let aud = "//iam.googleapis.com/projects/12345/locations/global/workloadIdentityPools/my-pool/providers/my-provider";
        assert_eq!(
            external_account_lookup_url(aud).unwrap(),
            "https://iamcredentials.googleapis.com/v1/projects/12345/locations/global/workloadIdentityPools/my-pool/allowedLocations"
        );
    }

    #[test]
    #[parallel]
    fn test_external_account_url_workforce() {
        let aud =
            "//iam.googleapis.com/locations/global/workforcePools/my-pool/providers/my-provider";
        assert_eq!(
            external_account_lookup_url(aud).unwrap(),
            "https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/my-pool/allowedLocations"
        );
    }

    #[test]
    #[parallel]
    fn test_external_account_url_invalid() {
        assert!(external_account_lookup_url("invalid").is_none());
        assert!(
            external_account_lookup_url("//iam.googleapis.com/projects/123/locations/global/wrong")
                .is_none()
        );
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_success() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/allowedLocations")).respond_with(
                json_encoded(json!(
                    {
                        "encodedLocations": "0x123",
                        "locations": ["us-east1"]
                    }
                )),
            ),
        );

        let mut mock = MockTokenProvider::new();
        mock.expect_token().returning(|| {
            Ok(Token {
                token: "test-token".into(),
                token_type: "Bearer".into(),
                expires_at: None,
                metadata: None,
            })
        });

        let token_provider = TokenCache::new(mock);
        let url = server.url("/allowedLocations").to_string();

        let result = fetch_access_boundary(&token_provider, &url).await?;
        assert_eq!(result.as_deref(), Some("0x123"), "{result:?}");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_empty() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/allowedLocations")).respond_with(
                json_encoded(json!({
                    "encodedLocations": "",
                    "locations": []
                })),
            ),
        );

        let mut mock = MockTokenProvider::new();
        mock.expect_token().returning(|| {
            Ok(Token {
                token: "test-token".into(),
                token_type: "Bearer".into(),
                expires_at: None,
                metadata: None,
            })
        });

        let token_provider = TokenCache::new(mock);
        let url = server.url("/allowedLocations").to_string();

        let val = fetch_access_boundary(&token_provider, &url).await?;
        assert!(val.is_none(), "{val:?}");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_error() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/allowedLocations"))
                .respond_with(status_code(503)),
        );

        let mut mock = MockTokenProvider::new();
        mock.expect_token().returning(|| {
            Ok(Token {
                token: "test-token".into(),
                token_type: "Bearer".into(),
                expires_at: None,
                metadata: None,
            })
        });

        let token_provider = TokenCache::new(mock);
        let url = server.url("/allowedLocations").to_string();

        let result = fetch_access_boundary(&token_provider, &url).await;
        let err = result.unwrap_err();
        assert!(err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_token_error() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token().returning(|| {
            Err(CredentialsError::from_msg(
                false,
                "invalid creds".to_string(),
            ))
        });

        let token_provider = TokenCache::new(mock);
        let result = fetch_access_boundary(&token_provider, "http://localhost").await;
        let err = result.unwrap_err();
        assert!(!err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    #[serial]
    async fn test_access_boundary_new_disabled() {
        let _env = ScopedEnv::remove(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR);

        let mut mock = MockTokenProvider::new();
        mock.expect_token().returning(|| {
            Ok(Token {
                token: "test-token".into(),
                token_type: "Bearer".into(),
                expires_at: None,
                metadata: None,
            })
        });

        let token_provider = TokenCache::new(mock);
        let access_boundary = AccessBoundary::new(token_provider, "http://localhost".to_string());

        let val = access_boundary.header_value();
        assert!(val.is_none(), "{val:?}");
    }

    #[test]
    #[parallel]
    fn test_access_boundary_header_value_no_op() {
        let (tx, rx_header) = watch::channel(None);
        let access_boundary = AccessBoundary { rx_header };

        let _ = tx.send(Some("0x123".to_string()));
        assert_eq!(access_boundary.header_value().as_deref(), Some("0x123"));

        let _ = tx.send(Some(NO_OP_ENCODED_LOCATIONS.to_string()));
        let val = access_boundary.header_value();
        assert!(val.is_none(), "{val:?}");

        let _ = tx.send(None);
        let val = access_boundary.header_value();
        assert!(val.is_none(), "{val:?}");
    }

    #[test_case(Some("true"), true; "with_true")]
    #[test_case(Some("TRUE"), true; "with_true_uppercase")]
    #[test_case(Some("1"), true; "with_one")]
    #[test_case(Some("false"), false; "with_false")]
    #[test_case(Some("0"), false; "with_zero")]
    #[test_case(None, false; "with_none")]
    #[serial]
    #[tokio::test]
    async fn test_is_regional_access_boundaries_enabled(
        env_val: Option<&str>,
        expected: bool,
    ) -> TestResult {
        let _env = match env_val {
            Some(val) => ScopedEnv::set(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR, val),
            None => ScopedEnv::remove(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR),
        };
        assert_eq!(
            AccessBoundary::is_enabled(),
            expected,
            "Failed on case: env_val={:?}, expected={}",
            env_val,
            expected
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn test_refresh_task_backoff() {
        let mut mock_provider = MockAccessBoundaryProvider::new();
        mock_provider
            .expect_fetch_access_boundary()
            .times(2)
            .returning(|| Err(CredentialsError::from_msg(false, "test error")));

        mock_provider
            .expect_fetch_access_boundary()
            .times(1)
            .return_once(|| Ok(Some("0x123".to_string())));

        let (tx, rx) = watch::channel::<Option<String>>(None);

        tokio::spawn(async move {
            refresh_task(Arc::new(mock_provider), tx).await;
        });

        // allow task to start and fail the first request
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        let val = rx.borrow().clone();
        assert!(val.is_none(), "should be None on startup/error: {val:?}");

        // advance 15 minutes, next call fails
        tokio::time::advance(COOLDOWN_INTERVAL).await;
        tokio::task::yield_now().await;

        let val = rx.borrow().clone();
        assert!(
            val.is_none(),
            "should still be None after second error: {val:?}"
        );

        // advance 15 minutes, third call succeeds
        tokio::time::advance(COOLDOWN_INTERVAL).await;
        tokio::task::yield_now().await;

        let val = rx.borrow().clone();
        assert_eq!(val.as_deref(), Some("0x123"));
    }
}
