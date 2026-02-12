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
use crate::token::CachedTokenProvider;
use http::Extensions;
use reqwest::Client;
use std::clone::Clone;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, sleep};

const REGIONAL_ACCESS_BOUNDARIES_ENV_VAR: &str = "GOOGLE_AUTH_ENABLE_TRUST_BOUNDARIES";
const NO_OP_ENCODED_LOCATIONS: &str = "0x0";

// TTL: 6 hours
const DEFAULT_TTL: Duration = Duration::from_secs(6 * 60 * 60);
// Refresh slack: an hour before the TTL expires
const REFRESH_SLACK: Duration = Duration::from_secs(60 * 60);
// Period to wait after an error: 15 minutes
const COOLDOWN_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub(crate) struct AccessBoundary {
    /// A channel to keep track of the boundary state.
    /// - `None`: We haven't fetched anything yet (uninitialized).
    /// - `Some(...)`: We successfully talked to the IAM service or have a customer provided override.
    ///   These values come with a TTL so we know how long to keep them around.
    rx_header: watch::Receiver<Option<BoundaryValue>>,
}

#[derive(Debug, Clone)]
struct BoundaryValue {
    /// This is an `Option` because the IAM service can signal that the
    /// given credential has no access boundary. In that case, we save it as `None`
    /// (along with the TTL in `expires_at`) so we don't repeatedly
    /// fetch a non-existent boundary.
    value: Option<String>,
    expires_at: Instant,
}

impl BoundaryValue {
    fn new(value: Option<String>) -> Self {
        Self {
            value,
            expires_at: Instant::now() + DEFAULT_TTL,
        }
    }
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

    #[cfg(test)]
    // only used for testing
    pub(crate) fn new_with_mock_provider<T>(provider: T) -> Self
    where
        T: AccessBoundaryProvider + 'static,
    {
        let (tx_header, rx_header) = watch::channel(None);
        tokio::spawn(refresh_task(Arc::new(provider), tx_header));
        Self { rx_header }
    }

    fn is_enabled() -> bool {
        std::env::var(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR)
            .map(|v| v.to_lowercase())
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
    }

    pub(crate) fn header_value(&self) -> Option<String> {
        let val = self.rx_header.borrow().clone();
        val.filter(|b| b.expires_at >= Instant::now()) // fail open if expired
            .and_then(|b| b.value)
            .filter(|v| v != NO_OP_ENCODED_LOCATIONS)
    }
}

// internal trait for testability and avoid dependency on reqwest
// which causes issues with tokio::time::advance and tokio::task::yield_now
#[async_trait::async_trait]
pub(crate) trait AccessBoundaryProvider: std::fmt::Debug + Send + Sync {
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

async fn refresh_task<T>(provider: Arc<T>, tx_header: watch::Sender<Option<BoundaryValue>>)
where
    T: AccessBoundaryProvider,
{
    loop {
        match provider.fetch_access_boundary().await {
            Ok(val) => {
                let _ = tx_header.send(Some(BoundaryValue::new(val)));
                sleep(DEFAULT_TTL - REFRESH_SLACK).await
            }
            Err(_e) => {
                sleep(COOLDOWN_INTERVAL).await;
            }
        }
    }
}

pub(crate) fn service_account_lookup_url(email: &str) -> String {
    format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{email}/allowedLocations"
    )
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

    #[tokio::test]
    #[parallel]
    async fn test_access_boundary_header_value_no_op() {
        let (tx, rx_header) = watch::channel(None);
        let access_boundary = AccessBoundary { rx_header };

        let _ = tx.send(Some(BoundaryValue::new(Some("0x123".to_string()))));
        assert_eq!(access_boundary.header_value().as_deref(), Some("0x123"));

        let _ = tx.send(Some(BoundaryValue::new(Some(
            NO_OP_ENCODED_LOCATIONS.to_string(),
        ))));
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

        let (tx, rx) = watch::channel::<Option<BoundaryValue>>(None);

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
        let val = val.as_ref().and_then(|v| v.value.as_deref());
        assert_eq!(val, Some("0x123"), "{val:?}");
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn expired_access_boundary_returns_none() {
        let (tx, rx_header) = watch::channel::<Option<BoundaryValue>>(None);
        let access_boundary = AccessBoundary { rx_header };

        let ttl = Duration::from_secs(10);
        let expires_at = Instant::now() + ttl;
        let _ = tx.send(Some(BoundaryValue {
            value: Some("old-value".to_string()),
            expires_at,
        }));

        // value is valid
        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("old-value"), "{val:?}");

        // advance time plus some buffer to expire the value
        tokio::time::advance(ttl + Duration::from_secs(1)).await;

        // value should return None if expired (non-blocking)
        let val = access_boundary.header_value();
        assert!(val.is_none(), "{val:?}");

        // update with new value
        let _ = tx.send(Some(BoundaryValue::new(Some("new-value".to_string()))));

        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("new-value"), "{val:?}");
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn access_boundary_provider_refreshes() {
        let mut mock_provider = MockAccessBoundaryProvider::new();

        mock_provider
            .expect_fetch_access_boundary()
            .times(1)
            .returning(|| Ok(Some("old-value".to_string())));

        mock_provider
            .expect_fetch_access_boundary()
            .times(1)
            .returning(|| Ok(Some("new-value".to_string())));

        let access_boundary = AccessBoundary::new_with_mock_provider(mock_provider);

        // allow task to start and fail the first request
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        // value is valid
        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("old-value"), "{val:?}");

        // advance time beyond the time to refresh
        tokio::time::advance(DEFAULT_TTL).await;
        tokio::task::yield_now().await;

        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("new-value"), "{val:?}");
    }
}
