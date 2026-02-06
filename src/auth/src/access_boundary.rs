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
use gax::backoff_policy::BackoffPolicy;
use gax::exponential_backoff::ExponentialBackoffBuilder;
use gax::retry_state::RetryState;
use http::Extensions;
use reqwest::Client;
use std::clone::Clone;
use std::fmt::Debug;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};

const REGIONAL_ACCESS_BOUNDARIES_ENV_VAR: &str = "GOOGLE_AUTH_ENABLE_TRUST_BOUNDARIES";
const NO_OP_ENCODED_LOCATIONS: &str = "0x0";

// TTL: 6 hours
const DEFAULT_TTL: Duration = Duration::from_hours(6);
// Refresh interval: 1 hour of TTL
const REFRESH_INTERVAL: Duration = Duration::from_hours(1);
// Period to wait after err: 15 minutes
const COOLDOWN_INTERVAL: Duration = Duration::from_mins(15);
// Max period to wait after err: 6 hours
const MAX_COOLDOWN_INTERVAL: Duration = Duration::from_hours(6);

#[derive(Debug)]
pub(crate) struct AccessBoundary {
    rx_header: watch::Receiver<Option<String>>,
}

impl AccessBoundary {
    pub(crate) fn new<T>(token_provider: T, url: String) -> Self
    where
        T: CachedTokenProvider + 'static,
    {
        let enabled = Self::is_regional_access_boundaries_enabled();
        let (tx_header, rx_header) = watch::channel(None);

        if enabled {
            tokio::spawn(refresh_task(token_provider, url, tx_header));
        }

        Self { rx_header }
    }

    fn is_regional_access_boundaries_enabled() -> bool {
        std::env::var(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR)
            .map(|v| v.to_lowercase())
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
    }

    pub(crate) fn header_value(&self) -> Option<String> {
        // TODO(#4186): handle expiration. Each new entry should have TTL per design doc.
        let val = self.rx_header.borrow().clone();
        if let Some(ref v) = val {
            if v == NO_OP_ENCODED_LOCATIONS {
                return None;
            }
        }
        val
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
    T: CachedTokenProvider,
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

async fn refresh_task<T>(token_provider: T, url: String, tx_header: watch::Sender<Option<String>>)
where
    T: CachedTokenProvider,
{
    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_delay(COOLDOWN_INTERVAL)
        .with_maximum_delay(Duration::from_hours(6))
        .with_scaling(2.0)
        .build()
        .expect("static cooldown settings should be valid");

    let mut state = RetryState::new(true);

    loop {
        match fetch_access_boundary(&token_provider, &url).await {
            Ok(val) => {
                let _ = tx_header.send(val);
                state = RetryState::new(true); // reset backoff on success
                sleep(REFRESH_INTERVAL).await;
            }
            Err(_e) => {
                let delay = backoff.on_failure(&state);
                sleep(delay).await;
                state = state.clone().set_attempt_count(state.attempt_count + 1);
            }
        }
    }
}

pub(crate) fn service_account_lookup_url(email: &str) -> String {
    format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}/allowedLocations",
        email
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
    use tokio::time::Instant;

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
    async fn test_fetch_access_boundary_success() {
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

        let result = fetch_access_boundary(&token_provider, &url).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("0x123".to_string()));
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_empty() {
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

        let result = fetch_access_boundary(&token_provider, &url).await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(result.unwrap(), None);
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

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to fetch access boundary")
        );
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_token_error() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token().returning(|| {
            Err(CredentialsError::from_msg(
                true,
                "invalid creds".to_string(),
            ))
        });

        let token_provider = TokenCache::new(mock);
        let result = fetch_access_boundary(&token_provider, "http://localhost").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid creds"));
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

        assert_eq!(access_boundary.header_value(), None);
    }

    #[test]
    #[parallel]
    fn test_access_boundary_header_value_no_op() {
        let (tx, rx_header) = watch::channel(None);
        let access_boundary = AccessBoundary { rx_header };

        let _ = tx.send(Some("0x123".to_string()));
        assert_eq!(access_boundary.header_value(), Some("0x123".to_string()));

        let _ = tx.send(Some(NO_OP_ENCODED_LOCATIONS.to_string()));
        assert_eq!(access_boundary.header_value(), None);

        let _ = tx.send(None);
        assert_eq!(access_boundary.header_value(), None);
    }

    #[test]
    #[serial]
    fn test_is_regional_access_boundaries_enabled() {
        let cases = [
            (Some("true"), true),
            (Some("TrUe"), true),
            (Some("1"), true),
            (Some("false"), false),
            (Some("0"), false),
            (None, false),
        ];

        for (env_val, expected) in cases {
            let _env = match env_val {
                Some(val) => Some(ScopedEnv::set(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR, val)),
                None => {
                    let _ = ScopedEnv::remove(REGIONAL_ACCESS_BOUNDARIES_ENV_VAR);
                    None
                }
            };
            assert_eq!(
                AccessBoundary::is_regional_access_boundaries_enabled(),
                expected,
                "Failed on case: env_val={:?}, expected={}",
                env_val,
                expected
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_refresh_task_backoff() {
        let now = Instant::now();

        let initial_token = Token {
            token: "valid-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + Duration::from_secs(3600)),
            metadata: None,
        };

        let mut mock_provider = MockTokenProvider::new();
        mock_provider
            .expect_token()
            .returning(move || Ok(initial_token.clone()));

        let cached_provider = TokenCache::new(mock_provider);

        let server = Server::run();

        server.expect(
            Expectation::matching(request::method_path("GET", "/accessBoundary"))
                .times(2)
                .respond_with(status_code(503)),
        );

        server.expect(
            Expectation::matching(request::method_path("GET", "/accessBoundary"))
                .times(1)
                .respond_with(json_encoded(json!({
                    "locations": [],
                    "encodedLocations": "0x123",
                }))),
        );

        let (tx, rx) = watch::channel::<Option<String>>(None);

        tokio::spawn(async move {
            refresh_task(
                cached_provider,
                server.url("/accessBoundary").to_string(),
                tx,
            )
            .await;
        });

        // allow task to start and fail the first request
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        let val = rx.borrow().clone();
        assert!(val.is_none(), "should be None on startup/error");

        // advance 15 minutes, next call fails
        tokio::time::advance(Duration::from_mins(15)).await;
        tokio::task::yield_now().await;

        let val = rx.borrow().clone();
        assert!(val.is_none(), "should still be None after second error");

        // advance 30 minutes, third call succeeds
        tokio::time::advance(Duration::from_mins(30)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }

        let val = rx.borrow().clone();
        assert_eq!(val, Some("0x123".to_string()));
    }
}
