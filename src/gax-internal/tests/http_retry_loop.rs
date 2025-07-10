// Copyright 2024 Google LLC
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

//! These tests use mocks to verify the gax client uses the retry policy,
//! backoff policy, and retry throttler as expected. They do not test the
//! policy implementations, that is done in the unit tests. Though the may use
//! the policies where mocking would just require a lot of uninteresting code.
//!
//! The tests use an HTTP server that returns a sequence of responses. The
//! sequence is specific to each test, intended to drive the retry loop as
//! needed for that test.

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use axum::extract::State;
    use axum::http::StatusCode;
    use gax::backoff_policy::BackoffPolicy;
    use gax::exponential_backoff::ExponentialBackoffBuilder;
    use gax::options::*;
    use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    use google_cloud_gax_internal::http::ReqwestClient;
    use google_cloud_gax_internal::options::ClientConfig;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::task::JoinHandle;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_no_retry_immediate_success() -> Result<()> {
        let (endpoint, _server) = start(vec![success()]).await?;

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let response = client
            .execute::<serde_json::Value, serde_json::Value>(
                builder,
                Some(body),
                RequestOptions::default(),
            )
            .await;
        let response = response?.into_body();
        assert_eq!(response, json!({"status": "done"}));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_no_retry_immediate_failure() -> Result<()> {
        let (endpoint, _server) = start(vec![permanent()]).await?;

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let response = client
            .execute::<serde_json::Value, serde_json::Value>(
                builder,
                Some(body),
                RequestOptions::default(),
            )
            .await;
        let err = response.unwrap_err();
        assert_eq!(err.http_status_code(), Some(permanent().0.as_u16()));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_retry_success() -> Result<()> {
        // We create a server that will return two transient errors and then succeed.
        let (endpoint, _server) = start(vec![transient(), transient(), success()]).await?;

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_retry_policy(AlwaysRetry.with_attempt_limit(3));
            options.set_backoff_policy(test_backoff());
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response?.into_body();
        assert_eq!(response, json!({"status": "done"}));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_too_many_transients() -> Result<()> {
        // We create a server that will return two transient errors and then succeed.
        let (endpoint, _server) = start(vec![transient(), transient(), transient()]).await?;

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_retry_policy(AlwaysRetry.with_attempt_limit(3));
            options.set_backoff_policy(test_backoff());
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    fn success() -> (StatusCode, String) {
        let response = json!({
            "status": "done"
        });
        (StatusCode::OK, response.to_string())
    }

    fn transient() -> (StatusCode, String) {
        let status = json!({"error": {
            "code": StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            "status": "UNAVAILABLE",
            "message": "try-again",
        }});
        (StatusCode::SERVICE_UNAVAILABLE, status.to_string())
    }

    fn permanent() -> (StatusCode, String) {
        let status = json!({"error": {
            "code": StatusCode::BAD_REQUEST.as_u16(),
            "status": "INVALID_ARGUMENT",
            "message": "uh-oh",
        }});
        (StatusCode::BAD_REQUEST, status.to_string())
    }

    fn test_config() -> ClientConfig {
        ClientConfig {
            cred: auth::credentials::testing::test_credentials().into(),
            ..ClientConfig::default()
        }
    }

    fn test_backoff() -> impl BackoffPolicy {
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(1))
            .with_maximum_delay(Duration::from_millis(1))
            .clamp()
    }

    struct RetrySharedState {
        responses: std::collections::VecDeque<(StatusCode, String)>,
    }

    type RetryState = Arc<Mutex<RetrySharedState>>;

    pub async fn start(responses: Vec<(StatusCode, String)>) -> Result<(String, JoinHandle<()>)> {
        let state = Arc::new(Mutex::new(RetrySharedState {
            responses: responses.into(),
        }));
        let app = axum::Router::new()
            .route("/retry", axum::routing::get(retry))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        Ok((format!("http://{}:{}", addr.ip(), addr.port()), server))
    }

    async fn retry(State(state): State<RetryState>) -> (StatusCode, String) {
        let mut state = state.lock().expect("retry state is poisoned");
        state
            .responses
            .pop_front()
            .unwrap_or_else(|| (StatusCode::BAD_REQUEST, "exhausted retry data".to_string()))
    }
}
