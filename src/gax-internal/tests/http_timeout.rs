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

#[cfg(all(test, feature = "_internal-http-client"))]
mod test {
    use gax::options::*;
    use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    use gax::retry_throttler::{CircuitBreaker, RetryThrottlerArg};
    use google_cloud_gax_internal::http::ReqwestClient;
    use google_cloud_gax_internal::options::ClientConfig;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[tokio::test(start_paused = true)]
    async fn test_no_timeout() -> Result<()> {
        let (endpoint, server) = echo_server::start().await?;
        let config = test_config();
        let client = ReqwestClient::new(config, &endpoint).await?;

        let delay = Duration::from_millis(200);
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let builder = client
            .builder(reqwest::Method::GET, "/echo".into())
            .query(&[("delay_ms", format!("{}", delay.as_millis()))]);
        let response = client.execute::<serde_json::Value, serde_json::Value>(
            builder,
            Some(json!({})),
            RequestOptions::default(),
        );

        tokio::pin!(server);
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => { },
                r = &mut response => {
                    let response = r?.into_body();
                    assert_eq!(
                        get_query_value(&response, "delay_ms"),
                        Some("200".to_string())
                    );
                    break;
                },
                _ = interval.tick() => { },
            }
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout_does_not_expire() -> Result<()> {
        let (endpoint, server) = echo_server::start().await?;
        let config = test_config();
        let client = ReqwestClient::new(config, &endpoint).await?;

        let delay = Duration::from_millis(200);
        let timeout = Duration::from_millis(2000);
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let builder = client
            .builder(reqwest::Method::GET, "/echo".into())
            .query(&[("delay_ms", format!("{}", delay.as_millis()))]);
        let response = client.execute::<serde_json::Value, serde_json::Value>(
            builder,
            Some(json!({})),
            test_options(&timeout),
        );

        tokio::pin!(server);
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => {  },
                r = &mut response => {
                    let response = r?.into_body();
                    assert_eq!(
                        get_query_value(&response, "delay_ms"),
                        Some("200".to_string())
                    );
                    break;
                },
                _ = interval.tick() => { },
            }
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout_expires() -> Result<()> {
        let (endpoint, server) = echo_server::start().await?;
        let config = test_config();
        let client = ReqwestClient::new(config, &endpoint).await?;

        let delay = Duration::from_millis(200);
        let timeout = Duration::from_millis(150);
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let builder = client
            .builder(reqwest::Method::GET, "/echo".into())
            .query(&[("delay_ms", format!("{}", delay.as_millis()))]);
        let response = client.execute::<serde_json::Value, serde_json::Value>(
            builder,
            Some(json!({})),
            test_options(&timeout),
        );

        let start = tokio::time::Instant::now();
        tokio::pin!(server);
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => {  },
                r = &mut response => {
                    use gax::error::ErrorKind;
                    assert!(
                        r.is_err(),
                        "expected an error when timeout={}, got={:?}",
                        timeout.as_millis(),
                        r
                    );
                    let err = r.err().unwrap();
                    assert_eq!(err.kind(), ErrorKind::Io);
                    break;
                },
                _ = interval.tick() => { },
            }
        }

        let elapsed = tokio::time::Instant::now() - start;
        assert_eq!(elapsed, timeout);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_effective_timeout() -> Result<()> {
        let (endpoint, server) = echo_server::start().await?;
        let config = test_config();
        let client = ReqwestClient::new(config, &endpoint).await?;

        // The first attempt should timeout, because of the attempt timeout of
        // 100ms. The next attempt should timeout, because of the overall
        // timeout at 150ms.
        let delay = Duration::from_millis(2000);
        let attempt_timeout = Duration::from_millis(100);
        let overall_timeout = Duration::from_millis(150);
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let builder = client
            .builder(reqwest::Method::GET, "/echo".into())
            .query(&[("delay_ms", format!("{}", delay.as_millis()))]);

        #[derive(Default, Debug)]
        struct TestBackoffPolicy {
            pub elapsed_on_failure: Arc<Mutex<Option<Duration>>>,
        }

        impl gax::backoff_policy::BackoffPolicy for TestBackoffPolicy {
            fn on_failure(
                &self,
                loop_start: std::time::Instant,
                attempt_count: u32,
            ) -> std::time::Duration {
                if attempt_count == 1 {
                    *self.elapsed_on_failure.lock().unwrap() =
                        Some(tokio::time::Instant::now().into_std() - loop_start);
                }

                std::time::Duration::ZERO
            }
        }

        let elapsed_on_failure = Arc::new(Mutex::new(None));
        let mut request_options: RequestOptions = RequestOptions::default();
        request_options.set_attempt_timeout(attempt_timeout);
        request_options.set_retry_policy(AlwaysRetry.with_time_limit(overall_timeout));
        request_options.set_backoff_policy(TestBackoffPolicy {
            elapsed_on_failure: elapsed_on_failure.clone(),
        });
        disable_throttling(&mut request_options);

        let response = client.execute::<serde_json::Value, serde_json::Value>(
            builder,
            Some(json!({})),
            request_options,
        );

        let start = tokio::time::Instant::now();
        tokio::pin!(server);
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => {  },
                r = &mut response => {
                    use gax::error::ErrorKind;
                    assert!(
                        r.is_err(),
                        "expected a timeout error, got={:?}",
                        r
                    );
                    let err = r.err().unwrap();
                    assert_eq!(err.kind(), ErrorKind::Io);
                    break;
                },
                _ = interval.tick() => { },
            }
        }

        // Verify the time at which we expect the initial attempt to complete
        let elapsed = elapsed_on_failure
            .lock()
            .unwrap()
            .expect("Backoff policy should be called.");
        assert_eq!(elapsed, attempt_timeout);

        // Verify the time at which we expect the operation to complete
        let elapsed = tokio::time::Instant::now() - start;
        assert_eq!(elapsed, overall_timeout);

        Ok(())
    }

    fn test_options(timeout: &Duration) -> RequestOptions {
        let mut options = RequestOptions::default();
        options.set_attempt_timeout(*timeout);
        options
    }

    fn disable_throttling(o: &mut RequestOptions) {
        o.set_retry_throttler(RetryThrottlerArg::from(
            CircuitBreaker::new(1000, 0, 0).expect("This is a valid configuration"),
        ));
    }

    fn get_query_value(response: &serde_json::Value, name: &str) -> Option<String> {
        response
            .as_object()
            .and_then(|o| o.get("query"))
            .and_then(|h| h.get(name))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    fn test_config() -> ClientConfig {
        ClientConfig {
            cred: auth::credentials::testing::test_credentials().into(),
            ..Default::default()
        }
    }
}
