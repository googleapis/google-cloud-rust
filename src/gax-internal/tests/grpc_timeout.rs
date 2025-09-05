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

#[cfg(all(test, feature = "_internal-grpc-client"))]
mod tests {
    use anyhow::Result;
    use auth::credentials::testing::test_credentials;
    use gax::options::*;
    use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    use gax::retry_state::RetryState;
    use gax::retry_throttler::{CircuitBreaker, RetryThrottlerArg};
    use google_cloud_gax_internal::grpc;
    use grpc_server::{builder, google, start_echo_server};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn test_no_timeout() -> Result<()> {
        let (endpoint, server) = start_echo_server().await?;
        let client = test_client(endpoint).await?;

        let delay = Duration::from_millis(200);
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let request_options = RequestOptions::default();
        let response = send_request(client, request_options, "great success!", Some(delay));

        tokio::pin!(server);
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => { },
                r = &mut response => {
                    let response = r?;
                    assert_eq!(response.message, "great success!");
                    break;
                },
                _ = interval.tick() => { },
            }
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout_does_not_expire() -> Result<()> {
        let (endpoint, server) = start_echo_server().await?;
        let client = test_client(endpoint).await?;

        let delay = Duration::from_millis(200);
        let timeout = Duration::from_millis(2000);
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let mut request_options = RequestOptions::default();
        request_options.set_attempt_timeout(timeout);
        let response = send_request(client, request_options, "great success!", Some(delay));

        tokio::pin!(server);
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => {  },
                r = &mut response => {
                    let response = r?;
                    assert_eq!(response.message, "great success!");
                    break;
                },
                _ = interval.tick() => { },
            }
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout_expires() -> Result<()> {
        let (endpoint, server) = start_echo_server().await?;
        let client = test_client(endpoint).await?;
        let mut server = connect_client(client.clone(), server).await?;

        let delay = Duration::from_millis(200);
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let timeout = Duration::from_millis(150);
        let mut request_options = RequestOptions::default();
        request_options.set_attempt_timeout(timeout);
        let response = send_request(client, request_options, "should timeout", Some(delay));

        let start = tokio::time::Instant::now();
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => {  },
                r = &mut response => {
                    assert!(
                        r.is_err(),
                        "expected an error when timeout={}, got={r:?}",
                        timeout.as_millis()
                    );
                    let err = r.unwrap_err();
                    assert!(err.is_timeout(), "{err:?}");
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
        let (endpoint, server) = start_echo_server().await?;
        let client = test_client(endpoint).await?;
        let mut server = connect_client(client.clone(), server).await?;

        // The first attempt should timeout, because of the attempt timeout of
        // 100ms. The next attempt should timeout, because of the overall
        // timeout at 150ms.
        let attempt_timeout = Duration::from_millis(100);
        let overall_timeout = Duration::from_millis(150);
        let delay = Duration::from_millis(200);
        let mut interval = tokio::time::interval(Duration::from_millis(10));

        #[derive(Default, Debug)]
        struct TestBackoffPolicy {
            pub elapsed_on_failure: Arc<Mutex<Option<Duration>>>,
        }

        impl gax::backoff_policy::BackoffPolicy for TestBackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> std::time::Duration {
                if state.attempt_count == 1 {
                    *self.elapsed_on_failure.lock().unwrap() =
                        Some(tokio::time::Instant::now().into_std() - state.start);
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
        let response = send_request(client, request_options, "should timeout", Some(delay));

        let start = tokio::time::Instant::now();
        tokio::pin!(response);
        loop {
            tokio::select! {
                _ = &mut server => {  },
                r = &mut response => {
                    assert!(
                        r.is_err(),
                        "expected a timeout error, got={r:?}"
                    );
                    let err = r.unwrap_err();
                    assert!(err.is_timeout(), "{err:?}");
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

    async fn test_client(endpoint: String) -> gax::client_builder::Result<grpc::Client> {
        builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await
    }

    async fn connect_client(
        client: grpc::Client,
        server: tokio::task::JoinHandle<()>,
    ) -> anyhow::Result<std::pin::Pin<Box<tokio::task::JoinHandle<()>>>> {
        // Make sure the client is connected, the first request has higher
        // latency. It is hard to write a timeout test that includes the request.
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        let mut server = Box::pin(server);

        let request_options = RequestOptions::default();
        let response = send_request(client, request_options, "great success!", None);
        tokio::pin!(response);
        for _ in 0..100 {
            tokio::select! {
                _ = &mut server => {  },
                r = &mut response => {
                    assert!(r.is_ok(), "{r:?}");
                    return Ok(server);
                },
                _ = interval.tick() => { },
            }
        }
        Err(anyhow::Error::msg("cannot connect client"))
    }

    fn disable_throttling(o: &mut RequestOptions) {
        o.set_retry_throttler(RetryThrottlerArg::from(
            CircuitBreaker::new(1000, 0, 0).expect("This is a valid configuration"),
        ));
    }

    async fn send_request(
        client: grpc::Client,
        request_options: RequestOptions,
        msg: &str,
        delay: Option<Duration>,
    ) -> gax::Result<google::test::v1::EchoResponse> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = {
            let delay_ms = delay.map(|d| u64::try_from(d.as_millis()).unwrap());
            google::test::v1::EchoRequest {
                message: msg.into(),
                delay_ms,
            }
        };
        client
            .execute(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                request_options,
                "test-only-api-client/1.0",
                "name=test-only",
            )
            .await
            .map(tonic::Response::into_inner)
    }
}
