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
    use google_cloud_auth::credentials::{Credentials, anonymous::Builder as Anonymous};
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::Error;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use google_cloud_gax_internal::attempt_interceptor::AttemptInterceptor;
    use google_cloud_gax_internal::grpc;
    use google_cloud_gax_internal::options::ClientConfig;
    use grpc_server::google::test::v1::EchoResponse;
    use grpc_server::transport::{EchoTransport, transport_builder};
    use grpc_server::{both_transports, google, start_fixed_responses};
    use http::HeaderMap;
    use pretty_assertions::assert_eq;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    both_transports!(no_retry_immediate_success);
    async fn no_retry_immediate_success<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_fixed_responses(vec![success()]).await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act
        let response = send_request(client, "no_retry_immediate_success").await?;

        // Assert
        assert_eq!(response.message, "success!");
        Ok(())
    }

    both_transports!(no_retry_immediate_error);
    async fn no_retry_immediate_error<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_fixed_responses(vec![permanent()]).await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Act
        let response = send_request(client, "no_retry_immediate_error").await;

        // Assert
        let err = response.expect_err("the permanent error should be returned");
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    both_transports!(retry_then_success);
    async fn retry_then_success<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), success()]).await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .with_backoff_policy(test_backoff())
            .build()
            .await?;

        // Act
        let response = send_request(client, "retry_then_success").await?;

        // Assert
        assert_eq!(response.message, "success!");
        Ok(())
    }

    both_transports!(retry_then_error);
    async fn retry_then_error<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), permanent()]).await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .with_backoff_policy(test_backoff())
            .build()
            .await?;

        // Act
        let response = send_request(client, "retry_then_error").await;

        // Assert
        let err = response.expect_err("the permanent error should be returned");
        assert_eq!(err.status().map(|s| s.code), Some(Code::PermissionDenied));
        Ok(())
    }

    both_transports!(retry_policy_exhausted);
    async fn retry_policy_exhausted<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_fixed_responses((0..3).map(|_| transient())).await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .with_retry_policy(Aip194Strict.with_attempt_limit(3))
            .with_backoff_policy(test_backoff())
            .build()
            .await?;

        // Act
        let response = send_request(client, "retry_policy_exhausted").await;

        // Assert
        let err = response.expect_err("the retry policy should be exhausted");
        let status = err.status().expect("the error should have a status");
        assert_eq!(status.code, Code::Unavailable);
        assert_eq!(status.message, "try-again");
        Ok(())
    }

    #[tokio::test]
    async fn interceptor_on_retry() -> anyhow::Result<()> {
        #[derive(Debug, Default)]
        struct AttemptTracker {
            attempts: Mutex<Vec<u32>>,
        }
        impl AttemptInterceptor for AttemptTracker {
            fn intercept(&self, _headers: &mut HeaderMap, attempt: u32) {
                self.attempts.lock().expect("lock failed").push(attempt);
            }
        }

        let tracker = Arc::new(AttemptTracker::default());
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), success()]).await?;

        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint);
        config.backoff_policy = Some(Arc::new(test_backoff()));

        let mut client = grpc::Client::new(config, "https://test-only.googleapis.com").await?;
        client.set_attempt_interceptor(tracker.clone());
        let _response = send_request(client, "interceptor_on_retry").await?;

        let attempts = tracker.attempts.lock().expect("lock failed").clone();
        assert_eq!(attempts, vec![1, 2, 3]);

        Ok(())
    }

    #[tokio::test]
    async fn interceptor_on_attempt_complete() -> anyhow::Result<()> {
        #[derive(Clone, Debug, Default)]
        struct AttemptCompletionRecord {
            method: String,
            attempt: u32,
            has_response_headers: bool,
            has_error: bool,
            status_code: Option<Code>,
            measured_start_time: bool,
        }

        #[derive(Debug, Default)]
        struct AttemptCompletionTracker {
            completed_attempts: Mutex<Vec<AttemptCompletionRecord>>,
        }
        impl AttemptInterceptor for AttemptCompletionTracker {
            fn intercept(&self, _headers: &mut HeaderMap, _attempt: u32) {}

            fn on_attempt_complete(
                &self,
                method: &str,
                attempt: u32,
                start_time: Instant,
                response_headers: Option<&HeaderMap>,
                error: Option<&Error>,
                _options: &RequestOptions,
            ) {
                self.completed_attempts.lock().expect("lock failed").push(
                    AttemptCompletionRecord {
                        method: method.to_string(),
                        attempt,
                        has_response_headers: response_headers.is_some(),
                        has_error: error.is_some(),
                        status_code: error.and_then(|e| e.status().map(|s| s.code)),
                        measured_start_time: start_time.elapsed().as_nanos() > 0,
                    },
                );
            }
        }

        let tracker = Arc::new(AttemptCompletionTracker::default());
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), success()]).await?;

        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint);
        config.backoff_policy = Some(Arc::new(test_backoff()));

        let mut client = grpc::Client::new(config, "https://test-only.googleapis.com").await?;
        client.set_attempt_interceptor(tracker.clone());
        let _response = send_request(client, "interceptor_on_attempt_complete").await?;

        let completed = tracker
            .completed_attempts
            .lock()
            .expect("lock failed")
            .clone();
        assert_eq!(completed.len(), 3);

        // Attempt 1: Transient unavailable error
        assert_eq!(completed[0].method, "/google.test.v1.EchoService/Echo");
        assert_eq!(completed[0].attempt, 1);
        assert!(completed[0].has_error);
        assert_eq!(completed[0].status_code, Some(Code::Unavailable));
        assert!(completed[0].measured_start_time);

        // Attempt 2: Transient unavailable error
        assert_eq!(completed[1].method, "/google.test.v1.EchoService/Echo");
        assert_eq!(completed[1].attempt, 2);
        assert!(completed[1].has_error);
        assert_eq!(completed[1].status_code, Some(Code::Unavailable));
        assert!(completed[1].measured_start_time);

        // Attempt 3: Success with response headers
        assert_eq!(completed[2].method, "/google.test.v1.EchoService/Echo");
        assert_eq!(completed[2].attempt, 3);
        assert!(!completed[2].has_error);
        assert_eq!(completed[2].status_code, None);
        assert!(completed[2].has_response_headers);
        assert!(completed[2].measured_start_time);

        Ok(())
    }

    /// Returns a successful [`EchoResponse`].
    fn success() -> tonic::Result<tonic::Response<EchoResponse>> {
        Ok(tonic::Response::new(EchoResponse {
            message: "success!".into(),
            metadata: std::collections::HashMap::default(),
            ..EchoResponse::default()
        }))
    }

    /// Returns a transient (`Unavailable`) gRPC error.
    fn transient() -> tonic::Result<tonic::Response<EchoResponse>> {
        Err(tonic::Status::unavailable("try-again"))
    }

    /// Returns a permanent (`PermissionDenied`) gRPC error.
    fn permanent() -> tonic::Result<tonic::Response<EchoResponse>> {
        Err(tonic::Status::permission_denied("uh-oh"))
    }

    fn test_backoff() -> impl BackoffPolicy {
        use std::time::Duration;
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }

    async fn send_request<C: EchoTransport>(
        client: C,
        msg: &str,
    ) -> google_cloud_gax::Result<EchoResponse> {
        let request = google::test::v1::EchoRequest {
            message: msg.into(),
            ..google::test::v1::EchoRequest::default()
        };
        let request_options = {
            let mut o = RequestOptions::default();
            o.set_idempotency(true);
            o
        };
        client
            .echo(request, request_options, "name=test-only")
            .await
            .map(tonic::Response::into_inner)
    }
}
