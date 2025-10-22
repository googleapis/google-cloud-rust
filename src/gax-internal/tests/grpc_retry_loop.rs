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
    use gax::options::*;
    use gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use google_cloud_gax_internal::grpc;
    use grpc_server::google::test::v1::EchoResponse;
    use grpc_server::{builder, google, start_fixed_responses};

    fn test_credentials() -> auth::credentials::Credentials {
        auth::credentials::anonymous::Builder::new().build()
    }

    #[tokio::test]
    async fn no_retry_immediate_success() -> anyhow::Result<()> {
        let (endpoint, _server) = start_fixed_responses(vec![success()]).await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let response = send_request(client, "no_retry_immediate_success").await;
        assert!(response.is_ok(), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn no_retry_immediate_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_fixed_responses(vec![transient()]).await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let response = send_request(client, "no_retry_immediate_error").await;
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn retry_then_success() -> anyhow::Result<()> {
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), success()]).await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .with_backoff_policy(test_backoff())
            .build()
            .await?;
        let response = send_request(client, "retry_then_success").await;
        assert!(response.is_ok(), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn retry_then_error() -> anyhow::Result<()> {
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), permanent()]).await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .with_backoff_policy(test_backoff())
            .build()
            .await?;
        let response = send_request(client, "no_retry_immediate_error").await;
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn retry_policy_exhausted() -> anyhow::Result<()> {
        let (endpoint, _server) = start_fixed_responses((0..3).map(|_| transient())).await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .with_retry_policy(Aip194Strict.with_attempt_limit(3))
            .with_backoff_policy(test_backoff())
            .build()
            .await?;
        let response = send_request(client, "no_retry_immediate_error").await;
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    fn success() -> tonic::Result<tonic::Response<EchoResponse>> {
        Ok(tonic::Response::new(EchoResponse {
            message: "success!".into(),
            metadata: std::collections::HashMap::default(),
        }))
    }

    fn transient() -> tonic::Result<tonic::Response<EchoResponse>> {
        Err(tonic::Status::unavailable("try-again"))
    }

    fn permanent() -> tonic::Result<tonic::Response<EchoResponse>> {
        Err(tonic::Status::permission_denied("uh-oh"))
    }

    fn test_backoff() -> impl gax::backoff_policy::BackoffPolicy {
        use std::time::Duration;
        gax::exponential_backoff::ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }

    pub async fn send_request(client: grpc::Client, msg: &str) -> gax::Result<EchoResponse> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
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
