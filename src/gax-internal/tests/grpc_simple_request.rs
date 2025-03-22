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

#[cfg(all(test, feature = "_internal_grpc_client"))]
mod test {
    use auth::credentials::testing::test_credentials;
    use gax::options::*;
    use google_cloud_gax_internal::grpc;
    use grpc_server::{google, start_echo_server};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_endpoint() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let config = ClientConfig::default().set_credential(test_credentials());
        let client = grpc::Client::new(config, &endpoint).await?;
        check_simple_request(client).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn override_endpoint() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let config = ClientConfig::default()
            .set_endpoint(&endpoint)
            .set_credential(test_credentials());
        let client = grpc::Client::new(config, "unused").await?;

        check_simple_request(client).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credentials_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let config = ClientConfig::default()
            .set_endpoint(&endpoint)
            .set_credential(auth::credentials::testing::error_credentials(true));
        let client = grpc::Client::new(config, "unused").await?;

        let response = send_request(client, "credentials error").await;
        let err = response.err().unwrap();
        assert_eq!(err.kind(), gax::error::ErrorKind::Authentication, "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_error() -> anyhow::Result<()> {
        let config = ClientConfig::default()
            .set_endpoint("http://127.0.0.1:1")
            .set_credential(test_credentials());
        let client = grpc::Client::new(config, "unused").await;
        let err = client.err().unwrap();
        assert_eq!(err.kind(), gax::error::ErrorKind::Io, "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endpoint_error() -> anyhow::Result<()> {
        let config = ClientConfig::default()
            .set_endpoint("http:/invalid-invalid")
            .set_credential(test_credentials());
        let client = grpc::Client::new(config, "unused").await;
        let err = client.err().unwrap();
        assert_eq!(err.kind(), gax::error::ErrorKind::Other, "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let config = ClientConfig::default()
            .set_endpoint(&endpoint)
            .set_credential(test_credentials());
        let client = grpc::Client::new(config, "unused").await?;

        let response = send_request(client, "").await;
        let err = response.err().unwrap();
        assert_eq!(err.kind(), gax::error::ErrorKind::Rpc, "{err:?}");
        Ok(())
    }

    async fn send_request(
        client: grpc::Client,
        msg: &str,
    ) -> gax::Result<google::test::v1::EchoResponse> {
        let request = google::test::v1::EchoRequest {
            message: msg.into(),
        };
        let request_options = RequestOptions::default();
        client
            .execute(
                tonic::GrpcMethod::new("google.test.v1.EchoServices", "Echo"),
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                request_options,
                "test-only-api-client/1.0",
                "name=test-only",
            )
            .await
    }

    async fn check_simple_request(client: grpc::Client) -> anyhow::Result<()> {
        let response = send_request(client, "test message").await?;
        assert_eq!(&response.message, "test message");
        assert_eq!(
            response
                .metadata
                .get("x-goog-api-client")
                .map(String::as_str),
            Some("test-only-api-client/1.0")
        );
        assert_eq!(
            response
                .metadata
                .get("x-goog-request-params")
                .map(String::as_str),
            Some("name=test-only")
        );
        let got_user_agent = response.metadata.get("user-agent").unwrap();
        assert!(got_user_agent.contains("tonic/"), "{got_user_agent:?}");
        Ok(())
    }
}
