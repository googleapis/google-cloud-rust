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
mod test {
    use gax::options::*;
    use google_cloud_gax_internal::grpc;
    use grpc_server::{builder, google, start_echo_server};

    fn test_credentials() -> auth::credentials::Credentials {
        auth::credentials::anonymous::Builder::new().build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_user_agent() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let options = {
            let mut o = RequestOptions::default();
            o.set_user_agent("custom-user-agent/v1.2.3");
            o
        };
        let response = send_request(client, options).await?;
        let user_agent = response
            .metadata
            .get(http::header::USER_AGENT.as_str())
            .map(String::as_str)
            .expect("There should be a User-Agent header");
        let components: Vec<&str> = user_agent.split(' ').collect();
        assert!(
            components.contains(&"custom-user-agent/v1.2.3"),
            "User-Agent: {user_agent}"
        );
        Ok(())
    }

    async fn send_request(
        client: grpc::Client,
        options: gax::options::RequestOptions,
    ) -> gax::Result<google::test::v1::EchoResponse> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = google::test::v1::EchoRequest {
            message: "message".into(),
            ..google::test::v1::EchoRequest::default()
        };
        client
            .execute(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                options,
                "test-only-api-client/1.0",
                "",
            )
            .await
            .map(tonic::Response::into_inner)
    }
}
