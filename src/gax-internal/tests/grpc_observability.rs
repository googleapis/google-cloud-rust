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

#[cfg(all(test, feature = "_internal-grpc-client", google_cloud_unstable_tracing))]
mod tests {
    use gax::options::RequestOptions;
    use google_cloud_gax_internal::grpc;
    use google_cloud_test_utils::test_layer::TestLayer;
    use grpc_server::{google, start_echo_server};

    fn test_credentials() -> auth::credentials::Credentials {
        auth::credentials::anonymous::Builder::new().build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_grpc_basic_span() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        // Configure client with tracing enabled
        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

        let client = grpc::Client::new(config, &endpoint).await?;

        // Send a request
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = google::test::v1::EchoRequest {
            message: "test message".into(),
            ..Default::default()
        };
        let _ = client
            .execute::<_, google::test::v1::EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                RequestOptions::default(),
                "test-only-api-client/1.0",
                "name=test-only",
            )
            .await?;

        let spans = TestLayer::capture(&guard);
        let grpc_spans: Vec<_> = spans.iter().filter(|s| s.name == "grpc.request").collect();
        assert_eq!(
            grpc_spans.len(),
            1,
            "Should capture one grpc.request span: {:?}",
            grpc_spans
        );

        Ok(())
    }
}
