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
        use google_cloud_gax_internal::observability::attributes::keys::*;
        use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};

        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        // Configure client with tracing enabled
        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

        lazy_static::lazy_static! {
            static ref TEST_INFO: google_cloud_gax_internal::options::InstrumentationClientInfo = {
                let mut info = google_cloud_gax_internal::options::InstrumentationClientInfo::default();
                info.service_name = "test-service";
                info.client_version = "1.0.0";
                info.client_artifact = "test-artifact";
                info.default_host = "example.com";
                info
            };
        }

        let client = grpc::Client::new_with_instrumentation(config, &endpoint, &TEST_INFO).await?;

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
            "Should capture one grpc.request span. Captured: {:?}",
            spans
        );

        let span = &grpc_spans[0];
        let attrs = &span.attributes;

        // Parse the endpoint to get expected values
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: std::collections::HashMap<
            String,
            google_cloud_test_utils::test_layer::AttributeValue,
        > = [
            (OTEL_NAME, "google.test.v1.EchoService/Echo".into()),
            (otel_trace::RPC_SYSTEM, "grpc".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.test.v1.EchoService".into()),
            (otel_trace::RPC_METHOD, "Echo".into()),
            (otel_trace::SERVER_ADDRESS, expected_host.clone().into()),
            (otel_trace::SERVER_PORT, (expected_port as i64).into()),
            (otel_attr::URL_DOMAIN, expected_host.into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
            (otel_attr::RPC_GRPC_STATUS_CODE, 0_i64.into()),
            (GCP_CLIENT_SERVICE, "test-service".into()),
            (GCP_CLIENT_VERSION, "1.0.0".into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_ARTIFACT, "test-artifact".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_grpc_custom_endpoint() -> anyhow::Result<()> {
        use google_cloud_gax_internal::observability::attributes::keys::*;
        use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};

        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        // Configure client with tracing enabled and a custom endpoint
        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());
        // We use the actual echo server endpoint but pretend it's a custom one for config purposes
        // Note: Client::new uses the config.endpoint if set, otherwise default_endpoint.
        // Here we want to test parsing logic, so we set config.endpoint.
        config.endpoint = Some(endpoint.clone());

        let client = grpc::Client::new(config, "http://unused.default.com").await?;

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
            "Should capture one grpc.request span. Captured: {:?}",
            spans
        );

        let span = &grpc_spans[0];
        let attrs = &span.attributes;

        // Verify parsing of the custom endpoint
        // The endpoint string from start_echo_server is like "http://127.0.0.1:12345"
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: std::collections::HashMap<
            String,
            google_cloud_test_utils::test_layer::AttributeValue,
        > = [
            (OTEL_NAME, "google.test.v1.EchoService/Echo".into()),
            (otel_trace::RPC_SYSTEM, "grpc".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.test.v1.EchoService".into()),
            (otel_trace::RPC_METHOD, "Echo".into()),
            (otel_trace::SERVER_ADDRESS, expected_host.clone().into()),
            (otel_trace::SERVER_PORT, (expected_port as i64).into()),
            (otel_attr::URL_DOMAIN, "unused.default.com".into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
            (otel_attr::RPC_GRPC_STATUS_CODE, 0_i64.into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_grpc_regional_endpoint() -> anyhow::Result<()> {
        use gax::retry_policy::RetryPolicyExt;
        use google_cloud_gax_internal::observability::attributes::keys::*;
        use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
        use std::sync::Arc;

        let guard = TestLayer::initialize();

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());
        config.endpoint = Some("https://foo.bar.rep.googleapis.com".to_string());
        // Disable retries to avoid multiple spans on connection failure
        config.retry_policy = Some(Arc::new(
            gax::retry_policy::Aip194Strict.with_attempt_limit(1),
        ));

        // We don't need a real server, just need the client to attempt a request.
        // The request will fail, but the span should be created.
        let client = grpc::Client::new(config, "https://foo.googleapis.com").await?;

        let extensions = tonic::Extensions::new();
        let request = google::test::v1::EchoRequest::default();

        // This will fail, but we just want to capture the span
        let _ = client
            .execute::<_, google::test::v1::EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                RequestOptions::default(),
                "test-client",
                "",
            )
            .await;

        let spans = TestLayer::capture(&guard);
        let grpc_spans: Vec<_> = spans.iter().filter(|s| s.name == "grpc.request").collect();
        assert_eq!(
            grpc_spans.len(),
            1,
            "Should capture one grpc.request span. Captured: {:?}",
            spans
        );

        let span = &grpc_spans[0];
        let attrs = &span.attributes;

        let expected_attributes: std::collections::HashMap<
            String,
            google_cloud_test_utils::test_layer::AttributeValue,
        > = [
            (OTEL_NAME, "google.test.v1.EchoService/Echo".into()),
            (otel_trace::RPC_SYSTEM, "grpc".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.test.v1.EchoService".into()),
            (otel_trace::RPC_METHOD, "Echo".into()),
            (
                otel_trace::SERVER_ADDRESS,
                "foo.bar.rep.googleapis.com".into(),
            ),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_attr::URL_DOMAIN, "foo.googleapis.com".into()), // Expect default domain
            (OTEL_STATUS_CODE, "ERROR".into()),
            (otel_trace::ERROR_TYPE, "CLIENT_CONNECTION_ERROR".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_grpc_error_status() -> anyhow::Result<()> {
        use google_cloud_gax_internal::observability::attributes::keys::*;
        use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};

        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

        let client = grpc::Client::new(config, &endpoint).await?;

        // Send a request to a non-existent method
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "NonExistentMethod",
            ));
            e
        };
        let request = google::test::v1::EchoRequest::default();

        // Expect an error
        let _ = client
            .execute::<_, google::test::v1::EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static(
                    "/google.test.v1.EchoService/NonExistentMethod",
                ),
                request,
                RequestOptions::default(),
                "test-client",
                "",
            )
            .await;

        let spans = TestLayer::capture(&guard);
        let grpc_spans: Vec<_> = spans.iter().filter(|s| s.name == "grpc.request").collect();
        assert_eq!(grpc_spans.len(), 1);

        let span = &grpc_spans[0];
        let attrs = &span.attributes;

        // Parse the endpoint to get expected values
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: std::collections::HashMap<
            String,
            google_cloud_test_utils::test_layer::AttributeValue,
        > = [
            (
                OTEL_NAME,
                "google.test.v1.EchoService/NonExistentMethod".into(),
            ),
            (otel_trace::RPC_SYSTEM, "grpc".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.test.v1.EchoService".into()),
            (otel_trace::RPC_METHOD, "NonExistentMethod".into()),
            (otel_trace::SERVER_ADDRESS, expected_host.clone().into()),
            (otel_trace::SERVER_PORT, (expected_port as i64).into()),
            (otel_attr::URL_DOMAIN, expected_host.into()),
            (OTEL_STATUS_CODE, "ERROR".into()),
            (otel_attr::RPC_GRPC_STATUS_CODE, 12_i64.into()), // UNIMPLEMENTED = 12
            (otel_trace::ERROR_TYPE, "UNIMPLEMENTED".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(google_cloud_unstable_storage_bidi)]
    async fn test_grpc_streaming_span() -> anyhow::Result<()> {
        use google_cloud_gax_internal::observability::attributes::keys::*;
        use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
        use tokio_stream::StreamExt;

        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        // Configure client with tracing enabled
        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

        let client = grpc::Client::new(config, &endpoint).await?;

        // Send a streaming request
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Chat",
            ));
            e
        };

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        let mut response_stream = client
            .bidi_stream::<_, google::test::v1::EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Chat"),
                request_stream,
                RequestOptions::default(),
                "test-only-api-client/1.0",
                "name=test-only",
            )
            .await?
            .into_inner();

        // Send a message
        let request = google::test::v1::EchoRequest {
            message: "test message".into(),
            ..Default::default()
        };
        tx.send(request).await?;

        // Receive a response
        let response = response_stream.next().await.expect("stream closed")?;
        assert_eq!(response.message, "test message");

        // Close the stream
        drop(tx);
        assert!(response_stream.next().await.is_none());

        let spans = TestLayer::capture(&guard);
        let grpc_spans: Vec<_> = spans.iter().filter(|s| s.name == "grpc.request").collect();
        assert_eq!(
            grpc_spans.len(),
            1,
            "Should capture one grpc.request span. Captured: {:?}",
            spans
        );

        let span = &grpc_spans[0];
        let attrs = &span.attributes;

        // Parse the endpoint to get expected values
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: std::collections::HashMap<
            String,
            google_cloud_test_utils::test_layer::AttributeValue,
        > = [
            (OTEL_NAME, "google.test.v1.EchoService/Chat".into()),
            (otel_trace::RPC_SYSTEM, "grpc".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.test.v1.EchoService".into()),
            (otel_trace::RPC_METHOD, "Chat".into()),
            (otel_trace::SERVER_ADDRESS, expected_host.clone().into()),
            (otel_trace::SERVER_PORT, (expected_port as i64).into()),
            (otel_attr::URL_DOMAIN, expected_host.into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
            (otel_attr::RPC_GRPC_STATUS_CODE, 0_i64.into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(google_cloud_unstable_storage_bidi)]
    async fn test_grpc_streaming_error() -> anyhow::Result<()> {
        use google_cloud_gax_internal::observability::attributes::keys::*;
        use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
        use tokio_stream::StreamExt;

        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

        let client = grpc::Client::new(config, &endpoint).await?;

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Chat",
            ));
            e
        };

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        let mut response_stream = client
            .bidi_stream::<_, google::test::v1::EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Chat"),
                request_stream,
                RequestOptions::default(),
                "test-only-api-client/1.0",
                "name=test-only",
            )
            .await?
            .into_inner();

        // Send an empty message to trigger an error in the stream
        let request = google::test::v1::EchoRequest {
            message: "".into(), // Empty message triggers error in echo-server
            ..Default::default()
        };
        tx.send(request).await?;

        // Receive the error
        let response = response_stream.next().await;
        assert!(response.is_some());
        let result = response.unwrap();
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        // Close the stream
        drop(tx);

        let spans = TestLayer::capture(&guard);
        let grpc_spans: Vec<_> = spans.iter().filter(|s| s.name == "grpc.request").collect();
        assert_eq!(
            grpc_spans.len(),
            1,
            "Should capture one grpc.request span. Captured: {:?}",
            spans
        );

        let span = &grpc_spans[0];
        let attrs = &span.attributes;

        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: std::collections::HashMap<
            String,
            google_cloud_test_utils::test_layer::AttributeValue,
        > = [
            (OTEL_NAME, "google.test.v1.EchoService/Chat".into()),
            (otel_trace::RPC_SYSTEM, "grpc".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.test.v1.EchoService".into()),
            (otel_trace::RPC_METHOD, "Chat".into()),
            (otel_trace::SERVER_ADDRESS, expected_host.clone().into()),
            (otel_trace::SERVER_PORT, (expected_port as i64).into()),
            (otel_attr::URL_DOMAIN, expected_host.into()),
            (OTEL_STATUS_CODE, "ERROR".into()),
            (otel_attr::RPC_GRPC_STATUS_CODE, 3_i64.into()), // INVALID_ARGUMENT = 3
            (otel_trace::ERROR_TYPE, "INVALID_ARGUMENT".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_grpc_cancellation() -> anyhow::Result<()> {
        use google_cloud_gax_internal::observability::attributes;
        use google_cloud_gax_internal::observability::attributes::keys::*;
        use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
        use std::time::Duration;

        let guard = TestLayer::initialize();

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

        // Use a a real but non-existentaddress to ensure the request hangs and stays Pending,
        // allowing us to drop it and trigger cancellation.
        let blackhole_endpoint = "http://192.0.2.1:1234";
        let client = grpc::Client::new(config, blackhole_endpoint).await?;

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

        let future = client.execute::<_, google::test::v1::EchoResponse>(
            extensions,
            http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
            request,
            RequestOptions::default(),
            "test-client",
            "",
        );

        // Poll the future once to ensure the span is created and entered, then drop it
        // We use `tokio::time::timeout` with a very short duration to force a drop
        let _ = tokio::time::timeout(Duration::from_micros(1), future).await;

        // Wait a bit for the span to be processed (though drop should happen immediately)
        tokio::time::sleep(Duration::from_millis(10)).await;

        let spans = TestLayer::capture(&guard);
        let grpc_spans: Vec<_> = spans.iter().filter(|s| s.name == "grpc.request").collect();
        assert_eq!(
            grpc_spans.len(),
            1,
            "Should capture one grpc.request span. Captured: {:?}",
            spans
        );

        let span = &grpc_spans[0];
        let attrs = &span.attributes;

        let expected_attributes: std::collections::HashMap<
            String,
            google_cloud_test_utils::test_layer::AttributeValue,
        > = [
            (OTEL_NAME, "google.test.v1.EchoService/Echo".into()),
            (otel_trace::RPC_SYSTEM, "grpc".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SERVICE, "google.test.v1.EchoService".into()),
            (otel_trace::RPC_METHOD, "Echo".into()),
            (otel_trace::SERVER_ADDRESS, "192.0.2.1".into()),
            (otel_trace::SERVER_PORT, 1234_i64.into()),
            (otel_attr::URL_DOMAIN, "192.0.2.1".into()),
            (OTEL_STATUS_CODE, "ERROR".into()),
            (
                otel_trace::ERROR_TYPE,
                attributes::error_type_values::CLIENT_CANCELLED.into(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);

        Ok(())
    }
}
