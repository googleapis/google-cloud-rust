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
    use google_cloud_auth::credentials::Credentials;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{RequestOptionsExt, ResourceName};
    use google_cloud_gax::retry_policy::Aip194Strict;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_gax_internal::grpc;
    use google_cloud_gax_internal::observability::{ClientRequestAttributes, RequestRecorder};
    use google_cloud_gax_internal::options::{ClientConfig, InstrumentationClientInfo};
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer, TestLayerGuard};
    use grpc_server::{google, start_echo_server};
    use pretty_assertions::assert_eq;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::LazyLock;
    use tokio_stream::StreamExt;

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    static TEST_INFO: LazyLock<InstrumentationClientInfo> = LazyLock::new(|| {
        let mut info = InstrumentationClientInfo::default();
        info.service_name = "test-service";
        info.client_version = "1.0.0";
        info.client_artifact = "test-artifact";
        info.default_host = "example.com";
        info
    });

    #[track_caller]
    fn grpc_request_attributes(guard: &TestLayerGuard) -> BTreeMap<String, AttributeValue> {
        let captured = TestLayer::capture(guard);
        let grpc_spans = captured
            .iter()
            .filter(|s| s.name == "grpc.request")
            .collect::<Vec<_>>();
        let span = match grpc_spans[..] {
            [span] => span,
            _ => panic!("should capture one `grpc.request` span, captured: {captured:?}"),
        };
        BTreeMap::from_iter(span.attributes.clone())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic_span() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        // Configure client with tracing enabled
        let mut config = ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

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
        let recorder = RequestRecorder::new(*TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default().set_url_template("/google.test.v1.EchoService/Echo"),
        );
        let _ = recorder
            .scope(async {
                client
                    .execute::<_, google::test::v1::EchoResponse>(
                        extensions,
                        http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                        request,
                        RequestOptions::default(),
                        "test-only-api-client/1.0",
                        "name=test-only",
                    )
                    .await
            })
            .await?;

        let attrs = grpc_request_attributes(&guard);

        // Parse the endpoint to get expected values
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: BTreeMap<String, AttributeValue> = [
            ("otel.name", "google.test.v1.EchoService/Echo".into()),
            ("rpc.system.name", "grpc".into()),
            ("otel.kind", "Client".into()),
            ("rpc.method", "google.test.v1.EchoService/Echo".into()),
            ("server.address", expected_host.clone().into()),
            ("server.port", (expected_port as i64).into()),
            ("url.domain", expected_host.into()),
            ("otel.status_code", "UNSET".into()),
            ("rpc.response.status_code", "OK".into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.service", "test-service".into()),
            ("gcp.client.version", "1.0.0".into()),
            ("gcp.client.artifact", "test-artifact".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_endpoint() -> anyhow::Result<()> {
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
        let recorder = RequestRecorder::new(*TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default().set_url_template("/google.test.v1.EchoService/Echo"),
        );
        let _ = recorder
            .scope(async {
                client
                    .execute::<_, google::test::v1::EchoResponse>(
                        extensions,
                        http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                        request,
                        RequestOptions::default(),
                        "test-only-api-client/1.0",
                        "name=test-only",
                    )
                    .await
            })
            .await?;

        let attrs = grpc_request_attributes(&guard);

        // Verify parsing of the custom endpoint
        // The endpoint string from start_echo_server is like "http://127.0.0.1:12345"
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: BTreeMap<String, AttributeValue> = [
            ("otel.name", "google.test.v1.EchoService/Echo".into()),
            ("rpc.system.name", "grpc".into()),
            ("otel.kind", "Client".into()),
            ("rpc.method", "google.test.v1.EchoService/Echo".into()),
            ("server.address", expected_host.clone().into()),
            ("server.port", (expected_port as i64).into()),
            ("url.domain", "unused.default.com".into()),
            ("otel.status_code", "UNSET".into()),
            ("rpc.response.status_code", "OK".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn regional_endpoint() -> anyhow::Result<()> {
        let guard = TestLayer::initialize();

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());
        config.endpoint = Some("http://foo.bar.rep.googleapis.com".to_string());
        // Disable retries to avoid multiple spans on connection failure
        config.retry_policy = Some(Arc::new(Aip194Strict.with_attempt_limit(1)));

        // We don't need a real server, just need the client to attempt a request.
        // The request will fail, but the span should be created.
        let client = grpc::Client::new(config, "https://foo.googleapis.com").await?;

        let extensions = tonic::Extensions::new();
        let request = google::test::v1::EchoRequest::default();

        // This will fail, but we just want to capture the span
        let recorder = RequestRecorder::new(*TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default().set_url_template("/google.test.v1.EchoService/Echo"),
        );
        let _ = recorder
            .scope(async {
                client
                    .execute::<_, google::test::v1::EchoResponse>(
                        extensions,
                        http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                        request,
                        RequestOptions::default(),
                        "test-client",
                        "",
                    )
                    .await
            })
            .await;

        let attrs = grpc_request_attributes(&guard);

        let expected_attributes: BTreeMap<String, AttributeValue> = [
            ("otel.name", "google.test.v1.EchoService/Echo".into()),
            ("rpc.system.name", "grpc".into()),
            ("otel.kind", "Client".into()),
            ("rpc.method", "google.test.v1.EchoService/Echo".into()),
            ("server.address", "foo.bar.rep.googleapis.com".into()),
            ("server.port", 80_i64.into()),
            ("url.domain", "foo.googleapis.com".into()), // Expect default domain
            ("otel.status_code", "ERROR".into()),
            ("error.type", "CLIENT_CONNECTION_ERROR".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn error_status() -> anyhow::Result<()> {
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
        let recorder = RequestRecorder::new(*TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template("/google.test.v1.EchoService/NonExistentMethod"),
        );
        let _ = recorder
            .scope(async {
                client
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
                    .await
            })
            .await;

        let attrs = grpc_request_attributes(&guard);

        // Parse the endpoint to get expected values
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: BTreeMap<String, AttributeValue> = [
            (
                "otel.name",
                "google.test.v1.EchoService/NonExistentMethod".into(),
            ),
            ("rpc.system.name", "grpc".into()),
            ("otel.kind", "Client".into()),
            (
                "rpc.method",
                "google.test.v1.EchoService/NonExistentMethod".into(),
            ),
            ("server.address", expected_host.clone().into()),
            ("server.port", (expected_port as i64).into()),
            ("url.domain", expected_host.into()),
            ("otel.status_code", "ERROR".into()),
            ("rpc.response.status_code", "UNIMPLEMENTED".into()), // UNIMPLEMENTED = 12
            ("error.type", "UNIMPLEMENTED".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, expected_attributes);

        Ok(())
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_grpc_streaming_span() -> anyhow::Result<()> {
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
        let next = response_stream.next().await;
        assert!(next.is_none(), "{next:?}");

        let attrs = grpc_request_attributes(&guard);

        // Parse the endpoint to get expected values
        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: BTreeMap<String, AttributeValue> = [
            ("otel.name", "google.test.v1.EchoService/Chat".into()),
            ("rpc.system.name", "grpc".into()),
            ("otel.kind", "Client".into()),
            ("rpc.method", "google.test.v1.EchoService/Chat".into()),
            ("server.address", expected_host.clone().into()),
            ("server.port", (expected_port as i64).into()),
            ("url.domain", expected_host.into()),
            ("otel.status_code", "UNSET".into()),
            ("rpc.response.status_code", "OK".into()), // OK = 0
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, expected_attributes);

        Ok(())
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_error() -> anyhow::Result<()> {
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
        assert!(response.is_some(), "{response_stream:?}");
        let result = response.unwrap();
        assert!(result.is_err(), "{result:?}");
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        // Close the stream
        drop(tx);

        let attrs = grpc_request_attributes(&guard);

        let uri: http::Uri = endpoint.parse().unwrap();
        let expected_host = uri.host().unwrap().to_string();
        let expected_port = uri.port_u16().unwrap();

        let expected_attributes: BTreeMap<String, AttributeValue> = [
            ("otel.name", "google.test.v1.EchoService/Chat".into()),
            ("rpc.system.name", "grpc".into()),
            ("otel.kind", "Client".into()),
            ("rpc.method", "google.test.v1.EchoService/Chat".into()),
            ("server.address", expected_host.clone().into()),
            ("server.port", (expected_port as i64).into()),
            ("url.domain", expected_host.into()),
            ("otel.status_code", "ERROR".into()),
            ("rpc.response.status_code", "INVALID_ARGUMENT".into()), // INVALID_ARGUMENT = 3
            ("error.type", "INVALID_ARGUMENT".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation() -> anyhow::Result<()> {
        use std::time::Duration;

        let guard = TestLayer::initialize();

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());

        // Use a a real but non-existent address to ensure the request hangs and stays Pending,
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

        let recorder = RequestRecorder::new(*TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default().set_url_template("/google.test.v1.EchoService/Echo"),
        );
        let future = client.execute::<_, google::test::v1::EchoResponse>(
            extensions,
            http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
            request,
            RequestOptions::default(),
            "test-client",
            "",
        );
        let scoped_future = recorder.scope(future);

        // Poll the future once to ensure the span is created and entered, then drop it
        // We use `tokio::time::timeout` with a very short duration to force a drop
        let _ = tokio::time::timeout(Duration::from_micros(1), scoped_future).await;

        // Wait a bit for the span to be processed (though drop should happen immediately)
        tokio::time::sleep(Duration::from_millis(10)).await;

        let attrs = grpc_request_attributes(&guard);

        let expected_attributes: BTreeMap<String, AttributeValue> = [
            ("otel.name", "google.test.v1.EchoService/Echo".into()),
            ("rpc.system.name", "grpc".into()),
            ("otel.kind", "Client".into()),
            ("rpc.method", "google.test.v1.EchoService/Echo".into()),
            ("server.address", "192.0.2.1".into()),
            ("server.port", 1234_i64.into()),
            ("url.domain", "192.0.2.1".into()),
            ("otel.status_code", "ERROR".into()),
            ("error.type", "CLIENT_CANCELLED".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, expected_attributes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resource_name_in_span() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let guard = TestLayer::initialize();

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());
        let client = grpc::Client::new(config, &endpoint).await?;

        let options = RequestOptions::default().insert_extension(ResourceName(
            "projects/p/locations/l/resources/r".to_string(),
        ));

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new("google.test.v1.EchoService", "Echo"));
            e
        };
        let request = google::test::v1::EchoRequest {
            message: "test message".into(),
            ..Default::default()
        };

        let recorder = RequestRecorder::new(*TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template("/google.test.v1.EchoService/Echo")
                .set_resource_name("projects/p/locations/l/resources/r".to_string()),
        );
        let _ = recorder
            .scope(async {
                client
                    .execute::<_, google::test::v1::EchoResponse>(
                        extensions,
                        http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                        request,
                        options,
                        "test-client",
                        "",
                    )
                    .await
            })
            .await?;

        let attrs = grpc_request_attributes(&guard);

        assert_eq!(
            attrs.get("gcp.resource.destination.id"),
            Some(&AttributeValue::String(
                "projects/p/locations/l/resources/r".into()
            )),
            "{attrs:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn propagate_trace_context() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.cred = Some(test_credentials());
        let client = grpc::Client::new(config, &endpoint).await?;

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = opentelemetry::trace::TracerProvider::tracer(&tracer_provider, "test");
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        use tracing_subscriber::layer::SubscriberExt;
        let subscriber = tracing_subscriber::registry().with(telemetry);
        let _guard = tracing::subscriber::set_default(subscriber);

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

        use tracing::Instrument;
        let span = tracing::info_span!("parent_span");
        let response = client
            .execute::<_, google::test::v1::EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                RequestOptions::default(),
                "test-client",
                "",
            )
            .instrument(span)
            .await?;

        let inner = response.into_inner();
        assert!(
            inner.metadata.contains_key("traceparent"),
            "Metadata should contain traceparent. Metadata: {:?}",
            inner.metadata
        );

        Ok(())
    }
}
