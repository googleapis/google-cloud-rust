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

/// Validates that HTTP tracing makes it all the way to OTLP collectors like
/// Cloud Telemetry.
///
/// This test sets up an in-memory service endpoint and OTLP collector
/// endpoint and installs a standard Rust tracing -> authenticated OTLP tracer
/// provider.  Then it uses the showcase client library to make a request and
/// checks that the right spans are collected.
///
/// This makes sure that the end-to-end system of tracing to OpenTelemetry
/// works as intended, value types are preserved, etc.
#[cfg(google_cloud_unstable_tracing)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_tracing_to_otlp() -> anyhow::Result<()> {
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use integration_tests::observability::mock_collector::MockCollector;
    use integration_tests::observability::otlp::CloudTelemetryTracerProviderBuilder;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // 1. Start Mock OTLP Collector
    let mock_collector = MockCollector::default();
    let otlp_endpoint = mock_collector.start().await;

    // 2. Configure OTel Provider
    let provider = CloudTelemetryTracerProviderBuilder::new("test-project", "integration-tests")
        .with_endpoint(otlp_endpoint)
        .with_credentials(auth::credentials::anonymous::Builder::new().build())
        .build()
        .await?;

    // 3. Install Tracing Subscriber
    let _guard = tracing_subscriber::Registry::default()
        .with(integration_tests::observability::tracing::layer(
            provider.clone(),
        ))
        .set_default();

    // 4. Start Mock HTTP Server (Showcase Echo)
    let echo_server = Server::run();
    echo_server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:echo"),
        ])
        .respond_with(status_code(200).body(r#"{"content": "test"}"#)),
    );

    // 5. Configure Client
    let endpoint = echo_server.url("/").to_string();
    let endpoint = endpoint.trim_end_matches('/');
    let client = showcase::client::Echo::builder()
        .with_endpoint(endpoint)
        .with_credentials(auth::credentials::anonymous::Builder::new().build())
        .with_tracing()
        .build()
        .await?;

    // 6. Make Request
    let _ = client.echo().set_content("test").send().await;

    // 7. Flush Spans
    let _ = provider.force_flush();

    // 8. Verify Spans
    let requests = mock_collector.requests.lock().unwrap();
    assert!(
        !requests.is_empty(),
        "Should have received at least one OTLP request"
    );

    let request = &requests[0];
    assert!(
        !request.resource_spans.is_empty(),
        "Should have received at least one resource span"
    );
    let scope_spans = &request.resource_spans[0].scope_spans;
    assert!(!scope_spans.is_empty(), "Should have scope spans");
    let spans = &scope_spans[0].spans;
    assert!(!spans.is_empty(), "Should have spans");

    // Verify we captured the client span
    let client_span = spans.iter().find(|s| s.kind == 3 /* CLIENT */); // 3 is SPAN_KIND_CLIENT
    assert!(client_span.is_some(), "Should have a CLIENT span");

    // 9. Verify HTTP Span Details
    let client_span = client_span.unwrap();
    assert_eq!(client_span.name, "POST /v1beta1/echo:echo");

    let attributes: std::collections::HashMap<String, _> = client_span
        .attributes
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone().unwrap()))
        .collect();

    // Helper to get string value from AnyValue
    let get_string = |key: &str| -> Option<String> {
        attributes.get(key).and_then(|v| match &v.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                Some(s.clone())
            }
            _ => None,
        })
    };

    assert_eq!(get_string("http.request.method").as_deref(), Some("POST"));
    assert_eq!(
        get_string("gcp.client.repo").as_deref(),
        Some("googleapis/google-cloud-rust")
    );
    assert!(get_string("gcp.client.version").is_some());

    Ok(())
}

#[cfg(google_cloud_unstable_tracing)]
async fn setup_echo_client() -> (
    google_cloud_test_utils::test_layer::TestLayerGuard,
    httptest::Server,
    showcase::client::Echo,
    u16,
) {
    use google_cloud_test_utils::test_layer::TestLayer;
    use httptest::Server;

    let guard = TestLayer::initialize();
    let server = Server::run();
    let endpoint = server.url("/").to_string();
    let endpoint = endpoint.trim_end_matches('/');
    let client = showcase::client::Echo::builder()
        .with_endpoint(endpoint)
        .with_credentials(auth::credentials::anonymous::Builder::new().build())
        .with_tracing()
        .build()
        .await
        .expect("failed to build client");
    let port = server.addr().port();

    (guard, server, client, port)
}

#[cfg(google_cloud_unstable_tracing)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_tracing_success_testlayer() -> anyhow::Result<()> {
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use httptest::{Expectation, matchers::*, responders::status_code};
    use std::collections::HashMap;

    let (guard, echo_server, client, server_port) = setup_echo_client().await;

    echo_server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:echo"),
        ])
        .respond_with(status_code(200).body(r#"{"content": "test"}"#)),
    );

    let _ = client.echo().set_content("test").send().await?;

    let spans = TestLayer::capture(&guard);

    let client_request_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "client_request")
        .collect();

    assert_eq!(
        client_request_spans.len(),
        1,
        "Should have exactly one 'client_request' span. Found spans: {:?}",
        spans
    );

    let t3_span = client_request_spans[0];
    let expected_otel_name = "google-cloud-showcase-v1beta1::client::Echo::echo";

    let expected_attributes: HashMap<String, AttributeValue> = [
        ("otel.name", expected_otel_name.into()),
        ("otel.kind", "Internal".into()),
        ("rpc.system", "http".into()),
        ("rpc.service", "showcase".into()),
        ("rpc.method", "echo".into()),
        ("gcp.client.service", "showcase".into()),
        ("gcp.client.version", "1.0.0".into()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        (
            "gcp.client.artifact",
            "google-cloud-showcase-v1beta1".into(),
        ),
        ("gcp.client.language", "rust".into()),
        ("otel.status_code", "OK".into()),
        ("gax.client.span", true.into()),
        ("http.response.status_code", 200_i64.into()),
        ("http.request.method", "POST".into()),
        ("server.address", "::1".into()),
        ("server.port", (server_port as i64).into()),
        (
            "url.full",
            format!(
                "http://[::1]:{}/v1beta1/echo:echo?%24alt=json%3Benum-encoding%3Dint",
                server_port
            )
            .into(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    assert_eq!(t3_span.attributes, expected_attributes);

    Ok(())
}

#[cfg(google_cloud_unstable_tracing)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_tracing_parse_error() -> anyhow::Result<()> {
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use httptest::{Expectation, matchers::*, responders::status_code};
    use std::collections::HashMap;

    let (guard, echo_server, client, server_port) = setup_echo_client().await;

    // Return invalid JSON (missing closing brace)
    echo_server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:echo"),
        ])
        .respond_with(status_code(200).body(r#"{"content": "test""#)),
    );

    let result = client.echo().set_content("test").send().await;
    assert!(result.is_err(), "Request should fail due to parse error");

    let spans = TestLayer::capture(&guard);

    let client_request_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "client_request")
        .collect();

    assert_eq!(
        client_request_spans.len(),
        1,
        "Should have exactly one 'client_request' span. Found spans: {:?}",
        spans
    );

    let t3_span = client_request_spans[0];
    let expected_otel_name = "google-cloud-showcase-v1beta1::client::Echo::echo";

    let expected_attributes: HashMap<String, AttributeValue> = [
        ("otel.name", expected_otel_name.into()),
        ("otel.kind", "Internal".into()),
        ("rpc.system", "http".into()),
        ("rpc.service", "showcase".into()),
        ("rpc.method", "echo".into()),
        ("gcp.client.service", "showcase".into()),
        ("gcp.client.version", "1.0.0".into()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        (
            "gcp.client.artifact",
            "google-cloud-showcase-v1beta1".into(),
        ),
        ("gcp.client.language", "rust".into()),
        ("otel.status_code", "ERROR".into()),
        ("gax.client.span", true.into()),
        ("http.response.status_code", 200_i64.into()),
        ("http.request.method", "POST".into()),
        ("error.type", "CLIENT_RESPONSE_DECODE_ERROR".into()),
        (
            "otel.status_description",
            "cannot deserialize the response EOF while parsing an object at line 1 column 18"
                .into(),
        ),
        ("server.address", "::1".into()),
        ("server.port", (server_port as i64).into()),
        (
            "url.full",
            format!(
                "http://[::1]:{}/v1beta1/echo:echo?%24alt=json%3Benum-encoding%3Dint",
                server_port
            )
            .into(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    assert_eq!(t3_span.attributes, expected_attributes);

    Ok(())
}

#[cfg(google_cloud_unstable_tracing)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_tracing_api_error() -> anyhow::Result<()> {
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use httptest::{Expectation, matchers::*, responders::status_code};
    use std::collections::HashMap;

    let (guard, echo_server, client, server_port) = setup_echo_client().await;

    // 404 Not Found
    echo_server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:echo"),
        ])
        .respond_with(status_code(404).body(r#"{"error": {"code": 404, "message": "Not Found"}}"#)),
    );

    let result = client.echo().set_content("test").send().await;
    assert!(result.is_err(), "Request should fail with API error");

    let spans = TestLayer::capture(&guard);

    let client_request_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "client_request")
        .collect();

    assert_eq!(
        client_request_spans.len(),
        1,
        "Should have exactly one 'client_request' span. Found spans: {:?}",
        spans
    );

    let t3_span = client_request_spans[0];
    let expected_otel_name = "google-cloud-showcase-v1beta1::client::Echo::echo";

    let expected_attributes: HashMap<String, AttributeValue> = [
        ("otel.name", expected_otel_name.into()),
        ("otel.kind", "Internal".into()),
        ("rpc.system", "http".into()),
        ("rpc.service", "showcase".into()),
        ("rpc.method", "echo".into()),
        ("gcp.client.service", "showcase".into()),
        ("gcp.client.version", "1.0.0".into()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        (
            "gcp.client.artifact",
            "google-cloud-showcase-v1beta1".into(),
        ),
        ("gcp.client.language", "rust".into()),
        ("otel.status_code", "ERROR".into()),
        ("gax.client.span", true.into()),
        ("http.response.status_code", 404_i64.into()),
        ("http.request.method", "POST".into()),
        ("error.type", "UNKNOWN".into()),
        (
            "otel.status_description",
            "the service reports an error with code UNKNOWN described as: Not Found".into(),
        ),
        ("server.address", "::1".into()),
        ("server.port", (server_port as i64).into()),
        (
            "url.full",
            format!(
                "http://[::1]:{}/v1beta1/echo:echo?%24alt=json%3Benum-encoding%3Dint",
                server_port
            )
            .into(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    assert_eq!(t3_span.attributes, expected_attributes);

    Ok(())
}
