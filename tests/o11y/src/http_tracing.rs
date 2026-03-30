// Copyright 2026 Google LLC
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

use super::Anonymous;
use crate::mock_collector::MockCollector;
use crate::otlp::logs::Builder as LoggerProviderBuilder;
use crate::otlp::trace::Builder as TracerProviderBuilder;
use google_cloud_showcase_v1beta1::client::Echo;
use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer, format_server_address};
use httptest::{Expectation, Server, matchers::*, responders::status_code};
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// The query parameters injected by the client library.
///
/// A constant to make the expected values slightly more readable.
const EXPECTED_QUERY_PARAMETERS: &str =
    "%24alt=json%3Benum-encoding%3Dint&%24apiVersion=v1_20240408";

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
pub async fn to_otlp() -> anyhow::Result<()> {
    // 1. Start Mock OTLP Collector
    let mock_collector = MockCollector::default();
    let otlp_endpoint = mock_collector.start().await;

    // 2. Configure OTel Provider
    let provider = TracerProviderBuilder::new("test-project", "integration-tests")
        .with_endpoint(otlp_endpoint)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    // 3. Install Tracing Subscriber
    let _guard = tracing_subscriber::Registry::default()
        .with(crate::tracing::trace_layer(provider.clone()))
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
    let client = Echo::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await?;

    // 6. Make Request
    let _ = client.echo().set_content("test").send().await;

    // 7. Flush Spans
    let _ = provider.force_flush();

    // 8. Verify Spans
    let (_metadata, _, request) = mock_collector
        .traces
        .lock()
        .expect("never poisoned")
        .pop()
        .expect("should have received at least one trace request")
        .into_parts();

    let scope_spans = &request.resource_spans[0].scope_spans;
    assert!(
        !scope_spans.is_empty(),
        "request {request:?} should have scope spans"
    );
    let spans = &scope_spans[0].spans;
    assert!(!spans.is_empty(), "{request:?} should have spans");

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
    assert!(get_string("gcp.client.version").is_some(), "{attributes:?}");

    Ok(())
}

/// Validates that an HTTP error carrying AIP-193 ErrorInfo extracts
/// the metadata and records them as attributes and events exported to OTLP.
pub async fn to_otlp_debug_event() -> anyhow::Result<()> {
    let mock_collector = MockCollector::default();
    let otlp_endpoint = mock_collector.start().await;

    let tracer_provider = TracerProviderBuilder::new("test-project", "integration-tests")
        .with_endpoint(otlp_endpoint.clone())
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let logger_provider = LoggerProviderBuilder::new("test-project", "integration-tests")
        .with_endpoint(otlp_endpoint.parse().expect("Failed to parse URI"))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let _guard = tracing_subscriber::Registry::default()
        .with(crate::tracing::trace_layer(tracer_provider.clone()))
        .with(crate::tracing::log_layer(logger_provider.clone()))
        .set_default();

    let echo_server = Server::run();
    // A mock structured ErrorInfo response matching what GCP would return in JSON
    let error_body = r#"{
        "error": {
            "code": 400,
            "message": "API key not valid",
            "status": "INVALID_ARGUMENT",
            "details": [
                {
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "API_KEY_INVALID",
                    "domain": "pubsub.googleapis.com",
                    "metadata": {
                        "zone": "us-east1-a",
                        "project": "test-project"
                    }
                }
            ]
        }
    }"#;

    echo_server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:echo"),
        ])
        .respond_with(status_code(400).body(error_body)),
    );

    let endpoint = echo_server.url("/").to_string();
    let endpoint = endpoint.trim_end_matches('/');
    let client = Echo::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await?;

    // Execute the request; expect evaluating to Err
    let result = client.echo().set_content("test").send().await;
    assert!(
        result.is_err(),
        "Expected the request to fail with an HTTP 400 error"
    );

    tracer_provider
        .force_flush()
        .expect("Failed to flush tracing provider");

    let (_metadata, _, request) = mock_collector
        .traces
        .lock()
        .expect("never poisoned")
        .pop()
        .expect("should have received trace export request")
        .into_parts();

    let resource_span = request
        .resource_spans
        .first()
        .expect("request should have resource spans");
    let scope_span = resource_span
        .scope_spans
        .first()
        .expect("should have scope spans");
    let spans = &scope_span.spans;

    let attempt_span = spans
        .iter()
        .find(|s| s.kind == 3 /* CLIENT */)
        .expect("Should have found a SPAN_KIND_CLIENT span");

    logger_provider
        .force_flush()
        .expect("Failed to flush logger provider");

    let logs_requests = mock_collector.logs.lock().unwrap();
    let log_event = logs_requests
        .iter()
        .flat_map(|r| r.get_ref().resource_logs.clone())
        .flat_map(|rl| rl.scope_logs)
        // Many things emit debug logs, we are only interested in the L4 logs.
        .filter(|sl| {
            sl.scope
                .as_ref()
                .is_some_and(|i| i.name == "google_cloud_gax_internal::observability::errors")
        })
        .flat_map(|sl| sl.log_records)
        .find(|l| l.span_id == attempt_span.span_id)
        .unwrap_or_else(|| {
            panic!(
                "cannot find log matching span {:?} in capture: {logs_requests:#?}",
                attempt_span.span_id
            )
        });

    // Ensure the log was correctly correlated back to the original client request trace buffer (attempt span)
    assert_eq!(
        log_event.trace_id, attempt_span.trace_id,
        "Log traceId correlation failed"
    );
    assert_eq!(
        log_event.span_id, attempt_span.span_id,
        "Log spanId correlation failed"
    );

    let expected_event_attributes: std::collections::BTreeMap<String, String> = [
        ("error.type", "API_KEY_INVALID"),
        ("gcp.errors.domain", "pubsub.googleapis.com"),
        (
            "gcp.errors.metadata",
            r#"{"project":"test-project","zone":"us-east1-a"}"#,
        ),
        ("http.response.status_code", "400"),
        ("rpc.response.status_code", "INVALID_ARGUMENT"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    let got = std::collections::BTreeMap::from_iter(log_event.attributes.iter().map(|kv| {
        let val_str = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                s.clone()
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                i.to_string()
            }
            _ => format!("{:?}", kv.value),
        };
        (kv.key.clone(), val_str)
    }));

    for (key, expected) in &expected_event_attributes {
        assert_eq!(
            got.get(key),
            Some(expected),
            "mismatch for key: {key} in got: {got:?}\n\nrequests = {logs_requests:#?}\n"
        );
    }

    // Verify DEBUG level was correctly mapped to the OTLP log object.
    assert_eq!(log_event.severity_text, "DEBUG", "severity_text mismatch");

    Ok(())
}

async fn setup_echo_client() -> (
    google_cloud_test_utils::test_layer::TestLayerGuard,
    httptest::Server,
    Echo,
) {
    let guard = TestLayer::initialize();
    let server = Server::run();
    let endpoint = server.url("/").to_string();
    let endpoint = endpoint.trim_end_matches('/');
    let client = Echo::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await
        .expect("failed to build client");

    (guard, server, client)
}

pub async fn success_testlayer() -> anyhow::Result<()> {
    let (guard, echo_server, client) = setup_echo_client().await;
    let server_addr = echo_server.addr();

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
    let expected_otel_name = "google_cloud_showcase_v1beta1::client::Echo::echo";

    // In general it is bad practice to use the "got" data in a comparison. We
    // care that the key exists, and we cannot hard-code the value because the
    // client library version is bumped from time to time.
    let version = t3_span.attributes.get("gcp.client.version").unwrap();
    let expected_attributes: BTreeMap<String, AttributeValue> = [
        ("otel.name", expected_otel_name.into()),
        ("otel.kind", "Internal".into()),
        ("rpc.system", "http".into()),
        ("rpc.service", "showcase".into()),
        ("rpc.method", "google.showcase.v1beta1.Echo/Echo".into()),
        ("gcp.client.service", "showcase".into()),
        ("gcp.client.version", version.clone()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        (
            "gcp.client.artifact",
            "google-cloud-showcase-v1beta1".into(),
        ),
        (
            "gcp.schema.url",
            "https://opentelemetry.io/schemas/1.39.0".into(),
        ),
        ("otel.status_code", "UNSET".into()),
        ("http.response.status_code", 200_i64.into()),
        ("http.request.method", "POST".into()),
        ("server.address", format_server_address(server_addr).into()),
        ("server.port", (server_addr.port() as i64).into()),
        ("network.peer.address", server_addr.ip().to_string().into()),
        ("network.peer.port", (server_addr.port() as i64).into()),
        (
            "url.full",
            format!(
                "http://{}/v1beta1/echo:echo?{EXPECTED_QUERY_PARAMETERS}",
                server_addr
            )
            .into(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let got = BTreeMap::from_iter(t3_span.attributes.clone());
    assert_eq!(got, expected_attributes);

    Ok(())
}

pub async fn parse_error() -> anyhow::Result<()> {
    let (guard, echo_server, client) = setup_echo_client().await;
    let server_addr = echo_server.addr();

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
    let expected_otel_name = "google_cloud_showcase_v1beta1::client::Echo::echo";

    // In general it is bad practice to use the "got" data in a comparison. We
    // care that the key exists, and we cannot hard-code the value because the
    // client library version is bumped from time to time.
    let version = t3_span.attributes.get("gcp.client.version").unwrap();
    let expected_attributes: BTreeMap<String, AttributeValue> = [
        ("otel.name", expected_otel_name.into()),
        ("otel.kind", "Internal".into()),
        ("rpc.system", "http".into()),
        ("rpc.service", "showcase".into()),
        ("rpc.method", "google.showcase.v1beta1.Echo/Echo".into()),
        ("gcp.client.service", "showcase".into()),
        ("gcp.client.version", version.clone()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        (
            "gcp.client.artifact",
            "google-cloud-showcase-v1beta1".into(),
        ),
        (
            "gcp.schema.url",
            "https://opentelemetry.io/schemas/1.39.0".into(),
        ),
        ("otel.status_code", "ERROR".into()),
        ("http.response.status_code", 200_i64.into()),
        ("http.request.method", "POST".into()),
        ("error.type", "CLIENT_RESPONSE_DECODE_ERROR".into()),
        (
            "otel.status_description",
            "cannot deserialize the response EOF while parsing an object at line 1 column 18"
                .into(),
        ),
        ("server.address", format_server_address(server_addr).into()),
        ("server.port", (server_addr.port() as i64).into()),
        ("network.peer.address", server_addr.ip().to_string().into()),
        ("network.peer.port", (server_addr.port() as i64).into()),
        (
            "url.full",
            format!(
                "http://{}/v1beta1/echo:echo?{EXPECTED_QUERY_PARAMETERS}",
                server_addr
            )
            .into(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let got = BTreeMap::from_iter(t3_span.attributes.clone());
    assert_eq!(got, expected_attributes);

    Ok(())
}

pub async fn api_error() -> anyhow::Result<()> {
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use httptest::{Expectation, matchers::*, responders::status_code};

    let (guard, echo_server, client) = setup_echo_client().await;
    let server_addr = echo_server.addr();

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
    let expected_otel_name = "google_cloud_showcase_v1beta1::client::Echo::echo";

    // In general it is bad practice to use the "got" data in a comparison. We
    // care that the key exists, and we cannot hard-code the value because the
    // client library version is bumped from time to time.
    let version = t3_span.attributes.get("gcp.client.version").unwrap();
    let expected_attributes: BTreeMap<String, AttributeValue> = [
        ("otel.name", expected_otel_name.into()),
        ("otel.kind", "Internal".into()),
        ("rpc.system", "http".into()),
        ("rpc.service", "showcase".into()),
        ("rpc.method", "google.showcase.v1beta1.Echo/Echo".into()),
        ("gcp.client.service", "showcase".into()),
        ("gcp.client.version", version.clone()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        (
            "gcp.client.artifact",
            "google-cloud-showcase-v1beta1".into(),
        ),
        (
            "gcp.schema.url",
            "https://opentelemetry.io/schemas/1.39.0".into(),
        ),
        ("otel.status_code", "ERROR".into()),
        ("http.request.method", "POST".into()),
        ("http.response.status_code", 404_i64.into()),
        ("error.type", "UNKNOWN".into()),
        (
            "otel.status_description",
            "the service reports an error with code UNKNOWN described as: Not Found".into(),
        ),
        ("server.address", format_server_address(server_addr).into()),
        ("server.port", (server_addr.port() as i64).into()),
        ("network.peer.address", server_addr.ip().to_string().into()),
        ("network.peer.port", (server_addr.port() as i64).into()),
        (
            "url.full",
            format!(
                "http://{}/v1beta1/echo:echo?{EXPECTED_QUERY_PARAMETERS}",
                server_addr
            )
            .into(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let got = BTreeMap::from_iter(t3_span.attributes.clone());
    assert_eq!(got, expected_attributes);

    Ok(())
}
