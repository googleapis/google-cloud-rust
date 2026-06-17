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

#![cfg(google_cloud_unstable_tracing)]

use super::Anonymous;
use google_cloud_lro::Poller;
use google_cloud_showcase_v1beta1::client::Echo;
use google_cloud_test_utils::test_layer::{
    AttributeValue, CapturedSpan, TestLayer, TestLayerGuard,
};
use httptest::{Expectation, Server, cycle, matchers::*, responders::status_code};

/// Sets up a mock Server and an Echo client configured with tracing.
async fn setup_echo_client() -> (TestLayerGuard, Server, Echo) {
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

/// The main entry point to run all Long-Running Operation (LRO) tracing integration tests.
pub async fn lro_tracing_testlayer() -> anyhow::Result<()> {
    let (guard, mut server, client) = setup_echo_client().await;

    // Run the successful LRO polling path test.
    test_lro_success(&guard, &server, client.clone()).await?;
    server.verify_and_clear();

    test_lro_logical_error(&guard, &server, client.clone()).await?;
    server.verify_and_clear();

    Ok(())
}

fn attribute_str<'a>(span: &'a CapturedSpan, name: &str) -> Option<&'a str> {
    span.attributes.get(name).and_then(|v| match v {
        AttributeValue::String(s) => Some(s.as_ref()),
        _ => None,
    })
}

fn attribute_u64(span: &CapturedSpan, name: &str) -> Option<u64> {
    span.attributes.get(name).and_then(|v| match v {
        AttributeValue::UInt64(u) => Some(*u),
        _ => None,
    })
}

fn attribute_i64(span: &CapturedSpan, name: &str) -> Option<i64> {
    span.attributes.get(name).and_then(|v| v.as_i64())
}

fn attribute_bool(span: &CapturedSpan, name: &str) -> Option<bool> {
    span.attributes.get(name).and_then(|v| v.as_bool())
}

/// Helper to filter for T3 client request query/poll spans (GetOperation).
fn find_get_operation_spans(spans: &[CapturedSpan]) -> Vec<&CapturedSpan> {
    spans
        .iter()
        .filter(|s| {
            s.name == "client_request"
                && s.attributes
                    .get("rpc.method")
                    .map(|v| {
                        v.as_string()
                            == Some("google.longrunning.Operations/GetOperation".to_string())
                    })
                    .unwrap_or(false)
        })
        .collect()
}

/// Tests a successful LRO polling workflow where the LRO completes after two poll attempts.
async fn test_lro_success(
    guard: &TestLayerGuard,
    server: &Server,
    client: Echo,
) -> anyhow::Result<()> {
    // Expect the initial POST request that starts the LRO.
    server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:wait"),
        ])
        .respond_with(status_code(200).body(r#"{"name": "operations/wait-1", "done": false}"#)),
    );

    // Expect the subsequent GET requests to query operation status (polled twice).
    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path("/v1beta1/operations/wait-1"),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(200).body(r#"{"name": "operations/wait-1", "done": false}"#),
            status_code(200).body(
                r#"
                {
                    "name": "operations/wait-1",
                    "done": true,
                    "response": {
                        "@type": "type.googleapis.com/google.showcase.v1beta1.WaitResponse",
                        "content": "success-content"
                    }
                }
            "#
            ),
        ]),
    );

    let res = client.wait().poller().until_done().await?;
    assert_eq!(res.content, "success-content");

    let spans = TestLayer::capture(guard);

    // 1. T2 span "LRO Wait"
    let lro_wait_span = spans
        .iter()
        .find(|s| s.name == "LRO Wait")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Wait span in {spans:#?}"))?;

    assert_eq!(
        attribute_str(lro_wait_span, "otel.name"),
        Some("google_cloud_showcase_v1beta1::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "gcp.rpc.method"),
        Some("google_cloud_showcase_v1beta1::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "gcp.resource.destination.id"),
        Some("operations/wait-1")
    );
    assert_eq!(attribute_str(lro_wait_span, "otel.status_code"), None);

    // 2. Client request spans for get_operation (T3 spans)
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(
        get_op_spans.len(),
        2,
        "expected 2 get_operation spans: {spans:#?}"
    );

    // GetOperation attempt 1
    let span0 = get_op_spans[0];
    assert_eq!(
        attribute_u64(span0, "gcp.longrunning.poll_attempt_count"),
        Some(1)
    );
    assert_eq!(
        attribute_str(span0, "gcp.resource.destination.id"),
        Some("operations/wait-1")
    );
    assert_eq!(attribute_bool(span0, "gcp.longrunning.done"), Some(false));
    assert_eq!(attribute_str(span0, "otel.status_code"), Some("UNSET"));

    // GetOperation attempt 2
    let span1 = get_op_spans[1];
    assert_eq!(
        attribute_u64(span1, "gcp.longrunning.poll_attempt_count"),
        Some(2)
    );
    assert_eq!(
        attribute_str(span1, "gcp.resource.destination.id"),
        Some("operations/wait-1")
    );
    assert_eq!(attribute_bool(span1, "gcp.longrunning.done"), Some(true));
    assert_eq!(attribute_i64(span1, "gcp.longrunning.status_code"), Some(0));
    assert_eq!(attribute_str(span1, "otel.status_code"), Some("UNSET"));

    let _lro_sleep_span = spans
        .iter()
        .find(|s| s.name == "LRO Sleep")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Sleep span in {spans:#?}"))?;

    Ok(())
}

/// Tests a logical LRO failure where the final query status returns completed with an error.
async fn test_lro_logical_error(
    guard: &TestLayerGuard,
    server: &Server,
    client: Echo,
) -> anyhow::Result<()> {
    server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:wait"),
        ])
        .respond_with(status_code(200).body(r#"{"name": "operations/wait-2", "done": false}"#)),
    );

    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path("/v1beta1/operations/wait-2"),
        ])
        .respond_with(status_code(200).body(
            r#"
            {
                "name": "operations/wait-2",
                "done": true,
                "error": {
                    "code": 3,
                    "message": "logical-error-msg"
                }
            }
        "#,
        )),
    );

    let res = client.wait().poller().until_done().await;
    assert!(res.is_err());
    let err_msg = res.err().unwrap().to_string();
    assert!(
        err_msg.contains("logical-error-msg"),
        "error message: {err_msg}"
    );

    let spans = TestLayer::capture(guard);

    // 1. T2 span "LRO Wait" should be ERROR
    let lro_wait_span = spans
        .iter()
        .find(|s| s.name == "LRO Wait")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Wait span in {spans:#?}"))?;

    assert_eq!(
        attribute_str(lro_wait_span, "otel.name"),
        Some("google_cloud_showcase_v1beta1::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "gcp.rpc.method"),
        Some("google_cloud_showcase_v1beta1::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "otel.status_code"),
        Some("ERROR")
    );
    let actual_desc = attribute_str(lro_wait_span, "otel.status_description").unwrap_or_default();
    assert!(
        actual_desc.contains("logical-error-msg"),
        "expected description to contain 'logical-error-msg', got '{actual_desc}'"
    );
    assert_eq!(
        attribute_str(lro_wait_span, "error.type"),
        Some("INVALID_ARGUMENT")
    );

    // 2. T3 span "client_request" (get_operation) should have error details
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(
        get_op_spans.len(),
        1,
        "expected exactly 1 GetOperation span"
    );
    let get_op_span = get_op_spans[0];

    assert_eq!(
        attribute_u64(get_op_span, "gcp.longrunning.poll_attempt_count"),
        Some(1)
    );
    assert_eq!(
        attribute_bool(get_op_span, "gcp.longrunning.done"),
        Some(true)
    );
    assert_eq!(
        attribute_i64(get_op_span, "gcp.longrunning.status_code"),
        Some(3)
    );
    assert_eq!(
        attribute_str(get_op_span, "otel.status_code"),
        Some("ERROR")
    );
    assert_eq!(
        attribute_str(get_op_span, "otel.status_description"),
        Some("logical-error-msg")
    );
    assert_eq!(
        attribute_str(get_op_span, "error.type"),
        Some("INVALID_ARGUMENT")
    );

    Ok(())
}
