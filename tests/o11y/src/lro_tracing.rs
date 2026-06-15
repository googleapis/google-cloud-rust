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

    Ok(())
}

/// Helper to assert attributes on the T2 "LRO Wait" span.
fn assert_lro_wait_span(
    span: &CapturedSpan,
    expected_method: &str,
    expected_destination_id: Option<&str>,
    expected_status: Option<&str>,
    expected_description_contains: Option<&str>,
) {
    assert_eq!(
        span.attributes.get("otel.name").and_then(|v| v.as_string()),
        Some(expected_method.to_string())
    );
    assert_eq!(
        span.attributes
            .get("gcp.rpc.method")
            .and_then(|v| v.as_string()),
        Some(expected_method.to_string())
    );
    if let Some(dest_id) = expected_destination_id {
        assert_eq!(
            span.attributes
                .get("gcp.resource.destination.id")
                .and_then(|v| v.as_string()),
            Some(dest_id.to_string())
        );
    }
    assert_eq!(
        span.attributes
            .get("otel.status_code")
            .and_then(|v| v.as_string()),
        expected_status.map(|s| s.to_string())
    );
    if let Some(desc) = expected_description_contains {
        let actual_desc = span
            .attributes
            .get("otel.status_description")
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        assert!(
            actual_desc.contains(desc),
            "expected description to contain '{}', got '{}'",
            desc,
            actual_desc
        );
    }
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

/// Helper to assert attributes on a T3 query/poll span.
fn assert_get_operation_span(
    span: &CapturedSpan,
    expected_poll_attempt: u64,
    expected_destination_id: Option<&str>,
    expected_done: Option<bool>,
    expected_status_code: Option<i64>,
    expected_otel_status: &str,
    expected_otel_desc: Option<&str>,
) {
    assert_eq!(
        span.attributes.get("gcp.longrunning.poll_attempt_count"),
        Some(&AttributeValue::UInt64(expected_poll_attempt))
    );
    if let Some(dest_id) = expected_destination_id {
        assert_eq!(
            span.attributes
                .get("gcp.resource.destination.id")
                .and_then(|v| v.as_string()),
            Some(dest_id.to_string())
        );
    }
    if let Some(done) = expected_done {
        assert_eq!(
            span.attributes
                .get("gcp.longrunning.done")
                .and_then(|v| v.as_bool()),
            Some(done)
        );
    }
    if let Some(status) = expected_status_code {
        assert_eq!(
            span.attributes
                .get("gcp.longrunning.status_code")
                .and_then(|v| v.as_i64()),
            Some(status)
        );
    }
    assert_eq!(
        span.attributes
            .get("otel.status_code")
            .and_then(|v| v.as_string()),
        Some(expected_otel_status.to_string())
    );
    if let Some(desc) = expected_otel_desc {
        assert_eq!(
            span.attributes
                .get("otel.status_description")
                .and_then(|v| v.as_string()),
            Some(desc.to_string())
        );
    }
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

    assert_lro_wait_span(
        lro_wait_span,
        "google_cloud_showcase_v1beta1::client::Echo::wait::until_done",
        Some("operations/wait-1"),
        None,
        None,
    );

    // 2. Client request spans for get_operation (T3 spans)
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(
        get_op_spans.len(),
        2,
        "expected 2 get_operation spans: {spans:#?}"
    );

    // GetOperation attempt 1
    assert_get_operation_span(
        get_op_spans[0],
        1,
        Some("operations/wait-1"),
        Some(false),
        None,
        "UNSET",
        None,
    );

    // GetOperation attempt 2
    assert_get_operation_span(
        get_op_spans[1],
        2,
        Some("operations/wait-1"),
        Some(true),
        Some(0),
        "UNSET",
        None,
    );

    let _lro_sleep_span = spans
        .iter()
        .find(|s| s.name == "LRO Sleep")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Sleep span in {spans:#?}"))?;

    Ok(())
}
