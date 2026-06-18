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
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::polling_error_policy::Aip194Strict;
use google_cloud_lro::Poller;
use google_cloud_lro::internal::{
    DiscoveryOperation, PollerExt, PollerOptions, TracingDetails, new_discovery_poller,
};
use google_cloud_showcase_v1beta1::client::Echo;
use google_cloud_test_utils::test_layer::{
    AttributeValue, CapturedSpan, TestLayer, TestLayerGuard,
};
use httptest::{Expectation, Server, cycle, matchers::*, responders::status_code};
use std::sync::Arc;

#[derive(Clone, Debug)]
struct MockDiscoveryOperation {
    name: String,
    done: bool,
    error: Option<google_cloud_gax::error::rpc::Status>,
}

impl DiscoveryOperation for MockDiscoveryOperation {
    fn done(&self) -> bool {
        self.done
    }
    fn name(&self) -> Option<&String> {
        Some(&self.name)
    }
    fn error(&self) -> Option<google_cloud_gax::error::rpc::Status> {
        self.error.clone()
    }
}

impl MockDiscoveryOperation {
    fn new(name: impl Into<String>, done: bool) -> Self {
        Self {
            name: name.into(),
            done,
            error: None,
        }
    }

    fn new_with_error(
        name: impl Into<String>,
        done: bool,
        error: google_cloud_gax::error::rpc::Status,
    ) -> Self {
        Self {
            name: name.into(),
            done,
            error: Some(error),
        }
    }
}

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

    // Run the Showcase (GAPIC) LRO tracing tests.
    test_lro_success(&guard, &server, client.clone()).await?;
    server.verify_and_clear();

    test_lro_logical_error(&guard, &server, client.clone()).await?;
    server.verify_and_clear();

    test_lro_transient_rpc_error(&guard, &server, client.clone()).await?;
    server.verify_and_clear();

    test_lro_permanent_rpc_error(&guard, &server, client.clone()).await?;
    server.verify_and_clear();

    // Run the Discovery LRO tracing tests.
    test_discovery_lro_success(&guard).await?;
    test_discovery_lro_logical_error(&guard).await?;
    test_discovery_lro_transient_rpc_error(&guard).await?;
    test_discovery_lro_permanent_rpc_error(&guard).await?;

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

/// Tests the behavior when a transient network error (HTTP 503 Service Unavailable) occurs during polling.
async fn test_lro_transient_rpc_error(
    guard: &TestLayerGuard,
    server: &Server,
    client: Echo,
) -> anyhow::Result<()> {
    server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:wait"),
        ])
        .respond_with(status_code(200).body(r#"{"name": "operations/wait-3", "done": false}"#)),
    );

    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path("/v1beta1/operations/wait-3"),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(503),
            status_code(200).body(
                r#"
                {
                    "name": "operations/wait-3",
                    "done": true,
                    "response": {
                        "@type": "type.googleapis.com/google.showcase.v1beta1.WaitResponse",
                        "content": "recovered-content"
                    }
                }
            "#
            ),
        ]),
    );

    let res = client.wait().poller().until_done().await?;
    assert_eq!(res.content, "recovered-content");

    let spans = TestLayer::capture(guard);

    // 1. T2 span "LRO Wait" should be UNSET (successful overall LRO!)
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
    assert_eq!(attribute_str(lro_wait_span, "otel.status_code"), None);

    // 2. 2 get_operation spans: 1st should be ERROR, 2nd should be UNSET
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(get_op_spans.len(), 2);

    let span0 = get_op_spans[0];
    assert_eq!(
        attribute_u64(span0, "gcp.longrunning.poll_attempt_count"),
        Some(1)
    );
    assert_eq!(attribute_str(span0, "otel.status_code"), Some("ERROR"));
    assert_eq!(attribute_str(span0, "error.type"), Some("503"));

    let span1 = get_op_spans[1];
    assert_eq!(
        attribute_u64(span1, "gcp.longrunning.poll_attempt_count"),
        Some(2)
    );
    assert_eq!(attribute_bool(span1, "gcp.longrunning.done"), Some(true));
    assert_eq!(attribute_str(span1, "otel.status_code"), Some("UNSET"));

    Ok(())
}

/// Tests the behavior when a permanent network error (HTTP 404 Not Found) occurs during polling.
async fn test_lro_permanent_rpc_error(
    guard: &TestLayerGuard,
    server: &Server,
    client: Echo,
) -> anyhow::Result<()> {
    server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:wait"),
        ])
        .respond_with(status_code(200).body(r#"{"name": "operations/wait-4", "done": false}"#)),
    );

    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path("/v1beta1/operations/wait-4"),
        ])
        .respond_with(
            status_code(404)
                .body(r#"{"error": {"code": 404, "message": "not found", "status": "NOT_FOUND"}}"#),
        ),
    );

    let res = client.wait().poller().until_done().await;
    assert!(res.is_err());

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
    assert_eq!(
        attribute_str(lro_wait_span, "error.type"),
        Some("NOT_FOUND")
    );

    // 2. T3 span "client_request" (get_operation) should be ERROR
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
        attribute_str(get_op_span, "otel.status_code"),
        Some("ERROR")
    );
    assert_eq!(attribute_str(get_op_span, "error.type"), Some("NOT_FOUND"));

    Ok(())
}

/// Tests a successful LRO polling workflow for discovery-based client pollers.
async fn test_discovery_lro_success(guard: &TestLayerGuard) -> anyhow::Result<()> {
    let error_policy = Arc::new(Aip194Strict);
    let backoff_policy = Arc::new(ExponentialBackoff::default());

    let start = || async {
        Ok(MockDiscoveryOperation::new(
            "discovery-operations/wait-100",
            false,
        ))
    };

    let query = move |name: String| async move {
        let span = gaxi::client_request_signals!(
            info: &gaxi::options::InstrumentationClientInfo::default(),
            method: "google.longrunning.Operations/GetOperation"
        );
        span.record("rpc.method", "google.longrunning.Operations/GetOperation");

        let res = span.in_scope(|| {
            google_cloud_lro::record_polling_attributes!(&span);

            span.record("gcp.longrunning.done", true);

            MockDiscoveryOperation::new(name, true)
        });

        Ok(res)
    };

    let poller = new_discovery_poller(error_policy, backoff_policy, start, query);

    let mut poller_options = PollerOptions::default();
    let mut details = TracingDetails::default();
    details.method_name = "discovery::client::Echo::wait::until_done";
    poller_options.tracing = Some(details);

    let poller = poller.with_options(poller_options);

    let res = poller.until_done().await?;
    assert!(res.done);

    let spans = TestLayer::capture(guard);

    // 1. T2 span "LRO Wait"
    let lro_wait_span = spans
        .iter()
        .find(|s| s.name == "LRO Wait")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Wait span in {spans:#?}"))?;

    assert_eq!(
        attribute_str(lro_wait_span, "otel.name"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "gcp.rpc.method"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(attribute_str(lro_wait_span, "otel.status_code"), None);

    // 2. T3 get_operation span
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(get_op_spans.len(), 1);

    let get_op_span = get_op_spans[0];
    assert_eq!(
        attribute_u64(get_op_span, "gcp.longrunning.poll_attempt_count"),
        Some(1)
    );
    assert_eq!(
        attribute_str(get_op_span, "gcp.resource.destination.id"),
        Some("discovery-operations/wait-100")
    );
    assert_eq!(
        attribute_bool(get_op_span, "gcp.longrunning.done"),
        Some(true)
    );

    Ok(())
}

/// Tests a logical LRO failure for discovery-based client pollers.
async fn test_discovery_lro_logical_error(guard: &TestLayerGuard) -> anyhow::Result<()> {
    let error_policy = Arc::new(Aip194Strict);
    let backoff_policy = Arc::new(ExponentialBackoff::default());

    let start = || async {
        Ok(MockDiscoveryOperation::new(
            "discovery-operations/wait-200",
            false,
        ))
    };

    let query = move |name: String| async move {
        let span = gaxi::client_request_signals!(
            info: &gaxi::options::InstrumentationClientInfo::default(),
            method: "google.longrunning.Operations/GetOperation"
        );
        span.record("rpc.method", "google.longrunning.Operations/GetOperation");

        let res = span.in_scope(|| {
            google_cloud_lro::record_polling_attributes!(&span);

            span.record("gcp.longrunning.done", true);

            MockDiscoveryOperation::new_with_error(
                name,
                true,
                google_cloud_gax::error::rpc::Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::InvalidArgument)
                    .set_message("logical-error-msg"),
            )
        });

        Ok(res)
    };

    let poller = new_discovery_poller(error_policy, backoff_policy, start, query);

    let mut poller_options = PollerOptions::default();
    let mut details = TracingDetails::default();
    details.method_name = "discovery::client::Echo::wait::until_done";
    poller_options.tracing = Some(details);

    let poller = poller.with_options(poller_options);

    let res = poller.until_done().await?;
    assert!(res.done);
    assert!(res.error.is_some());

    let spans = TestLayer::capture(guard);

    // 1. T2 span "LRO Wait" should be ERROR because the operation failed.
    let lro_wait_span = spans
        .iter()
        .find(|s| s.name == "LRO Wait")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Wait span in {spans:#?}"))?;

    assert_eq!(
        attribute_str(lro_wait_span, "otel.name"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "gcp.rpc.method"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "otel.status_code"),
        Some("ERROR")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "otel.status_description"),
        Some("logical-error-msg")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "error.type"),
        Some("INVALID_ARGUMENT")
    );

    // 2. T3 span "client_request" (get_operation) should be success
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(get_op_spans.len(), 1);
    let get_op_span = get_op_spans[0];
    assert_eq!(
        attribute_str(get_op_span, "otel.status_code"),
        Some("UNSET")
    );
    assert!(!get_op_span.attributes.contains_key("error.type"));

    Ok(())
}

/// Tests transient network error retries for discovery-based client pollers.
async fn test_discovery_lro_transient_rpc_error(guard: &TestLayerGuard) -> anyhow::Result<()> {
    let error_policy = Arc::new(Aip194Strict);
    let backoff_policy = Arc::new(ExponentialBackoff::default());

    let start = || async {
        Ok(MockDiscoveryOperation::new(
            "discovery-operations/wait-300",
            false,
        ))
    };

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let attempts_clone = attempts.clone();
    let query = move |name: String| {
        let attempts_inner = attempts_clone.clone();
        async move {
            let attempt = attempts_inner.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            let span = gaxi::client_request_signals!(
                info: &gaxi::options::InstrumentationClientInfo::default(),
                method: "google.longrunning.Operations/GetOperation"
            );
            span.record("rpc.method", "google.longrunning.Operations/GetOperation");

            if attempt == 0 {
                span.in_scope(|| {
                    google_cloud_lro::record_polling_attributes!(&span);

                    span.record("otel.status_code", "ERROR");
                    span.record("error.type", "UNAVAILABLE");

                    Err(google_cloud_gax::error::Error::service(
                        google_cloud_gax::error::rpc::Status::default()
                            .set_code(google_cloud_gax::error::rpc::Code::Unavailable)
                            .set_message("transient-network-error"),
                    ))
                })
            } else {
                span.in_scope(|| {
                    google_cloud_lro::record_polling_attributes!(&span);

                    span.record("gcp.longrunning.done", true);

                    Ok(MockDiscoveryOperation::new(name, true))
                })
            }
        }
    };

    let poller = new_discovery_poller(error_policy, backoff_policy, start, query);

    let mut poller_options = PollerOptions::default();
    let mut details = TracingDetails::default();
    details.method_name = "discovery::client::Echo::wait::until_done";
    poller_options.tracing = Some(details);

    let poller = poller.with_options(poller_options);

    let res = poller.until_done().await?;
    assert!(res.done);

    let spans = TestLayer::capture(guard);

    // 1. T2 span "LRO Wait" should be UNSET/None
    let lro_wait_span = spans
        .iter()
        .find(|s| s.name == "LRO Wait")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Wait span in {spans:#?}"))?;

    assert_eq!(
        attribute_str(lro_wait_span, "otel.name"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "gcp.rpc.method"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(attribute_str(lro_wait_span, "otel.status_code"), None);

    // 2. T3 spans
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(get_op_spans.len(), 2);

    let span0 = get_op_spans[0];
    assert_eq!(
        attribute_u64(span0, "gcp.longrunning.poll_attempt_count"),
        Some(1)
    );
    assert_eq!(attribute_str(span0, "otel.status_code"), Some("ERROR"));
    assert_eq!(attribute_str(span0, "error.type"), Some("UNAVAILABLE"));

    let span1 = get_op_spans[1];
    assert_eq!(
        attribute_u64(span1, "gcp.longrunning.poll_attempt_count"),
        Some(2)
    );
    assert_eq!(attribute_bool(span1, "gcp.longrunning.done"), Some(true));

    Ok(())
}

/// Tests permanent network errors for discovery-based client pollers.
async fn test_discovery_lro_permanent_rpc_error(guard: &TestLayerGuard) -> anyhow::Result<()> {
    let error_policy = Arc::new(Aip194Strict);
    let backoff_policy = Arc::new(ExponentialBackoff::default());

    let start = || async {
        Ok(MockDiscoveryOperation::new(
            "discovery-operations/wait-400",
            false,
        ))
    };

    let query = move |_name: String| async move {
        let span = gaxi::client_request_signals!(
            info: &gaxi::options::InstrumentationClientInfo::default(),
            method: "google.longrunning.Operations/GetOperation"
        );
        span.record("rpc.method", "google.longrunning.Operations/GetOperation");

        span.in_scope(|| {
            google_cloud_lro::record_polling_attributes!(&span);

            span.record("otel.status_code", "ERROR");
            span.record("error.type", "NOT_FOUND");

            Err(google_cloud_gax::error::Error::service(
                google_cloud_gax::error::rpc::Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::NotFound)
                    .set_message("not-found-error"),
            ))
        })
    };

    let poller = new_discovery_poller(error_policy, backoff_policy, start, query);

    let mut poller_options = PollerOptions::default();
    let mut details = TracingDetails::default();
    details.method_name = "discovery::client::Echo::wait::until_done";
    poller_options.tracing = Some(details);

    let poller = poller.with_options(poller_options);

    let res = poller.until_done().await;
    assert!(res.is_err());

    let spans = TestLayer::capture(guard);

    // 1. T2 span "LRO Wait" should be ERROR
    let lro_wait_span = spans
        .iter()
        .find(|s| s.name == "LRO Wait")
        .ok_or_else(|| anyhow::anyhow!("missing LRO Wait span in {spans:#?}"))?;

    assert_eq!(
        attribute_str(lro_wait_span, "otel.name"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "gcp.rpc.method"),
        Some("discovery::client::Echo::wait::until_done")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "otel.status_code"),
        Some("ERROR")
    );
    assert_eq!(
        attribute_str(lro_wait_span, "error.type"),
        Some("NOT_FOUND")
    );

    // 2. T3 span "client_request" (get_operation) should be ERROR
    let get_op_spans = find_get_operation_spans(&spans);
    assert_eq!(get_op_spans.len(), 1);

    let get_op_span = get_op_spans[0];
    assert_eq!(
        attribute_u64(get_op_span, "gcp.longrunning.poll_attempt_count"),
        Some(1)
    );
    assert_eq!(
        attribute_str(get_op_span, "otel.status_code"),
        Some("ERROR")
    );
    assert_eq!(attribute_str(get_op_span, "error.type"), Some("NOT_FOUND"));

    Ok(())
}
