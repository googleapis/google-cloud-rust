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

#[cfg(all(test, feature = "run-integration-tests"))]
mod compute {
    use google_cloud_test_utils::errors::anydump;
    use google_cloud_test_utils::tracing::enable_tracing;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_zones() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::zones()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_errors() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::errors()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_lro_errors() -> anyhow::Result<()> {
        let _guard = google_cloud_test_utils::test_layer::TestLayer::initialize();

        integration_tests_discovery::lro_errors()
            .await
            .inspect_err(anydump)?;

        {
            let spans = google_cloud_test_utils::test_layer::TestLayer::capture(&_guard);

            // 1. Assert on the "LRO Wait" (T2) span
            let lro_wait_span = spans
                .iter()
                .find(|s| s.name == "LRO Wait")
                .ok_or_else(|| anyhow::anyhow!("missing LRO Wait span in {spans:#?}"))?;

            assert_eq!(
                attribute_value_str(lro_wait_span, "otel.status_code"),
                Some("ERROR".to_string())
            );
            let lro_status_desc =
                attribute_value_str(lro_wait_span, "otel.status_description").unwrap_or_default();
            assert!(
                !lro_status_desc.is_empty(),
                "otel.status_description should be populated from the LRO error, got: {lro_status_desc}"
            );
            let lro_error_type =
                attribute_value_str(lro_wait_span, "error.type").unwrap_or_default();
            assert!(
                lro_error_type == "RESOURCE_EXHAUSTED" || lro_error_type == "UNAVAILABLE",
                "error.type should be RESOURCE_EXHAUSTED or UNAVAILABLE, got: {lro_error_type}"
            );

            // 2. Assert on the "client_request" (T3) span for get_operation
            let get_op_span = spans
                .iter()
                .rfind(|s| {
                    s.name == "client_request"
                        && attribute_value_str(s, "rpc.method")
                            == Some(
                                "google.cloud.compute.v1.zoneOperations/getOperation".to_string(),
                            )
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("missing getOperation client_request span in {spans:#?}")
                })?;

            assert_eq!(
                attribute_value_str(get_op_span, "gcp.longrunning.done"),
                Some("true".to_string())
            );
            let get_op_status_code =
                attribute_value_str(get_op_span, "gcp.longrunning.status_code").unwrap_or_default();
            assert!(
                get_op_status_code == "8" || get_op_status_code == "14",
                "gcp.longrunning.status_code should be 8 (RESOURCE_EXHAUSTED) or 14 (UNAVAILABLE), got: {get_op_status_code}"
            );
            assert_eq!(
                attribute_value_str(get_op_span, "otel.status_code"),
                Some("ERROR".to_string())
            );
            let get_op_status_desc =
                attribute_value_str(get_op_span, "otel.status_description").unwrap_or_default();
            assert!(
                !get_op_status_desc.is_empty(),
                "otel.status_description should be populated, got: {get_op_status_desc}"
            );
            let get_op_error_type =
                attribute_value_str(get_op_span, "error.type").unwrap_or_default();
            assert!(
                get_op_error_type == "RESOURCE_EXHAUSTED" || get_op_error_type == "UNAVAILABLE",
                "error.type should be RESOURCE_EXHAUSTED or UNAVAILABLE, got: {get_op_error_type}"
            );
        }
        Ok(())
    }

    fn attribute_value_str(
        span: &google_cloud_test_utils::test_layer::CapturedSpan,
        key: &str,
    ) -> Option<String> {
        use google_cloud_test_utils::test_layer::AttributeValue;
        span.attributes.get(key).map(|v| match v {
            AttributeValue::String(s) => s.to_string(),
            AttributeValue::Boolean(b) => b.to_string(),
            AttributeValue::Int64(i) => i.to_string(),
            AttributeValue::UInt64(u) => u.to_string(),
            AttributeValue::Double(d) => d.to_string(),
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_machine_types() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::machine_types()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_images() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::images()
            .await
            .inspect_err(anydump)
    }

    #[ignore = "TODO(#4894) - disabled because it was flaky"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_instances() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::instances()
            .await
            .inspect_err(anydump)
    }

    #[ignore = "TODO(#4894) - disabled because it was flaky"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_region_instances() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::region_instances()
            .await
            .inspect_err(anydump)
    }
}
