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

use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::StorageControl;
use integration_tests_o11y::storage_grpc_tracing_common::setup_o11y;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_reports_client_failure() -> anyhow::Result<()> {
    let setup = setup_o11y().await?;

    // Use a bogus endpoint to trigger a client failure (connection refused)
    let endpoint = "http://127.0.0.1:12345";

    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(google_cloud_gax::retry_policy::NeverRetry)
        .with_tracing()
        .build()
        .await?;

    let _ = client
        .delete_bucket()
        .set_name("projects/_/buckets/test-bucket")
        .send()
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = setup.provider.force_flush();
    let _ = setup.meter_provider.force_flush();
    let _ = setup.logger_provider.force_flush();

    let (_, _, request) = setup
        .mock_collector
        .traces
        .lock()
        .expect("never poisoned")
        .pop()
        .expect("should have received at least one trace request")
        .into_parts();

    let mut all_spans = Vec::new();
    for rs in request.resource_spans {
        for ss in rs.scope_spans {
            all_spans.extend(ss.spans);
        }
    }

    let client_span = all_spans
        .iter()
        .find(|s| s.name == "delete_bucket" || s.name == "google.storage.v2.Storage/DeleteBucket")
        .expect("Should have a DeleteBucket span");

    // Verify metrics
    let mut metrics_requests = setup.mock_collector.metrics.lock().expect("never poisoned");
    let mut found_duration_metric = false;
    while let Some(req) = metrics_requests.pop() {
        let (_, _, metrics_request) = req.into_parts();
        for rm in metrics_request.resource_metrics {
            for sm in rm.scope_metrics {
                for m in sm.metrics {
                    if m.name.contains("gcp.client.request.duration")
                        || m.name.contains("test.client.duration")
                    {
                        found_duration_metric = true;
                    }
                }
            }
        }
    }
    assert!(found_duration_metric, "Should have found duration metric");

    // Verify logs
    let logs_requests = setup.mock_collector.logs.lock().unwrap();
    let log_event = logs_requests
        .iter()
        .flat_map(|r| r.get_ref().resource_logs.clone())
        .flat_map(|rl| rl.scope_logs)
        .flat_map(|sl| sl.log_records)
        .find(|l| l.span_id == client_span.span_id);

    assert!(log_event.is_some(), "Should have found log matching span");

    Ok(())
}
