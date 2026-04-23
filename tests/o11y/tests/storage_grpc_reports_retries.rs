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
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_storage::client::StorageControl;
use integration_tests_o11y::mock_collector::MockCollector;
use integration_tests_o11y::otlp::trace::Builder as TracerProviderBuilder;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use storage_grpc_mock::{MockStorage, start};
use tonic::{Code, Status};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn grpc_reports_retries() -> anyhow::Result<()> {
    let mock_collector = MockCollector::default();
    let otlp_endpoint: String = mock_collector.start().await;

    let provider: opentelemetry_sdk::trace::SdkTracerProvider =
        TracerProviderBuilder::new("test-project", "integration-tests")
            .with_endpoint(otlp_endpoint.clone())
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

    let _guard = tracing_subscriber::Registry::default()
        .with(integration_tests_o11y::tracing::trace_layer(
            provider.clone(),
        ))
        .set_default();

    let mut mock = MockStorage::new();

    mock.expect_delete_bucket()
        .returning(|_| Err(Status::new(Code::Unavailable, "try again")));

    let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
    let endpoint = endpoint.trim_end_matches('/');

    let backoff_policy = google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder::new()
        .with_initial_delay(std::time::Duration::from_millis(10))
        .with_maximum_delay(std::time::Duration::from_millis(50))
        .with_scaling(1.5)
        .build()
        .unwrap();

    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(google_cloud_gax::retry_policy::AlwaysRetry)
        .with_backoff_policy(backoff_policy)
        .with_tracing()
        .build()
        .await?;

    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(2000),
        client
            .delete_bucket()
            .set_name("projects/_/buckets/test-bucket")
            .with_retry_policy(google_cloud_gax::retry_policy::AlwaysRetry)
            .send(),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = provider.force_flush();

    let requests = mock_collector
        .traces
        .lock()
        .expect("never poisoned")
        .drain(..)
        .collect::<Vec<_>>();

    let mut all_spans = Vec::new();
    for req in requests {
        let req: tonic::Request<ExportTraceServiceRequest> = req;
        let (_, _, request) = req.into_parts();
        for rs in request.resource_spans {
            for ss in rs.scope_spans {
                all_spans.extend(ss.spans);
            }
        }
    }

    let attempt_spans: Vec<_> = all_spans
        .iter()
        .filter(|s| s.name == "google.storage.v2.Storage/DeleteBucket")
        .collect();

    assert!(
        attempt_spans.len() > 1,
        "Should have recorded multiple attempt spans for retries, found only: {}",
        attempt_spans.len()
    );

    let last_span = attempt_spans.last().unwrap();

    let attributes: std::collections::HashMap<String, _> = last_span
        .attributes
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone().unwrap()))
        .collect();

    let get_int = |key: &str| -> Option<i64> {
        attributes.get(key).and_then(|v| match &v.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => Some(*i),
            _ => None,
        })
    };

    assert!(get_int("gcp.grpc.resend_count").is_some());

    Ok(())
}
