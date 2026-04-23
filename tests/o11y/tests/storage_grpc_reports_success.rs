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
use integration_tests_o11y::mock_collector::MockCollector;
use integration_tests_o11y::otlp::trace::Builder as TracerProviderBuilder;
use storage_grpc_mock::{MockStorage, start};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_reports_success() -> anyhow::Result<()> {
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
        .returning(|_| Ok(tonic::Response::new(())));

    let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
    let endpoint = endpoint.trim_end_matches('/');

    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await?;

    let _ = client
        .delete_bucket()
        .set_name("projects/_/buckets/test-bucket")
        .send()
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = provider.force_flush();

    let (_, _, request) = mock_collector
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
        .find(|s| s.name == "google.storage.v2.Storage/DeleteBucket")
        .expect("Should have a DeleteBucket span");

    assert_eq!(client_span.kind, 3); // SPAN_KIND_CLIENT

    let status_code = client_span.status.as_ref().map(|s| s.code).unwrap_or(0);
    assert!(
        status_code == 0 || status_code == 1,
        "status code should be UNSET (0) or OK (1), was {}",
        status_code
    );

    let attributes: std::collections::HashMap<String, _> = client_span
        .attributes
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone().unwrap()))
        .collect();

    let get_string = |key: &str| -> Option<String> {
        attributes.get(key).and_then(|v| match &v.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                Some(s.clone())
            }
            _ => None,
        })
    };

    assert_eq!(get_string("rpc.system.name").as_deref(), Some("grpc"));
    assert_eq!(
        get_string("rpc.method").as_deref(),
        Some("google.storage.v2.Storage/DeleteBucket")
    );
    assert_eq!(
        get_string("rpc.response.status_code").as_deref(),
        Some("OK")
    );
    assert!(get_string("error.type").is_none());

    Ok(())
}
