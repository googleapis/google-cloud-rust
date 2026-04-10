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

use gaxi::observability::RequestRecorder;
use gaxi::options::InstrumentationClientInfo;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::StorageControl;
use integration_tests_o11y::mock_collector::MockCollector;
use integration_tests_o11y::otlp::logs::Builder as LoggerProviderBuilder;
use integration_tests_o11y::otlp::metrics::Builder as MeterProviderBuilder;
use integration_tests_o11y::otlp::trace::Builder as TracerProviderBuilder;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use storage_grpc_mock::{MockStorage, start};
use tonic::{Code, Status};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub struct TestSetup {
    pub mock_collector: MockCollector,
    pub otlp_endpoint: String,
    pub provider: opentelemetry_sdk::trace::SdkTracerProvider,
    pub meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    pub logger_provider: opentelemetry_sdk::logs::SdkLoggerProvider,
    pub guard: tracing::subscriber::DefaultGuard,
}

pub async fn setup_o11y() -> anyhow::Result<TestSetup> {
    let mock_collector = MockCollector::default();
    let otlp_endpoint: String = mock_collector.start().await;

    let provider = TracerProviderBuilder::new("test-project", "integration-tests")
        .with_endpoint(otlp_endpoint.clone())
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let meter_provider = MeterProviderBuilder::new("test-project", "integration-tests")
        .with_endpoint(
            otlp_endpoint
                .parse::<http::Uri>()
                .expect("Failed to parse URI"),
        )
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let logger_provider = LoggerProviderBuilder::new("test-project", "integration-tests")
        .with_endpoint(
            otlp_endpoint
                .parse::<http::Uri>()
                .expect("Failed to parse URI"),
        )
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let guard = tracing_subscriber::Registry::default()
        .with(integration_tests_o11y::tracing::trace_layer(
            provider.clone(),
        ))
        .with(integration_tests_o11y::tracing::log_layer(
            logger_provider.clone(),
        ))
        .set_default();

    Ok(TestSetup {
        mock_collector,
        otlp_endpoint,
        provider,
        meter_provider,
        logger_provider,
        guard,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn grpc_can_be_disabled() -> anyhow::Result<()> {
    let setup = setup_o11y().await?;

    let mut mock = MockStorage::new();
    mock.expect_delete_bucket()
        .return_once(|_| Err(Status::new(Code::NotFound, "Object not found")));

    let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
    let endpoint = endpoint.trim_end_matches('/');

    // Intentionally omit .with_tracing()
    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let _ = client
        .delete_bucket()
        .set_name("projects/_/buckets/test-bucket")
        .send()
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = setup.provider.force_flush();

    let mut traces_lock = setup.mock_collector.traces.lock().expect("never poisoned");
    // Verify no spans with CLIENT kind exist
    for request in traces_lock.drain(..) {
        let req: tonic::Request<ExportTraceServiceRequest> = request;
        let (_, _, req) = req.into_parts();
        for rs in req.resource_spans {
            for ss in rs.scope_spans {
                for span in ss.spans {
                    assert_ne!(span.kind, 3, "Should not emit CLIENT spans when disabled");
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn grpc_reports_client_failure() -> anyhow::Result<()> {
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

    let _client_span = all_spans
        .iter()
        .find(|s| s.name == "delete_bucket" || s.name == "google.storage.v2.Storage/DeleteBucket")
        .expect("Should have a DeleteBucket span");

    Ok(())
}

fn verify_metrics(mock_collector: &MockCollector) {
    let mut metrics_requests = mock_collector.metrics.lock().expect("never poisoned");
    let mut found_duration_metric = false;
    while let Some(req) = metrics_requests.pop() {
        let req: tonic::Request<ExportMetricsServiceRequest> = req;
        let (_, _, metrics_request) = req.into_parts();
        for rm in metrics_request.resource_metrics {
            for sm in rm.scope_metrics {
                if let Some(scope) = &sm.scope {
                    let mut scope_attrs = std::collections::HashMap::new();
                    for kv in &scope.attributes {
                        scope_attrs.insert(kv.key.clone(), kv.value.clone().unwrap());
                    }
                    let get_scope_string = |key: &str| -> Option<String> {
                        scope_attrs.get(key).and_then(|v| match &v.value {
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => Some(s.clone()),
                            _ => None,
                        })
                    };
                    assert_eq!(
                        get_scope_string("gcp.client.repo").as_deref(),
                        Some("googleapis/google-cloud-rust")
                    );
                    assert_eq!(
                        get_scope_string("gcp.client.artifact").as_deref(),
                        Some("google-cloud-storage")
                    );
                    assert!(get_scope_string("gcp.client.version").is_some());
                    assert_eq!(
                        get_scope_string("gcp.client.service").as_deref(),
                        Some("storage")
                    );
                }
                for m in sm.metrics {
                    if m.name.contains("test.client.duration")
                        || m.name.contains("gcp.client.request.duration")
                    {
                        found_duration_metric = true;
                        if let Some(
                            opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(h),
                        ) = m.data
                        {
                            let point = h.data_points.first().expect("should have a data point");
                            assert_eq!(
                                point.explicit_bounds,
                                vec![
                                    0.0, 0.0001, 0.0005, 0.0010, 0.005, 0.010, 0.050, 0.100, 0.5,
                                    1.0, 5.0, 10.0, 60.0, 300.0, 900.0, 3600.0
                                ]
                            );

                            let mut metric_attributes = std::collections::HashMap::new();
                            for kv in &point.attributes {
                                metric_attributes.insert(kv.key.clone(), kv.value.clone().unwrap());
                            }

                            let get_metric_string = |key: &str| -> Option<String> {
                                metric_attributes.get(key).and_then(|v| match &v.value {
                                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                                        Some(s.clone())
                                    }
                                    _ => None,
                                })
                            };

                            let get_metric_int = |key: &str| -> Option<i64> {
                                metric_attributes.get(key).and_then(|v| match &v.value {
                                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                                        Some(*i)
                                    }
                                    _ => None,
                                })
                            };

                            assert_eq!(
                                get_metric_string("rpc.system.name").as_deref(),
                                Some("grpc")
                            );
                            assert_eq!(
                                get_metric_string("rpc.method").as_deref(),
                                Some("google.storage.v2.Storage/BidiReadObject")
                            );

                            assert_eq!(
                                get_metric_string("rpc.response.status_code").as_deref(),
                                Some("NOT_FOUND")
                            );
                            assert_eq!(
                                get_metric_string("error.type").as_deref(),
                                Some("NOT_FOUND")
                            );

                            let actual_addr = get_metric_string("server.address").unwrap();
                            assert!(
                                actual_addr == "127.0.0.1"
                                    || actual_addr == "::1"
                                    || actual_addr == "0.0.0.0",
                                "address was {}",
                                actual_addr
                            );
                            assert!(get_metric_int("server.port").is_some());
                        }
                    }
                }
            }
        }
    }
    assert!(found_duration_metric, "Should have found duration metric");
}

fn verify_logs(
    mock_collector: &MockCollector,
    client_span: &opentelemetry_proto::tonic::trace::v1::Span,
) {
    let logs_requests = mock_collector.logs.lock().unwrap();
    let log_event = logs_requests
        .iter()
        .flat_map(|r: &tonic::Request<ExportLogsServiceRequest>| r.get_ref().resource_logs.clone())
        .flat_map(|rl| rl.scope_logs)
        .filter(|sl| {
            sl.scope
                .as_ref()
                .is_some_and(|i| i.name == "google_cloud_gax_internal::observability::errors")
        })
        .flat_map(|sl| sl.log_records)
        .find(|l| l.span_id == client_span.span_id)
        .unwrap_or_else(|| panic!("cannot find log matching span {:?}", client_span.span_id));

    assert_eq!(
        log_event.trace_id, client_span.trace_id,
        "Log traceId correlation failed"
    );
    assert_eq!(
        log_event.span_id, client_span.span_id,
        "Log spanId correlation failed"
    );

    let mut got_log_attrs = std::collections::HashMap::new();
    for kv in &log_event.attributes {
        let val_str = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                s.clone()
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                i.to_string()
            }
            _ => format!("{:?}", kv.value),
        };
        got_log_attrs.insert(kv.key.clone(), val_str);
    }

    println!("LOG ATTRIBUTES = {:?}", got_log_attrs.keys());

    assert_eq!(
        got_log_attrs.get("error.type").map(String::as_str),
        Some("NOT_FOUND")
    );

    assert_eq!(
        got_log_attrs
            .get("rpc.response.status_code")
            .map(String::as_str),
        Some("NOT_FOUND")
    );

    assert_eq!(log_event.severity_text, "DEBUG", "severity_text mismatch");
}



#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn grpc_reports_server_error() -> anyhow::Result<()> {
    let setup = setup_o11y().await?;

    let mut mock = MockStorage::new();
    mock.expect_delete_bucket()
        .return_once(|_| Err(Status::new(Code::NotFound, "Object not found")));

    let (endpoint, _server): (String, tokio::task::JoinHandle<()>) =
        start("0.0.0.0:0", mock).await?;
    let endpoint = endpoint.trim_end_matches('/');

    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await?;

    let _ = {
        let mut info = InstrumentationClientInfo::default();
        info.service_name = "storage";
        info.client_version = "1.10.0";
        info.client_artifact = "google-cloud-storage";
        info.default_host = "storage.googleapis.com";
        let recorder = RequestRecorder::new(info);
        recorder
            .scope(async {
                client
                    .delete_bucket()
                    .set_name("projects/_/buckets/test-bucket")
                    .send()
                    .await
            })
            .await
    };

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
        .find(|s| {
            (s.name == "delete_bucket" || s.name == "google.storage.v2.Storage/DeleteBucket")
                && (s.kind == 1 || s.kind == 3)
        })
        .expect("Should have a DeleteBucket span");

    verify_metrics(&setup.mock_collector);
    verify_logs(&setup.mock_collector, client_span);

    Ok(())
}
