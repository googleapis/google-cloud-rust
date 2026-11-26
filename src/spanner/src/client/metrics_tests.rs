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

//! Unit and mock server tests for `Spanner::builder()` and `DatabaseClient` telemetry
//! under the standalone `unstable-metrics` feature (without requiring `builtin-metrics`).

use crate::client::{InstanceType, Spanner, SpannerBuilderExt};
use crate::key::KeySet;
use crate::observability::test_utils::{
    find_metric, find_point, get_histogram_counts, get_sum_u64_points,
};
use crate::observability::{Observability, SharedMeterProvider};
use crate::read::ReadRequest;
use crate::result_set::tests::adapt;
use crate::retry_delay::extract_status_code_from_error;
use crate::statement::Statement;
use anyhow::Result;
use gaxi::grpc::tonic::{Response, Status as TonicStatus};
use gaxi::options::ClientConfig;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::error::rpc::Code;
use google_cloud_test_macros::tokio_test_no_panics;
use mockall::Sequence;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use prost_types::Value as ProstValue;
use prost_types::value::Kind as ProstValueKind;
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::spanner::v1::struct_type::Field;
use spanner_grpc_mock::google::spanner::v1::{
    PartialResultSet, ResultSetMetadata, Session, StructType, Type as SpannerType, TypeCode,
};
use spanner_grpc_mock::start;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Guard that aborts the mock gRPC server task when dropped to prevent leaking background listeners.
struct ServerGuard(JoinHandle<()>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Creates an in-memory metric exporter and an associated SdkMeterProvider in an Arc for testing.
fn create_test_meter_provider() -> (InMemoryMetricExporter, Arc<SdkMeterProvider>) {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone()).build();
    let meter_provider = Arc::new(SdkMeterProvider::builder().with_reader(reader).build());
    (exporter, meter_provider)
}

/// Configures standard multiplexed session creation expectation on the mock server.
fn setup_mock_session(mock: &mut MockSpanner) {
    mock.expect_create_session().returning(|request| {
        let request = request.into_inner();
        let session = request.session.expect("session present in request");
        assert!(session.multiplexed, "session should be multiplexed");

        Ok(Response::new(Session {
            name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
                .to_string(),
            multiplexed: true,
            ..Default::default()
        }))
    });
}

/// Starts a mock Spanner server with session creation configured for client priming.
async fn setup_mock_priming_server() -> Result<(String, ServerGuard)> {
    let mut mock = MockSpanner::new();
    setup_mock_session(&mut mock);
    let (address, server) = start("127.0.0.1:0", mock).await?;
    Ok((address, ServerGuard(server)))
}

/// Starts a mock Spanner server with session creation and ExecuteStreamingSql configured.
async fn setup_mock_spanner_server(timing_header: Option<&str>) -> Result<(String, ServerGuard)> {
    let mut mock = MockSpanner::new();
    setup_mock_session(&mut mock);

    let timing_header_owned = timing_header.map(String::from);
    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |_| {
            let result_set = PartialResultSet {
                metadata: Some(ResultSetMetadata {
                    row_type: Some(StructType {
                        fields: vec![Field {
                            name: "num".to_string(),
                            r#type: Some(SpannerType {
                                code: TypeCode::Int64 as i32,
                                ..Default::default()
                            }),
                        }],
                    }),
                    ..Default::default()
                }),
                values: vec![ProstValue {
                    kind: Some(ProstValueKind::StringValue("42".to_string())),
                }],
                last: true,
                ..Default::default()
            };

            let mut response = Response::new(adapt([Ok(result_set)]));
            if let Some(ref header_str) = timing_header_owned {
                let header_value = header_str
                    .parse()
                    .expect("server-timing header must be valid ASCII");
                response
                    .metadata_mut()
                    .insert("server-timing", header_value);
            }
            Ok(response)
        });

    let (address, server) = start("127.0.0.1:0", mock).await?;
    Ok((address, ServerGuard(server)))
}

/// Executes a test query using a single-use transaction and returns row count and value.
async fn execute_test_query(spanner: &Spanner) -> Result<(u64, i64)> {
    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .build()
        .await?;

    let single_use_transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT 1 AS num").build();
    let mut result_set = single_use_transaction.execute_query(statement).await?;

    let mut row_count = 0;
    let mut last_value = 0;
    while let Some(row) = result_set.next().await.transpose()? {
        last_value = row.get("num")?;
        row_count += 1;
    }
    Ok((row_count, last_value))
}

/// Asserts that standard metrics for Spanner.ExecuteStreamingSql and Spanner.CreateSession
/// are present with the expected names and attributes.
fn assert_streaming_sql_metrics(
    finished_metrics: &[ResourceMetrics],
    check_timing_latencies: bool,
) {
    // Note: OpenTelemetry export to custom MeterProviders uses the "spanner/client/*" namespace
    // rather than the internal Google Cloud Monitoring "spanner.googleapis.com/internal/client/*" prefix.
    let operation_count_metric = find_metric(finished_metrics, "spanner/client/operation_count")
        .expect("spanner/client/operation_count must be recorded on custom provider");
    let operation_points = get_sum_u64_points(operation_count_metric);
    assert_eq!(
        operation_points.len(),
        2,
        "Expected 2 operation_count points"
    );
    assert_eq!(
        find_point(&operation_points, "Spanner.ExecuteStreamingSql", "OK"),
        Some(1),
        "ExecuteStreamingSql operation_count value with status OK should be 1"
    );
    assert_eq!(
        find_point(&operation_points, "Spanner.CreateSession", "OK"),
        Some(1),
        "CreateSession operation_count value with status OK should be 1"
    );

    let attempt_count_metric = find_metric(finished_metrics, "spanner/client/attempt_count")
        .expect("spanner/client/attempt_count must be recorded on custom provider");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(attempt_points.len(), 2, "Expected 2 attempt_count points");
    assert_eq!(
        find_point(&attempt_points, "Spanner.ExecuteStreamingSql", "OK"),
        Some(1),
        "ExecuteStreamingSql attempt_count value with status OK should be 1"
    );
    assert_eq!(
        find_point(&attempt_points, "Spanner.CreateSession", "OK"),
        Some(1),
        "CreateSession attempt_count value with status OK should be 1"
    );

    let operation_latencies_metric =
        find_metric(finished_metrics, "spanner/client/operation_latencies")
            .expect("spanner/client/operation_latencies must be recorded on custom provider");
    let operation_latency_counts = get_histogram_counts(operation_latencies_metric);
    assert_eq!(
        operation_latency_counts.len(),
        2,
        "Expected 2 operation_latencies histogram points"
    );

    let attempt_latencies_metric =
        find_metric(finished_metrics, "spanner/client/attempt_latencies")
            .expect("spanner/client/attempt_latencies must be recorded on custom provider");
    let attempt_latency_counts = get_histogram_counts(attempt_latencies_metric);
    assert_eq!(
        attempt_latency_counts.len(),
        2,
        "Expected 2 attempt_latencies histogram points"
    );

    if check_timing_latencies {
        let gfe_latencies_metric = find_metric(finished_metrics, "spanner/client/gfe_latencies")
            .expect("spanner/client/gfe_latencies must be recorded when server-timing is present");
        let gfe_counts = get_histogram_counts(gfe_latencies_metric);
        assert!(
            !gfe_counts.is_empty(),
            "gfe_latencies must have at least one data point"
        );

        let access_front_end_metric = find_metric(finished_metrics, "spanner/client/afe_latencies")
            .expect("spanner/client/afe_latencies must be recorded when server-timing is present");
        let access_front_end_counts = get_histogram_counts(access_front_end_metric);
        assert!(
            !access_front_end_counts.is_empty(),
            "spanner/client/afe_latencies must have at least one data point"
        );
    }
}

/// Asserts that no Spanner built-in metrics were recorded or exported to the custom provider.
fn assert_no_custom_metrics_exported(finished_metrics: &[ResourceMetrics]) {
    assert!(
        find_metric(finished_metrics, "spanner/client/operation_count").is_none(),
        "operation_count must not be exported"
    );
    assert!(
        find_metric(finished_metrics, "spanner/client/attempt_count").is_none(),
        "attempt_count must not be exported"
    );
    assert!(
        find_metric(finished_metrics, "spanner/client/operation_latencies").is_none(),
        "operation_latencies must not be exported"
    );
    assert!(
        find_metric(finished_metrics, "spanner/client/attempt_latencies").is_none(),
        "attempt_latencies must not be exported"
    );
}

#[tokio_test_no_panics]
async fn spanner_builder_with_meter_provider_cloud_spanner_default_does_not_export_builtin_metrics()
-> Result<()> {
    let (address, _server_guard) = setup_mock_priming_server().await?;
    let (_exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = meter_provider as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_meter_provider(provider)
        .build()
        .await?;

    assert!(
        spanner.meter_provider().is_some(),
        "provider should be stored on spanner"
    );
    assert_eq!(
        spanner.export_builtin_metrics_to_custom_provider(),
        None,
        "export_builtin_metrics_to_custom_provider should default to None"
    );

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .build()
        .await?;

    assert!(
        !database_client.o11y.is_enabled(),
        "Observability should be disabled for built-in metrics on Cloud Spanner by default"
    );
    assert!(
        database_client.o11y.caller_meter_provider.is_some(),
        "caller_meter_provider should be preserved on the client"
    );
    assert_eq!(
        database_client.o11y.metrics.len(),
        0,
        "Cloud Spanner default does not duplicate built-in metrics to caller provider"
    );
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_builder_with_export_builtin_metrics_to_custom_provider() -> Result<()> {
    let (address, _server_guard) = setup_mock_priming_server().await?;
    let (_exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_meter_provider(provider)
        .with_export_builtin_metrics_to_custom_provider(true)
        .build()
        .await?;

    assert_eq!(
        spanner.export_builtin_metrics_to_custom_provider(),
        Some(true),
        "export flag should be Some(true)"
    );

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .build()
        .await?;

    assert!(
        database_client.o11y.is_enabled(),
        "Observability should be enabled when opting in to export built-in metrics"
    );
    assert_eq!(
        database_client.o11y.metrics.len(),
        1,
        "expected 1 metrics sink for the custom provider"
    );
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_builder_with_export_opt_in_without_meter_provider() -> Result<()> {
    let (address, _server_guard) = setup_mock_priming_server().await?;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_export_builtin_metrics_to_custom_provider(true)
        .build()
        .await?;

    assert!(
        spanner.meter_provider().is_none(),
        "meter_provider should be None when not configured"
    );
    assert_eq!(
        spanner.export_builtin_metrics_to_custom_provider(),
        Some(true),
        "export flag should be Some(true)"
    );

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .build()
        .await?;

    assert!(
        !database_client.o11y.is_enabled(),
        "Observability should be disabled when export is true but no provider was supplied"
    );
    assert_eq!(
        database_client.o11y.metrics.len(),
        0,
        "No metrics sinks should be created without a MeterProvider"
    );
    Ok(())
}

#[tokio_test_no_panics]
async fn observability_init_invalid_database_name_with_custom_provider() -> Result<()> {
    let (_exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = meter_provider as _;

    let observability = Observability::init(
        &ClientConfig::default(),
        InstanceType::Omni,
        "invalid-database-name",
        false,
        None,
        None,
        Some(SharedMeterProvider::from(provider)),
    )
    .await;

    assert!(
        !observability.is_enabled(),
        "Observability must be disabled when database name parsing fails"
    );
    assert!(
        observability.caller_meter_provider.is_some(),
        "Caller meter provider must still be preserved in disabled state"
    );
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_builder_omni_with_meter_provider_defaults_to_export_builtin_metrics() -> Result<()>
{
    let (address, _server_guard) = setup_mock_priming_server().await?;
    let (_exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_instance_type(InstanceType::Omni)
        .with_meter_provider(provider)
        .build()
        .await?;

    assert_eq!(
        spanner.export_builtin_metrics_to_custom_provider(),
        None,
        "export flag defaults to None"
    );

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .build()
        .await?;

    assert!(
        database_client.o11y.is_enabled(),
        "Observability should be enabled on Omni by default when provider is supplied"
    );
    assert_eq!(
        database_client.o11y.metrics.len(),
        1,
        "expected 1 metrics sink for Omni custom provider"
    );
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_builder_with_raw_meter_provider_extension() -> Result<()> {
    let (_exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = meter_provider as _;

    let spanner = Spanner::builder()
        .with_endpoint("http://127.0.0.1:1")
        .with_credentials(Anonymous::new().build())
        .with_extension(provider)
        .build()
        .await?;

    assert!(
        spanner.meter_provider().is_some(),
        "raw Arc<dyn MeterProvider> extension should be recognized"
    );
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_omni_meter_provider_exports_metrics_in_isolation() -> Result<()> {
    let (address, _server_guard) =
        setup_mock_spanner_server(Some("gfet4t7;dur=20.5,afe;dur=8.1")).await?;

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_instance_type(InstanceType::Omni)
        .with_meter_provider(provider)
        .build()
        .await?;

    let (row_count, value) = execute_test_query(&spanner).await?;
    assert_eq!(row_count, 1, "Exactly one row should be yielded");
    assert_eq!(value, 42, "Returned value should match mock data");

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;
    assert_streaming_sql_metrics(&finished_metrics, true);

    meter_provider.shutdown()?;
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_cloud_with_export_builtin_metrics_exports_to_custom_provider_in_isolation()
-> Result<()> {
    let (address, _server_guard) =
        setup_mock_spanner_server(Some("gfet4t7;dur=20.5,afe;dur=8.1")).await?;

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_meter_provider(provider)
        .with_export_builtin_metrics_to_custom_provider(true)
        .build()
        .await?;

    let (row_count, value) = execute_test_query(&spanner).await?;
    assert_eq!(row_count, 1, "Exactly one row should be yielded");
    assert_eq!(value, 42, "Returned value should match mock data");

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;
    assert_streaming_sql_metrics(&finished_metrics, true);

    meter_provider.shutdown()?;
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_cloud_default_does_not_export_metrics_to_custom_provider_in_isolation()
-> Result<()> {
    let (address, _server_guard) = setup_mock_spanner_server(None).await?;

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_meter_provider(provider)
        .build()
        .await?;

    let (row_count, value) = execute_test_query(&spanner).await?;
    assert_eq!(row_count, 1, "Exactly one row should be yielded");
    assert_eq!(value, 42, "Returned value should match mock data");

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;
    assert_no_custom_metrics_exported(&finished_metrics);

    meter_provider.shutdown()?;
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_omni_with_export_builtin_metrics_false_disables_export_in_isolation() -> Result<()>
{
    let (address, _server_guard) = setup_mock_spanner_server(None).await?;

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_instance_type(InstanceType::Omni)
        .with_meter_provider(provider)
        .with_export_builtin_metrics_to_custom_provider(false)
        .build()
        .await?;

    assert_eq!(
        spanner.export_builtin_metrics_to_custom_provider(),
        Some(false),
        "export flag should be Some(false)"
    );

    let (row_count, value) = execute_test_query(&spanner).await?;
    assert_eq!(row_count, 1, "Exactly one row should be yielded");
    assert_eq!(value, 42, "Returned value should match mock data");

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;
    assert_no_custom_metrics_exported(&finished_metrics);

    meter_provider.shutdown()?;
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_custom_meter_provider_records_error_status_in_isolation() -> Result<()> {
    let mut mock = MockSpanner::new();
    setup_mock_session(&mut mock);

    mock.expect_execute_streaming_sql().once().returning(|_| {
        Err(TonicStatus::not_found(
            "table or statement target not found",
        ))
    });

    let (address, server) = start("127.0.0.1:0", mock).await?;
    let _server_guard = ServerGuard(server);

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_instance_type(InstanceType::Omni)
        .with_meter_provider(provider)
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .build()
        .await?;

    let single_use_transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT 1 AS num").build();
    let error = single_use_transaction
        .execute_query(statement)
        .await
        .expect_err("execute_query should fail with NotFound");
    let status_code = extract_status_code_from_error(&error);
    assert_eq!(
        status_code,
        Some(Code::NotFound),
        "status code should be NotFound"
    );

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;

    let operation_count_metric = find_metric(&finished_metrics, "spanner/client/operation_count")
        .expect("spanner/client/operation_count must be recorded on custom provider");
    let operation_points = get_sum_u64_points(operation_count_metric);
    assert_eq!(
        operation_points.len(),
        2,
        "Expected 2 operation_count points"
    );

    assert_eq!(
        find_point(
            &operation_points,
            "Spanner.ExecuteStreamingSql",
            "NOT_FOUND"
        ),
        Some(1),
        "ExecuteStreamingSql operation_count value with NOT_FOUND should be 1"
    );
    assert_eq!(
        find_point(&operation_points, "Spanner.CreateSession", "OK"),
        Some(1),
        "CreateSession operation_count value with OK should be 1"
    );

    let attempt_count_metric = find_metric(&finished_metrics, "spanner/client/attempt_count")
        .expect("spanner/client/attempt_count must be recorded on custom provider");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(attempt_points.len(), 2, "Expected 2 attempt_count points");

    assert_eq!(
        find_point(&attempt_points, "Spanner.ExecuteStreamingSql", "NOT_FOUND"),
        Some(1),
        "ExecuteStreamingSql attempt_count value with NOT_FOUND should be 1"
    );
    assert_eq!(
        find_point(&attempt_points, "Spanner.CreateSession", "OK"),
        Some(1),
        "CreateSession attempt_count value with OK should be 1"
    );

    let connectivity_error_metric = find_metric(
        &finished_metrics,
        "spanner/client/gfe_connectivity_error_count",
    )
    .expect("gfe_connectivity_error_count must be recorded when server-timing is absent");
    let connectivity_points = get_sum_u64_points(connectivity_error_metric);
    assert_eq!(
        find_point(
            &connectivity_points,
            "Spanner.ExecuteStreamingSql",
            "NOT_FOUND"
        ),
        Some(1),
        "ExecuteStreamingSql attempt without server-timing must increment gfe_connectivity_error_count"
    );

    meter_provider.shutdown()?;
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_custom_meter_provider_records_retry_attempts_in_isolation() -> Result<()> {
    let mut mock = MockSpanner::new();
    let mut sequence = Sequence::new();

    mock.expect_create_session()
        .once()
        .in_sequence(&mut sequence)
        .returning(|_| {
            Err(TonicStatus::unavailable(
                "server is temporarily unavailable",
            ))
        });

    mock.expect_create_session()
        .once()
        .in_sequence(&mut sequence)
        .returning(|request| {
            let request = request.into_inner();
            let session = request.session.expect("session present in request");
            assert!(session.multiplexed, "session should be multiplexed");

            Ok(Response::new(Session {
                name:
                    "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
                        .to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

    mock.expect_execute_streaming_sql().once().returning(|_| {
        let result_set = PartialResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType {
                    fields: vec![Field {
                        name: "num".to_string(),
                        r#type: Some(SpannerType {
                            code: TypeCode::Int64 as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            values: vec![ProstValue {
                kind: Some(ProstValueKind::StringValue("42".to_string())),
            }],
            last: true,
            ..Default::default()
        };
        Ok(Response::new(adapt([Ok(result_set)])))
    });

    let (address, server) = start("127.0.0.1:0", mock).await?;
    let _server_guard = ServerGuard(server);

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_instance_type(InstanceType::Omni)
        .with_meter_provider(provider)
        .build()
        .await?;

    let (row_count, value) = execute_test_query(&spanner).await?;
    assert_eq!(row_count, 1, "Exactly one row should be yielded");
    assert_eq!(value, 42, "Returned value should match mock data");

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;

    let attempt_count_metric = find_metric(&finished_metrics, "spanner/client/attempt_count")
        .expect("spanner/client/attempt_count must be recorded on custom provider");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(
        attempt_points.len(),
        3,
        "Expected 3 attempt_count points: 1 sql attempt + 2 session attempts"
    );

    assert_eq!(
        find_point(&attempt_points, "Spanner.CreateSession", "UNAVAILABLE"),
        Some(1),
        "Failed Spanner.CreateSession attempt must be recorded with UNAVAILABLE"
    );
    assert_eq!(
        find_point(&attempt_points, "Spanner.CreateSession", "OK"),
        Some(1),
        "Successful Spanner.CreateSession attempt must be recorded with OK"
    );
    assert_eq!(
        find_point(&attempt_points, "Spanner.ExecuteStreamingSql", "OK"),
        Some(1),
        "Spanner.ExecuteStreamingSql attempt must be recorded with OK"
    );

    meter_provider.shutdown()?;
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_custom_meter_provider_records_streaming_read_in_isolation() -> Result<()> {
    let mut mock = MockSpanner::new();
    setup_mock_session(&mut mock);

    mock.expect_streaming_read().once().returning(|_| {
        let result_set = PartialResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType {
                    fields: vec![Field {
                        name: "num".to_string(),
                        r#type: Some(SpannerType {
                            code: TypeCode::Int64 as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            values: vec![ProstValue {
                kind: Some(ProstValueKind::StringValue("99".to_string())),
            }],
            last: true,
            ..Default::default()
        };

        let mut response = Response::new(adapt([Ok(result_set)]));
        response.metadata_mut().insert(
            "server-timing",
            "gfet4t7;dur=15.0".parse().expect("valid server-timing"),
        );
        Ok(response)
    });

    let (address, server) = start("127.0.0.1:0", mock).await?;
    let _server_guard = ServerGuard(server);

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_instance_type(InstanceType::Omni)
        .with_meter_provider(provider)
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-db")
        .build()
        .await?;

    let single_use_transaction = database_client.single_use().build();
    let read_request = ReadRequest::builder("Singers", vec!["num"])
        .with_keys(KeySet::all())
        .build();
    let mut result_set = single_use_transaction.execute_read(read_request).await?;

    let mut row_count = 0;
    let mut last_value = 0;
    while let Some(row) = result_set.next().await.transpose()? {
        last_value = row.get("num")?;
        row_count += 1;
    }
    assert_eq!(row_count, 1, "Exactly one row should be yielded");
    assert_eq!(last_value, 99, "Returned value should match mock data");

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;

    let operation_count_metric = find_metric(&finished_metrics, "spanner/client/operation_count")
        .expect("spanner/client/operation_count must be recorded on custom provider");
    let operation_points = get_sum_u64_points(operation_count_metric);
    assert_eq!(
        operation_points.len(),
        2,
        "Expected 2 operation_count points"
    );

    assert_eq!(
        find_point(&operation_points, "Spanner.StreamingRead", "OK"),
        Some(1),
        "StreamingRead count with status OK should be 1"
    );

    let attempt_count_metric = find_metric(&finished_metrics, "spanner/client/attempt_count")
        .expect("spanner/client/attempt_count must be recorded on custom provider");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(attempt_points.len(), 2, "Expected 2 attempt_count points");
    assert_eq!(
        find_point(&attempt_points, "Spanner.StreamingRead", "OK"),
        Some(1),
        "StreamingRead attempt count with status OK should be 1"
    );

    let gfe_latencies_metric = find_metric(&finished_metrics, "spanner/client/gfe_latencies")
        .expect("spanner/client/gfe_latencies must be recorded when server-timing is present");
    let gfe_counts = get_histogram_counts(gfe_latencies_metric);
    assert!(
        !gfe_counts.is_empty(),
        "gfe_latencies must have data points for StreamingRead"
    );

    meter_provider.shutdown()?;
    Ok(())
}

#[tokio_test_no_panics]
async fn spanner_emulator_suppresses_metrics_to_custom_provider_in_isolation() -> Result<()> {
    let (address, _server_guard) = setup_mock_spanner_server(None).await?;

    let (exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let mut spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_instance_type(InstanceType::Omni)
        .with_meter_provider(provider)
        .with_export_builtin_metrics_to_custom_provider(true)
        .build()
        .await?;
    // Set emulator mode directly on Spanner without mutating process-global
    // environment variables (which would contaminate concurrent parallel tests).
    spanner.is_emulator = true;

    assert!(
        spanner.is_emulator(),
        "Client is_emulator flag should be true"
    );

    let (row_count, value) = execute_test_query(&spanner).await?;
    assert_eq!(row_count, 1, "Exactly one row should be yielded");
    assert_eq!(value, 42, "Returned value should match mock data");

    meter_provider.force_flush()?;

    let finished_metrics = exporter.get_finished_metrics()?;
    assert_no_custom_metrics_exported(&finished_metrics);

    meter_provider.shutdown()?;
    Ok(())
}

#[cfg(not(feature = "builtin-metrics"))]
#[tokio_test_no_panics]
async fn spanner_standalone_unstable_metrics_without_builtin_metrics_has_no_gcm_export()
-> Result<()> {
    let (_exporter, meter_provider) = create_test_meter_provider();
    let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::clone(&meter_provider) as _;

    let spanner = Spanner::builder()
        .with_endpoint("http://127.0.0.1:1")
        .with_credentials(Anonymous::new().build())
        .with_meter_provider(provider)
        .build()
        .await?;

    assert!(
        spanner.meter_provider().is_some(),
        "custom MeterProvider must be preserved on standalone client"
    );
    assert_eq!(
        spanner.export_builtin_metrics_to_custom_provider(),
        None,
        "export_builtin_metrics_to_custom_provider should default to None"
    );

    meter_provider.shutdown()?;
    Ok(())
}

#[test]
fn spanner_reexports_opentelemetry_crate() {
    use crate::opentelemetry::KeyValue;

    let key_value = KeyValue::new("k", "v");
    assert_eq!(key_value.key.as_str(), "k");
    assert_eq!(key_value.value.as_str(), "v");
}
