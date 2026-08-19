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

use crate::client::Spanner;
use crate::database_client::DatabaseClient;
use crate::key::KeySet;
use crate::observability::exporter::{
    convert_metric_to_time_series, resource_to_monitored_resource,
};
use crate::observability::metrics::{Observability, SpannerMetrics, client_name};
use crate::read::ReadRequest;
use crate::statement::Statement;
use gaxi::grpc::tonic::{Code as GrpcCode, Response, Status};
use google_cloud_api::model::metric_descriptor::MetricKind;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_test_macros::tokio_test_no_panics;
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, Metric as OTelMetric, MetricData, ResourceMetrics},
};
use prost_types::{Value as ProstValue, value::Kind as ProstValueKind};
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::spanner::v1::{
    PartialResultSet, ResultSetMetadata, Session, StructType, Type as SpannerType, TypeCode,
    struct_type::Field,
};
use spanner_grpc_mock::start;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc::{Receiver, channel};
use tokio::task::JoinHandle;

const TEST_CLIENT_UID: &str = "test-uid-12345";
const TEST_CLIENT_HASH: &str = "abc1234";
const TEST_DATABASE: &str = "test-database";
const TEST_INSTANCE: &str = "test-instance";
const TEST_LOCATION: &str = "global";
const TEST_INSTANCE_CONFIG: &str = "test-config";

#[tokio_test_no_panics]
async fn streaming_sql_happy_path_records_all_metrics_and_time_series() -> anyhow::Result<()> {
    let mut mock = MockSpanner::new();

    mock.expect_create_session().returning(|_| {
        Ok(Response::new(Session {
            name:
                "projects/test-project/instances/test-instance/databases/test-database/sessions/s1"
                    .to_string(),
            multiplexed: true,
            ..Default::default()
        }))
    });

    mock.expect_execute_streaming_sql().returning(|_request| {
        let stream = adapt(vec![Ok(create_test_result_set("42", true))].into_iter());
        let mut response = Response::from(stream);
        response.metadata_mut().insert(
            "server-timing",
            "gfet4t7;dur=20.5,afe;dur=8.1"
                .parse()
                .expect("valid server-timing header"),
        );
        Ok(response)
    });

    let (database_client, exporter, meter_provider, _server_handle) =
        setup_mock_client_with_metrics(mock).await?;

    let single_use_transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT 1 AS num").build();
    let mut result_set = single_use_transaction.execute_query(statement).await?;

    let mut row_count = 0;
    while let Some(row) = result_set.next().await.transpose()? {
        let value: i64 = row.get("num");
        assert_eq!(value, 42, "Returned value should match mock data");
        row_count += 1;
    }
    assert_eq!(row_count, 1, "Exactly one row should be yielded");

    meter_provider
        .force_flush()
        .expect("force_flush should succeed");

    let resource_metrics_list = exporter
        .get_finished_metrics()
        .expect("finished metrics should be present");

    // 1. Validate operation_count
    let operation_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_count",
    )
    .expect("operation_count metric must be recorded");
    let operation_points = get_sum_u64_points(operation_count_metric);
    assert_eq!(
        operation_points.len(),
        1,
        "Expected 1 operation_count point"
    );
    assert_eq!(
        operation_points[0].0, 1,
        "operation_count value should be 1"
    );
    assert_operation_labels(&operation_points[0].1, "Spanner.ExecuteStreamingSql", "OK");

    // 2. Validate attempt_count
    let attempt_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_count",
    )
    .expect("attempt_count metric must be recorded");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(attempt_points.len(), 1, "Expected 1 attempt_count point");
    assert_eq!(attempt_points[0].0, 1, "attempt_count value should be 1");
    assert_attempt_labels(&attempt_points[0].1, "Spanner.ExecuteStreamingSql", "OK");

    // 3. Validate latencies histograms
    let operation_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_latencies",
    )
    .expect("operation_latencies metric must be recorded");
    let operation_latency_counts = get_histogram_counts(operation_latencies_metric);
    assert_eq!(
        operation_latency_counts.len(),
        1,
        "Expected 1 operation latency entry"
    );
    assert_eq!(
        operation_latency_counts[0].0, 1,
        "Count in operation_latencies histogram should be 1"
    );
    assert_operation_labels(
        &operation_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "OK",
    );

    let attempt_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_latencies",
    )
    .expect("attempt_latencies metric must be recorded");
    let attempt_latency_counts = get_histogram_counts(attempt_latencies_metric);
    assert_eq!(
        attempt_latency_counts.len(),
        1,
        "Expected 1 attempt latency entry"
    );
    assert_eq!(
        attempt_latency_counts[0].0, 1,
        "Count in attempt_latencies histogram should be 1"
    );
    assert_attempt_labels(
        &attempt_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "OK",
    );

    let gfe_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_latencies",
    )
    .expect("gfe_latencies metric must be recorded");
    let gfe_latency_counts = get_histogram_counts(gfe_metric);
    assert_eq!(gfe_latency_counts.len(), 1, "Expected 1 GFE latency entry");
    assert_eq!(
        gfe_latency_counts[0].0, 1,
        "GFE histogram count should be 1"
    );
    assert_attempt_labels(
        &gfe_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "OK",
    );

    let access_frontend_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/afe_latencies",
    )
    .expect("access frontend latencies metric must be recorded");
    let access_frontend_latency_counts = get_histogram_counts(access_frontend_metric);
    assert_eq!(
        access_frontend_latency_counts.len(),
        1,
        "Expected 1 access frontend latency entry"
    );
    assert_eq!(
        access_frontend_latency_counts[0].0, 1,
        "Access frontend histogram count should be 1"
    );
    assert_attempt_labels(
        &access_frontend_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "OK",
    );

    // 4. Validate Cloud Monitoring TimeSeries conversion & MonitoredResource schema
    let mut time_series_list = Vec::new();
    for resource_metrics in &resource_metrics_list {
        let monitored_resource = resource_to_monitored_resource(resource_metrics.resource());
        assert_eq!(
            monitored_resource.r#type, "spanner_instance_client",
            "Resource type must be spanner_instance_client"
        );
        assert_eq!(
            monitored_resource
                .labels
                .get("instance_id")
                .map(String::as_str),
            Some(TEST_INSTANCE),
            "MonitoredResource must contain instance_id"
        );
        assert_eq!(
            monitored_resource
                .labels
                .get("location")
                .map(String::as_str),
            Some(TEST_LOCATION),
            "MonitoredResource must contain location"
        );
        assert_eq!(
            monitored_resource
                .labels
                .get("instance_config")
                .map(String::as_str),
            Some(TEST_INSTANCE_CONFIG),
            "MonitoredResource must contain instance_config"
        );
        assert_eq!(
            monitored_resource
                .labels
                .get("client_hash")
                .map(String::as_str),
            Some(TEST_CLIENT_HASH),
            "MonitoredResource must contain client_hash"
        );

        for scope_metrics in resource_metrics.scope_metrics() {
            for metric in scope_metrics.metrics() {
                convert_metric_to_time_series(metric, &monitored_resource, &mut time_series_list);
            }
        }
    }

    assert!(
        !time_series_list.is_empty(),
        "TimeSeries list should not be empty"
    );
    for time_series in &time_series_list {
        assert_eq!(
            time_series.metric_kind,
            MetricKind::Cumulative,
            "Metric kind must be Cumulative"
        );
        let metric = time_series
            .metric
            .as_ref()
            .expect("metric field must be present");
        assert!(
            metric
                .r#type
                .starts_with("spanner.googleapis.com/internal/client/"),
            "Metric type {} must have client prefix",
            metric.r#type
        );
        assert_eq!(
            metric.labels.get("database").map(String::as_str),
            Some(TEST_DATABASE),
            "Database label must be present on TimeSeries"
        );
        assert_eq!(
            metric.labels.get("client_uid").map(String::as_str),
            Some(TEST_CLIENT_UID),
            "client_uid label must be present on TimeSeries"
        );
        assert_eq!(
            metric.labels.get("client_name").map(String::as_str),
            Some(client_name()),
            "client_name label must be present on TimeSeries"
        );

        let point = time_series
            .points
            .first()
            .expect("TimeSeries point must be present");
        let interval = point
            .interval
            .as_ref()
            .expect("Point interval must be present");
        let start_time = interval
            .start_time
            .as_ref()
            .expect("start_time must be present");
        let end_time = interval
            .end_time
            .as_ref()
            .expect("end_time must be present");
        assert!(
            start_time.seconds() <= end_time.seconds(),
            "start_time must precede or equal end_time"
        );
    }

    Ok(())
}

#[tokio_test_no_panics]
async fn streaming_read_happy_path_records_all_metrics() -> anyhow::Result<()> {
    let mut mock = MockSpanner::new();

    mock.expect_create_session().returning(|_| {
        Ok(Response::new(Session {
            name:
                "projects/test-project/instances/test-instance/databases/test-database/sessions/s1"
                    .to_string(),
            multiplexed: true,
            ..Default::default()
        }))
    });

    mock.expect_streaming_read().returning(|_request| {
        let stream = adapt(vec![Ok(create_test_result_set("100", true))].into_iter());
        let mut response = Response::from(stream);
        response.metadata_mut().insert(
            "server-timing",
            "gfet4t7;dur=12.0".parse().expect("valid server-timing"),
        );
        Ok(response)
    });

    let (database_client, exporter, meter_provider, _server_handle) =
        setup_mock_client_with_metrics(mock).await?;

    let single_use_transaction = database_client.single_use().build();
    let read_request = ReadRequest::builder("Singers", vec!["num"])
        .with_keys(KeySet::all())
        .build();
    let mut result_set = single_use_transaction.execute_read(read_request).await?;

    let mut count = 0;
    while let Some(row) = result_set.next().await.transpose()? {
        let value: i64 = row.get("num");
        assert_eq!(value, 100);
        count += 1;
    }
    assert_eq!(count, 1);

    meter_provider
        .force_flush()
        .expect("force_flush should succeed");

    let resource_metrics_list = exporter
        .get_finished_metrics()
        .expect("finished metrics should be present");

    // 1. operation_count
    let operation_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_count",
    )
    .expect("operation_count metric must be recorded");
    let operation_points = get_sum_u64_points(operation_count_metric);
    assert_eq!(operation_points.len(), 1);
    assert_eq!(operation_points[0].0, 1);
    assert_operation_labels(&operation_points[0].1, "Spanner.StreamingRead", "OK");

    // 2. attempt_count
    let attempt_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_count",
    )
    .expect("attempt_count metric must be recorded");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(attempt_points.len(), 1);
    assert_eq!(attempt_points[0].0, 1);
    assert_attempt_labels(&attempt_points[0].1, "Spanner.StreamingRead", "OK");

    // 3. operation_latencies
    let operation_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_latencies",
    )
    .expect("operation_latencies metric must be recorded");
    let operation_latency_counts = get_histogram_counts(operation_latencies_metric);
    assert_eq!(operation_latency_counts.len(), 1);
    assert_eq!(operation_latency_counts[0].0, 1);
    assert_operation_labels(
        &operation_latency_counts[0].1,
        "Spanner.StreamingRead",
        "OK",
    );

    // 4. attempt_latencies
    let attempt_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_latencies",
    )
    .expect("attempt_latencies metric must be recorded");
    let attempt_latency_counts = get_histogram_counts(attempt_latencies_metric);
    assert_eq!(attempt_latency_counts.len(), 1);
    assert_eq!(attempt_latency_counts[0].0, 1);
    assert_attempt_labels(&attempt_latency_counts[0].1, "Spanner.StreamingRead", "OK");

    // 5. gfe_latencies
    let gfe_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_latencies",
    )
    .expect("gfe_latencies metric must be recorded");
    let gfe_latency_counts = get_histogram_counts(gfe_metric);
    assert_eq!(gfe_latency_counts.len(), 1);
    assert_eq!(gfe_latency_counts[0].0, 1);
    assert_attempt_labels(&gfe_latency_counts[0].1, "Spanner.StreamingRead", "OK");

    Ok(())
}

#[tokio_test_no_panics]
async fn streaming_sql_hard_error_records_failure_status() -> anyhow::Result<()> {
    let mut mock = MockSpanner::new();

    mock.expect_create_session().returning(|_| {
        Ok(Response::new(Session {
            name:
                "projects/test-project/instances/test-instance/databases/test-database/sessions/s1"
                    .to_string(),
            multiplexed: true,
            ..Default::default()
        }))
    });

    mock.expect_execute_streaming_sql().returning(|_request| {
        let stream = adapt(vec![Err(Status::new(
            GrpcCode::InvalidArgument,
            "Syntax error in SQL query statement",
        ))]);
        Ok(Response::from(stream))
    });

    let (database_client, exporter, meter_provider, _server_handle) =
        setup_mock_client_with_metrics(mock).await?;

    let single_use_transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT syntax_error FROM").build();
    let query_result = single_use_transaction.execute_query(statement).await;

    assert!(
        query_result.is_err(),
        "Query execution should fail with hard error"
    );

    meter_provider
        .force_flush()
        .expect("force_flush should succeed");

    let resource_metrics_list = exporter
        .get_finished_metrics()
        .expect("finished metrics should be present");

    // 1. operation_count (INVALID_ARGUMENT)
    let operation_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_count",
    )
    .expect("operation_count metric must be recorded");
    let operation_points = get_sum_u64_points(operation_count_metric);
    assert_eq!(operation_points.len(), 1);
    assert_eq!(operation_points[0].0, 1);
    assert_operation_labels(
        &operation_points[0].1,
        "Spanner.ExecuteStreamingSql",
        "INVALID_ARGUMENT",
    );

    // 2. attempt_count (INVALID_ARGUMENT)
    let attempt_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_count",
    )
    .expect("attempt_count metric must be recorded");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(attempt_points.len(), 1);
    assert_eq!(attempt_points[0].0, 1);
    assert_attempt_labels(
        &attempt_points[0].1,
        "Spanner.ExecuteStreamingSql",
        "INVALID_ARGUMENT",
    );

    // 3. operation_latencies (INVALID_ARGUMENT)
    let operation_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_latencies",
    )
    .expect("operation_latencies metric must be recorded");
    let operation_latency_counts = get_histogram_counts(operation_latencies_metric);
    assert_eq!(operation_latency_counts.len(), 1);
    assert_eq!(operation_latency_counts[0].0, 1);
    assert_operation_labels(
        &operation_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "INVALID_ARGUMENT",
    );

    // 4. attempt_latencies (INVALID_ARGUMENT)
    let attempt_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_latencies",
    )
    .expect("attempt_latencies metric must be recorded");
    let attempt_latency_counts = get_histogram_counts(attempt_latencies_metric);
    assert_eq!(attempt_latency_counts.len(), 1);
    assert_eq!(attempt_latency_counts[0].0, 1);
    assert_attempt_labels(
        &attempt_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "INVALID_ARGUMENT",
    );

    // 5. gfe_connectivity_error_count (incremented on error without server-timing)
    let gfe_error_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_connectivity_error_count",
    )
    .expect("gfe_connectivity_error_count metric must be recorded");
    let gfe_error_points = get_sum_u64_points(gfe_error_metric);
    assert_eq!(gfe_error_points.len(), 1);
    assert_eq!(gfe_error_points[0].0, 1);
    assert_attempt_labels(
        &gfe_error_points[0].1,
        "Spanner.ExecuteStreamingSql",
        "INVALID_ARGUMENT",
    );

    // 6. gfe_latencies must NOT be recorded
    let gfe_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_latencies",
    );
    assert!(
        gfe_latencies_metric.is_none(),
        "gfe_latencies must not be recorded when error has no server-timing"
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn streaming_sql_transient_retry_records_multiple_attempts_and_single_operation()
-> anyhow::Result<()> {
    let mut mock = MockSpanner::new();

    mock.expect_create_session().returning(|_| {
        Ok(Response::new(Session {
            name:
                "projects/test-project/instances/test-instance/databases/test-database/sessions/s1"
                    .to_string(),
            multiplexed: true,
            ..Default::default()
        }))
    });

    let attempt_counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&attempt_counter);

    mock.expect_execute_streaming_sql()
        .returning(move |_request| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                let stream = adapt(vec![Err(Status::new(
                    GrpcCode::Unavailable,
                    "Spanner backend temporarily unavailable",
                ))]);
                Ok(Response::from(stream))
            } else {
                let stream = adapt(vec![Ok(create_test_result_set("77", true))].into_iter());
                let mut response = Response::from(stream);
                response.metadata_mut().insert(
                    "server-timing",
                    "gfet4t7;dur=18.0".parse().expect("valid server-timing"),
                );
                Ok(response)
            }
        });

    let (database_client, exporter, meter_provider, _server_handle) =
        setup_mock_client_with_metrics(mock).await?;

    let single_use_transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT 77 AS num").build();
    let mut result_set = single_use_transaction.execute_query(statement).await?;

    let mut row_count = 0;
    while let Some(row) = result_set.next().await {
        let row = row?;
        let value: i64 = row.get("num");
        assert_eq!(value, 77);
        row_count += 1;
    }
    assert_eq!(row_count, 1, "Retry should succeed and yield row");
    assert_eq!(
        attempt_counter.load(Ordering::SeqCst),
        2,
        "Mock should have received 2 gRPC attempts"
    );

    meter_provider
        .force_flush()
        .expect("force_flush should succeed");

    let resource_metrics_list = exporter
        .get_finished_metrics()
        .expect("finished metrics should be present");

    // 1. attempt_count: 2 points (UNAVAILABLE and OK)
    let attempt_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_count",
    )
    .expect("attempt_count metric must be recorded");
    let attempt_points = get_sum_u64_points(attempt_count_metric);
    assert_eq!(
        attempt_points.len(),
        2,
        "Expected 2 attempt_count points (UNAVAILABLE and OK)"
    );

    let unavailable_attempt = attempt_points
        .iter()
        .find(|(value, labels)| {
            *value == 1 && labels.get("status").map(String::as_str) == Some("UNAVAILABLE")
        })
        .expect("Must record attempt with status UNAVAILABLE");
    assert_attempt_labels(
        &unavailable_attempt.1,
        "Spanner.ExecuteStreamingSql",
        "UNAVAILABLE",
    );

    let ok_attempt = attempt_points
        .iter()
        .find(|(value, labels)| {
            *value == 1 && labels.get("status").map(String::as_str) == Some("OK")
        })
        .expect("Must record attempt with status OK");
    assert_attempt_labels(&ok_attempt.1, "Spanner.ExecuteStreamingSql", "OK");

    // 2. attempt_latencies: 2 points (UNAVAILABLE and OK)
    let attempt_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/attempt_latencies",
    )
    .expect("attempt_latencies metric must be recorded");
    let attempt_latency_counts = get_histogram_counts(attempt_latencies_metric);
    assert_eq!(
        attempt_latency_counts.len(),
        2,
        "Expected 2 attempt_latencies points (UNAVAILABLE and OK)"
    );

    // 3. operation_count: exactly 1 point for the overall completed operation (OK)
    let operation_count_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_count",
    )
    .expect("operation_count metric must be recorded");
    let operation_points = get_sum_u64_points(operation_count_metric);
    assert_eq!(
        operation_points.len(),
        1,
        "Expected exactly 1 operation_count point for the completed operation"
    );
    assert_eq!(operation_points[0].0, 1);
    assert_operation_labels(&operation_points[0].1, "Spanner.ExecuteStreamingSql", "OK");

    // 4. operation_latencies: exactly 1 point (OK)
    let operation_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/operation_latencies",
    )
    .expect("operation_latencies metric must be recorded");
    let operation_latency_counts = get_histogram_counts(operation_latencies_metric);
    assert_eq!(operation_latency_counts.len(), 1);
    assert_eq!(operation_latency_counts[0].0, 1);
    assert_operation_labels(
        &operation_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "OK",
    );

    // 5. gfe_connectivity_error_count: 1 point for attempt 1 (UNAVAILABLE)
    let gfe_error_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_connectivity_error_count",
    )
    .expect("gfe_connectivity_error_count metric must be recorded");
    let gfe_error_points = get_sum_u64_points(gfe_error_metric);
    assert_eq!(gfe_error_points.len(), 1);
    assert_eq!(gfe_error_points[0].0, 1);
    assert_attempt_labels(
        &gfe_error_points[0].1,
        "Spanner.ExecuteStreamingSql",
        "UNAVAILABLE",
    );

    // 6. gfe_latencies: 1 point for attempt 2 (OK with server-timing)
    let gfe_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_latencies",
    )
    .expect("gfe_latencies metric must be recorded");
    let gfe_latency_counts = get_histogram_counts(gfe_metric);
    assert_eq!(gfe_latency_counts.len(), 1);
    assert_eq!(gfe_latency_counts[0].0, 1);
    assert_attempt_labels(
        &gfe_latency_counts[0].1,
        "Spanner.ExecuteStreamingSql",
        "OK",
    );

    Ok(())
}

#[tokio_test_no_panics]
async fn streaming_sql_missing_server_timing_increments_gfe_connectivity_error()
-> anyhow::Result<()> {
    let mut mock = MockSpanner::new();

    mock.expect_create_session().returning(|_| {
        Ok(Response::new(Session {
            name:
                "projects/test-project/instances/test-instance/databases/test-database/sessions/s1"
                    .to_string(),
            multiplexed: true,
            ..Default::default()
        }))
    });

    // Response with NO server-timing header
    mock.expect_execute_streaming_sql().returning(|_request| {
        let stream = adapt(vec![Ok(create_test_result_set("123", true))].into_iter());
        Ok(Response::from(stream))
    });

    let (database_client, exporter, meter_provider, _server_handle) =
        setup_mock_client_with_metrics(mock).await?;

    let single_use_transaction = database_client.single_use().build();
    let statement = Statement::builder("SELECT 123 AS num").build();
    let mut result_set = single_use_transaction.execute_query(statement).await?;

    while let Some(row) = result_set.next().await.transpose()? {
        let value: i64 = row.get("num");
        assert_eq!(value, 123);
    }

    meter_provider
        .force_flush()
        .expect("force_flush should succeed");

    let resource_metrics_list = exporter
        .get_finished_metrics()
        .expect("finished metrics should be present");

    // gfe_connectivity_error_count must be incremented
    let gfe_error_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_connectivity_error_count",
    )
    .expect("gfe_connectivity_error_count metric must be recorded");
    let gfe_error_points = get_sum_u64_points(gfe_error_metric);
    assert_eq!(gfe_error_points.len(), 1);
    assert_eq!(
        gfe_error_points[0].0, 1,
        "gfe_connectivity_error_count value should be 1"
    );
    assert_attempt_labels(&gfe_error_points[0].1, "Spanner.ExecuteStreamingSql", "OK");

    // gfe_latencies, access frontend latencies, and connectivity error count should NOT be recorded
    let gfe_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/gfe_latencies",
    );
    assert!(
        gfe_latencies_metric.is_none(),
        "gfe_latencies must not be recorded when server-timing is absent"
    );

    let access_frontend_latencies_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/afe_latencies",
    );
    assert!(
        access_frontend_latencies_metric.is_none(),
        "access frontend latencies must not be recorded when server-timing is absent"
    );

    let access_frontend_connectivity_error_metric = find_metric(
        &resource_metrics_list,
        "spanner.googleapis.com/internal/client/afe_connectivity_error_count",
    );
    assert!(
        access_frontend_connectivity_error_metric.is_none(),
        "access frontend connectivity error count must not be recorded"
    );

    Ok(())
}

// --- Test Helper Functions & Fixtures ---

fn assert_operation_labels(labels: &HashMap<String, String>, method: &str, status: &str) {
    assert_eq!(
        labels.get("client_uid").map(String::as_str),
        Some(TEST_CLIENT_UID),
        "client_uid label mismatch"
    );
    assert_eq!(
        labels.get("client_name").map(String::as_str),
        Some(client_name()),
        "client_name label mismatch"
    );
    assert_eq!(
        labels.get("database").map(String::as_str),
        Some(TEST_DATABASE),
        "database label mismatch"
    );
    assert_eq!(
        labels.get("method").map(String::as_str),
        Some(method),
        "method label mismatch"
    );
    assert_eq!(
        labels.get("status").map(String::as_str),
        Some(status),
        "status label mismatch"
    );
    assert_eq!(
        labels.get("directpath_enabled").map(String::as_str),
        Some("false"),
        "directpath_enabled label mismatch"
    );
}

fn assert_attempt_labels(labels: &HashMap<String, String>, method: &str, status: &str) {
    assert_operation_labels(labels, method, status);
    assert_eq!(
        labels.get("directpath_used").map(String::as_str),
        Some("false"),
        "directpath_used label mismatch"
    );
}

fn adapt<I, T>(items: I) -> Receiver<T>
where
    I: IntoIterator<Item = T>,
    I::IntoIter: ExactSizeIterator,
{
    let items = items.into_iter();
    let (sender, receiver) = channel(items.len().max(1));
    for item in items {
        sender
            .try_send(item)
            .expect("allocated channel capacity must be sufficient");
    }
    receiver
}

fn create_test_result_set(num_value: &str, last: bool) -> PartialResultSet {
    PartialResultSet {
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
            kind: Some(ProstValueKind::StringValue(num_value.to_string())),
        }],
        last,
        ..Default::default()
    }
}

async fn setup_mock_client_with_metrics(
    mock: MockSpanner,
) -> anyhow::Result<(
    DatabaseClient,
    InMemoryMetricExporter,
    SdkMeterProvider,
    JoinHandle<()>,
)> {
    let (address, server_handle) = start("127.0.0.1:0", mock).await?;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let mut database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await?;

    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("project_id", "test-project"),
            KeyValue::new("instance_id", TEST_INSTANCE),
            KeyValue::new("location", TEST_LOCATION),
            KeyValue::new("instance_config", TEST_INSTANCE_CONFIG),
            KeyValue::new("client_hash", TEST_CLIENT_HASH),
        ])
        .build();

    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone()).build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();
    let meter = meter_provider.meter("cloud.google.com/rust");
    let metrics = SpannerMetrics::new(meter);

    let common_attributes = [
        KeyValue::new("client_uid", TEST_CLIENT_UID),
        KeyValue::new("client_name", client_name()),
        KeyValue::new("database", TEST_DATABASE),
    ];

    let observability = Observability {
        metrics: Some(Arc::new(metrics)),
        common_attributes,
        meter_provider: Some(Arc::new(meter_provider.clone())),
    };

    database_client.o11y = Arc::new(observability);

    Ok((database_client, exporter, meter_provider, server_handle))
}

fn find_metric<'a>(
    resource_metrics_list: &'a [ResourceMetrics],
    expected_name: &str,
) -> Option<&'a OTelMetric> {
    for resource_metrics in resource_metrics_list {
        for scope_metrics in resource_metrics.scope_metrics() {
            for metric in scope_metrics.metrics() {
                if metric.name() == expected_name {
                    return Some(metric);
                }
            }
        }
    }
    None
}

fn get_sum_u64_points(metric: &OTelMetric) -> Vec<(u64, HashMap<String, String>)> {
    let mut points = Vec::new();
    if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
        for data_point in sum.data_points() {
            let labels: HashMap<String, String> = data_point
                .attributes()
                .map(|key_value| {
                    (
                        key_value.key.as_str().to_string(),
                        key_value.value.as_str().to_string(),
                    )
                })
                .collect();
            points.push((data_point.value(), labels));
        }
    }
    points
}

fn get_histogram_counts(metric: &OTelMetric) -> Vec<(u64, HashMap<String, String>)> {
    let mut counts = Vec::new();
    if let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() {
        for data_point in histogram.data_points() {
            let labels: HashMap<String, String> = data_point
                .attributes()
                .map(|key_value| {
                    (
                        key_value.key.as_str().to_string(),
                        key_value.value.as_str().to_string(),
                    )
                })
                .collect();
            counts.push((data_point.count(), labels));
        }
    }
    counts
}
