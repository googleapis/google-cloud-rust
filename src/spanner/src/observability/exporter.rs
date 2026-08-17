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

use google_cloud_api::model::distribution::{BucketOptions, bucket_options::Explicit};
use google_cloud_api::model::{Distribution, Metric, MonitoredResource, metric_descriptor};
use google_cloud_gax::error::rpc::Code;
use google_cloud_monitoring_v3::client::MetricService;
use google_cloud_monitoring_v3::model::typed_value::Value;
use google_cloud_monitoring_v3::model::{Point, TimeInterval, TimeSeries, TypedValue};
use opentelemetry::{KeyValue, Value as OTelValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, HistogramDataPoint, Metric as OTelMetric, MetricData, ResourceMetrics,
    SumDataPoint,
};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;

const SPANNER_METER_NAME: &str = "cloud.google.com/rust";
const NATIVE_METRICS_PREFIX: &str = "spanner.googleapis.com/internal/client/";
const SPANNER_RESOURCE_TYPE: &str = "spanner_instance_client";
const SEND_BATCH_SIZE: usize = 200;
const INITIAL_TIME_SERIES_CAPACITY: usize = 32;

#[derive(Clone, Debug)]
pub(crate) struct GcpMonitoringExporter {
    client: Arc<MetricService>,
    project_name: String,
    handle: Option<Handle>,
}

impl GcpMonitoringExporter {
    pub(crate) fn new(client: MetricService, project_id: &str) -> Self {
        Self {
            client: Arc::new(client),
            project_name: format!("projects/{}", project_id),
            handle: Handle::try_current().ok(),
        }
    }
}

impl PushMetricExporter for GcpMonitoringExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut time_series_list = Vec::with_capacity(INITIAL_TIME_SERIES_CAPACITY);
        let monitored_resource = resource_to_monitored_resource(metrics.resource());

        for scope_metrics in metrics.scope_metrics() {
            let scope_name = scope_metrics.scope().name();
            if scope_name != SPANNER_METER_NAME
                && !scope_name.contains("cloud.google.com")
                && !scope_name.contains("spanner")
            {
                continue;
            }

            for metric in scope_metrics.metrics() {
                convert_metric_to_time_series(metric, &monitored_resource, &mut time_series_list);
            }
        }

        if time_series_list.is_empty() {
            return Ok(());
        }

        let client = Arc::clone(&self.client);
        let project_name = self.project_name.clone();

        let result = if Handle::try_current().is_ok() {
            send_time_series_batches(client, project_name, time_series_list)
                .await
                .map_err(|e| e.to_string())
        } else if let Some(handle) = self.handle.as_ref() {
            match handle
                .spawn(send_time_series_batches(
                    client,
                    project_name,
                    time_series_list,
                ))
                .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e.to_string()),
                Err(e) => Err(format!(
                    "Failed to join Tokio task for metrics batch export: {e}"
                )),
            }
        } else {
            Err("No Tokio runtime handle available for exporting Spanner metrics batch".to_string())
        };

        if let Err(err_msg) = result {
            tracing::warn!("Failed to export Spanner metrics batch: {err_msg}");
            return Err(OTelSdkError::InternalFailure(err_msg));
        }

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown()
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

async fn send_time_series_batches(
    client: Arc<MetricService>,
    project_name: String,
    time_series_list: Vec<TimeSeries>,
) -> crate::Result<()> {
    let mut last_error = None;
    let mut iter = time_series_list.into_iter();
    loop {
        let chunk: Vec<_> = iter.by_ref().take(SEND_BATCH_SIZE).collect();
        if chunk.is_empty() {
            break;
        }
        let res = client
            .create_service_time_series()
            .set_name(project_name.clone())
            .set_time_series(chunk)
            .send()
            .await;

        if let Err(e) = res {
            if is_permission_denied(&e) {
                tracing::warn!(
                    "Failed to export Spanner metrics batch: Need monitoring metric writer permission on {}. Follow https://cloud.google.com/spanner/docs/view-manage-client-side-metrics#access-client-side-metrics to set up permissions. Error: {:?}",
                    project_name,
                    e
                );
            } else {
                tracing::warn!("Failed to export Spanner metrics batch: {:?}", e);
            }
            last_error = Some(e);
        }
    }

    if let Some(error) = last_error {
        return Err(error);
    }
    Ok(())
}

fn is_permission_denied(err: &crate::Error) -> bool {
    err.status()
        .map(|s| s.code == Code::PermissionDenied)
        .unwrap_or(false)
}

fn convert_metric_to_time_series(
    metric: &OTelMetric,
    monitored_resource: &MonitoredResource,
    out: &mut Vec<TimeSeries>,
) {
    let metric_name = metric.name();
    let metric_type = if metric_name.starts_with("spanner.googleapis.com/") {
        metric_name.to_string()
    } else {
        format!("{NATIVE_METRICS_PREFIX}{metric_name}")
    };

    match metric.data() {
        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
            let start = histogram.start_time();
            let end = histogram.time();
            for dp in histogram.data_points() {
                out.push(convert_histogram_point(
                    &metric_type,
                    monitored_resource,
                    dp,
                    start,
                    end,
                ));
            }
        }
        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
            let start = sum.start_time();
            let end = sum.time();
            for dp in sum.data_points() {
                out.push(convert_u64_point(
                    &metric_type,
                    monitored_resource,
                    dp,
                    start,
                    end,
                ));
            }
        }
        AggregatedMetrics::F64(MetricData::Sum(sum)) => {
            let start = sum.start_time();
            let end = sum.time();
            for dp in sum.data_points() {
                out.push(convert_f64_point(
                    &metric_type,
                    monitored_resource,
                    dp,
                    start,
                    end,
                ));
            }
        }
        _ => {}
    }
}

fn system_time_to_timestamp(st: SystemTime) -> wkt::Timestamp {
    let duration = st
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    wkt::Timestamp::clamp(duration.as_secs() as i64, duration.subsec_nanos() as i32)
}

fn create_time_interval(start: SystemTime, end: SystemTime) -> TimeInterval {
    TimeInterval::new()
        .set_start_time(system_time_to_timestamp(start))
        .set_end_time(system_time_to_timestamp(end))
}

fn value_to_string(value: &OTelValue) -> String {
    value.to_string()
}

fn is_monitored_resource_label(key: &str) -> bool {
    matches!(
        key,
        "project_id" | "instance_id" | "location" | "instance_config" | "client_hash"
    )
}

fn key_values_to_metric_labels<'a>(
    attrs: impl Iterator<Item = &'a KeyValue>,
) -> HashMap<String, String> {
    let (lower_bound, _) = attrs.size_hint();
    let mut labels = HashMap::with_capacity(lower_bound);
    for kv in attrs {
        let key_str = kv.key.as_str();
        if is_monitored_resource_label(key_str) {
            continue;
        }
        let clean_key = if key_str.contains('.') {
            key_str.replace('.', "_")
        } else {
            key_str.to_string()
        };
        labels.insert(clean_key, value_to_string(&kv.value));
    }
    labels
}

fn resource_to_monitored_resource(resource: &Resource) -> MonitoredResource {
    let mut labels = HashMap::new();
    for (key, val) in resource.iter() {
        let key_str = key.as_str();
        match key_str {
            "instance_id" | "location" | "instance_config" | "client_hash" => {
                labels.insert(key_str.to_string(), value_to_string(val));
            }
            _ => {}
        }
    }

    MonitoredResource::new()
        .set_type(SPANNER_RESOURCE_TYPE.to_string())
        .set_labels(labels)
}

fn create_time_series<'a>(
    metric_type: &str,
    monitored_resource: &MonitoredResource,
    attributes: impl Iterator<Item = &'a KeyValue>,
    start_time: SystemTime,
    end_time: SystemTime,
    typed_value: TypedValue,
    value_type: metric_descriptor::ValueType,
) -> TimeSeries {
    let point = Point::new()
        .set_interval(create_time_interval(start_time, end_time))
        .set_value(typed_value);

    let metric = Metric::new()
        .set_type(metric_type.to_string())
        .set_labels(key_values_to_metric_labels(attributes));

    TimeSeries::new()
        .set_metric(metric)
        .set_resource(monitored_resource.clone())
        .set_metric_kind(metric_descriptor::MetricKind::Cumulative)
        .set_value_type(value_type)
        .set_points(vec![point])
}

fn convert_histogram_point(
    metric_type: &str,
    monitored_resource: &MonitoredResource,
    dp: &HistogramDataPoint<f64>,
    start_time: SystemTime,
    end_time: SystemTime,
) -> TimeSeries {
    let count = dp.count() as i64;
    let mean = if count == 0 {
        0.0
    } else {
        dp.sum() / (count as f64)
    };

    let explicit = Explicit::new().set_bounds(dp.bounds().collect::<Vec<f64>>());
    let bucket_options = BucketOptions::new().set_explicit_buckets(explicit);

    let distribution = Distribution::new()
        .set_count(count)
        .set_mean(mean)
        .set_bucket_options(bucket_options)
        .set_bucket_counts(dp.bucket_counts().map(|c| c as i64).collect::<Vec<i64>>());

    let typed_value = TypedValue::new().set_value(Value::DistributionValue(Box::new(distribution)));

    create_time_series(
        metric_type,
        monitored_resource,
        dp.attributes(),
        start_time,
        end_time,
        typed_value,
        metric_descriptor::ValueType::Distribution,
    )
}

fn convert_u64_point(
    metric_type: &str,
    monitored_resource: &MonitoredResource,
    dp: &SumDataPoint<u64>,
    start_time: SystemTime,
    end_time: SystemTime,
) -> TimeSeries {
    let typed_value = TypedValue::new().set_value(Value::Int64Value(dp.value() as i64));

    create_time_series(
        metric_type,
        monitored_resource,
        dp.attributes(),
        start_time,
        end_time,
        typed_value,
        metric_descriptor::ValueType::Int64,
    )
}

fn convert_f64_point(
    metric_type: &str,
    monitored_resource: &MonitoredResource,
    dp: &SumDataPoint<f64>,
    start_time: SystemTime,
    end_time: SystemTime,
) -> TimeSeries {
    let typed_value = TypedValue::new().set_value(Value::DoubleValue(dp.value()));

    create_time_series(
        metric_type,
        monitored_resource,
        dp.attributes(),
        start_time,
        end_time,
        typed_value,
        metric_descriptor::ValueType::Double,
    )
}

#[cfg(all(test, feature = "_experimental-builtin-metrics"))]
mod tests {
    use super::*;
    use opentelemetry::metrics::{Counter, Histogram, MeterProvider as _};
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use std::fmt::Debug;
    use std::time::SystemTime;

    static_assertions::assert_impl_all!(
        GcpMonitoringExporter: Send,
        Sync,
        Debug,
        Clone,
        PushMetricExporter
    );

    #[test]
    fn system_time_to_timestamp() {
        let now = SystemTime::now();
        let ts = super::system_time_to_timestamp(now);
        assert!(ts.seconds() > 0, "Timestamp seconds should be positive");
    }

    #[test]
    fn key_values_to_metric_labels() {
        let attrs = [
            KeyValue::new("method", "ExecuteSql"),
            KeyValue::new("status.code", "OK"),
            KeyValue::new("retry.count", 3_i64),
            KeyValue::new("is_retry", true),
            KeyValue::new("instance_id", "my-instance"),
        ];
        let labels = super::key_values_to_metric_labels(attrs.iter());
        assert_eq!(labels.get("method").map(|s| s.as_str()), Some("ExecuteSql"));
        assert_eq!(labels.get("status_code").map(|s| s.as_str()), Some("OK"));
        assert_eq!(labels.get("retry_count").map(|s| s.as_str()), Some("3"));
        assert_eq!(labels.get("is_retry").map(|s| s.as_str()), Some("true"));
        assert!(!labels.contains_key("instance_id"));
    }

    #[test]
    fn resource_to_monitored_resource_filtering() {
        let resource = Resource::builder()
            .with_attributes([
                KeyValue::new("project_id", "my-project"),
                KeyValue::new("instance_id", "my-instance"),
                KeyValue::new("location", "us-central1"),
                KeyValue::new("instance_config", "regional-us-central1"),
                KeyValue::new("client_hash", "abc1234"),
                KeyValue::new("service.name", "my-app"),
                KeyValue::new("telemetry.sdk.version", "1.0.0"),
            ])
            .build();

        let monitored_res = super::resource_to_monitored_resource(&resource);

        assert_eq!(monitored_res.r#type, "spanner_instance_client");
        assert_eq!(
            monitored_res.labels.get("instance_id").map(|s| s.as_str()),
            Some("my-instance")
        );
        assert_eq!(
            monitored_res.labels.get("location").map(|s| s.as_str()),
            Some("us-central1")
        );
        assert_eq!(
            monitored_res
                .labels
                .get("instance_config")
                .map(|s| s.as_str()),
            Some("regional-us-central1")
        );
        assert_eq!(
            monitored_res.labels.get("client_hash").map(|s| s.as_str()),
            Some("abc1234")
        );

        // project_id must be excluded
        assert!(!monitored_res.labels.contains_key("project_id"));

        // Unrelated OpenTelemetry resource attributes must be filtered out
        assert!(!monitored_res.labels.contains_key("service.name"));
        assert!(!monitored_res.labels.contains_key("telemetry.sdk.version"));
    }

    #[test]
    fn create_time_series() {
        let now = SystemTime::now();
        let attrs = [KeyValue::new("method", "Commit")];
        let typed_val = TypedValue::new().set_value(Value::Int64Value(42));
        let resource = Resource::builder()
            .with_attributes([KeyValue::new("instance_id", "test-instance")])
            .build();
        let monitored_resource = super::resource_to_monitored_resource(&resource);
        let ts = super::create_time_series(
            "spanner.googleapis.com/internal/client/operation_count",
            &monitored_resource,
            attrs.iter(),
            now,
            now,
            typed_val,
            metric_descriptor::ValueType::Int64,
        );

        let metric = ts.metric.as_ref().expect("metric should be set");
        assert_eq!(
            metric.r#type,
            "spanner.googleapis.com/internal/client/operation_count"
        );
        assert_eq!(
            metric.labels.get("method").map(|s| s.as_str()),
            Some("Commit")
        );
        assert_eq!(ts.metric_kind, metric_descriptor::MetricKind::Cumulative);
        assert_eq!(ts.value_type, metric_descriptor::ValueType::Int64);
        assert_eq!(ts.points.len(), 1);

        let res = ts
            .resource
            .as_ref()
            .expect("monitored resource should be set");
        assert_eq!(res.r#type, "spanner_instance_client");
        assert_eq!(
            res.labels.get("instance_id").map(|s| s.as_str()),
            Some("test-instance")
        );
    }

    #[test]
    fn convert_metric_to_time_series_histogram_and_sums() {
        let exporter = InMemoryMetricExporter::default();
        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter.clone()).build();
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader)
            .build();

        let meter = provider.meter("cloud.google.com/rust");
        let histogram: Histogram<f64> = meter.f64_histogram("operation_latencies").build();
        let counter_u64: Counter<u64> = meter.u64_counter("operation_count").build();
        let counter_f64: Counter<f64> = meter
            .f64_counter("spanner.googleapis.com/internal/client/custom_latency")
            .build();

        histogram.record(123.45, &[KeyValue::new("method", "ExecuteSql")]);
        counter_u64.add(1, &[KeyValue::new("method", "ExecuteSql")]);
        counter_f64.add(99.5, &[KeyValue::new("method", "ExecuteSql")]);

        provider.force_flush().expect("force_flush failed");

        let resource_metrics_list = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics failed");

        let mut time_series_list = Vec::new();
        for resource_metrics in &resource_metrics_list {
            let monitored_res = super::resource_to_monitored_resource(resource_metrics.resource());
            for scope_metrics in resource_metrics.scope_metrics() {
                for m in scope_metrics.metrics() {
                    convert_metric_to_time_series(m, &monitored_res, &mut time_series_list);
                }
            }
        }

        assert_eq!(time_series_list.len(), 3);

        let ts_hist = time_series_list
            .iter()
            .find(|ts| {
                ts.metric
                    .as_ref()
                    .map(|m| {
                        m.r#type == "spanner.googleapis.com/internal/client/operation_latencies"
                    })
                    .unwrap_or(false)
            })
            .expect("histogram time series should be present");
        assert_eq!(
            ts_hist.value_type,
            metric_descriptor::ValueType::Distribution
        );

        let ts_counter = time_series_list
            .iter()
            .find(|ts| {
                ts.metric
                    .as_ref()
                    .map(|m| m.r#type == "spanner.googleapis.com/internal/client/operation_count")
                    .unwrap_or(false)
            })
            .expect("u64 counter time series should be present");
        assert_eq!(ts_counter.value_type, metric_descriptor::ValueType::Int64);

        let ts_f64 = time_series_list
            .iter()
            .find(|ts| {
                ts.metric
                    .as_ref()
                    .map(|m| m.r#type == "spanner.googleapis.com/internal/client/custom_latency")
                    .unwrap_or(false)
            })
            .expect("f64 counter time series should be present");
        assert_eq!(ts_f64.value_type, metric_descriptor::ValueType::Double);
    }

    #[test]
    fn resource_metrics_scope_filtering() {
        let exporter = InMemoryMetricExporter::default();
        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter.clone()).build();

        // 1. Meter with valid scope name
        let provider_valid = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader)
            .build();
        let meter_valid = provider_valid.meter("cloud.google.com/rust");
        let counter_valid: Counter<u64> = meter_valid.u64_counter("valid_metric").build();
        counter_valid.add(1, &[]);

        provider_valid.force_flush().expect("force_flush failed");

        let resource_metrics_list = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics failed");

        let mut time_series_list = Vec::new();
        for resource_metrics in &resource_metrics_list {
            let monitored_res = super::resource_to_monitored_resource(resource_metrics.resource());
            for scope_metrics in resource_metrics.scope_metrics() {
                let scope_name = scope_metrics.scope().name();
                if scope_name != SPANNER_METER_NAME
                    && !scope_name.contains("cloud.google.com")
                    && !scope_name.contains("spanner")
                {
                    continue;
                }

                for m in scope_metrics.metrics() {
                    convert_metric_to_time_series(m, &monitored_res, &mut time_series_list);
                }
            }
        }

        assert_eq!(time_series_list.len(), 1);
        assert_eq!(
            time_series_list[0].metric.as_ref().expect("metric").r#type,
            "spanner.googleapis.com/internal/client/valid_metric"
        );
    }

    #[test]
    fn is_permission_denied() {
        let status_pd =
            google_cloud_gax::error::rpc::Status::default().set_code(Code::PermissionDenied);
        let err_pd = crate::Error::service(status_pd);
        assert!(super::is_permission_denied(&err_pd));

        let status_nf = google_cloud_gax::error::rpc::Status::default().set_code(Code::NotFound);
        let err_nf = crate::Error::service(status_nf);
        assert!(!super::is_permission_denied(&err_nf));
    }

    #[test]
    fn value_to_string_all_variants() {
        assert_eq!(value_to_string(&OTelValue::from("hello")), "hello");
        assert_eq!(value_to_string(&OTelValue::from(42_i64)), "42");
        assert_eq!(value_to_string(&OTelValue::from(123.456_f64)), "123.456");
        assert_eq!(value_to_string(&OTelValue::from(true)), "true");
        assert_eq!(value_to_string(&OTelValue::from(false)), "false");
    }
}
