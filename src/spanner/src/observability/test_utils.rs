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

//! Shared test utilities for inspecting and asserting OpenTelemetry metric exports.

use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, Metric as OTelMetric, MetricData, ResourceMetrics,
};
use std::collections::HashMap;

/// Extracts OpenTelemetry label attributes into a HashMap for easy test assertions.
pub(crate) fn extract_attributes<'a>(
    attributes: impl IntoIterator<Item = &'a KeyValue>,
) -> HashMap<String, String> {
    attributes
        .into_iter()
        .map(|key_value| {
            (
                key_value.key.as_str().to_string(),
                key_value.value.as_str().to_string(),
            )
        })
        .collect()
}

/// Locates a metric by its exact name within exported resource metrics.
pub(crate) fn find_metric<'a>(
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

/// Extracts u64 sum data points and their labels from an OpenTelemetry metric.
pub(crate) fn get_sum_u64_points(metric: &OTelMetric) -> Vec<(u64, HashMap<String, String>)> {
    let mut points = Vec::new();
    if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
        for data_point in sum.data_points() {
            points.push((
                data_point.value(),
                extract_attributes(data_point.attributes()),
            ));
        }
    }
    points
}

/// Extracts histogram sample counts and their labels from an OpenTelemetry metric.
pub(crate) fn get_histogram_counts(metric: &OTelMetric) -> Vec<(u64, HashMap<String, String>)> {
    let mut counts = Vec::new();
    if let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() {
        for data_point in histogram.data_points() {
            counts.push((
                data_point.count(),
                extract_attributes(data_point.attributes()),
            ));
        }
    }
    counts
}

/// Helper to find a point count matching the given method and status labels.
#[cfg(feature = "unstable-metrics")]
pub(crate) fn find_point(
    points: &[(u64, HashMap<String, String>)],
    expected_method: &str,
    expected_status: &str,
) -> Option<u64> {
    points.iter().find_map(|(count, labels)| {
        (labels.get("method").map(String::as_str) == Some(expected_method)
            && labels.get("status").map(String::as_str) == Some(expected_status))
        .then_some(*count)
    })
}
