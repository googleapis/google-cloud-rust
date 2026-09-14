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

use crate::sample::{Sample, SampleStatus};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Serializes a [`Duration`] as fractional milliseconds.
///
/// The serde default for [`Duration`] emits `{"secs": 1, "nanos": 234000000}`,
/// which is awkward to chart or load into BigQuery.
mod duration_millis {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Duration, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_f64(duration.as_secs_f64() * 1_000.0)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
        let millis = f64::deserialize(de)?;
        Ok(Duration::from_secs_f64(millis / 1_000.0))
    }
}

/// Summary percentiles and metrics for execution latencies.
///
/// Durations are serialized as fractional milliseconds.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencySummary {
    #[serde(with = "duration_millis", rename = "min_millis")]
    pub min: Duration,
    #[serde(with = "duration_millis", rename = "max_millis")]
    pub max: Duration,
    #[serde(with = "duration_millis", rename = "mean_millis")]
    pub mean: Duration,
    #[serde(with = "duration_millis", rename = "p50_millis")]
    pub p50: Duration,
    #[serde(with = "duration_millis", rename = "p90_millis")]
    pub p90: Duration,
    #[serde(with = "duration_millis", rename = "p99_millis")]
    pub p99: Duration,
    pub count: usize,
}

/// Computes statistical metrics (min, max, mean, p50, p90, p99) from a slice of latencies.
pub fn compute_metrics(latencies: &[Duration]) -> Option<LatencySummary> {
    if latencies.is_empty() {
        return None;
    }

    let mut sorted = latencies.to_vec();
    sorted.sort();

    let count = sorted.len();
    let min = sorted[0];
    let max = sorted[count - 1];
    let sum: Duration = sorted.iter().sum();
    let mean = sum / count as u32;

    let p50 = sorted[((count - 1) as f64 * 0.50).round() as usize];
    let p90 = sorted[((count - 1) as f64 * 0.90).round() as usize];
    let p99 = sorted[((count - 1) as f64 * 0.99).round() as usize];

    Some(LatencySummary {
        min,
        max,
        mean,
        p50,
        p90,
        p99,
        count,
    })
}

/// OpenTelemetry metrics instruments for a single scenario.
///
/// The scenario/status attribute sets are built once and reused, so recording a
/// sample allocates nothing.
#[derive(Clone)]
pub struct OtelMetrics {
    queries_total: Counter<u64>,
    queries_success: Counter<u64>,
    queries_error: Counter<u64>,
    queries_retried: Counter<u64>,
    rows_read: Counter<u64>,
    bytes_processed: Counter<u64>,
    query_duration: Histogram<f64>,
    send_duration: Histogram<f64>,
    poll_duration: Histogram<f64>,
    read_duration: Histogram<f64>,
    ok_attrs: [KeyValue; 2],
    error_attrs: [KeyValue; 2],
}

impl OtelMetrics {
    /// Creates the instruments for `scenario`.
    ///
    /// Every counter is seeded with 0 so the time series exist in Cloud
    /// Monitoring even if no errors or retries occur during the run.
    pub fn new(scenario: &str) -> Self {
        let meter = opentelemetry::global::meter("bigquery-benchmark-queries");

        let metrics = Self {
            queries_total: meter
                .u64_counter("bigquery.queries.total")
                .with_description("Total number of BigQuery queries attempted")
                .build(),
            queries_success: meter
                .u64_counter("bigquery.queries.success")
                .with_description("Number of BigQuery queries completed successfully")
                .build(),
            queries_error: meter
                .u64_counter("bigquery.queries.error")
                .with_description("Number of BigQuery queries that failed")
                .build(),
            queries_retried: meter
                .u64_counter("bigquery.queries.retries_detected")
                .with_description(
                    "Number of queries where an under-the-hood job retry was detected",
                )
                .build(),
            rows_read: meter
                .u64_counter("bigquery.queries.rows_read")
                .with_description("Total count of rows read from query results")
                .build(),
            bytes_processed: meter
                .u64_counter("bigquery.queries.bytes_processed")
                .with_description("Total estimated bytes processed by BigQuery jobs")
                .build(),
            query_duration: meter
                .f64_histogram("bigquery.queries.duration_seconds")
                .with_description("Total query end-to-end duration in seconds")
                .build(),
            send_duration: meter
                .f64_histogram("bigquery.queries.send_duration_seconds")
                .with_description("Duration for Query::send() execution")
                .build(),
            poll_duration: meter
                .f64_histogram("bigquery.queries.poll_duration_seconds")
                .with_description("Duration for Query::until_done() polling execution")
                .build(),
            read_duration: meter
                .f64_histogram("bigquery.queries.read_duration_seconds")
                .with_description("Duration for CompleteQuery::read() row streaming")
                .build(),
            ok_attrs: [
                KeyValue::new("scenario", scenario.to_string()),
                KeyValue::new("status", "ok"),
            ],
            error_attrs: [
                KeyValue::new("scenario", scenario.to_string()),
                KeyValue::new("status", "error"),
            ],
        };

        metrics.queries_total.add(0, &metrics.ok_attrs);
        metrics.queries_total.add(0, &metrics.error_attrs);
        metrics.queries_success.add(0, &metrics.ok_attrs);
        metrics.queries_error.add(0, &metrics.error_attrs);
        metrics.queries_retried.add(0, &metrics.ok_attrs);
        metrics.rows_read.add(0, &metrics.ok_attrs);
        metrics.bytes_processed.add(0, &metrics.ok_attrs);

        metrics
    }

    /// Records a single completed query execution.
    pub fn record_sample(&self, sample: &Sample) {
        let is_ok = sample.status == SampleStatus::Ok;
        let attrs = if is_ok {
            &self.ok_attrs
        } else {
            &self.error_attrs
        };

        self.queries_total.add(1, attrs);
        if is_ok {
            self.queries_success.add(1, attrs);
            if sample.retry_detected {
                self.queries_retried.add(1, attrs);
            }
            self.rows_read.add(sample.rows_count as u64, attrs);
            self.bytes_processed
                .add(sample.bytes_processed.max(0) as u64, attrs);

            self.send_duration
                .record(sample.send_duration().as_secs_f64(), attrs);
            self.poll_duration
                .record(sample.poll_duration().as_secs_f64(), attrs);
            self.read_duration
                .record(sample.read_duration().as_secs_f64(), attrs);
        } else {
            self.queries_error.add(1, attrs);
        }

        self.query_duration
            .record(sample.total_duration().as_secs_f64(), attrs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_metrics() {
        assert!(compute_metrics(&[]).is_none());

        let single = compute_metrics(&[Duration::from_millis(100)]).unwrap();
        assert_eq!(single.min, Duration::from_millis(100));
        assert_eq!(single.max, Duration::from_millis(100));
        assert_eq!(single.p50, Duration::from_millis(100));
        assert_eq!(single.p90, Duration::from_millis(100));
        assert_eq!(single.p99, Duration::from_millis(100));

        let hundred: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let summary = compute_metrics(&hundred).unwrap();
        assert_eq!(summary.min, Duration::from_millis(1));
        assert_eq!(summary.max, Duration::from_millis(100));
        assert_eq!(summary.p50, Duration::from_millis(51));
        assert_eq!(summary.p90, Duration::from_millis(90));
        assert_eq!(summary.p99, Duration::from_millis(99));
    }
}
