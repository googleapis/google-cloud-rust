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

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Summary percentiles and metrics for execution latencies.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencySummary {
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
    pub p50: Duration,
    pub p90: Duration,
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

// In-process global atomic counters for quick telemetry reporting.
static TOTAL_QUERIES: AtomicU64 = AtomicU64::new(0);
static SUCCESS_QUERIES: AtomicU64 = AtomicU64::new(0);
static ERROR_QUERIES: AtomicU64 = AtomicU64::new(0);
static RETRIED_QUERIES: AtomicU64 = AtomicU64::new(0);
static TOTAL_ROWS: AtomicU64 = AtomicU64::new(0);
static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn inc_total_queries() {
    TOTAL_QUERIES.fetch_add(1, Ordering::SeqCst);
}

#[inline]
pub fn inc_success_queries() {
    SUCCESS_QUERIES.fetch_add(1, Ordering::SeqCst);
}

#[inline]
pub fn inc_error_queries() {
    ERROR_QUERIES.fetch_add(1, Ordering::SeqCst);
}

#[inline]
pub fn inc_retried_queries() {
    RETRIED_QUERIES.fetch_add(1, Ordering::SeqCst);
}

#[inline]
pub fn add_rows_read(count: u64) {
    TOTAL_ROWS.fetch_add(count, Ordering::SeqCst);
}

#[inline]
pub fn add_bytes_processed(bytes: u64) {
    TOTAL_BYTES.fetch_add(bytes, Ordering::SeqCst);
}

/// Returns a snapshot of in-process counters.
pub fn get_counters() -> [(&'static str, u64); 6] {
    [
        ("total_queries", TOTAL_QUERIES.load(Ordering::Relaxed)),
        ("success_queries", SUCCESS_QUERIES.load(Ordering::Relaxed)),
        ("error_queries", ERROR_QUERIES.load(Ordering::Relaxed)),
        ("retried_queries", RETRIED_QUERIES.load(Ordering::Relaxed)),
        ("total_rows_read", TOTAL_ROWS.load(Ordering::Relaxed)),
        ("total_bytes_processed", TOTAL_BYTES.load(Ordering::Relaxed)),
    ]
}

/// OpenTelemetry metrics instruments.
#[derive(Clone)]
pub struct OtelMetrics {
    pub queries_total: Counter<u64>,
    pub queries_success: Counter<u64>,
    pub queries_error: Counter<u64>,
    pub queries_retried: Counter<u64>,
    pub rows_read: Counter<u64>,
    pub bytes_processed: Counter<u64>,
    pub query_duration: Histogram<f64>,
    pub send_duration: Histogram<f64>,
    pub poll_duration: Histogram<f64>,
    pub read_duration: Histogram<f64>,
}

impl OtelMetrics {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("bigquery-benchmark-queries");

        let queries_total = meter
            .u64_counter("bigquery.queries.total")
            .with_description("Total number of BigQuery queries attempted")
            .build();

        let queries_success = meter
            .u64_counter("bigquery.queries.success")
            .with_description("Number of BigQuery queries completed successfully")
            .build();

        let queries_error = meter
            .u64_counter("bigquery.queries.error")
            .with_description("Number of BigQuery queries that failed")
            .build();

        let queries_retried = meter
            .u64_counter("bigquery.queries.retries_detected")
            .with_description("Number of queries where an under-the-hood job retry was detected")
            .build();

        let rows_read = meter
            .u64_counter("bigquery.queries.rows_read")
            .with_description("Total count of rows read from query results")
            .build();

        let bytes_processed = meter
            .u64_counter("bigquery.queries.bytes_processed")
            .with_description("Total estimated bytes processed by BigQuery jobs")
            .build();

        let query_duration = meter
            .f64_histogram("bigquery.queries.duration_seconds")
            .with_description("Total query end-to-end duration in seconds")
            .build();

        let send_duration = meter
            .f64_histogram("bigquery.queries.send_duration_seconds")
            .with_description("Duration for Query::send() execution")
            .build();

        let poll_duration = meter
            .f64_histogram("bigquery.queries.poll_duration_seconds")
            .with_description("Duration for Query::until_done() polling execution")
            .build();

        let read_duration = meter
            .f64_histogram("bigquery.queries.read_duration_seconds")
            .with_description("Duration for CompleteQuery::read() row streaming")
            .build();

        Self {
            queries_total,
            queries_success,
            queries_error,
            queries_retried,
            rows_read,
            bytes_processed,
            query_duration,
            send_duration,
            poll_duration,
            read_duration,
        }
    }

    /// Initializes all counter metrics with 0 so time series exist in Cloud Monitoring
    /// even if no errors or retries occur during the benchmark run.
    pub fn init_scenario(&self, scenario: &str) {
        let ok_attrs = [
            KeyValue::new("scenario", scenario.to_string()),
            KeyValue::new("status", "ok"),
        ];
        let err_attrs = [
            KeyValue::new("scenario", scenario.to_string()),
            KeyValue::new("status", "error"),
        ];

        self.queries_total.add(0, &ok_attrs);
        self.queries_total.add(0, &err_attrs);
        self.queries_success.add(0, &ok_attrs);
        self.queries_error.add(0, &err_attrs);
        self.queries_retried.add(0, &ok_attrs);
        self.rows_read.add(0, &ok_attrs);
        self.bytes_processed.add(0, &ok_attrs);
    }

    pub fn record_sample(&self, scenario: &str, sample: &crate::sample::Sample) {
        let is_ok = sample.status == crate::sample::SampleStatus::Ok;
        let attrs = [
            KeyValue::new("scenario", scenario.to_string()),
            KeyValue::new("status", if is_ok { "ok" } else { "error" }),
        ];

        self.queries_total.add(1, &attrs);
        if is_ok {
            self.queries_success.add(1, &attrs);
            self.queries_error.add(
                0,
                &[
                    KeyValue::new("scenario", scenario.to_string()),
                    KeyValue::new("status", "error"),
                ],
            );
            if sample.retry_detected {
                self.queries_retried.add(1, &attrs);
            } else {
                self.queries_retried.add(0, &attrs);
            }
            self.rows_read.add(sample.rows_count as u64, &attrs);
            self.bytes_processed
                .add(sample.bytes_processed.max(0) as u64, &attrs);

            self.send_duration.record(
                Duration::from_micros(sample.send_duration_micros as u64).as_secs_f64(),
                &attrs,
            );
            self.poll_duration.record(
                Duration::from_micros(sample.poll_duration_micros as u64).as_secs_f64(),
                &attrs,
            );
            self.read_duration.record(
                Duration::from_micros(sample.read_duration_micros as u64).as_secs_f64(),
                &attrs,
            );
        } else {
            self.queries_error.add(1, &attrs);
            self.queries_success.add(
                0,
                &[
                    KeyValue::new("scenario", scenario.to_string()),
                    KeyValue::new("status", "ok"),
                ],
            );
        }

        self.query_duration.record(
            Duration::from_micros(sample.total_duration_micros as u64).as_secs_f64(),
            &attrs,
        );
    }
}

impl Default for OtelMetrics {
    fn default() -> Self {
        Self::new()
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
