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

use std::time::Duration;

/// Aggregated latency statistical metrics.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Mean execution latency.
    pub mean: Duration,
    /// 50th percentile (median) execution latency.
    pub p50: Duration,
    /// 90th percentile execution latency.
    pub p90: Duration,
    /// 99th percentile execution latency.
    pub p99: Duration,
    /// Mean throughput in MiB/s.
    pub throughput_mib_per_sec: f64,
}

/// Computes statistical metrics (mean, p50, p90, p99, throughput) from latencies.
pub fn compute_metrics(latencies: &[Duration], object_size_bytes: usize) -> Option<Metrics> {
    if latencies.is_empty() {
        return None;
    }

    let mut sorted = latencies.to_vec();
    sorted.sort();

    let sum: Duration = sorted.iter().sum();
    let mean = sum / sorted.len() as u32;
    let len = sorted.len();
    let p50 = sorted[((len - 1) as f64 * 0.50).round() as usize];
    let p90 = sorted[((len - 1) as f64 * 0.90).round() as usize];
    let p99 = sorted[((len - 1) as f64 * 0.99).round() as usize];

    let mean_secs = mean.as_secs_f64();
    let throughput_mib_per_sec = if mean_secs > 0.0 {
        (object_size_bytes as f64 / (1024.0 * 1024.0)) / mean_secs
    } else {
        0.0
    };

    Some(Metrics {
        mean,
        p50,
        p90,
        p99,
        throughput_mib_per_sec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_latencies() {
        assert!(compute_metrics(&[], 1024).is_none());
    }

    #[test]
    fn test_compute_metrics_single_element() {
        let metrics = compute_metrics(&[Duration::from_millis(1000)], 1024 * 1024).unwrap();
        assert_eq!(metrics.mean, Duration::from_millis(1000));
        assert_eq!(metrics.p50, Duration::from_millis(1000));
        assert_eq!(metrics.p90, Duration::from_millis(1000));
        assert_eq!(metrics.p99, Duration::from_millis(1000));
        assert!((metrics.throughput_mib_per_sec - 1.0).abs() < 1e-6);
    }
}
