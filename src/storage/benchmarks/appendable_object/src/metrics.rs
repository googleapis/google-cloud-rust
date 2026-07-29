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
#[derive(Debug)]
pub struct Metrics {
    /// Mean execution latency.
    pub mean: Duration,
    /// 50th percentile (median) execution latency.
    pub p50: Duration,
    /// 90th percentile execution latency.
    pub p90: Duration,
    /// 99th percentile execution latency.
    pub p99: Duration,
}

/// Computes statistical metrics (mean, p50, p90, p99) from a slice of latencies.
/// Returns `None` if `latencies` is empty.
pub fn compute_metrics(latencies: &[Duration]) -> Option<Metrics> {
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

    Some(Metrics {
        mean,
        p50,
        p90,
        p99,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_latencies() {
        assert!(compute_metrics(&[]).is_none());
    }

    #[test]
    fn test_compute_metrics_single_element() {
        let metrics = compute_metrics(&[Duration::from_millis(100)]).unwrap();
        assert_eq!(metrics.mean, Duration::from_millis(100));
        assert_eq!(metrics.p50, Duration::from_millis(100));
        assert_eq!(metrics.p90, Duration::from_millis(100));
        assert_eq!(metrics.p99, Duration::from_millis(100));
    }

    #[test]
    fn test_compute_metrics_hundred_elements() {
        let latencies: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let metrics = compute_metrics(&latencies).unwrap();
        // 1..=100: index 0 is 1ms, index 99 is 100ms.
        // p50: (99 * 0.50).round() = 50 -> index 50 = 51ms
        // p90: (99 * 0.90).round() = 89 -> index 89 = 90ms
        // p99: (99 * 0.99).round() = 98 -> index 98 = 99ms
        assert_eq!(metrics.p50, Duration::from_millis(51));
        assert_eq!(metrics.p90, Duration::from_millis(90));
        assert_eq!(metrics.p99, Duration::from_millis(99));
    }
}
