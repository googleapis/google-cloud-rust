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

#[derive(Debug, Clone)]
pub struct BenchmarkStats {
    pub elapsed: Duration,
    pub total_sent: u64,
    pub total_succeeded: u64,
    pub total_errors: u64,
    pub payload_size: usize,
    pub latencies: Vec<Duration>,
    pub total_rpc_requests: u64,
    pub hedged_rpc_requests: u64,
}

impl BenchmarkStats {
    pub fn print_summary(&mut self) {
        self.latencies.sort();
        let count = self.latencies.len();
        let elapsed_secs = self.elapsed.as_secs_f64();
        let throughput_msgs = if elapsed_secs > 0.0 {
            self.total_succeeded as f64 / elapsed_secs
        } else {
            0.0
        };
        let throughput_mb = if elapsed_secs > 0.0 {
            (self.total_succeeded as f64 * self.payload_size as f64) / (1_000_000.0 * elapsed_secs)
        } else {
            0.0
        };

        println!(
            "\n================================================================================"
        );
        println!(
            "                         Pub/Sub Benchmark Results                              "
        );
        println!(
            "================================================================================"
        );
        println!("Summary:");
        println!("  Elapsed time:            {:.2}s", elapsed_secs);
        println!("  Messages attempted:      {}", self.total_sent);
        println!("  Messages succeeded:      {}", self.total_succeeded);
        println!("  Errors:                  {}", self.total_errors);
        println!(
            "  Throughput:              {:.2} msgs/s ({:.2} MB/s)",
            throughput_msgs, throughput_mb
        );

        if count > 0 {
            let min = self.latencies[0];
            let max = self.latencies[count - 1];
            let avg = average(&self.latencies);
            let p50 = percentile(&self.latencies, 0.50);
            let p90 = percentile(&self.latencies, 0.90);
            let p95 = percentile(&self.latencies, 0.95);
            let p99 = percentile(&self.latencies, 0.99);
            let p99_9 = percentile(&self.latencies, 0.999);
            let p99_99 = percentile(&self.latencies, 0.9999);

            println!("\nLatency Distribution:");
            println!("  Min:                     {:.2?}", min);
            println!("  p50 (Median):            {:.2?}", p50);
            println!("  p90:                     {:.2?}", p90);
            println!("  p95:                     {:.2?}", p95);
            println!("  p99:                     {:.2?}", p99);
            println!("  p99.9:                   {:.2?}", p99_9);
            println!("  p99.99:                  {:.2?}", p99_99);
            println!("  Max:                     {:.2?}", max);
            println!("  Avg:                     {:.2?}", avg);

            let tail_count = 10.min(count);
            let tail_slice = &self.latencies[count - tail_count..];
            let tail_str: Vec<String> = tail_slice.iter().map(|d| format!("{:.2?}", d)).collect();
            println!(
                "  Top {} slowest:          {}",
                tail_count,
                tail_str.join(", ")
            );
        }

        let total_rpcs = self.total_rpc_requests;
        let hedged_rpcs = self.hedged_rpc_requests;
        let hedged_ratio = if total_rpcs > 0 {
            (hedged_rpcs as f64 / total_rpcs as f64) * 100.0
        } else {
            0.0
        };
        println!("\nMock Server / RPC Hedging Stats:");
        println!("  Total Publish RPCs:      {}", total_rpcs);
        println!(
            "  Hedged Publish RPCs:     {} ({:.2}%)",
            hedged_rpcs, hedged_ratio
        );
        let non_hedged = total_rpcs.saturating_sub(hedged_rpcs);
        let overhead_ratio = if non_hedged > 0 {
            (hedged_rpcs as f64 / non_hedged as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "  RPC Overhead from Hedge: +{:.2}% extra RPCs",
            overhead_ratio
        );
        println!(
            "================================================================================\n"
        );
    }
}

pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

pub fn average(latencies: &[Duration]) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }
    let total_nanos: u128 = latencies.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((total_nanos / latencies.len() as u128) as u64)
}
