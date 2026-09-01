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

use super::args::Args;
use super::metrics::Metrics;
use super::scenarios::IterationResult;
use std::fs::File;
use std::io::Write;
use std::path::Path;

/// Holds the structured results and parameters for a benchmark run.
#[derive(Debug)]
pub struct BenchmarkReport<'a> {
    /// Scenario name (e.g. Option A, Option B, Option C).
    pub scenario: &'a str,
    /// Object size in bytes.
    pub object_size: usize,
    /// Number of warmup iterations.
    pub warmup_iterations: usize,
    /// Number of measured iterations.
    pub measured_iterations: usize,
    /// Number of errors encountered.
    pub errors: usize,
    /// Calculated latency metrics.
    pub metrics: Option<Metrics>,
    /// Average precomputation duration if applicable (Option B).
    pub mean_precompute_ms: Option<f64>,
}

impl BenchmarkReport<'_> {
    /// Prints the report summary to standard output.
    pub fn print_stdout(&self) {
        println!("-----------------------------------------");
        println!("Scenario:            {}", self.scenario);
        println!(
            "Object Size:         {} bytes ({:.2} MiB)",
            self.object_size,
            self.object_size as f64 / (1024.0 * 1024.0)
        );
        println!("Warmup Iterations:   {}", self.warmup_iterations);
        println!("Measured Iterations: {}", self.measured_iterations);
        println!("Errors Recorded:     {}", self.errors);
        if let Some(m) = &self.metrics {
            println!("Mean Latency:        {:?}", m.mean);
            println!("P50 (Median) Lat:    {:?}", m.p50);
            println!("P90 Latency:         {:?}", m.p90);
            println!("P99 Latency:         {:?}", m.p99);
            println!("Throughput:          {:.2} MiB/s", m.throughput_mib_per_sec);
        }
        if let Some(precompute_ms) = self.mean_precompute_ms {
            println!("Pass 1 Hash Time:    {:.2} ms", precompute_ms);
        }
        println!("-----------------------------------------");
    }

    /// Writes the report summary in JSON format to a writer.
    pub fn write_json<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writeln!(writer, "{{")?;
        writeln!(writer, "  \"scenario\": \"{}\",", self.scenario)?;
        writeln!(writer, "  \"object_size_bytes\": {},", self.object_size)?;
        writeln!(
            writer,
            "  \"warmup_iterations\": {},",
            self.warmup_iterations
        )?;
        writeln!(
            writer,
            "  \"measured_iterations\": {},",
            self.measured_iterations
        )?;
        writeln!(writer, "  \"errors\": {},", self.errors)?;
        if let Some(m) = &self.metrics {
            writeln!(writer, "  \"mean_latency_ms\": {},", m.mean.as_millis())?;
            writeln!(writer, "  \"p50_latency_ms\": {},", m.p50.as_millis())?;
            writeln!(writer, "  \"p90_latency_ms\": {},", m.p90.as_millis())?;
            writeln!(writer, "  \"p99_latency_ms\": {},", m.p99.as_millis())?;
            writeln!(
                writer,
                "  \"throughput_mib_s\": {:.2},",
                m.throughput_mib_per_sec
            )?;
        } else {
            writeln!(writer, "  \"metrics\": null,")?;
        }
        if let Some(precompute_ms) = self.mean_precompute_ms {
            writeln!(writer, "  \"mean_precompute_ms\": {:.2}", precompute_ms)?;
        } else {
            writeln!(writer, "  \"mean_precompute_ms\": null")?;
        }
        writeln!(writer, "}}")
    }
}

/// Formats and outputs the benchmark metrics to stdout and optionally to output files.
pub fn report(
    scenario_name: &str,
    metrics: Option<Metrics>,
    results: &[IterationResult],
    errors: usize,
    args: &Args,
) -> anyhow::Result<()> {
    let mean_precompute_ms = if results.is_empty() {
        None
    } else {
        let precomputes: Vec<_> = results
            .iter()
            .filter_map(|r| r.precompute_duration)
            .collect();
        if precomputes.is_empty() {
            None
        } else {
            let sum_ms: f64 = precomputes.iter().map(|d| d.as_secs_f64() * 1000.0).sum();
            Some(sum_ms / precomputes.len() as f64)
        }
    };

    let report = BenchmarkReport {
        scenario: scenario_name,
        object_size: args.object_size,
        warmup_iterations: args.warmup_iterations,
        measured_iterations: args.measured_iterations,
        errors,
        metrics,
        mean_precompute_ms,
    };

    report.print_stdout();

    if let Some(dir_str) = args
        .output_dir
        .as_deref()
        .filter(|s| !s.is_empty() && !results.is_empty())
    {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let output_dir = Path::new(dir_str);
        std::fs::create_dir_all(output_dir)?;

        let csv_path = output_dir.join(format!(
            "{}_s{}_{}_raw.csv",
            scenario_name, args.object_size, timestamp
        ));
        let mut csv_file = File::create(&csv_path)?;
        writeln!(csv_file, "iteration,total_latency_ms,precompute_ms")?;
        for (i, r) in results.iter().enumerate() {
            let pre_ms = r
                .precompute_duration
                .map(|d| format!("{:.2}", d.as_secs_f64() * 1000.0))
                .unwrap_or_else(|| "0.0".to_string());
            writeln!(csv_file, "{},{},{}", i, r.total_elapsed.as_millis(), pre_ms)?;
        }
        println!("Raw latencies written to {}", csv_path.display());

        let json_path = output_dir.join(format!(
            "{}_s{}_{}_summary.json",
            scenario_name, args.object_size, timestamp
        ));
        let mut json_file = File::create(&json_path)?;
        report.write_json(&mut json_file)?;
        println!("Metrics summary written to {}", json_path.display());
    }

    Ok(())
}
