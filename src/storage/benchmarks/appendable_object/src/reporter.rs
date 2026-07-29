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
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

/// Holds the structured results and parameters for a benchmark run.
#[derive(Debug)]
pub struct BenchmarkReport<'a> {
    /// Name of the benchmark scenario executed.
    pub scenario: &'a str,
    /// Object size in bytes.
    pub object_size: usize,
    /// Chunk size in bytes.
    pub chunk_size: usize,
    /// Number of warmup iterations.
    pub warmup_iterations: usize,
    /// Number of measured iterations.
    pub measured_iterations: usize,
    /// Number of error occurrences recorded.
    pub errors: usize,
    /// Calculated latency metrics (if any iterations succeeded).
    pub metrics: Option<Metrics>,
}

impl BenchmarkReport<'_> {
    /// Prints the report summary to standard output.
    pub fn print_stdout(&self) {
        println!("-----------------------------------------");
        println!("Scenario:            {}", self.scenario);
        println!("Object Size:         {} bytes", self.object_size);
        println!("Chunk Size:          {} bytes", self.chunk_size);
        println!("Warmup Iterations:   {}", self.warmup_iterations);
        println!("Measured Iterations: {}", self.measured_iterations);
        println!("Errors Recorded:     {}", self.errors);
        if let Some(m) = &self.metrics {
            println!("Mean Latency:        {:?}", m.mean);
            println!("P50 (Median) Latency: {:?}", m.p50);
            println!("P90 Latency:         {:?}", m.p90);
            println!("P99 Latency:         {:?}", m.p99);
        } else {
            println!("No metrics to report");
        }
        println!("-----------------------------------------");
    }

    /// Writes the report summary in JSON format to a writer.
    pub fn write_json<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writeln!(writer, "{{")?;
        writeln!(writer, "  \"scenario\": \"{}\",", self.scenario)?;
        writeln!(writer, "  \"object_size_bytes\": {},", self.object_size)?;
        writeln!(writer, "  \"chunk_size_bytes\": {},", self.chunk_size)?;
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
            writeln!(writer, "  \"p99_latency_ms\": {}", m.p99.as_millis())?;
        } else {
            writeln!(writer, "  \"metrics\": null")?;
        }
        writeln!(writer, "}}")
    }
}

/// Formats and outputs the benchmark metrics to stdout and optionally to output files.
pub fn report(
    metrics: Option<Metrics>,
    latencies: &[Duration],
    errors: usize,
    args: &Args,
) -> anyhow::Result<()> {
    let report = BenchmarkReport {
        scenario: "scenario1_basic_steady_state",
        object_size: args.object_size,
        chunk_size: args.chunk_size,
        warmup_iterations: args.warmup_iterations,
        measured_iterations: args.measured_iterations,
        errors,
        metrics,
    };

    report.print_stdout();

    // If output directory is provided, write raw CSV and summary JSON.
    if let Some(dir_str) = args
        .output_dir
        .as_deref()
        .filter(|s| !s.is_empty() && !latencies.is_empty())
    {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let output_dir = Path::new(dir_str);
        std::fs::create_dir_all(output_dir)?;

        // Write raw per-iteration latencies CSV.
        let csv_path = output_dir.join(format!(
            "scenario1_s{}_c{}_{}_raw.csv",
            args.object_size, args.chunk_size, timestamp
        ));
        let mut csv_file = File::create(&csv_path)?;
        writeln!(csv_file, "iteration,latency_ms")?;
        for (i, l) in latencies.iter().enumerate() {
            writeln!(csv_file, "{},{}", i, l.as_millis())?;
        }
        println!("Raw latencies written to {}", csv_path.display());

        // Write summary JSON.
        let json_path = output_dir.join(format!(
            "scenario1_s{}_c{}_{}_summary.json",
            args.object_size, args.chunk_size, timestamp
        ));
        let mut json_file = File::create(&json_path)?;
        report.write_json(&mut json_file)?;
        println!("Metrics summary written to {}", json_path.display());
    }

    Ok(())
}
