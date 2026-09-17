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

use crate::args::Args;
use crate::metrics::{self, LatencySummary};
use crate::sample::{Sample, SampleStatus, display_job_id};
use crate::scenarios::Scenario;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;

/// Maximum number of error details retained in memory for the final report.
///
/// Every error is still written to the error log; this only bounds the
/// in-memory copy so that long endurance runs cannot exhaust memory.
const MAX_RETAINED_ERRORS: usize = 1_000;

/// Number of errors printed in the stdout summary.
const ERRORS_PRINTED: usize = 20;

/// How often the interim summary JSON is rebuilt, and the ceiling it backs off to.
///
/// Rebuilding sorts every retained latency, so the interval doubles as the run
/// grows to keep that cost from crowding out sample collection.
const MIN_SUMMARY_INTERVAL: Duration = Duration::from_secs(5);
const MAX_SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

/// Details of a single query error for diagnostics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub task_id: usize,
    pub iteration: u64,
    pub offset_secs: f64,
    pub initial_job_id: String,
    pub final_job_id: String,
    pub error_message: String,
}

/// Structured benchmark summary report.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub scenario: String,
    pub task_count: usize,
    pub total_samples: usize,
    pub success_count: usize,
    pub error_count: usize,
    pub retries_detected_count: usize,
    pub total_rows_read: usize,
    pub total_bytes_processed: i64,
    pub total_duration: Option<LatencySummary>,
    pub send_duration: Option<LatencySummary>,
    pub poll_duration: Option<LatencySummary>,
    pub read_duration: Option<LatencySummary>,
    /// The first [`MAX_RETAINED_ERRORS`] errors; see `error_count` for the total.
    pub errors: Vec<ErrorDetail>,
}

impl BenchmarkReport {
    /// Prints the benchmark summary report to stdout.
    pub fn print_stdout(&self) {
        println!("\n=======================================================");
        println!("               BigQuery Benchmark Report               ");
        println!("=======================================================");
        println!("Scenario:                 {}", self.scenario);
        println!("Task Count:               {}", self.task_count);
        println!("Total Queries Executed:   {}", self.total_samples);
        println!("Successful Queries:       {}", self.success_count);
        println!("Errors:                   {}", self.error_count);
        println!("Job Retries Detected:     {}", self.retries_detected_count);
        println!("Total Rows Read:          {}", self.total_rows_read);
        println!("Total Bytes Processed:    {}", self.total_bytes_processed);

        if let Some(total) = &self.total_duration {
            println!("\n--- End-to-End Query Latency ---");
            println!("  Min:   {:?}", total.min);
            println!("  Mean:  {:?}", total.mean);
            println!("  P50:   {:?}", total.p50);
            println!("  P90:   {:?}", total.p90);
            println!("  P99:   {:?}", total.p99);
            println!("  Max:   {:?}", total.max);
        }

        for (label, summary) in [
            ("Query::send()", &self.send_duration),
            ("Query::until_done() Polling", &self.poll_duration),
            ("CompleteQuery::read() Streaming", &self.read_duration),
        ] {
            if let Some(s) = summary {
                println!("\n--- {label} Latency ---");
                println!("  P50:   {:?} | P90: {:?} | P99: {:?}", s.p50, s.p90, s.p99);
            }
        }

        if !self.errors.is_empty() {
            println!("\n-------------------------------------------------------");
            println!("               QUERY ERRORS ({} total)", self.error_count);
            println!("-------------------------------------------------------");
            for (idx, err) in self.errors.iter().take(ERRORS_PRINTED).enumerate() {
                println!(
                    "  {}. Task {:>2} | Iteration {:>6} | Offset: {:>7.1}s | Job: {}",
                    idx + 1,
                    err.task_id,
                    err.iteration,
                    err.offset_secs,
                    display_job_id(&err.initial_job_id, &err.final_job_id)
                );
                println!("     Error: {}", err.error_message);
            }
            let shown = self.errors.len().min(ERRORS_PRINTED);
            if self.error_count > shown {
                println!(
                    "  ... and {} more error(s) recorded in full error log.",
                    self.error_count - shown
                );
            }
        }

        println!("=======================================================\n");
    }
}

/// Running totals over the samples received so far.
#[derive(Default)]
struct Accumulator {
    total_samples: usize,
    success_count: usize,
    error_count: usize,
    retries_detected_count: usize,
    total_rows_read: usize,
    total_bytes_processed: i64,
    total_durations: Vec<Duration>,
    send_durations: Vec<Duration>,
    poll_durations: Vec<Duration>,
    read_durations: Vec<Duration>,
    errors: Vec<ErrorDetail>,
}

impl Accumulator {
    fn push(&mut self, sample: &Sample) {
        self.total_samples += 1;

        if sample.status == SampleStatus::Ok {
            self.success_count += 1;
            self.total_durations.push(sample.total_duration());
            self.send_durations.push(sample.send_duration());
            self.poll_durations.push(sample.poll_duration());
            self.read_durations.push(sample.read_duration());
            self.total_rows_read += sample.rows_count;
            self.total_bytes_processed += sample.bytes_processed;
        } else {
            self.error_count += 1;
            if self.errors.len() < MAX_RETAINED_ERRORS {
                self.errors.push(ErrorDetail {
                    task_id: sample.task_id,
                    iteration: sample.iteration,
                    offset_secs: sample.start_offset_secs(),
                    initial_job_id: sample.initial_job_id.clone(),
                    final_job_id: sample.final_job_id.clone(),
                    error_message: sample.error_message.clone(),
                });
            }
        }

        if sample.retry_detected {
            self.retries_detected_count += 1;
        }
    }

    fn build_report(&self, scenario: &str, task_count: usize) -> BenchmarkReport {
        BenchmarkReport {
            scenario: scenario.to_string(),
            task_count,
            total_samples: self.total_samples,
            success_count: self.success_count,
            error_count: self.error_count,
            retries_detected_count: self.retries_detected_count,
            total_rows_read: self.total_rows_read,
            total_bytes_processed: self.total_bytes_processed,
            total_duration: metrics::compute_metrics(&self.total_durations),
            send_duration: metrics::compute_metrics(&self.send_durations),
            poll_duration: metrics::compute_metrics(&self.poll_durations),
            read_duration: metrics::compute_metrics(&self.read_durations),
            errors: self.errors.clone(),
        }
    }
}

/// The set of files written when `--output-dir` is provided.
struct OutputFiles {
    csv: BufWriter<File>,
    csv_path: PathBuf,
    json_path: PathBuf,
    errors: BufWriter<File>,
    errors_path: PathBuf,
}

impl OutputFiles {
    fn create(output_dir: &Path, scenario: &str) -> anyhow::Result<Self> {
        std::fs::create_dir_all(output_dir)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let csv_path = output_dir.join(format!("samples-{scenario}-{timestamp}.csv"));
        let json_path = output_dir.join(format!("summary-{scenario}-{timestamp}.json"));
        let errors_path = output_dir.join(format!("errors-{scenario}-{timestamp}.log"));

        let mut csv = BufWriter::new(File::create(&csv_path)?);
        writeln!(csv, "{}", Sample::HEADER)?;
        csv.flush()?;

        let mut errors = BufWriter::new(File::create(&errors_path)?);
        writeln!(
            errors,
            "# BigQuery Benchmark Error Log - Scenario: {scenario}, Timestamp: {timestamp}"
        )?;
        errors.flush()?;

        println!("Writing real-time samples to: {}", csv_path.display());
        println!("Writing real-time summary to: {}", json_path.display());
        println!("Writing error details to:     {}", errors_path.display());

        Ok(Self {
            csv,
            csv_path,
            json_path,
            errors,
            errors_path,
        })
    }

    fn write_sample(&mut self, sample: &Sample) {
        if let Err(err) = writeln!(self.csv, "{}", sample.to_csv_row()) {
            tracing::error!("Failed to write CSV sample row to disk: {err:?}");
        }
    }

    fn write_error(&mut self, sample: &Sample) {
        let _ = writeln!(
            self.errors,
            "Task: {}\nIteration: {}\nOffsetSecs: {:.3}\nInitialJobId: {}\nFinalJobId: {}\nError: {}\n{}",
            sample.task_id,
            sample.iteration,
            sample.start_offset_secs(),
            sample.initial_job_id,
            sample.final_job_id,
            sample.error_message,
            "-".repeat(80)
        );
        let _ = self.errors.flush();
    }

    fn write_summary(&mut self, report: &BenchmarkReport) {
        let _ = self.csv.flush();
        let _ = self.errors.flush();
        match File::create(&self.json_path) {
            Ok(file) => {
                if let Err(err) = serde_json::to_writer_pretty(file, report) {
                    tracing::error!("Failed to write summary JSON: {err:?}");
                }
            }
            Err(err) => tracing::error!("Failed to create summary JSON: {err:?}"),
        }
    }
}

/// Receives sample results, logs real-time output, and generates the final report.
pub async fn collect_and_report(
    mut rx: Receiver<Sample>,
    scenario: &Scenario,
    args: &Args,
) -> anyhow::Result<BenchmarkReport> {
    let mut acc = Accumulator::default();
    let mut files = args
        .output_dir
        .as_deref()
        .map(|dir| OutputFiles::create(dir, scenario.name))
        .transpose()?;

    let mut last_summary = Instant::now();
    let mut summary_interval = MIN_SUMMARY_INTERVAL;

    while let Some(sample) = rx.recv().await {
        acc.push(&sample);

        if sample.status != SampleStatus::Ok {
            // Loud alert to console immediately
            eprintln!(
                "\n🚨 [QUERY FAILURE] Task {:>2} | Iteration {:>6} | Offset: {:>7.1}s | Job: {} | Error: {}\n",
                sample.task_id,
                sample.iteration,
                sample.start_offset_secs(),
                sample.display_job_id(),
                sample.error_message
            );
        }

        if let Some(files) = &mut files {
            files.write_sample(&sample);
            if sample.status != SampleStatus::Ok {
                files.write_error(&sample);
            }

            if last_summary.elapsed() >= summary_interval {
                last_summary = Instant::now();
                summary_interval = (summary_interval * 2).min(MAX_SUMMARY_INTERVAL);
                files.write_summary(&acc.build_report(scenario.name, args.task_count));
            }
        }
    }

    let report = acc.build_report(scenario.name, args.task_count);
    report.print_stdout();

    if let Some(files) = &mut files {
        files.write_summary(&report);
        println!("Final samples saved to: {}", files.csv_path.display());
        println!("Final summary saved to: {}", files.json_path.display());
        if acc.error_count > 0 {
            println!("Errors logged to:       {}", files.errors_path.display());
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_sample(total_micros: u64) -> Sample {
        Sample {
            total_duration_micros: total_micros,
            rows_count: 10,
            bytes_processed: 100,
            status: SampleStatus::Ok,
            ..Sample::new(0, 0, 0)
        }
    }

    #[test]
    fn test_accumulator_tallies_successes_and_errors() {
        let mut acc = Accumulator::default();
        acc.push(&ok_sample(1_000));
        acc.push(&ok_sample(3_000));
        acc.push(&Sample {
            error_message: "boom".to_string(),
            ..Sample::new(1, 0, 0)
        });

        let report = acc.build_report("test", 2);
        assert_eq!(report.total_samples, 3);
        assert_eq!(report.success_count, 2);
        assert_eq!(report.error_count, 1);
        assert_eq!(report.total_rows_read, 20);
        assert_eq!(report.total_bytes_processed, 200);
        assert_eq!(report.errors.len(), 1);
        // Latency summaries only cover successful samples.
        assert_eq!(report.total_duration.unwrap().count, 2);
    }
}
