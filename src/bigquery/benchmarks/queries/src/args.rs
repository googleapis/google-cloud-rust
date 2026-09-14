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

use clap::{Parser, ValueEnum};
use humantime::parse_duration;
use std::path::PathBuf;
use std::time::Duration;

const DESCRIPTION: &str = concat!(
    "A benchmark and endurance test runner for the Rust BigQuery client library.\n\n",
    "Supports fixed-iteration benchmarking (measuring P50/P90/P99 latencies) and\n",
    "long-running endurance tests with OpenTelemetry trace and metric export."
);

/// Preset query scenarios for benchmarking.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum ScenarioName {
    /// Zero-dependency synthetic query generating 100,000 structured rows via UNNEST(GENERATE_ARRAY(1, 100000)).
    #[value(name = "synthetic-100k")]
    #[default]
    Synthetic100k,
    /// Zero-dependency synthetic query generating 10,000 structured rows.
    #[value(name = "synthetic-10k")]
    Synthetic10k,
    /// Scans up to 50,000 rows from `bigquery-public-data.usa_names.usa_1910_2013`.
    #[value(name = "usa-names-scan")]
    UsaNamesScan,
    /// Aggregation query grouping 5.5M rows by state and gender from `bigquery-public-data.usa_names.usa_1910_2013`.
    #[value(name = "usa-names-agg")]
    UsaNamesAgg,
    /// Aggregation query on `bigquery-public-data.samples.wikipedia`.
    #[value(name = "wikipedia-agg")]
    WikipediaAgg,
    /// Custom SQL query provided via `--sql` or `--sql-file`.
    #[value(name = "custom")]
    Custom,
}

/// Runs the BigQuery benchmark and endurance test suite.
#[derive(Clone, Debug, Parser)]
#[command(version, about, long_about = DESCRIPTION)]
pub struct Args {
    /// The Google Cloud Project ID used for BigQuery billing and OpenTelemetry export.
    ///
    /// If not provided, the default project from Application Default Credentials or
    /// the `GOOGLE_CLOUD_PROJECT` environment variable will be used.
    #[arg(long, env = "GOOGLE_CLOUD_PROJECT")]
    pub project_id: Option<String>,

    /// The geographic location for BigQuery datasets and job execution (e.g. `US`, `EU`).
    #[arg(long, default_value = "US")]
    pub location: String,

    /// The query workload scenario to run.
    #[arg(long, value_enum, default_value_t = ScenarioName::Synthetic100k)]
    pub scenario: ScenarioName,

    /// Custom SQL query string (required if `--scenario custom` and `--sql-file` is not set).
    #[arg(long)]
    pub sql: Option<String>,

    /// Path to a file containing custom SQL query.
    #[arg(long)]
    pub sql_file: Option<PathBuf>,

    /// Number of concurrent worker tasks running query loops.
    #[arg(long, default_value_t = 1)]
    pub task_count: usize,

    /// Number of query iterations per worker task.
    ///
    /// If neither `--iterations` nor `--duration` is set, the benchmark runs indefinitely until interrupted (Ctrl+C).
    #[arg(long)]
    pub iterations: Option<u64>,

    /// Total runtime duration for the benchmark or endurance test (e.g. "30s", "10m", "2h").
    ///
    /// When set, tasks will continue executing queries until the duration expires.
    #[arg(long, value_parser = parse_duration)]
    pub duration: Option<Duration>,

    /// The maximum number of rows per page returned from BigQuery (maps to `max_results`).
    #[arg(long)]
    pub max_results: Option<u32>,

    /// Whether to consume all returned rows by iterating over the stream (`read().next().await`).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub read_results: bool,

    /// Whether to enable BigQuery query results cache. Defaults to false so queries always execute against storage.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub use_query_cache: bool,

    /// Ramp-up delay between spawning subsequent worker tasks to prevent thundering herd.
    #[arg(long, value_parser = parse_duration, default_value = "250ms")]
    pub rampup_period: Duration,

    /// Timeout for an individual query iteration (including send, until_done, and row streaming).
    #[arg(long, value_parser = parse_duration, default_value = "120s")]
    pub query_timeout: Duration,

    /// Directory where raw CSV samples and summary JSON metrics will be written.
    #[arg(long)]
    pub output_dir: Option<PathBuf>,

    /// Custom OpenTelemetry collector endpoint for traces and metrics.
    ///
    /// Defaults to `https://telemetry.googleapis.com` if `--project-id` is provided.
    #[arg(long)]
    pub otlp_endpoint: Option<String>,

    /// Whether to log debug details for retry decisions.
    #[arg(long)]
    pub debug_retry: bool,
}

impl Args {
    /// Validates the command line arguments.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.task_count == 0 {
            anyhow::bail!("--task-count must be at least 1");
        }
        if self.scenario == ScenarioName::Custom && self.sql.is_none() && self.sql_file.is_none() {
            anyhow::bail!(
                "When using `--scenario custom`, either `--sql` or `--sql-file` must be provided."
            );
        }
        if let Some(iterations) = self.iterations
            && iterations == 0
            && self.duration.is_none()
        {
            anyhow::bail!("--iterations must be greater than 0");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_args_validation() {
        let args = Args::parse_from(["bigquery-benchmark-queries"]);
        assert!(args.validate().is_ok());
        assert_eq!(args.task_count, 1);
        assert_eq!(args.iterations, None);
        assert!(!args.use_query_cache);
    }

    #[test]
    fn test_custom_scenario_validation() {
        let args = Args::parse_from(["bigquery-benchmark-queries", "--scenario", "custom"]);
        assert!(args.validate().is_err());

        let args = Args::parse_from([
            "bigquery-benchmark-queries",
            "--scenario",
            "custom",
            "--sql",
            "SELECT 1",
        ]);
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_duration_mode() {
        let args = Args::parse_from(["bigquery-benchmark-queries", "--duration", "5m"]);
        assert!(args.validate().is_ok());
        assert_eq!(args.iterations, None);
        assert_eq!(args.duration, Some(Duration::from_secs(300)));
    }
}
