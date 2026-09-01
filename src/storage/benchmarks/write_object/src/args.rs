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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum UploadScenario {
    /// Option A: Baseline 1-pass unbuffered stream (no precomputed hash, client-side validation only)
    OptionA,
    /// Option B: 2-pass unbuffered stream (Pass 1: precomputed hash, Pass 2: continuous stream, server-side validation)
    OptionB,
    /// Option C: 1-pass buffered chunked upload (8 MiB chunks in RAM, server-side validation on final chunk)
    OptionC,
    /// Run all three scenarios (Option A, Option B, and Option C) for side-by-side comparison
    All,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "Benchmark write_object upload strategies in GCS", long_about = None)]
pub struct Args {
    /// The name of the bucket to use for the benchmark.
    #[arg(long, env = "GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET")]
    pub bucket_name: String,

    /// Number of measured iterations per scenario.
    #[arg(long, default_value_t = 5)]
    pub measured_iterations: usize,

    /// The size of the object to upload in bytes.
    #[arg(long, default_value_t = 67_108_864)] // 64 MiB default
    pub object_size: usize,

    /// Upload scenario / strategy to benchmark.
    #[arg(long, value_enum, default_value_t = UploadScenario::All)]
    pub scenario: UploadScenario,

    /// Whether to evict the test file from the OS page cache (RAM) before each iteration to
    /// simulate a cold physical disk read.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub cold_cache: bool,

    /// Directory on physical SSD storage for creating the temporary test file.
    #[arg(
        long,
        default_value = "/usr/local/google/tmp/rust-write-object-benchmarking-data"
    )]
    pub temp_dir: String,

    /// Directory for saving output artifacts (raw CSV latencies and summary JSON).
    #[arg(
        long,
        default_value = "/usr/local/google/tmp/rust-write-object-benchmarking-data/results"
    )]
    pub output_dir: String,

    /// Whether to delete test objects from GCS after each iteration.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub cleanup: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing_with_boolean_values() {
        let args = Args::try_parse_from([
            "benchmark",
            "--bucket-name",
            "test-bucket",
            "--object-size",
            "12582912",
            "--scenario",
            "option-a",
            "--cleanup",
            "false",
            "--cold-cache",
            "false",
            "--measured-iterations",
            "1",
        ])
        .unwrap();

        assert_eq!(args.bucket_name, "test-bucket");
        assert_eq!(args.object_size, 12_582_912);
        assert_eq!(args.scenario, UploadScenario::OptionA);
        assert!(!args.cleanup);
        assert!(!args.cold_cache);
        assert_eq!(args.measured_iterations, 1);
    }
}
