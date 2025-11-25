// Copyright 2025 Google LLC
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

use super::sample::Protocol;
use anyhow::bail;
use clap::Parser;
use humantime::parse_duration;
use std::time::Duration;

/// Configuration options for the benchmark.
#[derive(Clone, Debug, Parser)]
#[command(version, about, long_about = super::DESCRIPTION)]
pub struct Args {
    /// The name of the bucket used by the benchmark.
    ///
    /// You should use a regional bucket in the same region as the VM running
    /// the benchmark.
    #[arg(long)]
    pub bucket_name: String,

    /// Use existing objects for the test.
    ///
    /// When true, the benchmark uses existing objects in the bucket.
    /// It ignores objects that are too small.
    #[arg(long, default_value_t = false)]
    pub use_existing_dataset: bool,

    /// The number of concurrent tasks running the benchmark.
    #[arg(long, default_value_t = 1)]
    pub task_count: usize,

    /// The number of iterations for each task.
    #[arg(long, default_value_t = 1)]
    pub iterations: u64,

    /// The rampup period between new tasks.
    #[arg(long, value_parser = parse_duration, default_value = "500ms")]
    pub rampup_period: Duration,

    /// The minimum number of ranges per object.
    ///
    /// Before starting, the benchmark creates a number of objects to read from.
    /// Each object is at least of size `min_range_count * max_range_size`.
    #[arg(long, default_value_t = 16)]
    pub min_range_count: u64,

    /// The minimum size of each ranged read.
    #[arg(long, default_value_t = 8192, value_parser = parse_size_arg)]
    pub min_range_size: u64,

    /// The maximum size of each ranged read..
    #[arg(long, default_value_t = 8192, value_parser = parse_size_arg)]
    pub max_range_size: u64,

    /// The minimum number of concurrent reads in each iteration.˚˚
    #[arg(long, default_value_t = 16)]
    pub min_concurrent_reads: u64,

    /// The maximum number of concurrent reads in each iteration.
    #[arg(long, default_value_t = 16)]
    pub max_concurrent_reads: u64,

    /// The protocols used by the benchmark.
    pub protocols: Vec<Protocol>,
}

impl Args {
    /// Validates the arguments after parsing.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.min_range_count == 0 {
            bail!("invalid min-range-count, must be greater than zero")
        }
        if self.max_range_size < self.min_range_size {
            bail!(
                "invalid range for concurrent reads: [{}, {}]",
                self.min_range_size,
                self.max_range_size,
            );
        }
        if self.max_concurrent_reads < self.min_concurrent_reads {
            bail!(
                "invalid range for concurrent reads: [{}, {}]",
                self.min_concurrent_reads,
                self.max_concurrent_reads
            );
        }
        if self.protocols.is_empty() {
            bail!("the protocol set must be non-empty: {:?}", self.protocols);
        }
        Ok(())
    }
}

fn parse_size_arg(arg: &str) -> anyhow::Result<u64> {
    let value = parse_size::parse_size(arg)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case("2MiB", 2 * 1024 * 1024)]
    #[test_case("2MB", 2 * 1000 * 1000)]
    #[test_case("42", 42)]
    fn parse_size_success(input: &str, want: u64) -> anyhow::Result<()> {
        let got = parse_size_arg(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("abc")]
    #[test_case("-123")]
    #[test_case("abc123")]
    fn parse_size_error(input: &str) -> anyhow::Result<()> {
        let got = parse_size_arg(input);
        assert!(got.is_err(), "{got:?}");
        Ok(())
    }

    #[test]
    fn validate_success() -> anyhow::Result<()> {
        let args = Args::try_parse_from(["program", "--bucket-name=bucket", "json"])?;
        assert_eq!(args.bucket_name, "bucket");
        let got = args.validate();
        assert!(got.is_ok(), "{got:?} {args:?}");
        Ok(())
    }

    #[test_case(&["program", "--bucket-name=bucket"])]
    #[test_case(&["program", "--bucket-name=bucket", "json", "--min-range-count=0"])]
    #[test_case(&["program", "--bucket-name=bucket", "json", "--min-range-size=200", "--max-range-size=100"])]
    #[test_case(&["program", "--bucket-name=bucket", "json", "--min-concurrent-reads=200", "--max-concurrent-reads=100"])]
    fn validate(input: &[&str]) -> anyhow::Result<()> {
        let args = Args::try_parse_from(input)?;
        assert_eq!(args.bucket_name, "bucket");
        let got = args.validate();
        assert!(got.is_err(), "{got:?} {args:?}");
        Ok(())
    }
}
