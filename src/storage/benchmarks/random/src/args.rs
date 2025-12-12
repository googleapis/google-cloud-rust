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

use super::random_size::{RandomSize, parse_random_size_arg};
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
    #[arg(long, default_value_t = RandomSize::Values(vec![8192]), value_parser = parse_random_size_arg)]
    pub range_size: RandomSize,

    /// The minimum number of concurrent reads in each iteration.˚˚
    #[arg(long, default_value_t = 16)]
    pub min_concurrent_reads: u64,

    /// The maximum number of concurrent reads in each iteration.
    #[arg(long, default_value_t = 16)]
    pub max_concurrent_reads: u64,

    /// The protocols used by the benchmark.
    pub protocols: Vec<Protocol>,

    /// The number of gRPC subchannels.
    #[arg(long)]
    pub grpc_subchannel_count: Option<usize>,
}

impl Args {
    /// Validates the arguments after parsing.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.min_range_count == 0 {
            bail!("invalid min-range-count, must be greater than zero")
        }
        if let RandomSize::Values(v) = &self.range_size {
            if v.is_empty() {
                bail!("empty list of values in range-size")
            }
        }
        if self.range_size.min() == 0 {
            bail!("invalid range-size, minimum must be greater than zero")
        }
        if self.min_concurrent_reads > self.max_concurrent_reads {
            bail!(
                "invalid concurrent reads range, min-concurrent-range ({}) > max-concurrent-range ({})",
                self.min_concurrent_reads,
                self.max_concurrent_reads
            )
        }
        if self.protocols.is_empty() {
            bail!("the protocol set must be non-empty: {:?}", self.protocols);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test]
    fn validate_success() -> anyhow::Result<()> {
        let args = Args::try_parse_from(["program", "--bucket-name=bucket", "json"])?;
        assert_eq!(args.bucket_name, "bucket");
        let got = args.validate();
        assert!(got.is_ok(), "{got:?} {args:?}");
        Ok(())
    }

    #[test_case(&["program", "--bucket-name=bucket"])]
    #[test_case(&["program", "--bucket-name=bucket", "json", "--range-size=0"])]
    #[test_case(&["program", "--bucket-name=bucket", "json", "--min-concurrent-reads=200", "--max-concurrent-reads=100"])]
    fn validate(input: &[&str]) -> anyhow::Result<()> {
        let args = Args::try_parse_from(input)?;
        assert_eq!(args.bucket_name, "bucket");
        let got = args.validate();
        assert!(got.is_err(), "{got:?} {args:?}");
        Ok(())
    }
}
