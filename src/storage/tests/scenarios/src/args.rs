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

use crate::sample::Scenario;

use anyhow::bail;
use clap::Parser;
use jiff::fmt::friendly::SpanParser;
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

    /// The number of concurrent tasks running the test.
    #[arg(long, default_value_t = 1)]
    pub task_count: usize,

    /// The rampup period between new tasks.
    #[arg(long, value_parser = |s: &str| SpanParser::new().parse_unsigned_duration(s), default_value = "500ms")]
    pub rampup_period: Duration,

    /// The rampup period between new tasks to write data.
    #[arg(long, value_parser = |s: &str| SpanParser::new().parse_unsigned_duration(s), default_value = "10ms")]
    pub dataset_rampup_period: Duration,

    /// The number of iterations for the test.
    #[arg(long, default_value_t = 1)]
    pub iterations: u64,

    /// The number of gRPC subchannels.
    #[arg(long)]
    pub grpc_subchannel_count: Option<usize>,

    /// The scenarios for this test.
    pub scenarios: Vec<Scenario>,
}

impl Args {
    /// Validates the arguments after parsing.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.grpc_subchannel_count.is_some_and(|v| v == 0) {
            bail!("invalid number of gRPC subchannels, should be > 0")
        }
        Ok(())
    }

    /// The fully qualified bucket name.
    pub fn full_bucket_name(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test]
    fn validate_success() -> anyhow::Result<()> {
        let args = Args::try_parse_from(["program", "--bucket-name=bucket"])?;
        assert_eq!(args.bucket_name, "bucket");
        assert_eq!(args.rampup_period, Duration::from_millis(500));
        assert_eq!(args.dataset_rampup_period, Duration::from_millis(10));
        args.validate()?;

        let args = Args::try_parse_from([
            "program",
            "--bucket-name=bucket",
            "--rampup-period=1s",
            "--dataset-rampup-period=50ms",
        ])?;
        assert_eq!(args.rampup_period, Duration::from_secs(1));
        assert_eq!(args.dataset_rampup_period, Duration::from_millis(50));
        args.validate()?;
        Ok(())
    }

    #[test_case(&["program", "--bucket-name=bucket", "--grpc-subchannel-count=0"])]
    fn validate(input: &[&str]) -> anyhow::Result<()> {
        let args = Args::try_parse_from(input)?;
        assert_eq!(args.bucket_name, "bucket");
        let got = args.validate();
        assert!(got.is_err(), "{got:?} {args:?}");
        Ok(())
    }
}
