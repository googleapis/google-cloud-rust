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

use clap::Parser;
use humantime::parse_duration;
use std::time::Duration;

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "BigQuery Storage Write API Throughput Benchmark",
    long_about = None
)]
pub struct Config {
    #[arg(long, default_value = "", env = "GOOGLE_CLOUD_PROJECT")]
    pub project: String,

    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    pub report_interval: Duration,

    #[arg(long, value_parser = parse_duration, default_value = "1m")]
    pub duration: Duration,

    #[arg(long, default_value_t = 1024)]
    pub row_size: usize,

    #[arg(long, default_value_t = 1000)]
    pub rows_per_batch: usize,

    #[arg(long, default_value_t = 1)]
    pub num_tables: usize,

    #[arg(long, default_value_t = 1)]
    pub num_writers: usize,

    #[arg(long, default_value_t = 1)]
    pub grpc_channels: usize,

    #[arg(long, default_value = "")]
    pub dataset_id: String,
}

pub fn parse_args() -> Config {
    Config::parse()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_parse_args_defaults() -> anyhow::Result<()> {
        let args = Config::try_parse_from(["cmd"])?;
        assert_eq!(args.project, "");
        assert_eq!(args.row_size, 1024);
        assert_eq!(args.rows_per_batch, 1000);
        assert_eq!(args.num_tables, 1);
        assert_eq!(args.num_writers, 1);
        assert_eq!(args.grpc_channels, 1);
        assert_eq!(args.dataset_id, "");
        Ok(())
    }

    #[test]
    fn test_parse_args_custom() -> anyhow::Result<()> {
        let args = Config::try_parse_from([
            "cmd",
            "--project",
            "test-project",
            "--row-size",
            "2048",
            "--rows-per-batch",
            "50",
            "--num-tables",
            "3",
            "--num-writers",
            "5",
            "--grpc-channels",
            "4",
            "--dataset-id",
            "my_dataset",
        ])?;
        assert_eq!(args.project, "test-project");
        assert_eq!(args.row_size, 2048);
        assert_eq!(args.rows_per_batch, 50);
        assert_eq!(args.num_tables, 3);
        assert_eq!(args.num_writers, 5);
        assert_eq!(args.grpc_channels, 4);
        assert_eq!(args.dataset_id, "my_dataset");
        Ok(())
    }
}
