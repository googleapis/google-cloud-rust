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

use clap::{Args, Parser, Subcommand};
use jiff::fmt::friendly::SpanParser;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about = "Cloud Pub/Sub Throughput Benchmark", long_about = None)]
pub struct Config {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the publisher benchmark
    Publisher(PublisherArgs),
    /// Run the subscriber benchmark
    Subscriber(SubscriberArgs),
}

#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    #[arg(long, default_value = "", env = "GOOGLE_CLOUD_PROJECT")]
    pub project: String,

    #[arg(long, value_parser = |s: &str| SpanParser::new().parse_unsigned_duration(s), default_value = "5s")]
    pub report_interval: Duration,

    #[arg(long, value_parser = |s: &str| SpanParser::new().parse_unsigned_duration(s), default_value = "5m")]
    pub duration: Duration,

    #[arg(long, default_value_t = 1)]
    pub grpc_channels: usize,

    #[arg(long, default_value_t = 100000)]
    pub max_outstanding_messages: usize,
}

#[derive(Args, Debug, Clone)]
pub struct PublisherArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(long)]
    pub topic_id: String,

    #[arg(long, default_value_t = 1024)]
    pub payload_size: i64,

    #[arg(long, default_value_t = 1000)]
    pub batch_size: u32,

    #[arg(long, default_value_t = 10 * 1024 * 1024)] // 10 MB
    pub batch_bytes: u32,

    #[arg(long, value_parser = |s: &str| SpanParser::new().parse_unsigned_duration(s), default_value = "100ms")]
    pub batch_delay: Duration,

    #[arg(long, default_value_t = 0)]
    pub ordering_keys: usize,

    #[arg(long, default_value_t = 0)]
    pub active_ordering_keys: usize,
}

#[derive(Args, Debug, Clone)]
pub struct SubscriberArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(long)]
    pub subscription_id: String,

    #[arg(long, default_value_t = 1)]
    pub streams: usize,
}

pub fn parse_args() -> Config {
    Config::parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_durations() -> anyhow::Result<()> {
        let cfg = Config::try_parse_from(["pubsub-throughput", "publisher", "--topic-id=t"])?;
        let Commands::Publisher(args) = cfg.command else {
            anyhow::bail!("expected publisher subcommand");
        };
        assert_eq!(args.common.report_interval, Duration::from_secs(5));
        assert_eq!(args.common.duration, Duration::from_secs(300));
        assert_eq!(args.batch_delay, Duration::from_millis(100));

        let cfg = Config::try_parse_from([
            "pubsub-throughput",
            "publisher",
            "--topic-id=t",
            "--report-interval=10s",
            "--duration=1h",
            "--batch-delay=25ms",
        ])?;
        let Commands::Publisher(args) = cfg.command else {
            anyhow::bail!("expected publisher subcommand");
        };
        assert_eq!(args.common.report_interval, Duration::from_secs(10));
        assert_eq!(args.common.duration, Duration::from_secs(3600));
        assert_eq!(args.batch_delay, Duration::from_millis(25));
        Ok(())
    }
}
