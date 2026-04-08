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
use humantime::parse_duration;

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

    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    pub report_interval: std::time::Duration,

    #[arg(long, value_parser = parse_duration, default_value = "5m")]
    pub duration: std::time::Duration,

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

    #[arg(long, value_parser = parse_duration, default_value = "100ms")]
    pub batch_delay: std::time::Duration,
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
