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
    about = "Cloud Pub/Sub Publish Hedging Benchmark",
    long_about = "A benchmark tool to evaluate Cloud Pub/Sub publish latency with and without request hedging."
)]
pub struct Args {
    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    pub warmup: Duration,

    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    pub duration: Duration,

    #[arg(long, default_value_t = 200, value_parser = clap::value_parser!(u64).range(1..))]
    pub message_rate: u64,

    #[arg(long, default_value_t = false)]
    pub enable_hedging: bool,

    #[arg(long, value_parser = parse_duration, default_value = "100ms")]
    pub hedge_delay: Duration,

    #[arg(long, default_value_t = 50)]
    pub hedge_max_tokens: u32,

    #[arg(long, default_value_t = 0.1)]
    pub hedge_refill_ratio: f32,
}

pub fn parse_args() -> Args {
    Args::parse()
}
