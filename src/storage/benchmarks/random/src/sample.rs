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

use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Attempt {
    pub ttfb: Duration,
    pub ttlb: Duration,
}

#[derive(Clone, Debug)]
pub struct Sample {
    pub task: usize,
    pub iteration: u64,
    pub start: Duration,
    pub range_id: usize,
    pub range_count: usize,
    pub range_length: u64,
    pub protocol: Protocol,
    pub ttfb: Duration,
    pub ttlb: Duration,
    pub object: String,
    pub details: String,
}

impl Sample {
    pub const HEADER: &str = concat!(
        "Task,Iteration,IterationStart,RangeId,RangeCount",
        ",RangeSize,Protocol,TtfbMicroseconds,TtlbMicroseconds",
        ",Object,Details"
    );

    pub fn to_row(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            self.task,
            self.iteration,
            self.start.as_micros(),
            self.range_id,
            self.range_count,
            self.range_length,
            self.protocol.name(),
            self.ttfb.as_micros(),
            self.ttlb.as_micros(),
            self.object,
            self.details,
        )
    }
}

/// Available protocols for the benchmark.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum Protocol {
    /// Use bidirectional streaming RPC.
    #[cfg(google_cloud_unstable_storage_bidi)]
    Bidi,
    /// Use JSON ranged reads.
    Json,
}

impl Protocol {
    pub fn name(&self) -> &str {
        match self {
            #[cfg(google_cloud_unstable_storage_bidi)]
            Self::Bidi => "bidi",
            Self::Json => "json",
        }
    }
}
