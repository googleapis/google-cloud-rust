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

mod run;

use super::args::Args;
use google_cloud_storage::{client::Storage, model::Object};
use std::time::Duration;

#[derive(Debug)]
pub struct Attempt {
    pub open_latency: Duration,
    pub uploadid: String,
    pub object: String,
    pub result: anyhow::Result<()>,
}

#[derive(Clone, Debug)]
pub struct Sample {
    pub task: usize,
    pub iteration: u64,
    pub start: Duration,
    pub scenario: Scenario,
    pub open_latency: Duration,
    pub uploadid: String,
    pub object: String,
    pub details: String,
}

impl Sample {
    pub const HEADER: &str = concat!(
        "Task,Iteration,IterationStart,Scenario",
        ",OpenLatencyMicroseconds",
        ",UploadId,Object,Details"
    );

    pub fn to_row(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{}",
            self.task,
            self.iteration,
            self.start.as_micros(),
            self.scenario.name(),
            self.open_latency.as_micros(),
            self.uploadid,
            self.object,
            self.details,
        )
    }
}

/// Available protocols for the benchmark.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum Scenario {
    Json,
    Open,
    OpenRead,
    OpenReadAtomic,
    OpenReadDiscard,
    OpenReadAfterDrop,
    OpenConcurrentReads,
}

impl Scenario {
    pub fn name(&self) -> &str {
        match self {
            Self::Json => "json",
            Self::Open => "open",
            Self::OpenRead => "open_read",
            Self::OpenReadAtomic => "open_read_atomic",
            Self::OpenReadDiscard => "open_read_discard",
            Self::OpenReadAfterDrop => "open_read_after_drop",
            Self::OpenConcurrentReads => "open_concurrent_reads",
        }
    }

    pub async fn run(&self, _args: &Args, client: &Storage, objects: &[Object]) -> Attempt {
        match self {
            Self::Json => run::json(client, objects).await,
            Self::Open => run::open(client, objects).await,
            Self::OpenRead => run::open_read(client, objects).await,
            Self::OpenReadAtomic => run::open_read_atomic(client, objects).await,
            Self::OpenReadDiscard => run::open_read_discard(client, objects).await,
            Self::OpenReadAfterDrop => run::open_read_after_drop(client, objects).await,
            Self::OpenConcurrentReads => run::open_concurrent_reads(client, objects).await,
        }
    }
}
