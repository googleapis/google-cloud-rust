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

use super::args::Args;
use super::experiment::{Experiment, Range};
use super::sample::Attempt;
use anyhow::{Result, bail};
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use std::collections::HashMap;
use std::time::Instant;

pub struct Runner {
    descriptors: HashMap<String, ObjectDescriptor>,
}

impl Runner {
    pub async fn new(args: &Args, objects: Vec<String>, client: Storage) -> Result<Self> {
        let bucket_name = format!("projects/_/buckets/{}", args.bucket_name);
        let mut descriptors = HashMap::new();
        for name in objects {
            let descriptor = client
                .open_object(bucket_name.clone(), name.clone())
                .send()
                .await?;
            descriptors.insert(name, descriptor);
        }
        Ok(Self { descriptors })
    }

    pub async fn iteration(&self, experiment: &Experiment) -> Vec<Result<Attempt>> {
        let running = experiment
            .ranges
            .iter()
            .map(|r| self.attempt(r))
            .collect::<Vec<_>>();

        futures::future::join_all(running).await
    }

    async fn attempt(&self, range: &Range) -> Result<Attempt> {
        let start = Instant::now();
        let Some(descriptor) = self.descriptors.get(&range.object_name) else {
            bail!(
                "cannot find object {} in available descriptors",
                range.object_name
            );
        };
        let mut reader = descriptor
            .read_range(ReadRange::segment(range.read_offset, range.read_length))
            .await;
        let mut ttfb = None;
        let mut size = 0;
        while let Some(b) = reader.next().await.transpose()? {
            let _ = ttfb.get_or_insert(start.elapsed());
            size += b.len();
        }
        if size != range.read_length as usize {
            bail!("mismatched requested vs. received size");
        }
        let ttlb = start.elapsed();
        let ttfb = ttfb.unwrap_or(ttlb);
        Ok(Attempt { size, ttfb, ttlb })
    }
}
