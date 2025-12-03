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

use super::experiment::{Experiment, Range};
use super::sample::Attempt;
use anyhow::Result;
use google_cloud_auth::credentials::Credentials;
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use std::time::Instant;

pub struct Runner {
    client: Storage,
}

impl Runner {
    pub async fn new(credentials: Credentials) -> Result<Self> {
        let client = google_cloud_storage::client::Storage::builder()
            .with_credentials(credentials)
            .build()
            .await?;
        Ok(Self { client })
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
        let mut reader = self
            .client
            .read_object(range.bucket_name.clone(), range.object_name.clone())
            .set_read_range(ReadRange::segment(range.read_offset, range.read_length))
            .send()
            .await?;
        let ttfb = Instant::now() - start;
        let mut size = 0;
        while let Some(b) = reader.next().await.transpose()? {
            size += b.len();
        }
        let ttlb = Instant::now() - start;
        Ok(Attempt { size, ttfb, ttlb })
    }
}
