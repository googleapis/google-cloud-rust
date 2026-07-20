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

mod batch;
mod browse_table;
mod dry_run;
mod job_optional;
mod label_job;
mod legacy;
mod no_cache;
mod pagination;
mod params_arrays;
mod params_named;
mod params_positional;
mod params_timestamps;
#[allow(clippy::module_inception)]
mod query;
mod script;
mod total_rows;

use google_cloud_test_utils::runtime_config::project_id;
use std::future::Future;
use std::pin::Pin;

pub async fn run_samples() -> anyhow::Result<()> {
    let project_id = project_id()?;

    let pending: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>>>>> = vec![
        Box::pin(query::sample(&project_id)),
        Box::pin(no_cache::sample(&project_id)),
        Box::pin(batch::sample(&project_id)),
        Box::pin(dry_run::sample(&project_id)),
        Box::pin(legacy::sample(&project_id)),
        Box::pin(job_optional::sample(&project_id)),
        Box::pin(params_positional::sample(&project_id)),
        Box::pin(params_named::sample(&project_id)),
        Box::pin(params_arrays::sample(&project_id)),
        Box::pin(params_timestamps::sample(&project_id)),
        Box::pin(browse_table::sample(&project_id)),
        Box::pin(pagination::sample(&project_id)),
        Box::pin(total_rows::sample(&project_id)),
        Box::pin(label_job::sample(&project_id)),
        Box::pin(script::sample(&project_id)),
    ];
    let _ = futures::future::join_all(pending)
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(())
}
