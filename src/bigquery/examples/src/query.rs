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
mod dry_run;
mod job_optional;
mod legacy;
mod no_cache;
#[allow(clippy::module_inception)]
mod query;

use google_cloud_test_utils::runtime_config::project_id;

pub async fn run_samples() -> anyhow::Result<()> {
    let project_id = project_id()?;

    println!("Running sample for `bigquery_query`...");
    Box::pin(query::sample(&project_id)).await?;

    println!("Running sample for `bigquery_query_no_cache`...");
    Box::pin(no_cache::sample(&project_id)).await?;

    println!("Running sample for `bigquery_query_batch`...");
    Box::pin(batch::sample(&project_id)).await?;

    println!("Running sample for `bigquery_query_dry_run`...");
    Box::pin(dry_run::sample(&project_id)).await?;

    println!("Running sample for `bigquery_query_legacy`...");
    Box::pin(legacy::sample(&project_id)).await?;

    println!("Running sample for `bigquery_query_job_optional`...");
    Box::pin(job_optional::sample(&project_id)).await?;

    Ok(())
}
