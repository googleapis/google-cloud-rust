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

mod cancel_job;
mod create_job;
mod get_job;
mod list_jobs;

use google_cloud_test_utils::runtime_config::project_id;

pub async fn run_samples() -> anyhow::Result<()> {
    let project_id = project_id()?;

    let job_id = create_job::sample(&project_id).await?;
    get_job::sample(&project_id, &job_id).await?;
    list_jobs::sample(&project_id).await?;
    cancel_job::sample(&project_id).await?;

    Ok(())
}
