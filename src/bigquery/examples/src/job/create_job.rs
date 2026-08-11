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

// [START bigquery_create_job]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{Job, JobConfiguration, JobConfigurationQuery};

pub async fn sample(project_id: &str) -> anyhow::Result<String> {
    let job_service = JobService::builder().build().await?;
    let job = Job::new().set_configuration(
        JobConfiguration::new().set_query(
            JobConfigurationQuery::new()
                .set_query("SELECT 1")
                .set_use_legacy_sql(false),
        ),
    );

    let inserted = job_service
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .send()
        .await?;

    let job_ref = inserted.job_reference.unwrap();
    println!("Created job: {}", job_ref.job_id);

    // Wait for the job to complete
    loop {
        let current_job = job_service
            .get_job()
            .set_project_id(project_id)
            .set_job_id(&job_ref.job_id)
            .send()
            .await?;
        if current_job.status.unwrap().state == "DONE" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    println!("Job completed successfully.");
    Ok(job_ref.job_id)
}
// [END bigquery_create_job]
