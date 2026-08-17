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
use google_cloud_bigquery_v2::model::{Job, JobConfiguration, JobConfigurationQuery, JobReference};

pub async fn sample(project_id: &str) -> anyhow::Result<String> {
    let job_service = JobService::builder().build().await?;
    let job = Job::new()
        .set_job_reference(
            JobReference::new()
                .set_project_id(project_id)
                .set_job_id(format!("my_job_prefix_{}", rand::random::<u64>()))
                .set_location("US"),
        )
        .set_configuration(
            JobConfiguration::new()
                .set_labels([("example-label", "example-value")])
                .set_query(
                    JobConfigurationQuery::new()
                        .set_query("SELECT 1")
                        .set_use_legacy_sql(false)
                        .set_maximum_bytes_billed(10000000_i64),
                ),
        );

    let job = job_service
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .into_job_poller()
        .until_done()
        .await?;
    let job_id = job.job_reference.unwrap().job_id;
    println!("Job completed successfully: {}", job_id);

    Ok(job_id)
}
// [END bigquery_create_job]
