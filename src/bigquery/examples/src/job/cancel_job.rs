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

// [START bigquery_cancel_job]
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{Job, JobConfiguration, JobConfigurationQuery};

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let job_service = JobService::builder().build().await?;

    // Start a long-running analytical query job
    let job = Job::new().set_configuration(
        JobConfiguration::new().set_query(
            JobConfigurationQuery::new()
                .set_query("SELECT * FROM UNNEST(GENERATE_ARRAY(1, 10000000));")
                .set_use_legacy_sql(false),
        ),
    );

    let inserted = job_service
        .insert_job()
        .set_project_id(project_id)
        .set_job(job)
        .send()
        .await?;

    let job_id = inserted.job_reference.unwrap().job_id;
    println!("Created long-running job: {}", job_id);

    // Call the job cancellation API abruptly to abort its execution
    let cancelled = job_service
        .cancel_job()
        .set_project_id(project_id)
        .set_job_id(&job_id)
        .set_location("US")
        .send()
        .await;

    if let Err(e) = cancelled {
        if e.to_string().contains("Root element must be a message") {
            // NOTE: A known issue in protobuf/gax serialization for empty POSTs.
            println!("Successfully called cancel_job (caught expected empty body error).");
        } else {
            return Err(e.into());
        }
    } else {
        println!("Successfully cancelled job.");
    }

    Ok(())
}
// [END bigquery_cancel_job]
