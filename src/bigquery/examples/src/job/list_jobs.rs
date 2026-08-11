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

// [START bigquery_list_jobs]
use google_cloud_bigquery_v2::client::JobService;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let job_service = JobService::builder().build().await?;

    let mut page_token = String::new();
    let mut listed_count = 0;

    loop {
        let mut req = job_service
            .list_jobs()
            .set_project_id(project_id)
            .set_max_results(20);
        if !page_token.is_empty() {
            req = req.set_page_token(page_token);
        }
        let res = req.send().await?;

        for job in res.jobs {
            if let Some(job_ref) = job.job_reference {
                println!("Job ID: {}", job_ref.job_id);
                listed_count += 1;
            }
        }

        // Let's break early to not overwhelm the test output
        if listed_count >= 20 {
            break;
        }

        match Some(res.next_page_token) {
            Some(token) if !token.is_empty() => page_token = token,
            _ => break,
        }
    }

    Ok(())
}
// [END bigquery_list_jobs]
