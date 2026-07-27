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

// [START bigquery_query_job_optional]
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::model::QueryReference;
use google_cloud_bigquery::model::query_request::JobCreationMode;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let query = client
        .query(
            "SELECT \
        name, gender, SUM(number) AS total \
        FROM `bigquery-public-data.usa_names.usa_1910_2013` \
        GROUP BY name, gender \
        ORDER BY total DESC \
        LIMIT 10",
        )
        .with_project_id(project_id)
        .set_job_creation_mode(JobCreationMode::JobCreationOptional)
        .set_location("US")
        .run()
        .await?;

    match query.query_reference() {
        QueryReference::Stateless { query_id } => {
            println!("Query was run in optional job mode.  Query ID: \"{query_id}\"");
        }
        QueryReference::Job(job) => {
            let qualified_job_id = format!(
                "{}:{}.{}",
                job.project_id,
                job.location.as_deref().unwrap_or_default(),
                job.job_id
            );
            println!("Query was run with job state.  Job ID: \"{qualified_job_id}\"");
        }
        _ => {}
    }

    let mut rows = query.until_done().await?.read();

    while let Some(row) = rows.next().await.transpose()? {
        let name: String = row.get("name");
        let gender: String = row.get("gender");
        let total: i64 = row.get("total");
        println!("Name: {name}, Gender: {gender}, Total: {total}");
    }
    Ok(())
}
// [END bigquery_query_job_optional]
