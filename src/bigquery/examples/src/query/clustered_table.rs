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

// [START bigquery_query_clustered_table]
use google_cloud_bigquery::client::BigQuery;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let mut rows = client
        .query(
            "SELECT \
        title, views FROM `bigquery-public-data.wikipedia.pageviews_2020` \
        WHERE datehour = '2020-01-01 00:00:00' AND title = 'Google' \
        LIMIT 10",
        )
        .with_project_id(project_id)
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?
        .read();

    while let Some(row) = rows.next().await.transpose()? {
        let title: String = row.get("title");
        let views: i64 = row.get("views");
        println!("Title: {title}, Views: {views}");
    }
    Ok(())
}
// [END bigquery_query_clustered_table]
