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

// [START spanner_postgresql_query_with_parameter]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let statement = Statement::builder(
        r#"SELECT singerid AS "SingerId", firstname as "FirstName", lastname as "LastName"
         FROM Singers
         WHERE LastName = $1"#,
    )
    .add_param("p1", &"Garcia")
    .build();

    let transaction = client.single_use().build();
    let mut result_set = transaction.execute_query(statement).await?;

    while let Some(row) = result_set.next().await.transpose()? {
        let singer_id: i64 = row.get("SingerId");
        let first_name: String = row.get("FirstName");
        let last_name: String = row.get("LastName");
        println!("{singer_id} {first_name} {last_name}");
    }
    Ok(())
}
// [END spanner_postgresql_query_with_parameter]
