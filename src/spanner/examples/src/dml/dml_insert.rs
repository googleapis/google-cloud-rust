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

// [START spanner_dml_getting_started_insert]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let runner = client.read_write_transaction().build().await?;

    runner
        .run(async |transaction| {
            let sql = r#"INSERT INTO Singers (SingerId, FirstName, LastName) VALUES
                       (12, 'Melissa', 'Garcia'),
                       (13, 'Russell', 'Morales'),
                       (14, 'Jacqueline', 'Long'),
                       (15, 'Dylan', 'Shaw')"#;
            let statement = Statement::builder(sql).build();
            let row_count = transaction.execute_update(statement).await?;
            println!("{row_count} records inserted.");
            Ok(())
        })
        .await?;

    Ok(())
}
// [END spanner_dml_getting_started_insert]
