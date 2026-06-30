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

// [START spanner_read_only_transaction]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::key::KeySet;
use google_cloud_spanner::read::ReadRequest;
use google_cloud_spanner::statement::Statement;

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let transaction = client.read_only_transaction().build().await?;

    // 1. Execute a query using the read-only transaction
    let statement = Statement::builder("SELECT SingerId, AlbumId, AlbumTitle FROM Albums").build();
    let mut result_set = transaction.execute_query(statement).await?;
    println!("Results from query:");
    while let Some(row) = result_set.next().await.transpose()? {
        let singer_id: i64 = row.get(0);
        let album_id: i64 = row.get(1);
        let album_title: String = row.get(2);
        println!("{singer_id} {album_id} {album_title}");
    }

    // 2. Execute a read using the same read-only transaction
    let read_request = ReadRequest::builder("Albums", ["SingerId", "AlbumId", "AlbumTitle"])
        .with_keys(KeySet::all())
        .build();
    let mut result_set = transaction.execute_read(read_request).await?;
    println!("Results from read:");
    while let Some(row) = result_set.next().await.transpose()? {
        let singer_id: i64 = row.get(0);
        let album_id: i64 = row.get(1);
        let album_title: String = row.get(2);
        println!("{singer_id} {album_id} {album_title}");
    }

    Ok(())
}
// [END spanner_read_only_transaction]
