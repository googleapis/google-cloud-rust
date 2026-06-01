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

// [START spanner_read_data]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::{KeySet, ReadRequest};

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    // Build a read request to read all rows in the Albums table
    let read_request = ReadRequest::builder("Albums", ["SingerId", "AlbumId", "AlbumTitle"])
        .with_keys(KeySet::all())
        .build();

    let transaction = client.single_use().build();
    let mut result_set = transaction.execute_read(read_request).await?;

    println!("Reading albums data:");
    while let Some(row) = result_set.next().await.transpose()? {
        let singer_id = row.get::<i64, _>("SingerId");
        let album_id = row.get::<i64, _>("AlbumId");
        let album_title = row.get::<String, _>("AlbumTitle");
        println!("SingerId: {singer_id}, AlbumId: {album_id}, AlbumTitle: {album_title}");
    }
    println!("Done reading albums.");

    Ok(())
}
// [END spanner_read_data]
