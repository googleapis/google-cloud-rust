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

// [START spanner_create_index]
use google_cloud_lro::Poller;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;

pub async fn sample(admin_client: &DatabaseAdmin, database_name: &str) -> anyhow::Result<()> {
    let statements = vec!["CREATE INDEX AlbumsByAlbumTitle ON Albums(AlbumTitle)"];

    println!("Creating AlbumsByAlbumTitle index...");
    admin_client
        .update_database_ddl()
        .set_database(database_name)
        .set_statements(statements)
        .poller()
        .until_done()
        .await?;

    println!("Added AlbumsByAlbumTitle index");
    Ok(())
}
// [END spanner_create_index]
