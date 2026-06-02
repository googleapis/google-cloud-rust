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

// [START spanner_create_database]
use google_cloud_lro::Poller;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;

pub async fn sample(
    admin_client: &DatabaseAdmin,
    instance_name: &str,
    database_id: &str,
) -> anyhow::Result<()> {
    let create_statement = format!("CREATE DATABASE `{database_id}`");
    let extra_statements = vec![
        r#"CREATE TABLE Singers (
            SingerId INT64 NOT NULL,
            FirstName STRING(1024),
            LastName STRING(1024),
            SingerInfo BYTES(MAX),
            FullName STRING(2048) AS (ARRAY_TO_STRING([FirstName, LastName], " ")) STORED
         ) PRIMARY KEY (SingerId)"#
            .to_string(),
        r#"CREATE TABLE Albums (
            SingerId INT64 NOT NULL,
            AlbumId INT64 NOT NULL,
            AlbumTitle STRING(MAX)
         ) PRIMARY KEY (SingerId, AlbumId),
         INTERLEAVE IN PARENT Singers ON DELETE CASCADE"#
            .to_string(),
    ];

    println!("Creating database {database_id}...");
    let database = admin_client
        .create_database()
        .set_parent(instance_name.to_string())
        .set_create_statement(create_statement)
        .set_extra_statements(extra_statements)
        .poller()
        .until_done()
        .await?;

    println!("Created database [{}] successfully.", database.name);
    Ok(())
}
// [END spanner_create_database]
