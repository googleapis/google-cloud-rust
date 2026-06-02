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

// [START spanner_postgresql_create_database]
use google_cloud_lro::Poller;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;

pub async fn sample(
    admin_client: &DatabaseAdmin,
    instance_name: &str,
    database_id: &str,
) -> anyhow::Result<()> {
    let create_statement = format!("CREATE DATABASE \"{database_id}\"");

    println!("Creating PostgreSQL dialect database {database_id}...");
    let database = admin_client
        .create_database()
        .set_parent(instance_name.to_string())
        .set_create_statement(create_statement)
        .set_database_dialect(DatabaseDialect::Postgresql)
        .poller()
        .until_done()
        .await?;

    println!(
        "Created PostgreSQL dialect database [{}] successfully.",
        database.name
    );

    // Note: PostgreSQL dialect databases do not support adding extra statements
    // in the CreateDatabase operations. We must create the database first,
    // and then apply any schema DDL updates.
    let ddl_statements = vec![
        r#"CREATE TABLE Singers (
            SingerId   bigint NOT NULL,
            FirstName  character varying(1024),
            LastName   character varying(1024),
            SingerInfo bytea,
            FullName character varying(2048) GENERATED ALWAYS AS (FirstName || ' ' || LastName) STORED,
            PRIMARY KEY (SingerId)
         )"#
        .to_string(),
        r#"CREATE TABLE Albums (
            SingerId     bigint NOT NULL,
            AlbumId      bigint NOT NULL,
            AlbumTitle   character varying(1024),
            PRIMARY KEY (SingerId, AlbumId)
         ) INTERLEAVE IN PARENT Singers ON DELETE CASCADE"#
        .to_string(),
    ];

    println!(
        "Creating Singers and Albums tables in PostgreSQL database {}...",
        database.name
    );
    admin_client
        .update_database_ddl()
        .set_database(database.name.clone())
        .set_statements(ddl_statements)
        .poller()
        .until_done()
        .await?;

    println!("Created tables successfully.");
    Ok(())
}
// [END spanner_postgresql_create_database]
