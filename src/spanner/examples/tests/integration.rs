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

#[cfg(all(test, feature = "run-integration-tests"))]
mod tests {
    use google_cloud_auth::credentials::anonymous::Builder as AnonymousBuilder;
    use google_cloud_spanner::client::{DatabaseClient, Mutation, Statement};
    use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
    use google_cloud_test_utils::errors::anydump;
    use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
    use integration_tests_spanner::client::{
        create_database_client, get_emulator_host, get_emulator_rest_endpoint, provision_emulator,
        update_database_ddl_batch, wait_for_emulator,
    };
    use spanner_samples::{client, database, query};

    async fn setup_sample_emulator() -> anyhow::Result<Option<DatabaseClient>> {
        let Some(database_client) = create_database_client().await else {
            return Ok(None);
        };

        // Ensure the Singers and Albums tables exist in the provisioned test database.
        update_database_ddl_batch(vec![
            "CREATE TABLE IF NOT EXISTS Singers ( \
                SingerId INT64 NOT NULL, \
                FirstName STRING(1024), \
                LastName STRING(1024), \
                SingerInfo BYTES(MAX), \
                FullName STRING(2048) AS (ARRAY_TO_STRING([FirstName, LastName], \" \")) STORED \
             ) PRIMARY KEY (SingerId)"
                .to_string(),
            "CREATE TABLE IF NOT EXISTS Albums ( \
                SingerId INT64 NOT NULL, \
                AlbumId INT64 NOT NULL, \
                AlbumTitle STRING(MAX) \
             ) PRIMARY KEY (SingerId, AlbumId), \
             INTERLEAVE IN PARENT Singers ON DELETE CASCADE"
                .to_string(),
        ])
        .await?;

        // Populate standard sample data into Singers and Albums tables.
        let write_transaction = database_client.write_only_transaction().build();
        let mutations = vec![
            Mutation::new_insert_or_update_builder("Singers")
                .set("SingerId")
                .to(&1)
                .set("FirstName")
                .to(&"Marc")
                .set("LastName")
                .to(&"Richards")
                .build(),
            Mutation::new_insert_or_update_builder("Singers")
                .set("SingerId")
                .to(&2)
                .set("FirstName")
                .to(&"Catalina")
                .set("LastName")
                .to(&"Smith")
                .build(),
            Mutation::new_insert_or_update_builder("Singers")
                .set("SingerId")
                .to(&3)
                .set("FirstName")
                .to(&"Alice")
                .set("LastName")
                .to(&"Trentor")
                .build(),
            Mutation::new_insert_or_update_builder("Singers")
                .set("SingerId")
                .to(&4)
                .set("FirstName")
                .to(&"Lea")
                .set("LastName")
                .to(&"Martin")
                .build(),
            Mutation::new_insert_or_update_builder("Singers")
                .set("SingerId")
                .to(&5)
                .set("FirstName")
                .to(&"David")
                .set("LastName")
                .to(&"Lomond")
                .build(),
            Mutation::new_insert_or_update_builder("Albums")
                .set("SingerId")
                .to(&1)
                .set("AlbumId")
                .to(&1)
                .set("AlbumTitle")
                .to(&"Total Junk")
                .build(),
            Mutation::new_insert_or_update_builder("Albums")
                .set("SingerId")
                .to(&1)
                .set("AlbumId")
                .to(&2)
                .set("AlbumTitle")
                .to(&"Go, Go, Go")
                .build(),
            Mutation::new_insert_or_update_builder("Albums")
                .set("SingerId")
                .to(&2)
                .set("AlbumId")
                .to(&1)
                .set("AlbumTitle")
                .to(&"Green")
                .build(),
            Mutation::new_insert_or_update_builder("Albums")
                .set("SingerId")
                .to(&2)
                .set("AlbumId")
                .to(&2)
                .set("AlbumTitle")
                .to(&"Forever Hold Your Peace")
                .build(),
            Mutation::new_insert_or_update_builder("Albums")
                .set("SingerId")
                .to(&2)
                .set("AlbumId")
                .to(&3)
                .set("AlbumTitle")
                .to(&"Terrified")
                .build(),
        ];
        write_transaction.write_at_least_once(mutations).await?;

        Ok(Some(database_client))
    }

    #[tokio::test]
    async fn query_samples() -> anyhow::Result<()> {
        let Some(database_client) = setup_sample_emulator().await.inspect_err(anydump)? else {
            return Ok(());
        };

        query::query_data::sample(&database_client)
            .await
            .inspect_err(anydump)
    }

    async fn verify_table_exists(
        client: &DatabaseClient,
        table_name: &str,
        is_pg: bool,
    ) -> anyhow::Result<bool> {
        let statement = if is_pg {
            Statement::builder(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1",
            )
            .add_param("p1", &table_name)
            .build()
        } else {
            Statement::builder(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table_name",
            )
            .add_param("table_name", &table_name)
            .build()
        };

        let transaction = client.single_use().build();
        let mut result_set = transaction.execute_query(statement).await?;
        let exists = result_set.next().await.transpose()?.is_some();
        Ok(exists)
    }

    #[tokio::test]
    async fn client_and_database_samples() -> anyhow::Result<()> {
        let Some(emulator_host) = get_emulator_host() else {
            return Ok(());
        };

        // Ensure the emulator is running and the instance is provisioned
        wait_for_emulator(&emulator_host).await;
        provision_emulator(&emulator_host).await;

        // Initialize a temporary admin client for database creation samples
        let mut admin_builder = DatabaseAdmin::builder();
        let rest_endpoint = get_emulator_rest_endpoint(&emulator_host);
        admin_builder = admin_builder
            .with_endpoint(rest_endpoint)
            .with_credentials(AnonymousBuilder::new().build());
        let admin_client = admin_builder
            .build()
            .await
            .map_err(anyhow::Error::from)
            .inspect_err(anydump)?;

        let instance_name = "projects/test-project/instances/test-instance";
        let googlesql_database_id =
            format!("test-db-gsql-{}", LowercaseAlphanumeric.random_string(10));
        let pg_database_id = format!("test-db-pg-{}", LowercaseAlphanumeric.random_string(10));

        // 1. Test GoogleSQL Database Creation Sample
        database::create_database::sample(&admin_client, instance_name, &googlesql_database_id)
            .await
            .inspect_err(anydump)?;

        // 2. Test PostgreSQL Database Creation Sample
        database::pg_create_database::sample(&admin_client, instance_name, &pg_database_id)
            .await
            .inspect_err(anydump)?;

        // 3. Test the client initialization sample for the GoogleSQL database (now that it exists!)
        let gsql_database_name = format!("{instance_name}/databases/{googlesql_database_id}");
        let (gsql_database_client, _) = client::init_client::sample(&gsql_database_name)
            .await
            .inspect_err(anydump)?;

        // 4. Test the client initialization sample for the PostgreSQL database (now that it exists!)
        let pg_database_name = format!("{instance_name}/databases/{pg_database_id}");
        let (pg_database_client, _) = client::init_client::sample(&pg_database_name)
            .await
            .inspect_err(anydump)?;

        // 5. Verify GoogleSQL database tables using the database client
        let has_singers = verify_table_exists(&gsql_database_client, "Singers", false).await?;
        let has_albums = verify_table_exists(&gsql_database_client, "Albums", false).await?;
        assert!(has_singers, "GoogleSQL database missing Singers table");
        assert!(has_albums, "GoogleSQL database missing Albums table");

        // 6. Verify PostgreSQL database tables using the database client
        // Note: PostgreSQL table names default to lowercase in the schema.
        let has_singers = verify_table_exists(&pg_database_client, "singers", true).await?;
        let has_albums = verify_table_exists(&pg_database_client, "albums", true).await?;
        assert!(has_singers, "PostgreSQL database missing Singers table");
        assert!(has_albums, "PostgreSQL database missing Albums table");

        Ok(())
    }
}
