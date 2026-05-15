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
    use google_cloud_spanner::client::{DatabaseClient, Mutation};
    use google_cloud_test_utils::errors::anydump;
    use integration_tests_spanner::client::{create_database_client, update_database_ddl_batch};
    use spanner_samples::query;

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
}
