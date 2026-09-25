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

//! Canonical integration test recipe for applications testing against the Cloud Spanner Emulator.
//!
//! This test demonstrates how an external application or coding agent should write
//! idiomatic, leak-free integration tests using the Spanner emulator:
//! 1. Inspect the `SPANNER_EMULATOR_HOST` environment variable to skip or target the emulator.
//! 2. Initialize a `Spanner` client (which automatically detects emulator environment variables).
//! 3. Use `DatabaseAdmin` via `spanner.database_admin_builder()` to create a database and DDL schema.
//! 4. Obtain a long-lived `DatabaseClient` for query and transaction execution.
//! 5. Execute upsert mutations, parameterized queries, and read-write transactions.
//! 6. Verify assertions with explicit diagnostic messages.
//! 7. Tear down the test database on completion.

#[cfg(all(test, feature = "run-integration-tests"))]
mod tests {
    use google_cloud_lro::Poller;
    use google_cloud_spanner::client::Spanner;
    use google_cloud_spanner::mutation::Mutation;
    use google_cloud_spanner::statement::Statement;
    use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
    use integration_tests_spanner::client::{provision_emulator, wait_for_emulator};
    use std::env;

    #[tokio::test]
    async fn emulator_integration_workflow() -> anyhow::Result<()> {
        let Ok(emulator_host) = env::var("SPANNER_EMULATOR_HOST") else {
            eprintln!(
                "SPANNER_EMULATOR_HOST is not set; skipping emulator integration test recipe."
            );
            return Ok(());
        };

        // Internal repo test harness helpers that ensure the emulator container is ready
        // and that the default test instance ("projects/test-project/instances/test-instance") exists.
        // External applications should ensure the emulator is running and provision an instance
        // using the gcloud CLI or `spanner_client.instance_admin_builder().build().await?`.
        wait_for_emulator(&emulator_host).await;
        provision_emulator(&emulator_host).await;

        // When targeting the emulator, instances are created under the fixed project "test-project".
        let project_id = "test-project";
        let instance_id = "test-instance";
        let instance_name = format!("projects/{project_id}/instances/{instance_id}");

        let database_id = format!("recipe-db-{}", LowercaseAlphanumeric.random_string(8));
        let database_name = format!("{instance_name}/databases/{database_id}");

        // 1. Initialize root Spanner client (automatically targets SPANNER_EMULATOR_HOST)
        let spanner_client = Spanner::builder().build().await?;

        // 2. Pre-configured admin client for DDL operations on the emulator
        let database_admin_client = spanner_client.database_admin_builder().build().await?;

        // 3. Create a test database with initial schema
        let create_statement = format!("CREATE DATABASE `{database_id}`");
        let extra_statements = vec![
            r#"CREATE TABLE Singers (
                SingerId INT64 NOT NULL,
                FirstName STRING(1024),
                LastName STRING(1024)
             ) PRIMARY KEY (SingerId)"#
                .to_string(),
        ];

        let _created_database = database_admin_client
            .create_database()
            .set_parent(instance_name)
            .set_create_statement(create_statement)
            .set_extra_statements(extra_statements)
            .poller()
            .until_done()
            .await?;

        // 4. Create long-lived DatabaseClient (cheap to clone, thread-safe)
        let database_client = spanner_client
            .database_client(&database_name)
            .build()
            .await?;

        let workflow_result: anyhow::Result<()> = async {
            // 5. Upsert test records using Mutation API in a write-only transaction
            let mutations = vec![
                Mutation::new_insert_or_update_builder("Singers")
                    .set("SingerId")
                    .to(100)
                    .set("FirstName")
                    .to("Alice")
                    .set("LastName")
                    .to("Example")
                    .build(),
                Mutation::new_insert_or_update_builder("Singers")
                    .set("SingerId")
                    .to(101)
                    .set("FirstName")
                    .to("Bob")
                    .set("LastName")
                    .to("Example")
                    .build(),
            ];

            let write_transaction = database_client.write_only_transaction().build();
            write_transaction.write(mutations).await?;

            // 6. Execute parameterized query in a single-use transaction
            let query_statement = Statement::builder(
                "SELECT SingerId, FirstName, LastName FROM Singers WHERE SingerId = @id",
            )
            .add_param("id", 100i64)
            .build();

            let single_use_transaction = database_client.single_use().build();
            let mut result_set = single_use_transaction
                .execute_query(query_statement)
                .await?;

            let mut matched_singer_name = None;
            while let Some(row) = result_set.next().await.transpose()? {
                let first_name: String = row.get("FirstName");
                let last_name: String = row.get("LastName");
                matched_singer_name = Some(format!("{first_name} {last_name}"));
            }

            assert_eq!(
                matched_singer_name.as_deref(),
                Some("Alice Example"),
                "Retrieved singer name must match the upserted record"
            );

            // 7. Execute read-write transaction with automatic ABORTED retry handling
            let runner = database_client.read_write_transaction().build().await?;
            let update_result = runner
                .run(async |transaction| {
                    let update_statement = Statement::builder(
                        "UPDATE Singers SET FirstName = @firstName WHERE SingerId = @id",
                    )
                    .add_param("firstName", "Alicia")
                    .add_param("id", 100i64)
                    .build();

                    let updated_rows = transaction.execute_update(update_statement).await?;
                    Ok(updated_rows)
                })
                .await?;

            assert_eq!(
                update_result.result, 1,
                "Expected exactly one row updated by the read-write transaction"
            );

            Ok(())
        }
        .await;

        // 8. Clean up test database unconditionally
        let teardown_result = database_admin_client
            .drop_database()
            .set_database(database_name)
            .send()
            .await;

        workflow_result?;
        teardown_result?;
        Ok(())
    }
}
