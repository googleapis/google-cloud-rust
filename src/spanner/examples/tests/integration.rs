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
mod common;

#[cfg(all(test, feature = "run-integration-tests"))]
mod tests {
    use super::common::{setup_sample_database, teardown_sample_database};
    use google_cloud_spanner::client::DatabaseClient;
    use google_cloud_spanner::statement::Statement;
    use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;
    use google_cloud_test_utils::errors::anydump;
    use spanner_samples::{database, dml, mutation, query, read};

    /// Macro to define sample integration tests, managing database
    /// provisioning and automatic teardown.
    macro_rules! define_sample_tests {
        (
            $(
                async fn $name:ident($ctx_client:ident : &DatabaseClient, $ctx:ident : &TestDatabaseContext) -> anyhow::Result<()> [dialect = $dialect:expr] $body:block
            )*
        ) => {
            $(
                #[tokio::test]
                async fn $name() -> anyhow::Result<()> {
                    let Some(ctx) = setup_sample_database($dialect).await.inspect_err(anydump)? else {
                        return Ok(());
                    };

                    let $ctx_client = &ctx.client;
                    let $ctx = &ctx;

                    // Execute the test block
                    let res: Result<(), anyhow::Error> = async $body.await;

                    // Drop the provisioned database
                    teardown_sample_database(ctx).await.inspect_err(anydump)?;

                    res.inspect_err(anydump)
                }
            )*
        };
    }

    define_sample_tests! {
        async fn googlesql_samples(client: &DatabaseClient, ctx: &TestDatabaseContext) -> anyhow::Result<()> [dialect = DatabaseDialect::GoogleStandardSql] {
            // 1. Test spanner_insert_data sample
            mutation::insert_data::sample(client)
                .await
                .inspect_err(anydump)?;

            // 1b. Test spanner_dml_getting_started_insert sample
            dml::dml_insert::sample(client)
                .await
                .inspect_err(anydump)?;

            // 2. Test spanner_query_data sample
            query::query_data::sample(client)
                .await
                .inspect_err(anydump)?;

            // 3. Test spanner_query_with_parameter sample
            query::query_parameter::sample(client)
                .await
                .inspect_err(anydump)?;

            // 4. Test spanner_read_data sample
            read::read_data::sample(client)
                .await
                .inspect_err(anydump)?;

            // 4b. Test spanner_create_index sample
            database::create_index::sample(&ctx.admin_client, &ctx.database_name)
                .await
                .inspect_err(anydump)?;

            // 4c. Test spanner_read_data_with_index sample
            query::read_data_with_index::sample(client)
                .await
                .inspect_err(anydump)?;

            // 5. Test spanner_add_column sample
            database::add_column::sample(&ctx.admin_client, &ctx.database_name)
                .await
                .inspect_err(anydump)?;

            // 6. Test spanner_update_data sample
            mutation::update_data::sample(client)
                .await
                .inspect_err(anydump)?;

            // 6b. Test spanner_dml_getting_started_update sample
            dml::dml_update::sample(client)
                .await
                .inspect_err(anydump)?;

            // 7. Test spanner_query_data_with_new_column sample
            query::query_new_column::sample(client)
                .await
                .inspect_err(anydump)?;

            // 8. Test spanner_read_only_transaction sample
            query::read_only_transaction::sample(client)
                .await
                .inspect_err(anydump)?;

            Ok(())
        }

        async fn postgresql_samples(client: &DatabaseClient, ctx: &TestDatabaseContext) -> anyhow::Result<()> [dialect = DatabaseDialect::Postgresql] {
            // 1. Test spanner_insert_data sample (mutations are dialect-agnostic)
            mutation::insert_data::sample(client)
                .await
                .inspect_err(anydump)?;

            // 1b. Test spanner_postgresql_dml_getting_started_insert sample
            dml::pg_dml_insert::sample(client)
                .await
                .inspect_err(anydump)?;

            // 2. Test spanner_postgresql_query_with_parameter sample
            query::pg_query_parameter::sample(client)
                .await
                .inspect_err(anydump)?;

            // 2b. Test spanner_postgresql_create_index sample
            database::pg_create_index::sample(&ctx.admin_client, &ctx.database_name)
                .await
                .inspect_err(anydump)?;

            // 2c. Test spanner_postgresql_read_data_with_index sample
            query::pg_read_data_with_index::sample(client)
                .await
                .inspect_err(anydump)?;

            // 3. Test spanner_postgresql_add_column sample
            database::pg_add_column::sample(&ctx.admin_client, &ctx.database_name)
                .await
                .inspect_err(anydump)?;

            // 4. Test spanner_update_data sample
            mutation::update_data::sample(client)
                .await
                .inspect_err(anydump)?;

            // 4b. Test spanner_postgresql_dml_getting_started_update sample
            dml::pg_dml_update::sample(client)
                .await
                .inspect_err(anydump)?;

            // 5. Test spanner_postgresql_query_data_with_new_column sample
            query::pg_query_new_column::sample(client)
                .await
                .inspect_err(anydump)?;

            // 6. Test spanner_postgresql_read_only_transaction sample
            query::pg_read_only_transaction::sample(client)
                .await
                .inspect_err(anydump)?;

            Ok(())
        }

        async fn client_and_database_samples_googlesql(client: &DatabaseClient, _ctx: &TestDatabaseContext) -> anyhow::Result<()> [dialect = DatabaseDialect::GoogleStandardSql] {
            // Verify GoogleSQL database tables exist using the macro-provisioned database client
            let has_singers = verify_table_exists(client, "Singers", DatabaseDialect::GoogleStandardSql).await?;
            let has_albums = verify_table_exists(client, "Albums", DatabaseDialect::GoogleStandardSql).await?;
            assert!(has_singers, "GoogleSQL database missing Singers table");
            assert!(has_albums, "GoogleSQL database missing Albums table");
            Ok(())
        }

        async fn client_and_database_samples_postgresql(client: &DatabaseClient, _ctx: &TestDatabaseContext) -> anyhow::Result<()> [dialect = DatabaseDialect::Postgresql] {
            // Verify PostgreSQL database tables exist (respecting lowercase folding in schema)
            let has_singers = verify_table_exists(client, "singers", DatabaseDialect::Postgresql).await?;
            let has_albums = verify_table_exists(client, "albums", DatabaseDialect::Postgresql).await?;
            assert!(has_singers, "PostgreSQL database missing singers table");
            assert!(has_albums, "PostgreSQL database missing albums table");
            Ok(())
        }
    }

    async fn verify_table_exists(
        client: &DatabaseClient,
        table_name: &str,
        dialect: DatabaseDialect,
    ) -> anyhow::Result<bool> {
        let statement = match dialect {
            DatabaseDialect::Postgresql => {
                Statement::builder(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1",
                )
                .add_param("p1", &table_name)
                .build()
            }
            _ => {
                Statement::builder(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table_name",
                )
                .add_param("table_name", &table_name)
                .build()
            }
        };

        let transaction = client.single_use().build();
        let mut result_set = transaction.execute_query(statement).await?;
        let exists = result_set.next().await.transpose()?.is_some();
        Ok(exists)
    }
}
