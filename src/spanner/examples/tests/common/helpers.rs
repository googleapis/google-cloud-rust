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

use google_cloud_auth::credentials::anonymous::Builder as AnonymousBuilder;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use integration_tests_spanner::client::{
    get_emulator_host, get_emulator_rest_endpoint, provision_emulator, wait_for_emulator,
};
use spanner_samples::{client, database};

pub struct TestDatabaseContext {
    pub client: DatabaseClient,
    pub admin_client: DatabaseAdmin,
    pub database_name: String,
}

pub async fn setup_sample_database(
    dialect: DatabaseDialect,
) -> anyhow::Result<Option<TestDatabaseContext>> {
    let Some(emulator_host) = get_emulator_host() else {
        return Ok(None);
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
    let admin_client = admin_builder.build().await.map_err(anyhow::Error::from)?;

    let instance_name = "projects/test-project/instances/test-instance";
    let database_id = match dialect {
        DatabaseDialect::GoogleStandardSql => {
            format!("test-db-gsql-{}", LowercaseAlphanumeric.random_string(10))
        }
        DatabaseDialect::Postgresql => {
            format!("test-db-pg-{}", LowercaseAlphanumeric.random_string(10))
        }
        _ => anyhow::bail!("Unsupported database dialect"),
    };
    let database_name = format!("{instance_name}/databases/{database_id}");

    // Dynamically provision the test database and table schema using the correct dialect sample
    match dialect {
        DatabaseDialect::GoogleStandardSql => {
            database::create_database::sample(&admin_client, instance_name, &database_id).await?;
        }
        DatabaseDialect::Postgresql => {
            database::pg_create_database::sample(&admin_client, instance_name, &database_id)
                .await?;
        }
        _ => anyhow::bail!("Unsupported database dialect"),
    }

    // Test the client initialization sample targeting this unique database
    let (database_client, _) = client::init_client::sample(&database_name).await?;

    Ok(Some(TestDatabaseContext {
        client: database_client,
        admin_client,
        database_name,
    }))
}

pub async fn teardown_sample_database(context: TestDatabaseContext) -> anyhow::Result<()> {
    // Cleanly drop the dynamic database while the runtime is active and alive
    context
        .admin_client
        .drop_database()
        .set_database(context.database_name)
        .send()
        .await
        .map_err(anyhow::Error::from)?;
    Ok(())
}
