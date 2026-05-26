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
    use super::common::{clear_database_data, setup_sample_emulator};
    use google_cloud_test_utils::errors::anydump;
    use integration_tests_spanner::client::update_database_ddl_batch;
    use serial_test::serial;
    use spanner_samples::{mutation, query, read};

    #[tokio::test]
    #[serial]
    async fn query_samples() -> anyhow::Result<()> {
        let Some(database_client) = setup_sample_emulator().await.inspect_err(anydump)? else {
            return Ok(());
        };

        query::query_data::sample(&database_client)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    #[serial]
    async fn mutation_and_read_samples() -> anyhow::Result<()> {
        let Some(database_client) = setup_sample_emulator().await.inspect_err(anydump)? else {
            return Ok(());
        };

        // Clear data for clean mutations testing
        clear_database_data(&database_client).await?;

        // 1. Test spanner_insert_data sample
        mutation::insert_data::sample(&database_client)
            .await
            .inspect_err(anydump)?;

        // 2. Test spanner_read_data sample
        read::read_data::sample(&database_client)
            .await
            .inspect_err(anydump)?;

        // 3. Try to drop the column first in case a previous run failed, then add it
        let _ = update_database_ddl_batch(vec![
            "ALTER TABLE Albums DROP COLUMN MarketingBudget".to_string(),
        ])
        .await;
        update_database_ddl_batch(vec![
            "ALTER TABLE Albums ADD COLUMN MarketingBudget INT64".to_string(),
        ])
        .await
        .inspect_err(anydump)?;

        // 4. Test spanner_update_data sample
        mutation::update_data::sample(&database_client)
            .await
            .inspect_err(anydump)?;

        // 5. Clean up schema changes by dropping the column to restore the clean slate
        update_database_ddl_batch(vec![
            "ALTER TABLE Albums DROP COLUMN MarketingBudget".to_string(),
        ])
        .await
        .inspect_err(anydump)?;

        Ok(())
    }
}
