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
mod spanner {

    #[tokio::test]
    async fn run_query_tests() -> anyhow::Result<()> {
        let db_client = match integration_tests_spanner::client::create_database_client().await {
            Some(c) => c,
            None => return Ok(()),
        };

        integration_tests_spanner::query::simple_query(&db_client).await?;
        integration_tests_spanner::query::query_with_parameters(&db_client).await?;
        integration_tests_spanner::query::result_set_metadata(&db_client).await?;
        integration_tests_spanner::query::multi_use_read_only_transaction(&db_client).await?;
        integration_tests_spanner::query::multi_use_read_only_transaction_invalid_query_fallback(
            &db_client,
        )
        .await?;
        integration_tests_spanner::query::inline_begin_fallback(&db_client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn run_write_tests() -> anyhow::Result<()> {
        let db_client = match integration_tests_spanner::client::create_database_client().await {
            Some(c) => c,
            None => return Ok(()),
        };

        integration_tests_spanner::write::write_only_transaction(&db_client).await?;
        integration_tests_spanner::write::write(&db_client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn run_read_write_tests() -> anyhow::Result<()> {
        let db_client = match integration_tests_spanner::client::create_database_client().await {
            Some(c) => c,
            None => return Ok(()),
        };

        integration_tests_spanner::read_write_transaction::successful_read_write_transaction(
            &db_client,
        )
        .await?;
        integration_tests_spanner::read_write_transaction::rolled_back_read_write_transaction(
            &db_client,
        )
        .await?;

        integration_tests_spanner::read_write_transaction::concurrent_read_write_transaction_retries(
            &db_client,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn run_partitioned_dml_tests() -> anyhow::Result<()> {
        let db_client = match integration_tests_spanner::client::create_database_client().await {
            Some(c) => c,
            None => return Ok(()),
        };

        integration_tests_spanner::partitioned_dml::partitioned_dml_update(&db_client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn run_read_tests() -> anyhow::Result<()> {
        let db_client = match integration_tests_spanner::client::create_database_client().await {
            Some(c) => c,
            None => return Ok(()),
        };

        integration_tests_spanner::read::read_single_key(&db_client).await?;
        integration_tests_spanner::read::read_all_keys(&db_client).await?;
        integration_tests_spanner::read::read_key_range(&db_client).await?;
        integration_tests_spanner::read::read_with_limit(&db_client).await?;
        integration_tests_spanner::read::read_with_index(&db_client).await?;
        integration_tests_spanner::read::read_as_stream(&db_client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn run_concurrent_inline_begin_tests() -> anyhow::Result<()> {
        integration_tests_spanner::concurrent_inline_begin::test_concurrent_inline_begin_with_snapshot_consistency().await
    }

    #[tokio::test]
    async fn run_batch_read_only_transaction_tests() -> anyhow::Result<()> {
        let db_client = match integration_tests_spanner::client::create_database_client().await {
            Some(c) => c,
            None => return Ok(()),
        };

        integration_tests_spanner::batch_read_only_transaction::partitioned_query(&db_client)
            .await?;
        integration_tests_spanner::batch_read_only_transaction::partitioned_read(&db_client)
            .await?;

        Ok(())
    }
}
