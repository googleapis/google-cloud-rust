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
    use integration_tests_spanner::batch_write;
    use integration_tests_spanner::client;

    /// Defines the integration test suites and manages their lifecycle.
    ///
    /// This macro accomplishes several things:
    /// 1. **Dynamic Suite Counting**: It counts the exact number of test suites defined at compile time
    ///    to establish `TOTAL_TEST_SUITES`.
    /// 2. **Client Provisioning**: It attempts to create a database client for either the Spanner Emulator
    ///    or a real Spanner instance. If neither environment is configured, it gracefully skips the test.
    /// 3. **Shared Database Cleanup**: It invokes `client::finish_test(TOTAL_TEST_SUITES)` upon completion
    ///    of each test suite (regardless of whether the test succeeded, failed, or was skipped). If the tests
    ///    ran against a real Spanner instance, the final invocation safely drops the dynamically created database.
    macro_rules! define_test_suites {
        (
            $(
                async fn $name:ident($db_client:ident : &DatabaseClient) -> anyhow::Result<()> $body:block
            )*
        ) => {
            const TOTAL_TEST_SUITES: usize = 0 $( + { let _ = stringify!($name); 1 } )*;

            $(
                #[tokio::test]
                async fn $name() -> anyhow::Result<()> {
                    let db_client_val = match client::create_database_client().await {
                        Some(c) => c,
                        None => {
                            client::finish_test(TOTAL_TEST_SUITES).await;
                            return Ok(());
                        }
                    };

                    let $db_client = &db_client_val;

                    let res = async $body.await;

                    client::finish_test(TOTAL_TEST_SUITES).await;
                    res
                }
            )*
        };
    }

    define_test_suites! {
        async fn run_query_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::query::simple_query(db_client).await?;
            integration_tests_spanner::query::query_with_parameters(db_client).await?;
            integration_tests_spanner::query::result_set_metadata(db_client).await?;
            integration_tests_spanner::query::multi_use_read_only_transaction(db_client).await?;
            integration_tests_spanner::query::multi_use_read_only_transaction_invalid_query_fallback(
                db_client,
            )
            .await?;
            integration_tests_spanner::query::multi_use_read_only_transaction_interleaved(db_client)
                .await?;
            integration_tests_spanner::query::inline_begin_fallback(db_client).await?;
            integration_tests_spanner::query::query_with_options(db_client).await?;
            integration_tests_spanner::query::query_plan(db_client).await?;
            integration_tests_spanner::query::query_profile(db_client).await?;
            integration_tests_spanner::query::dml_plan(db_client).await?;
            Ok(())
        }

        async fn run_write_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::write::write_only_transaction(db_client).await?;
            integration_tests_spanner::write::write(db_client).await?;
            integration_tests_spanner::write::all_data_types_roundtrip(db_client).await?;
            integration_tests_spanner::write::all_data_types_parameter_binding(db_client).await?;
            integration_tests_spanner::write::interval_parameter_binding(db_client).await?;
            Ok(())
        }

        async fn run_batch_write_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            batch_write::batch_write(db_client).await?;
            batch_write::batch_write_partial_failure(db_client).await?;
            Ok(())
        }

        async fn run_read_write_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::read_write_transaction::successful_read_write_transaction(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::rolled_back_read_write_transaction(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::concurrent_read_write_transaction_retries(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::read_write_transaction_with_mutations(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::read_write_transaction_mutation_only(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::read_write_transaction_multiple_queries_and_dml(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::consecutive_reads(db_client).await?;
            integration_tests_spanner::read_write_transaction::mixed_reads_and_queries(db_client)
                .await?;
            integration_tests_spanner::read_write_transaction::multiple_execute_updates(db_client)
                .await?;
            integration_tests_spanner::read_write_transaction::read_your_writes_consistency(db_client)
                .await?;
            integration_tests_spanner::read_write_transaction::buffered_mutation_interleaving(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::initial_statement_failure_handling(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::intermediate_statement_constraint_error(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::buffered_mutation_commit_rejection(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::application_error_explicit_rollback(
                db_client,
            )
            .await?;
            integration_tests_spanner::read_write_transaction::continue_after_initial_query_error(
                db_client,
            )
            .await?;
            Ok(())
        }

        async fn run_partitioned_dml_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::partitioned_dml::partitioned_dml_update(db_client).await?;
            Ok(())
        }

        async fn run_read_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::read::read_single_key(db_client).await?;
            integration_tests_spanner::read::read_all_keys(db_client).await?;
            integration_tests_spanner::read::read_key_range(db_client).await?;
            integration_tests_spanner::read::read_with_limit(db_client).await?;
            integration_tests_spanner::read::read_with_index(db_client).await?;
            integration_tests_spanner::read::read_as_stream(db_client).await?;
            Ok(())
        }

        async fn run_read_only_transaction_options_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::read_only_transaction_options::read_only_bounded_staleness(db_client).await?;
            integration_tests_spanner::read_only_transaction_options::read_timestamp_unavailable_before_start(db_client).await?;
            integration_tests_spanner::read_only_transaction_options::read_timestamp_available_on_failed_first_query(db_client).await?;
            Ok(())
        }

        async fn run_batch_read_only_transaction_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::batch_read_only_transaction::partitioned_query(db_client)
                .await?;
            integration_tests_spanner::batch_read_only_transaction::partitioned_read(db_client)
                .await?;
            integration_tests_spanner::batch_read_only_transaction::partition_tuning_and_data_boost(db_client)
                .await?;
            integration_tests_spanner::batch_read_only_transaction::parallel_partition_execution(db_client)
                .await?;
            Ok(())
        }

        async fn run_concurrent_inline_begin_tests(_db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::concurrent_inline_begin::test_concurrent_inline_begin_with_snapshot_consistency().await?;
            Ok(())
        }

        async fn run_directed_read_tests(db_client: &DatabaseClient) -> anyhow::Result<()> {
            integration_tests_spanner::directed_read::read_only_with_directed_read(db_client).await?;
            integration_tests_spanner::directed_read::read_write_with_directed_read_error(db_client)
                .await?;
            Ok(())
        }
    }
}
