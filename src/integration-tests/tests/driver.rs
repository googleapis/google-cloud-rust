// Copyright 2024 Google LLC
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
mod driver {
    use storage::client::Storage;
    use storage_control::client::StorageControl;
    use test_case::test_case;

    fn retry_policy() -> impl gax::retry_policy::RetryPolicy {
        use gax::retry_policy::RetryPolicyExt;
        use std::time::Duration;
        gax::retry_policy::AlwaysRetry
            .with_time_limit(Duration::from_secs(15))
            .with_attempt_limit(5)
    }

    #[test_case(bigquery::client::DatasetService::builder().with_tracing().with_retry_policy(retry_policy()); "with [tracing, retry] enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_bigquery(
        builder: bigquery::builder::dataset_service::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::bigquery::dataset_admin(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(firestore::client::Firestore::builder(); "default")]
    #[test_case(firestore::client::Firestore::builder().with_tracing(); "with tracing enabled")]
    #[test_case(firestore::client::Firestore::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_firestore(
        builder: firestore::builder::firestore::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::firestore::basic(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(sm::client::SecretManagerService::builder(); "default")]
    #[test_case(sm::client::SecretManagerService::builder().with_tracing(); "with tracing enabled")]
    #[test_case(sm::client::SecretManagerService::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_protobuf(
        builder: sm::builder::secret_manager_service::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::secret_manager::protobuf::run(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(smo::client::SecretManagerService::builder(); "default")]
    #[test_case(smo::client::SecretManagerService::builder().with_tracing(); "with tracing enabled")]
    #[test_case(smo::client::SecretManagerService::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_openapi(
        builder: smo::builder::secret_manager_service::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::secret_manager::openapi::run(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(smo::client::SecretManagerService::builder(); "default")]
    #[test_case(smo::client::SecretManagerService::builder().with_tracing(); "with tracing enabled")]
    #[test_case(smo::client::SecretManagerService::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_openapi_locational(
        builder: smo::builder::secret_manager_service::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::secret_manager::openapi_locational::run(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(sql::client::SqlInstancesService::builder().with_tracing().with_retry_policy(retry_policy()); "with [tracing, retry] enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_sql(
        builder: sql::builder::sql_instances_service::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::sql::run_sql_instances_service(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(sql::client::SqlTiersService::builder().with_tracing().with_retry_policy(retry_policy()); "with [tracing, retry] enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_sql_tiers_service(
        builder: sql::builder::sql_tiers_service::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::sql::run_sql_tiers_service(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(StorageControl::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[test_case(StorageControl::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_control_buckets(
        builder: storage_control::client::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::storage::buckets(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects(
        builder: storage::client::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::storage::objects(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects_large_file(
        builder: storage::client::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::storage::objects_large_file(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects_upload_buffered(
        builder: storage::client::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::storage::objects_upload_buffered(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects_with_key(
        builder: storage::client::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::storage::objects_customer_supplied_encryption(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(ta::client::TelcoAutomation::builder().with_tracing(); "with tracing enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_error_details(
        builder: ta::builder::telco_automation::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::error_details::run(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(wf::client::Workflows::builder().with_tracing(); "with tracing enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_http(
        builder: wf::builder::workflows::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::error_details::check_code_for_http(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(StorageControl::builder().with_tracing(); "with tracing enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_grpc(
        builder: storage_control::client::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::error_details::check_code_for_grpc(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(wf::client::Workflows::builder(); "default")]
    #[test_case(wf::client::Workflows::builder().with_tracing(); "with tracing enabled")]
    #[test_case(wf::client::Workflows::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_until_done(
        builder: wf::builder::workflows::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::workflows::until_done(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(wf::client::Workflows::builder(); "default")]
    #[test_case(wf::client::Workflows::builder().with_tracing(); "with tracing enabled")]
    #[test_case(wf::client::Workflows::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_explicit(
        builder: wf::builder::workflows::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::workflows::explicit_loop(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(wf::client::Workflows::builder(); "default")]
    #[test_case(wf::client::Workflows::builder().with_tracing(); "with tracing enabled")]
    #[test_case(wf::client::Workflows::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_manual(
        builder: wf::builder::workflows::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::workflows::until_done(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(wfe::client::Executions::builder().with_retry_policy(retry_policy()).with_tracing(); "with tracing and retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_executions(
        builder: wfe::builder::executions::ClientBuilder,
    ) -> integration_tests::Result<()> {
        integration_tests::workflows_executions::list(builder)
            .await
            .map_err(integration_tests::report_error)
    }
}
