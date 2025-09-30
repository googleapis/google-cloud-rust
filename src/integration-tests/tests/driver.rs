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
    use storage::client::{Storage, StorageControl};
    use test_case::test_case;

    fn retry_policy() -> impl gax::retry_policy::RetryPolicy {
        use gax::retry_policy::RetryPolicyExt;
        use std::time::Duration;
        gax::retry_policy::AlwaysRetry
            .with_time_limit(Duration::from_secs(15))
            .with_attempt_limit(5)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_aiplatform() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::aiplatform::locational_endpoint()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_bigquery_dataset_service() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::bigquery::dataset_admin()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_bigquery_job_service() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::bigquery::job_service()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_zones() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::compute::zones()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_machine_types() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::compute::machine_types()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_images() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::compute::images()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_firestore() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::firestore::basic()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_pubsub_basic_topic() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::pubsub::basic_topic()
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(sm::client::SecretManagerService::builder(); "default")]
    #[test_case(sm::client::SecretManagerService::builder().with_tracing(); "with tracing enabled")]
    #[test_case(sm::client::SecretManagerService::builder().with_retry_policy(retry_policy()); "with retry enabled")]
    #[test_case(sm::client::SecretManagerService::builder().with_endpoint("https://www.googleapis.com"); "with alternative endpoint")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_protobuf(
        builder: sm::builder::secret_manager_service::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::secret_manager::protobuf::run(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_openapi() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::secret_manager::openapi::run()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_openapi_locational() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::secret_manager::openapi_locational::run()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_sql() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::sql::run_sql_instances_service()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_sql_tiers_service() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::sql::run_sql_tiers_service()
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(StorageControl::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[test_case(StorageControl::builder().with_endpoint("https://www.googleapis.com"); "with global endpoint")]
    #[test_case(StorageControl::builder().with_endpoint("https://us-central1-storage.googleapis.com"); "with locational endpoint")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_control_buckets(
        builder: storage::builder::storage_control::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::buckets(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[test_case(Storage::builder().with_endpoint("https://www.googleapis.com"); "with global endpoint")]
    #[test_case(Storage::builder().with_endpoint("https://us-central1-storage.googleapis.com"); "with locational endpoint")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::objects(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects_large_file(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::objects_large_file(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_upload_buffered(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::upload_buffered(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_upload_buffered_resumable_known_size(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::upload_buffered_resumable_known_size(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_upload_buffered_resumable_unknown_size(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::upload_buffered_resumable_unknown_size(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_upload_unbuffered_resumable_known_size(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::upload_unbuffered_resumable_known_size(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_upload_unbuffered_resumable_unknown_size(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::upload_unbuffered_resumable_unknown_size(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_abort_upload(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let result = integration_tests::storage::abort_upload(builder, &bucket.name)
            .await
            .map_err(integration_tests::report_error);
        let _ = storage_samples::cleanup_bucket(control, bucket.name).await;
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_checksums(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let result = integration_tests::storage::checksums(builder, &bucket.name)
            .await
            .map_err(integration_tests::report_error);
        let _ = storage_samples::cleanup_bucket(control, bucket.name).await;
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_ranged_reads(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let result = integration_tests::storage::ranged_reads(builder, &bucket.name)
            .await
            .map_err(integration_tests::report_error);
        let _ = storage_samples::cleanup_bucket(control, bucket.name).await;
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_object_names(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let result =
            integration_tests::storage::object_names(builder, control.clone(), &bucket.name)
                .await
                .map_err(integration_tests::report_error);
        let _ = storage_samples::cleanup_bucket(control, bucket.name).await;
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects_with_key(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::storage::objects_customer_supplied_encryption(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_error_details_http() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::error_details::error_details_http()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_error_details_grpc() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::error_details::error_details_grpc()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_http() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::error_details::check_code_for_http()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_grpc() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::error_details::check_code_for_grpc()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_until_done() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::workflows::until_done()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_explicit() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::workflows::explicit_loop()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_manual() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::workflows::until_done()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_executions() -> integration_tests::Result<()> {
        let _guard = integration_tests::enable_tracing();
        integration_tests::workflows_executions::list()
            .await
            .map_err(integration_tests::report_error)
    }
}
