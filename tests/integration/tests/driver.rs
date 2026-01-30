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
    use google_cloud_test_utils::tracing::enable_tracing;
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
    async fn run_bigquery_dataset_service() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::bigquery::dataset_admin()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_bigquery_job_service() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::bigquery::job_service()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_zones() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::compute::zones()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_errors() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::compute::errors()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_lro_errors() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::compute::lro_errors()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_machine_types() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::compute::machine_types()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_images() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::compute::images()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_instances() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::compute::instances()
            .await
            .map_err(integration_tests::report_error)
    }

    #[ignore = "TODO(#3691) - disabled because it was flaky"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_region_instances() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::compute::region_instances()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_firestore() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::firestore::basic()
            .await
            .map_err(integration_tests::report_error)
    }

    #[test_case(StorageControl::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[test_case(StorageControl::builder().with_endpoint("https://www.googleapis.com"); "with global endpoint")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_control_buckets(
        builder: storage::builder::storage_control::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::storage::buckets(builder)
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_objects() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let variants = || async {
            tracing::info!("default builder");
            let builder = Storage::builder();
            integration_tests::storage::objects(builder, &bucket.name, "default-endpoint").await?;
            tracing::info!("with global endpoint");

            let builder = Storage::builder().with_endpoint("https://www.googleapis.com");
            integration_tests::storage::objects(builder, &bucket.name, "global endpoint").await?;

            if std::env::var("GOOGLE_CLOUD_RUST_TEST_RUNNING_ON_GCB").is_ok_and(|s| s == "1") {
                tracing::info!("with locational endpoint");
                let builder =
                    Storage::builder().with_endpoint("https://us-central1-storage.googleapis.com");
                integration_tests::storage::objects(builder, &bucket.name, "locational-endpoint")
                    .await?;
            }
            Ok(())
        };
        let result = variants().await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_signed_urls() -> integration_tests::Result<()> {
        let _guard = enable_tracing();

        let signer = google_cloud_auth::credentials::Builder::default().build_signer();
        let signer = match signer {
            Ok(s) => s,
            Err(err) if err.is_not_supported() => {
                tracing::warn!("skipping run_storage_signed_urls: {err:?}");
                return Ok(());
            }
            Err(err) => {
                anyhow::bail!("error creating signer: {err:?}")
            }
        };

        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;

        let builder = Storage::builder();
        let result = integration_tests::storage::signed_urls(
            builder,
            &signer,
            &bucket.name,
            "default-endpoint",
        )
        .await
        .map_err(integration_tests::report_error);

        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_read_object(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let result = integration_tests::storage::read_object::run(builder, &bucket.name).await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_write_object(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let result = integration_tests::storage::write_object::run(builder, &bucket.name).await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_object_names(
        builder: storage::builder::storage::ClientBuilder,
    ) -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
        let result =
            integration_tests::storage::object_names(builder, control.clone(), &bucket.name)
                .await
                .map_err(integration_tests::report_error);
        let _ = storage_samples::cleanup_bucket(control, bucket.name).await;
        result
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_storage_bidi() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests::storage::create_test_hns_bucket().await?;
        let result = integration_tests::storage::bidi_read::run(&bucket.name).await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_error_details_http() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::error_details_http()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_error_details_grpc() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::error_details_grpc()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_http() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::check_code_for_http()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_grpc() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::check_code_for_grpc()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_until_done() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::workflows::until_done()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_explicit() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::workflows::explicit_loop()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn workflows_manual() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::workflows::until_done()
            .await
            .map_err(integration_tests::report_error)
    }
}
