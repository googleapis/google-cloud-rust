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
mod storage {
    use google_cloud_storage::client::{Storage, StorageControl};
    use google_cloud_test_utils::tracing::enable_tracing;
    use integration_tests_storage::StorageBuilder;
    use integration_tests_storage::StorageControlBuilder;
    use integration_tests_storage::retry_policy;
    use test_case::test_case;

    #[test_case(StorageControl::builder().with_tracing().with_retry_policy(retry_policy()); "with tracing and retry enabled")]
    #[test_case(StorageControl::builder().with_endpoint("https://www.googleapis.com"); "with global endpoint")]
    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_control_buckets(builder: StorageControlBuilder) -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_storage::buckets(builder).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_objects() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_bucket().await?;
        let variants = || async {
            tracing::info!("default builder");
            let builder = Storage::builder();
            integration_tests_storage::objects(builder, &bucket.name, "default-endpoint").await?;
            tracing::info!("with global endpoint");

            let builder = Storage::builder().with_endpoint("https://www.googleapis.com");
            integration_tests_storage::objects(builder, &bucket.name, "global endpoint").await?;

            if std::env::var("GOOGLE_CLOUD_RUST_TEST_RUNNING_ON_GCB").is_ok_and(|s| s == "1") {
                tracing::info!("with locational endpoint");
                let builder =
                    Storage::builder().with_endpoint("https://us-central1-storage.googleapis.com");
                integration_tests_storage::objects(builder, &bucket.name, "locational-endpoint")
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

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_signed_urls() -> anyhow::Result<()> {
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

        let (control, bucket) = integration_tests_storage::create_test_bucket().await?;

        let builder = Storage::builder();
        let result = integration_tests_storage::signed_urls(
            builder,
            &signer,
            &bucket.name,
            "default-endpoint",
        )
        .await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_read_object() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_bucket().await?;
        let result = integration_tests_storage::read_object::run(&bucket.name).await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_write_object() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_bucket().await?;
        let result = integration_tests_storage::write_object::run(&bucket.name).await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[test_case(Storage::builder(); "default")]
    #[tokio::test]
    async fn run_storage_object_names(builder: StorageBuilder) -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_bucket().await?;
        let result =
            integration_tests_storage::object_names(builder, control.clone(), &bucket.name).await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_bidi() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_hns_bucket().await?;
        let result = integration_tests_storage::bidi_read::run(&bucket.name).await;
        if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
            tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
        };
        result
    }
}
