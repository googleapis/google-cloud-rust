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
    use google_cloud_storage::client::Storage;
    use google_cloud_test_utils::errors::anydump;
    use google_cloud_test_utils::tracing::enable_tracing;

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_control_buckets() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_storage::buckets()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_objects() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_bucket()
            .await
            .inspect_err(anydump)?;

        let run_all = || async {
            tracing::info!("default builder");
            let builder = Storage::builder();
            integration_tests_storage::objects(builder, &bucket.name, "default-endpoint")
                .await
                .inspect_err(anydump)?;

            tracing::info!("with global endpoint");
            let builder = Storage::builder().with_endpoint("https://www.googleapis.com");
            integration_tests_storage::objects(builder, &bucket.name, "global-endpoint")
                .await
                .inspect_err(anydump)?;

            if std::env::var("GOOGLE_CLOUD_RUST_TEST_RUNNING_ON_GCB").is_ok_and(|s| s == "1") {
                tracing::info!("with locational endpoint");
                let builder =
                    Storage::builder().with_endpoint("https://us-central1-storage.googleapis.com");
                integration_tests_storage::objects(builder, &bucket.name, "locational-endpoint")
                    .await
                    .inspect_err(anydump)?;
            }

            let signer = google_cloud_auth::credentials::Builder::default().build_signer();
            match signer {
                Ok(s) => {
                    let builder = Storage::builder();
                    integration_tests_storage::signed_urls(
                        builder,
                        &s,
                        &bucket.name,
                        "default-endpoint",
                    )
                    .await
                    .inspect_err(anydump)?;

                    let builder = Storage::builder();
                    integration_tests_storage::signed_post_policies_v4(
                        builder,
                        control.clone(),
                        &s,
                        &bucket.name,
                        "default-endpoint",
                    )
                    .await
                    .inspect_err(anydump)?;
                }
                Err(err) if err.is_not_supported() => {
                    tracing::warn!("skipping run_storage_signed_urls: {err:?}");
                }
                Err(err) => {
                    anyhow::bail!("error creating signer: {err:?}");
                }
            }

            integration_tests_storage::read_object::run(&bucket.name)
                .await
                .inspect_err(anydump)?;

            integration_tests_storage::write_object::run(&bucket.name)
                .await
                .inspect_err(anydump)?;

            let builder = Storage::builder();
            integration_tests_storage::object_names(builder, control.clone(), &bucket.name)
                .await
                .inspect_err(anydump)?;

            Ok(())
        };

        let result = run_all().await.inspect_err(anydump);
        let _ =
            storage_samples::cleanup_bucket(control, bucket.name.clone(), bucket.project.clone())
                .await
                .inspect_err(|e| tracing::error!("error cleaning up bucket {}: {e:?}", bucket.name))
                .inspect_err(anydump);
        result
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_storage_bidi() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_bucket()
            .await
            .inspect_err(anydump)?;
        let result = integration_tests_storage::bidi_read::run(&bucket.name)
            .await
            .inspect_err(anydump);
        let _ =
            storage_samples::cleanup_bucket(control, bucket.name.clone(), bucket.project.clone())
                .await
                .inspect_err(|e| tracing::error!("error cleaning up bucket {}: {e:?}", bucket.name))
                .inspect_err(anydump);
        result
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(google_cloud_unstable_storage_bidi)]
    async fn run_storage_bidi_write() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (control, bucket) = integration_tests_storage::create_test_rapid_bucket()
            .await
            .inspect_err(anydump)?;
        let result = integration_tests_storage::bidi_write::run(&bucket.name)
            .await
            .inspect_err(anydump);
        let _ =
            storage_samples::cleanup_bucket(control, bucket.name.clone(), bucket.project.clone())
                .await
                .inspect_err(|e| tracing::error!("error cleaning up bucket {}: {e:?}", bucket.name))
                .inspect_err(anydump);
        result
    }
}
