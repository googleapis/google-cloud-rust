// Copyright 2025 Google LLC
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

pub mod storage {
    pub mod lros;
    pub mod mocking;
    pub mod polling_policies;
    pub mod queue;
    pub mod quickstart;
    pub mod rewrite_object;
    pub mod striped;
    pub mod terminate_uploads;

    use google_cloud_storage::client::StorageControl;
    pub use google_cloud_test_utils::resource_names::random_bucket_id;
    use storage_samples::custom_project_billing;

    #[cfg(all(test, feature = "run-integration-tests"))]
    mod driver {
        use super::*;
        use google_cloud_test_utils::errors::anydump;
        use google_cloud_test_utils::runtime_config::project_id;
        use storage_samples::create_test_hns_bucket;

        #[ignore = "TODO(#3916) - disabled because it is flaky"]
        #[tokio::test(flavor = "multi_thread")]
        async fn quickstart() -> anyhow::Result<()> {
            let project_id = project_id()?;
            let bucket_id = random_bucket_id();
            let result = super::quickstart::sample(&project_id, &bucket_id).await;
            let _ = super::cleanup_bucket(&format!("projects/_/buckets/{bucket_id}"))
                .await
                .inspect_err(|e| eprintln!("error cleaning up bucket {bucket_id}: {e:?}"))
                .inspect_err(anydump);
            result.inspect_err(anydump)
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn run() -> anyhow::Result<()> {
            let (control, bucket) = create_test_hns_bucket().await.inspect_err(anydump)?;
            let result = super::run(&control, &bucket.name).await;
            let _ = super::cleanup_bucket(&bucket.name)
                .await
                .inspect_err(|e| eprintln!("error cleaning up bucket {}: {e:?}", bucket.name))
                .inspect_err(anydump);
            result.inspect_err(anydump)
        }
    }

    pub async fn run(client: &StorageControl, bucket_name: &str) -> anyhow::Result<()> {
        queue::queue(bucket_name, "test-only").await?;
        println!("running rewrite_object() test");
        rewrite_object::rewrite_object(bucket_name).await?;
        println!("running rewrite_object_until_done() test");
        rewrite_object::rewrite_object_until_done(bucket_name).await?;
        {
            println!("running striped::test() test");
            let destination = tempfile::NamedTempFile::new()?;
            let path = destination
                .path()
                .to_str()
                .ok_or(anyhow::Error::msg("cannot open temporary file"))?;
            striped::test(bucket_name, path).await?;
        }
        println!("running terminate_uploads() test");
        terminate_uploads::attempt_upload(bucket_name).await?;
        if !custom_project_billing("the LRO operation used for testing").await? {
            println!("running lros() test");
            lros::test(client, bucket_name).await?;
        }
        println!("running polling_policies() test");
        polling_policies::test(client, bucket_name).await?;
        Ok(())
    }

    pub async fn cleanup_bucket(bucket_name: &str) -> anyhow::Result<()> {
        let control = google_cloud_storage::client::StorageControl::builder()
            .build()
            .await?;
        storage_samples::cleanup_bucket(control, bucket_name.to_string()).await
    }
}
