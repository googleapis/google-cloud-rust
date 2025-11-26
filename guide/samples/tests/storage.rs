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
    pub use storage_samples::random_bucket_id;

    #[cfg(all(test, feature = "run-integration-tests"))]
    mod driver {
        use super::*;

        #[ignore = "TODO(#3916) - disabled because it is flaky"]
        #[tokio::test(flavor = "multi_thread")]
        async fn quickstart() -> anyhow::Result<()> {
            let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
            let bucket_id = random_bucket_id();
            let result = super::quickstart::quickstart(&project_id, &bucket_id).await;
            if let Err(e) = super::cleanup_bucket(&bucket_id).await {
                eprintln!("error cleaning up quickstart bucket {bucket_id}: {e:?}");
            }
            result
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn run() -> anyhow::Result<()> {
            let (control, bucket) = integration_tests::storage::create_test_hns_bucket().await?;
            let result = super::run(&control, &bucket.name).await;
            if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
                eprintln!("error cleaning up run() bucket {}: {e:?}", bucket.name);
            }
            result
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
        println!("running lros() test");
        lros::test(client, bucket_name).await?;
        println!("running polling_policies() test");
        polling_policies::test(client, bucket_name).await?;
        Ok(())
    }

    pub async fn cleanup_bucket(bucket_id: &str) -> anyhow::Result<()> {
        let control = google_cloud_storage::client::StorageControl::builder()
            .build()
            .await?;
        storage_samples::cleanup_bucket(control, format!("projects/_/buckets/{bucket_id}")).await
    }
}
