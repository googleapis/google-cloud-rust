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
    use rand::{Rng, distr::Distribution};

    const BUCKET_ID_LENGTH: usize = 63;

    #[cfg(all(test, feature = "run-integration-tests"))]
    mod driver {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn quickstart() -> anyhow::Result<()> {
            let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
            let bucket_id = random_bucket_id();
            let response = super::quickstart::quickstart(&project_id, &bucket_id).await;
            // Ignore cleanup errors.
            let _ = super::cleanup_bucket(&bucket_id).await;
            response
        }
    }

    pub mod quickstart;
    pub async fn cleanup_bucket(bucket_id: &str) -> anyhow::Result<()> {
        use google_cloud_gax::paginator::ItemPaginator;
        let name = format!("projects/_/buckets/{bucket_id}");
        let client = google_cloud_storage::client::StorageControl::builder()
            .build()
            .await?;
        let mut objects = client
            .list_objects()
            .set_parent(&name)
            .set_versions(true)
            .by_item();
        let mut pending = Vec::new();
        while let Some(object) = objects.next().await {
            let object = object?;
            pending.push(
                client
                    .delete_object()
                    .set_bucket(object.bucket)
                    .set_object(object.name)
                    .set_generation(object.generation)
                    .send(),
            );
        }
        let _ = futures::future::join_all(pending).await;
        client.delete_bucket().set_name(&name).send().await?;
        Ok(())
    }

    pub fn random_bucket_id() -> String {
        const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

        let distr = RandomChars { chars: CHARSET };
        const PREFIX: &str = "rust-sdk-testing-";
        let bucket_id: String = rand::rng()
            .sample_iter(distr)
            .take(BUCKET_ID_LENGTH - PREFIX.len())
            .map(char::from)
            .collect();
        format!("{PREFIX}{bucket_id}")
    }

    pub(crate) struct RandomChars {
        chars: &'static [u8],
    }

    impl Distribution<u8> for RandomChars {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u8 {
            let index = rng.random_range(0..self.chars.len());
            self.chars[index]
        }
    }
}
