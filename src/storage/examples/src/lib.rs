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

pub mod change_default_storage_class;
pub mod create_bucket;
pub mod create_bucket_class_location;
pub mod create_bucket_dual_region;
pub mod create_bucket_hierarchical_namespace;
pub mod delete_bucket;
pub mod get_bucket_metadata;
pub mod list_buckets;
use google_cloud_storage::client::StorageControl;
use rand::{Rng, distr::Distribution};

pub const BUCKET_ID_LENGTH: usize = 63;

pub async fn run_bucket_examples(buckets: &mut Vec<String>) -> anyhow::Result<()> {
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let client = StorageControl::builder().build().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket example");
    create_bucket::create_bucket(&client, &project_id, &id).await?;
    tracing::info!("running change_default_storage_class example");
    change_default_storage_class::change_default_storage_class(&client, &id).await?;
    tracing::info!("running get_bucket_metadata example");
    get_bucket_metadata::get_bucket_metadata(&client, &id).await?;
    tracing::info!("running delete_bucket example");
    delete_bucket::delete_bucket(&client, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_class_and_location example");
    create_bucket_class_location::create_bucket_class_and_location(&client, &project_id, &id)
        .await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_dual_region example");
    create_bucket_dual_region::create_bucket_dual_region(&client, &project_id, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_hierarchical_namespace example");
    create_bucket_hierarchical_namespace::create_bucket_hierarchical_namespace(
        &client,
        &project_id,
        &id,
    )
    .await?;

    tracing::info!("running list_buckets example");
    list_buckets::list_buckets(&client, &project_id).await?;
    Ok(())
}

pub async fn cleanup_bucket(client: StorageControl, name: String) -> anyhow::Result<()> {
    use google_cloud_gax::paginator::ItemPaginator;

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

pub struct RandomChars {
    chars: &'static [u8],
}

impl RandomChars {
    pub fn new(chars: &'static [u8]) -> Self {
        Self { chars }
    }
}

impl Distribution<u8> for RandomChars {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u8 {
        let index = rng.random_range(0..self.chars.len());
        self.chars[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_chars() {
        let chars = RandomChars::new("abcde".as_bytes());
        let got: String = rand::rng()
            .sample_iter(chars)
            .take(64)
            .map(char::from)
            .collect();
        assert!(
            !got.contains(|c| !("abcde".contains(c))),
            "{got:?} contains unexpected character"
        );
    }
}
