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

mod add_bucket_owner;
mod change_default_storage_class;
mod control;
mod create_bucket;
mod create_bucket_class_location;
mod create_bucket_dual_region;
mod create_bucket_hierarchical_namespace;
mod delete_bucket;
mod disable_bucket_lifecycle_management;
mod disable_default_event_based_hold;
mod enable_bucket_lifecycle_management;
mod enable_default_event_based_hold;
mod get_bucket_metadata;
mod get_default_event_based_hold;
mod get_public_access_prevention;
mod list_buckets;
mod objects;
mod print_bucket_acl;
mod print_bucket_acl_for_user;
mod quickstart;
mod remove_bucket_owner;
mod set_lifecycle_abort_multipart_upload;
mod set_public_access_prevention_enforced;
mod set_public_access_prevention_inherited;
mod set_public_access_prevention_unspecified;
mod view_lifecycle_management_configuration;

use google_cloud_gax::throttle_result::ThrottleResult;
use google_cloud_gax::{
    exponential_backoff::ExponentialBackoffBuilder, retry_policy::RetryPolicyExt,
};
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use google_cloud_storage::retry_policy::RetryableErrors;
use rand::{Rng, distr::Distribution};
use std::time::Duration;

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

    // Avoid creating more than one bucket every 2 seconds:
    //     https://cloud.google.com/storage/quotas
    const BUCKET_CREATION_DELAY: Duration = Duration::from_secs(2);
    // Avoid mutating a bucket more than once per second:
    //     https://cloud.google.com/storage/quotas
    const BUCKET_MUTATION_DELAY: Duration = Duration::from_secs(1);

    // Use a longer than normal initial backoff, to better handle rate limit
    // errors.
    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_delay(std::cmp::max(BUCKET_CREATION_DELAY, BUCKET_MUTATION_DELAY))
        .with_maximum_delay(Duration::from_secs(60))
        .build()?;

    let client = StorageControl::builder()
        .with_backoff_policy(backoff)
        .with_retry_policy(
            // Retry all errors, the examples are tested with on newly created
            // buckets, using a static configuration. Most likely the errors are
            // network problems and can be safely retried. Or at least, we will
            // get fewer flakes from retrying failures vs. not.
            RetryableErrors
                .with_time_limit(Duration::from_secs(900))
                .always_idempotent(),
        )
        .build()
        .await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
    let service_account = std::env::var("GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT")?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket example");
    create_bucket::sample(&client, &project_id, &id).await?;
    tracing::info!("running list_buckets example");
    list_buckets::sample(&client, &project_id).await?;
    tracing::info!("running delete_bucket example");
    delete_bucket::sample(&client, &id).await?;

    // Create a new bucket for several tests.
    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket example [2]");
    create_bucket::sample(&client, &project_id, &id).await?;
    tracing::info!("running change_default_storage_class example");
    change_default_storage_class::sample(&client, &id).await?;
    tracing::info!("running get_bucket_metadata example");
    get_bucket_metadata::sample(&client, &id).await?;
    tracing::info!("running get_default_event_based_hold example");
    get_default_event_based_hold::sample(&client, &id).await?;
    tracing::info!("running enable_default_event_based_hold example");
    enable_default_event_based_hold::sample(&client, &id).await?;
    tracing::info!("running disable_default_event_based_hold example");
    disable_default_event_based_hold::sample(&client, &id).await?;

    tracing::info!("running set_public_access_prevention_unspecified example");
    set_public_access_prevention_unspecified::sample(&client, &id).await?;
    tracing::info!("running set_public_access_prevention_inherited example");
    set_public_access_prevention_inherited::sample(&client, &id).await?;
    tracing::info!("running get_public_access_prevention example");
    get_public_access_prevention::sample(&client, &id).await?;
    tracing::info!("running set_public_access_prevention_enforced example");
    set_public_access_prevention_enforced::sample(&client, &id).await?;
    tracing::info!("running get_public_access_prevention example");
    get_public_access_prevention::sample(&client, &id).await?;

    tracing::info!("running view_lifecycle_management_configuration example");
    view_lifecycle_management_configuration::sample(&client, &id).await?;
    tracing::info!("running enable_bucket_lifecycle_management example");
    enable_bucket_lifecycle_management::sample(&client, &id).await?;
    tracing::info!("running set_lifecycle_abort_multipart_upload example");
    set_lifecycle_abort_multipart_upload::sample(&client, &id).await?;
    tracing::info!("running disable_bucket_lifecycle_management example");
    disable_bucket_lifecycle_management::sample(&client, &id).await?;

    tracing::info!("running print_bucket_acl example");
    print_bucket_acl::sample(&client, &id).await?;
    tracing::info!("running add_bucket_owner example");
    add_bucket_owner::sample(&client, &id, &service_account).await?;
    tracing::info!("running remove_bucket_owner example");
    remove_bucket_owner::sample(&client, &id, &service_account).await?;
    tracing::info!("running print_bucket_acl_for_user example");
    print_bucket_acl_for_user::sample(&client, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running quickstart example");
    quickstart::sample(&client, &project_id, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_class_location example");
    create_bucket_class_location::sample(&client, &project_id, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_dual_region example");
    create_bucket_dual_region::sample(&client, &project_id, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_hierarchical_namespace example");
    create_bucket_hierarchical_namespace::sample(&client, &project_id, &id).await?;

    Ok(())
}

pub async fn run_managed_folder_examples(buckets: &mut Vec<String>) -> anyhow::Result<()> {
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
    create_bucket_hierarchical_namespace::sample(&client, &project_id, &id).await?;

    tracing::info!("running control::quickstart example");
    control::quickstart::sample(&client, &id).await?;

    tracing::info!("running control::managed_folder_create example");
    control::managed_folder_create::sample(&client, &id).await?;
    tracing::info!("running control::managed_folder_get example");
    control::managed_folder_get::sample(&client, &id).await?;
    tracing::info!("running control::managed_folder_list example");
    control::managed_folder_list::sample(&client, &id).await?;
    tracing::info!("running control::managed_folder_delete example");
    control::managed_folder_delete::sample(&client, &id).await?;

    tracing::info!("running control::create_folder example");
    control::create_folder::sample(&client, &id).await?;
    tracing::info!("running control::get_folder example");
    control::get_folder::sample(&client, &id).await?;
    tracing::info!("running control::rename_folder example");
    control::rename_folder::sample(&client, &id).await?;
    tracing::info!("running control::list_folders example");
    control::list_folders::sample(&client, &id).await?;

    // Create a folder for the delete_folder example.
    let _ = client
        .create_folder()
        .set_parent(format!("projects/_/buckets/{id}"))
        .set_folder_id("deleted-folder-id")
        .send()
        .await?;

    tracing::info!("running control::delete_folder example");
    control::delete_folder::sample(&client, &id).await?;

    Ok(())
}

pub async fn run_object_examples(buckets: &mut Vec<String>) -> anyhow::Result<()> {
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let control = StorageControl::builder().build().await?;
    let client = Storage::builder().build().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();

    let id = random_bucket_id();
    buckets.push(id.clone());
    create_bucket_hierarchical_namespace::sample(&control, &project_id, &id).await?;

    tracing::info!("create test objects for the examples");
    let writers = [
        "object-to-download.txt",
        "prefixes/are-not-always/folders-001",
        "prefixes/are-not-always/folders-002",
        "prefixes/are-not-always/folders-003",
        "prefixes/are-not-always/folders-004/abc",
        "prefixes/are-not-always/folders-004/def",
        "object-to-update",
        "deleted-object-name",
    ]
    .map(|name| make_object(&client, &id, name));
    let _ = futures::future::join_all(writers)
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;

    tracing::info!("running stream_file_upload example");
    objects::stream_file_upload::sample(&client, &id).await?;
    tracing::info!("running stream_file_download example");
    objects::stream_file_download::sample(&client, &id).await?;

    tracing::info!("running list_files example");
    objects::list_files::sample(&control, &id).await?;
    tracing::info!("running list_files_with_prefix example");
    objects::list_files_with_prefix::sample(&control, &id).await?;
    tracing::info!("running set_metadata example");
    objects::set_metadata::sample(&control, &id).await?;
    tracing::info!("running delete_file example");
    objects::delete_file::sample(&control, &id).await?;

    // Create a folder for the delete_folder example.
    let _ = control
        .create_folder()
        .set_parent(format!("projects/_/buckets/{id}"))
        .set_folder_id("deleted-folder-id")
        .send()
        .await?;

    tracing::info!("running control::delete_folder example");
    control::delete_folder::sample(&control, &id).await?;

    Ok(())
}

async fn make_object(client: &Storage, bucket_id: &str, name: &str) -> anyhow::Result<Object> {
    const VEXING: &str = "how vexingly quick daft zebras jump\n";
    let object = client
        .write_object(format!("projects/_/buckets/{bucket_id}"), name, VEXING)
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    Ok(object)
}

pub async fn cleanup_bucket(client: StorageControl, name: String) -> anyhow::Result<()> {
    use google_cloud_gax::paginator::ItemPaginator;

    let mut objects = client
        .list_objects()
        .set_parent(&name)
        .set_versions(true)
        .by_item();
    let mut pending = Vec::new();
    while let Some(item) = objects.next().await {
        let Ok(object) = item else {
            continue;
        };
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

    let mut pending = Vec::new();
    let mut folders = client.list_managed_folders().set_parent(&name).by_item();
    while let Some(item) = folders.next().await {
        let Ok(folder) = item else {
            continue;
        };
        pending.push(client.delete_managed_folder().set_name(folder.name).send());
    }
    let _ = futures::future::join_all(pending).await;

    let mut pending = Vec::new();
    let mut folders = client.list_folders().set_parent(&name).by_item();
    while let Some(item) = folders.next().await {
        let Ok(folder) = item else {
            continue;
        };
        pending.push(client.delete_folder().set_name(folder.name).send());
    }
    let _ = futures::future::join_all(pending).await;

    let mut pending = Vec::new();
    let mut caches = client.list_anywhere_caches().set_parent(&name).by_item();
    while let Some(item) = caches.next().await {
        let Ok(cache) = item else {
            continue;
        };
        pending.push(client.disable_anywhere_cache().set_name(cache.name).send());
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

trait RetryPolicyExt2: Sized {
    fn always_idempotent(self) -> AlwaysIdempotent<Self> {
        AlwaysIdempotent { inner: self }
    }
}

impl<T> RetryPolicyExt2 for T where T: RetryPolicy {}

#[derive(Clone, Debug)]
struct AlwaysIdempotent<T> {
    inner: T,
}

use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;

impl<T> RetryPolicy for AlwaysIdempotent<T>
where
    T: RetryPolicy,
{
    fn on_error(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        _idempotent: bool,
        error: GaxError,
    ) -> RetryResult {
        self.inner.on_error(loop_start, attempt_count, true, error)
    }
    fn on_throttle(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
        error: GaxError,
    ) -> ThrottleResult {
        self.inner.on_throttle(loop_start, attempt_count, error)
    }

    fn remaining_time(
        &self,
        loop_start: std::time::Instant,
        attempt_count: u32,
    ) -> Option<Duration> {
        self.inner.remaining_time(loop_start, attempt_count)
    }
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
