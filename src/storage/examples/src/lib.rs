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

mod buckets;
mod control;
mod objects;
mod quickstart;

use google_cloud_gax::throttle_result::ThrottleResult;
use google_cloud_gax::{
    exponential_backoff::ExponentialBackoffBuilder, retry_policy::RetryPolicyExt,
    retry_state::RetryState,
};
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use google_cloud_storage::retry_policy::RetryableErrors;
use rand::{Rng, distr::Distribution};
use std::time::Duration;

pub const BUCKET_ID_LENGTH: usize = 63;

pub async fn run_anywhere_cache_examples(buckets: &mut Vec<String>) -> anyhow::Result<()> {
    use google_cloud_storage::model::{
        Bucket,
        bucket::iam_config::UniformBucketLevelAccess,
        bucket::{HierarchicalNamespace, IamConfig},
    };
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let client = control_client().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();

    let id = random_bucket_id();
    buckets.push(id.clone());
    let zone = "us-central1-f";
    tracing::info!("Create bucket for anywhere cache examples");
    let _bucket = client
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(&id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_location("us-central1")
                .set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true))
                .set_iam_config(IamConfig::new().set_uniform_bucket_level_access(
                    UniformBucketLevelAccess::new().set_enabled(true),
                )),
        )
        .send()
        .await?;

    tracing::info!("running control_create_anywhere_cache");
    control::create_anywhere_cache::sample(&client, &id, zone).await?;
    tracing::info!("running control_get_anywhere_cache");
    control::get_anywhere_cache::sample(&client, &id, zone).await?;
    tracing::info!("running control_list_anywhere_caches");
    control::list_anywhere_caches::sample(&client, &id).await?;
    tracing::info!("running control_pause_anywhere_caches");
    control::pause_anywhere_cache::sample(&client, &id, zone).await?;
    tracing::info!("running control_resume_anywhere_caches");
    control::resume_anywhere_cache::sample(&client, &id, zone).await?;
    tracing::info!("running control_update_anywhere_caches");
    control::update_anywhere_cache::sample(&client, &id, zone).await?;
    tracing::info!("running control_disable_anywhere_caches");
    control::disable_anywhere_cache::sample(&client, &id, zone).await?;

    Ok(())
}

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

    let client = control_client().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
    let service_account = std::env::var("GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT")?;

    // We create multiple buckets because there is a rate limit on bucket
    // changes.
    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket example");
    buckets::create_bucket::sample(&client, &project_id, &id).await?;
    tracing::info!("running list_buckets example");
    buckets::list_buckets::sample(&client, &project_id).await?;
    tracing::info!("running delete_bucket example");
    buckets::delete_bucket::sample(&client, &id).await?;

    // Create a new bucket for several tests.
    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket example [2]");
    buckets::create_bucket::sample(&client, &project_id, &id).await?;
    tracing::info!("running change_default_storage_class example");
    buckets::change_default_storage_class::sample(&client, &id).await?;
    tracing::info!("running get_bucket_metadata example");
    buckets::get_bucket_metadata::sample(&client, &id).await?;
    tracing::info!("running add_bucket_label example");
    buckets::add_bucket_label::sample(&client, &id, "test-label", "test-value").await?;
    tracing::info!("running remove_bucket_label example");
    buckets::remove_bucket_label::sample(&client, &id, "test-label").await?;
    tracing::info!("running get_default_event_based_hold example");
    buckets::get_default_event_based_hold::sample(&client, &id).await?;
    tracing::info!("running enable_default_event_based_hold example");
    buckets::enable_default_event_based_hold::sample(&client, &id).await?;
    tracing::info!("running disable_default_event_based_hold example");
    buckets::disable_default_event_based_hold::sample(&client, &id).await?;
    tracing::info!("running set_public_access_prevention_unspecified example");
    buckets::set_public_access_prevention_unspecified::sample(&client, &id).await?;
    tracing::info!("running set_public_access_prevention_inherited example");
    buckets::set_public_access_prevention_inherited::sample(&client, &id).await?;
    tracing::info!("running get_public_access_prevention example");
    buckets::get_public_access_prevention::sample(&client, &id).await?;
    tracing::info!("running set_public_access_prevention_enforced example");
    buckets::set_public_access_prevention_enforced::sample(&client, &id).await?;
    tracing::info!("running get_public_access_prevention example");
    buckets::get_public_access_prevention::sample(&client, &id).await?;
    tracing::info!("running enable_uniform_bucket_level_access example");
    buckets::enable_uniform_bucket_level_access::sample(&client, &id).await?;
    tracing::info!("running get_uniform_bucket_level_access example");
    buckets::get_uniform_bucket_level_access::sample(&client, &id).await?;
    tracing::info!("running disable_uniform_bucket_level_access example");
    buckets::disable_uniform_bucket_level_access::sample(&client, &id).await?;
    tracing::info!("running view_versioning_status example");
    buckets::view_versioning_status::sample(&client, &id).await?;
    tracing::info!("running enable_versioning example");
    buckets::enable_versioning::sample(&client, &id).await?;
    tracing::info!("running view_versioning_status example");
    buckets::view_versioning_status::sample(&client, &id).await?;
    tracing::info!("running disable_versioning example");
    buckets::disable_versioning::sample(&client, &id).await?;
    tracing::info!("running view_versioning_status example");
    buckets::view_versioning_status::sample(&client, &id).await?;
    tracing::info!("running view_lifecycle_management_configuration example");
    buckets::view_lifecycle_management_configuration::sample(&client, &id).await?;
    tracing::info!("running enable_bucket_lifecycle_management example");
    buckets::enable_bucket_lifecycle_management::sample(&client, &id).await?;
    tracing::info!("running set_lifecycle_abort_multipart_upload example");
    buckets::set_lifecycle_abort_multipart_upload::sample(&client, &id).await?;
    tracing::info!("running disable_bucket_lifecycle_management example");
    buckets::disable_bucket_lifecycle_management::sample(&client, &id).await?;
    tracing::info!("running print_bucket_website_configuration example");
    buckets::print_bucket_website_configuration::sample(&client, &id).await?;
    tracing::info!("running define_bucket_website_configuration example");
    buckets::define_bucket_website_configuration::sample(&client, &id, "index.html", "404.html")
        .await?;
    tracing::info!("running cors_configuiration example");
    buckets::cors_configuration::sample(&client, &id).await?;
    tracing::info!("running remove_cors_configuration example");
    buckets::remove_cors_configuration::sample(&client, &id).await?;
    tracing::info!("running remove_retention_policy example");
    buckets::remove_retention_policy::sample(&client, &id).await?;
    tracing::info!("running set_retention_policy example");
    buckets::set_retention_policy::sample(&client, &id, 60).await?;
    tracing::info!("running get_retention_policy example");
    buckets::get_retention_policy::sample(&client, &id).await?;
    tracing::info!("running lock_retention_policy example");
    buckets::lock_retention_policy::sample(&client, &id).await?;
    tracing::info!("running print_bucket_acl example");
    buckets::print_bucket_acl::sample(&client, &id).await?;
    tracing::info!("running add_bucket_owner example");
    buckets::add_bucket_owner::sample(&client, &id, &service_account).await?;
    tracing::info!("running remove_bucket_owner example");
    buckets::remove_bucket_owner::sample(&client, &id, &service_account).await?;
    tracing::info!("running print_bucket_acl_for_user example");
    buckets::print_bucket_acl_for_user::sample(&client, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket example [3]");
    buckets::create_bucket::sample(&client, &project_id, &id).await?;
    tracing::info!("running set_autoclass example");
    buckets::set_autoclass::sample(&client, &id).await?;
    tracing::info!("running get_autoclass example");
    buckets::get_autoclass::sample(&client, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running quickstart example");
    quickstart::sample(&client, &project_id, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_class_location example");
    buckets::create_bucket_class_location::sample(&client, &project_id, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_dual_region example");
    buckets::create_bucket_dual_region::sample(&client, &project_id, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_turbo_replication example");
    buckets::create_bucket_turbo_replication::sample(&client, &project_id, &id).await?;
    tracing::info!("running set_rpo_default example");
    buckets::set_rpo_default::sample(&client, &id).await?;
    tracing::info!("running set_rpo_async_turbo example");
    buckets::set_rpo_async_turbo::sample(&client, &id).await?;

    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_hierarchical_namespace example");
    buckets::create_bucket_hierarchical_namespace::sample(&client, &project_id, &id).await?;

    // Use a new bucket to avoid clashing policies from the previous examples.
    let id = random_bucket_id();
    buckets.push(id.clone());
    tracing::info!("running create_bucket_hierarchical_namespace example [2]");
    buckets::create_bucket_hierarchical_namespace::sample(&client, &project_id, &id).await?;
    tracing::info!("running add_bucket_iam_member example");
    buckets::add_bucket_iam_member::sample(
        &client,
        &id,
        "roles/storage.objectViewer",
        &format!("serviceAccount:{service_account}"),
    )
    .await?;
    tracing::info!("running remove_bucket_iam_member example");
    buckets::remove_bucket_iam_member::sample(
        &client,
        &id,
        "roles/storage.objectViewer",
        &format!("serviceAccount:{service_account}"),
    )
    .await?;
    #[cfg(feature = "skipped-integration-tests")]
    {
        // Skip, the internal Google policies prevent granting public access to
        // any buckets in our test projects.
        tracing::info!("running set_bucket_public_iam example");
        buckets::set_bucket_public_iam::sample(&client, &id).await?;
    }
    tracing::info!("running add_bucket_conditional_iam_binding example");
    buckets::add_bucket_conditional_iam_binding::sample(&client, &id, &service_account).await?;
    tracing::info!("running remove_bucket_conditional_iam_binding example");
    buckets::remove_bucket_conditional_iam_binding::sample(&client, &id).await?;
    tracing::info!("running view_bucket_iam_members example");
    buckets::view_bucket_iam_members::sample(&client, &id).await?;

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

    let client = control_client().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();

    let id = random_bucket_id();
    buckets.push(id.clone());
    buckets::create_bucket_hierarchical_namespace::sample(&client, &project_id, &id).await?;

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

    let control = control_client().await?;
    let client = Storage::builder().build().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();

    let id = random_bucket_id();
    buckets.push(id.clone());
    buckets::create_bucket_hierarchical_namespace::sample(&control, &project_id, &id).await?;

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

    tracing::info!("create temp file for upload");
    let file_to_upload = tempfile::NamedTempFile::new()?;
    let file_to_upload_path = file_to_upload.path().to_str().unwrap();
    tokio::fs::write(file_to_upload_path, "hello world from file").await?;
    tracing::info!("running upload_file example");
    objects::upload_file::sample(&client, &id, "uploaded-file.txt", file_to_upload_path).await?;
    tracing::info!("running download_file example");
    let downloaded_file = tempfile::NamedTempFile::new()?;
    let downloaded_file_path = downloaded_file.path().to_str().unwrap();
    objects::download_file::sample(&client, &id, "uploaded-file.txt", downloaded_file_path).await?;
    tracing::info!("checking downloaded file content");
    let content = tokio::fs::read_to_string(downloaded_file_path).await?;
    assert_eq!(content, "hello world from file");

    tracing::info!("running download_byte_range example");
    objects::download_byte_range::sample(&client, &id, "object-to-download.txt", 4, 10).await?;

    tracing::info!("running generate_encryption_key example");
    let csek_key = objects::generate_encryption_key::sample()?;
    tracing::info!("running upload_encrypted_file example");
    objects::upload_encrypted_file::sample(&client, &id, "csek_file.txt", csek_key.clone()).await?;
    tracing::info!("running download_encrypted_file example");
    objects::download_encrypted_file::sample(&client, &id, "csek_file.txt", csek_key).await?;

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

async fn control_client() -> anyhow::Result<StorageControl> {
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

    let control = StorageControl::builder()
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
    Ok(control)
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
    fn on_error(&self, state: &RetryState, error: GaxError) -> RetryResult {
        self.inner
            .on_error(&state.clone().set_idempotent(true), error)
    }
    fn on_throttle(&self, state: &RetryState, error: GaxError) -> ThrottleResult {
        self.inner.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        self.inner.remaining_time(state)
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
