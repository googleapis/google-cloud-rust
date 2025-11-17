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

pub mod read_object;
pub mod write_object;

use crate::Result;
use gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use gax::options::RequestOptionsBuilder;
use gax::paginator::ItemPaginator as _;
use lro::Poller;
use std::time::Duration;
use storage::client::StorageControl;
use storage::model::Bucket;
use storage::model::bucket::iam_config::UniformBucketLevelAccess;
use storage::model::bucket::{HierarchicalNamespace, IamConfig};
use storage::read_object::ReadObjectResponse;
pub use storage_samples::{cleanup_stale_buckets, create_test_bucket, create_test_hns_bucket};

pub async fn objects(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
    prefix: &str,
) -> Result<()> {
    let client = builder.build().await?;
    tracing::info!("testing insert_object()");
    const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
    let insert = client
        .write_object(bucket_name, format!("{prefix}/quick.text"), CONTENTS)
        .set_metadata([("verify-metadata-works", "yes")])
        .set_content_type("text/plain")
        .set_content_language("en")
        .set_storage_class("STANDARD")
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(
        insert
            .metadata
            .get("verify-metadata-works")
            .map(String::as_str),
        Some("yes")
    );

    tracing::info!("testing read_object()");
    let mut response = client.read_object(bucket_name, &insert.name).send().await?;

    // Retrieve the metadata before reading the data.
    let object = response.object();
    assert!(object.generation > 0);
    assert!(object.metageneration > 0);
    assert_eq!(object.size, CONTENTS.len() as i64);
    assert_eq!(object.content_encoding, "identity");
    assert_eq!(
        object.checksums.unwrap().crc32c,
        Some(crc32c::crc32c(CONTENTS.as_bytes()))
    );
    assert_eq!(object.storage_class, "STANDARD");
    assert_eq!(object.content_language, "en");
    assert_eq!(object.content_type, "text/plain");
    assert!(!object.content_disposition.is_empty());
    assert!(!object.etag.is_empty());

    let mut contents = Vec::new();
    while let Some(b) = response.next().await.transpose()? {
        contents.extend_from_slice(&b);
    }
    let contents = bytes::Bytes::from_owner(contents);
    assert_eq!(contents, CONTENTS.as_bytes());
    tracing::info!("success with contents={contents:?}");

    Ok(())
}

#[cfg(google_cloud_unstable_signed_url)]
pub async fn signed_urls(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
    prefix: &str,
) -> anyhow::Result<()> {
    //let creds = auth::credentials::mds::Builder::default().build()?;
    let client = builder.build().await?;

    // let signer = auth::credentials::mds::Builder::default().build_signer()?;
    let signer = auth::credentials::Builder::default().build_signer()?;

    const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
    let insert = client
        .write_object(bucket_name, format!("{prefix}/quick.text"), CONTENTS)
        .set_content_type("text/plain")
        .set_content_language("en")
        .set_storage_class("STANDARD")
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");

    tracing::info!("testing signed_url()");
    let signed_url = client
        .signed_url(signer, bucket_name, &insert.name)
        .send()
        .await?;

    tracing::info!("signed_url={signed_url}");

    // Download the contents of the object using the signed URL.
    let client = reqwest::Client::new();
    let res = client.get(signed_url).send().await?;
    let out = res.text().await?;
    assert_eq!(out, CONTENTS);
    tracing::info!("signed url works and can read contents={out:?}");

    Ok(())
}

async fn read_all(mut response: ReadObjectResponse) -> Result<Vec<u8>> {
    let mut contents = Vec::new();
    while let Some(b) = response.next().await.transpose()? {
        contents.extend_from_slice(&b);
    }
    Ok(contents)
}

pub async fn object_names(
    builder: storage::builder::storage::ClientBuilder,
    control: storage::client::StorageControl,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("object names test, using bucket {bucket_name}");

    let client = builder.build().await?;

    let names = ["a/1", "a/2", "b/c/d/e", "b/c/d/e#1", "b/c/d/f", "b/c/d/g"];

    for name in names {
        let upload = client
            .write_object(bucket_name, name, "")
            .set_if_generation_match(0)
            .send_unbuffered()
            .await?;
        assert_eq!(upload.bucket, bucket_name);
        assert_eq!(upload.name, name);
        assert_eq!(upload.size, 0_i64);

        let reader = client
            .read_object(&upload.bucket, &upload.name)
            .set_generation(upload.generation)
            .send()
            .await?;
        let highlights = reader.object();
        assert_eq!(highlights.storage_class, "STANDARD");
        assert_eq!(highlights.generation, upload.generation);
        assert_eq!(highlights.metageneration, upload.metageneration);
        assert_eq!(highlights.etag, upload.etag);
        assert_eq!(highlights.size, upload.size);

        let get = control
            .get_object()
            .set_bucket(&upload.bucket)
            .set_object(&upload.name)
            .set_generation(upload.generation)
            .send()
            .await?;
        // Not all fields match (the service returns different values with
        // equivalent semantics).
        assert_eq!(get.bucket, upload.bucket);
        assert_eq!(get.name, upload.name);
        assert_eq!(get.etag, upload.etag);
        assert_eq!(get.generation, upload.generation);
        assert_eq!(get.metageneration, upload.metageneration);
        assert_eq!(get.storage_class, upload.storage_class);
        assert_eq!(get.size, upload.size);
        assert_eq!(get.create_time, upload.create_time);
        assert_eq!(get.checksums, upload.checksums);
    }

    use gax::paginator::ItemPaginator;
    let mut list = control
        .list_objects()
        .set_parent(bucket_name)
        .set_prefix("b/c")
        .by_item();
    let mut from_list = Vec::new();
    while let Some(o) = list.next().await.transpose()? {
        from_list.push(o.name);
    }
    use std::collections::BTreeSet;
    let got = from_list
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let want = names
        .iter()
        .filter_map(|n| n.strip_prefix("b/c").map(|_| *n))
        .collect::<BTreeSet<_>>();

    assert_eq!(got, want);

    for name in names {
        control
            .delete_object()
            .set_bucket(bucket_name)
            .set_object(name)
            .with_idempotency(true)
            .send()
            .await?;
    }

    Ok(())
}

pub async fn buckets(builder: storage::builder::storage_control::ClientBuilder) -> Result<()> {
    let project_id = crate::project_id()?;
    let client = builder.build().await?;

    cleanup_stale_buckets(&client, &project_id).await?;

    let bucket_id = crate::random_bucket_id();
    let bucket_name = format!("projects/_/buckets/{bucket_id}");

    println!("\nTesting create_bucket()");
    let create = client
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_labels([("integration-test", "true")])
                // We need to set these properties on the bucket to use it with
                // the Folders API.
                .set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true))
                .set_iam_config(IamConfig::new().set_uniform_bucket_level_access(
                    UniformBucketLevelAccess::new().set_enabled(true),
                )),
        )
        .with_backoff_policy(test_backoff())
        .with_idempotency(true)
        .send()
        .await?;
    println!("SUCCESS on create_bucket: {create:?}");
    assert_eq!(create.name, bucket_name);

    println!("\nTesting get_bucket()");
    let get = client.get_bucket().set_name(&bucket_name).send().await?;
    println!("SUCCESS on get_bucket: {get:?}");
    assert_eq!(get.name, bucket_name);

    println!("\nTesting list_buckets()");
    let mut buckets = client
        .list_buckets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    let mut bucket_names = Vec::new();
    while let Some(bucket) = buckets.next().await {
        bucket_names.push(bucket?.name);
    }
    println!("SUCCESS on list_buckets");
    assert!(
        bucket_names.iter().any(|name| name == &bucket_name),
        "missing bucket name {bucket_name} in {bucket_names:?}"
    );

    buckets_iam(&client, &bucket_name).await?;
    folders(&client, &bucket_name).await?;

    println!("\nTesting delete_bucket()");
    client
        .delete_bucket()
        .set_name(bucket_name)
        .with_idempotency(true)
        .send()
        .await?;
    println!("SUCCESS on delete_bucket");

    Ok(())
}

async fn buckets_iam(client: &StorageControl, bucket_name: &str) -> Result<()> {
    let service_account = crate::test_service_account()?;

    println!("\nTesting get_iam_policy()");
    let policy = client
        .get_iam_policy()
        .set_resource(bucket_name)
        .send()
        .await?;
    println!("SUCCESS on get_iam_policy = {policy:?}");

    println!("\nTesting test_iam_permissions()");
    let response = client
        .test_iam_permissions()
        .set_resource(bucket_name)
        .set_permissions(["storage.buckets.get"])
        .send()
        .await?;
    println!("SUCCESS on test_iam_permissions = {response:?}");

    println!("\nTesting set_iam_policy()");
    let mut new_policy = policy.clone();
    new_policy.bindings.push(
        iam_v1::model::Binding::new()
            .set_role("roles/storage.legacyBucketReader")
            .set_members([format!("serviceAccount:{service_account}")]),
    );
    let policy = client
        .set_iam_policy()
        .set_resource(bucket_name)
        .set_update_mask(wkt::FieldMask::default().set_paths(["bindings"]))
        .set_policy(new_policy)
        .with_idempotency(true)
        .send()
        .await?;
    println!("SUCCESS on set_iam_policy = {policy:?}");

    Ok(())
}

async fn folders(client: &StorageControl, bucket_name: &str) -> Result<()> {
    let folder_name = format!("{bucket_name}/folders/test-folder/");
    let folder_rename = format!("{bucket_name}/folders/renamed-test-folder/");

    println!("\nTesting create_folder()");
    let create = client
        .create_folder()
        .set_parent(bucket_name)
        .set_folder_id("test-folder/")
        .send()
        .await?;
    println!("SUCCESS on create_folder: {create:?}");
    assert_eq!(create.name, folder_name);

    println!("\nTesting get_folder()");
    let get = client.get_folder().set_name(&folder_name).send().await?;
    println!("SUCCESS on get_folder: {get:?}");
    assert_eq!(get.name, folder_name);

    println!("\nTesting list_folders()");
    let mut folders = client.list_folders().set_parent(bucket_name).by_item();
    let mut folder_names = Vec::new();
    while let Some(folder) = folders.next().await {
        folder_names.push(folder?.name);
    }
    println!("SUCCESS on list_folders");
    assert!(
        folder_names.iter().any(|name| name == &folder_name),
        "missing folder name {folder_name} in {folder_names:?}"
    );

    println!("\nTesting rename_folder()");
    let rename = client
        .rename_folder()
        .set_name(folder_name)
        .set_destination_folder_id("renamed-test-folder/")
        .poller()
        .until_done()
        .await?;
    println!("SUCCESS on rename_folder: {rename:?}");
    assert_eq!(rename.name, folder_rename);

    println!("\nTesting delete_folder()");
    client
        .delete_folder()
        .set_name(folder_rename)
        .send()
        .await?;
    println!("SUCCESS on delete_folder");

    Ok(())
}

fn test_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_secs(2))
        .with_maximum_delay(Duration::from_secs(10))
        .build()
        .unwrap()
}
