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

use crate::{Error, Result};
use gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use gax::options::RequestOptionsBuilder;
use gax::paginator::{ItemPaginator, Paginator};
use std::time::Duration;
use storage::model::Bucket;

pub async fn buckets(builder: storage::client::ClientBuilder) -> Result<()> {
    // Enable a basic subscriber. Useful to troubleshoot problems and visually
    // verify tracing is doing something.
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let project_id = crate::project_id()?;
    let client = builder.build().await?;

    cleanup_stale_buckets(&client, &project_id).await?;

    let bucket_id = crate::random_bucket_id();
    let bucket_name = format!("projects/_/buckets/{bucket_id}");

    println!("\nTesting create_bucket()");
    let create = client
        .create_bucket("projects/_", bucket_id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_labels([("integration-test", "true")]),
        )
        .with_backoff_policy(test_backoff())
        .send()
        .await?;
    println!("SUCCESS on create_bucket: {create:?}");
    assert_eq!(create.name, bucket_name);

    println!("\nTesting get_bucket()");
    let get = client.get_bucket(&bucket_name).send().await?;
    println!("SUCCESS on get_bucket: {get:?}");
    assert_eq!(get.name, bucket_name);

    println!("\nTesting list_buckets()");
    let mut buckets = client
        .list_buckets(format!("projects/{project_id}"))
        .paginator()
        .await
        .items();
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

    println!("\nTesting delete_bucket()");
    client.delete_bucket(bucket_name).send().await?;
    println!("SUCCESS on delete_bucket");

    Ok(())
}

async fn buckets_iam(client: &storage::client::Storage, bucket_name: &str) -> Result<()> {
    let service_account = crate::service_account_for_iam_tests()?;

    println!("\nTesting get_iam_policy()");
    let policy = client.get_iam_policy(bucket_name).send().await?;
    println!("SUCCESS on get_iam_policy = {policy:?}");

    println!("\nTesting test_iam_permissions()");
    let response = client
        .test_iam_permissions(bucket_name)
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
        .set_iam_policy(bucket_name)
        .set_update_mask(wkt::FieldMask::default().set_paths(["bindings"]))
        .set_policy(new_policy)
        .send()
        .await?;
    println!("SUCCESS on set_iam_policy = {policy:?}");

    Ok(())
}

async fn cleanup_stale_buckets(client: &storage::client::Storage, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Error::other)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut buckets = client
        .list_buckets(format!("projects/{project_id}"))
        .paginator()
        .await
        .items();
    let mut pending = Vec::new();
    let mut names = Vec::new();
    while let Some(bucket) = buckets.next().await {
        let bucket = bucket?;
        if let Some("true") = bucket.labels.get("integration-test").map(String::as_str) {
            if let Some(true) = bucket.create_time.map(|v| v < stale_deadline) {
                let client = client.clone();
                let name = bucket.name.clone();
                pending.push(tokio::spawn(
                    async move { cleanup_bucket(client, name).await },
                ));
                names.push(bucket.name);
            }
        }
    }

    let r: std::result::Result<Vec<_>, _> = futures::future::join_all(pending)
        .await
        .into_iter()
        .collect();
    r.map_err(Error::other)?
        .into_iter()
        .zip(names)
        .for_each(|(r, name)| println!("deleting bucket {name} resulted in {r:?}"));

    Ok(())
}

async fn cleanup_bucket(client: storage::client::Storage, name: String) -> Result<()> {
    let mut objects = client
        .list_objects(&name)
        .set_versions(true)
        .paginator()
        .await
        .items();
    let mut pending = Vec::new();
    while let Some(object) = objects.next().await {
        let object = object?;
        pending.push(
            client
                .delete_object(object.bucket, object.name)
                .set_generation(object.generation)
                .send(),
        );
    }
    let _ = futures::future::join_all(pending).await;
    client.delete_bucket(&name).send().await
}

fn test_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_secs(2))
        .with_maximum_delay(Duration::from_secs(10))
        .build()
        .unwrap()
}
