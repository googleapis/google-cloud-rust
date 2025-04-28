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

use crate::Error;
use crate::Result;
use gax::paginator::{ItemPaginator, Paginator};
use rand::Rng;
use storage::model::Bucket;

pub const BUCKET_ID_LENGTH: usize = 63;

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

    let bucket_id = random_bucket_id();
    let bucket_name = format!("projects/_/buckets/{bucket_id}");

    println!("\nTesting create_bucket()");
    let create = client
        .create_bucket("projects/_", bucket_id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_labels([("integration-test", "true")]),
        )
        .send()
        .await?;
    println!("SUCCESS on create_bucket: {create:?}");
    assert_eq!(create.name, bucket_name);

    println!("\nTesting get_bucket()");
    let get = client.get_bucket(&bucket_name).send().await?;
    println!("SUCCESS on get_bucket: {get:?}");
    assert_eq!(get.name, bucket_name);

    println!("\nTesting list_buckets()");
    let mut paginator = client
        .list_buckets(format!("projects/{project_id}"))
        .paginator()
        .await
        .items();
    let mut bucket_names = Vec::new();
    while let Some(bucket) = paginator.next().await {
        bucket_names.push(bucket?.name);
    }
    println!("SUCCESS on list_buckets");
    assert!(
        bucket_names.iter().any(|name| name == &bucket_name),
        "missing bucket name {} in {bucket_names:?}",
        &bucket_name
    );

    println!("\nTesting delete_bucket()");
    client.delete_bucket(bucket_name).send().await?;
    println!("SUCCESS on delete_bucket");

    Ok(())
}

async fn cleanup_stale_buckets(client: &storage::client::Storage, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Error::other)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut items = client
        .list_buckets(format!("projects/{project_id}"))
        .paginator()
        .await
        .items();
    let mut pending = Vec::new();
    let mut names = Vec::new();
    while let Some(bucket) = items.next().await {
        let item = bucket?;
        if let Some("true") = item.labels.get("integration-test").map(String::as_str) {
            if let Some(true) = item.create_time.map(|v| v < stale_deadline) {
                let client = client.clone();
                let name = item.name.clone();
                pending.push(tokio::spawn(
                    async move { cleanup_bucket(client, name).await },
                ));
                names.push(item.name);
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

pub(crate) fn random_bucket_id() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    const PREFIX: &str = "rust-sdk-testing-";
    let mut bucket_id = String::new();
    for _ in 0..(BUCKET_ID_LENGTH - PREFIX.len()) {
        let idx = rand::rng().random_range(0..CHARSET.len());
        bucket_id.push(CHARSET[idx] as char);
    }
    format!("{PREFIX}{bucket_id}")
}
