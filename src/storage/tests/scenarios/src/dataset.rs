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

use super::args::Args;
use super::names::random_object_name;
use google_cloud_auth::credentials::Credentials;
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model::Object,
};

use super::MIB;
const SIZE: usize = 16 * MIB;

/// Populates a bucket with a dataset for the benchmark.
///
/// This function finds objects that are large enough to run the experiments
/// described in `args`.
///
/// If `args.use_existing_dataset` is false this creates a new object to run the
/// experiment. The object is populated with random data.
pub async fn populate(args: &Args, credentials: Credentials) -> anyhow::Result<Vec<Object>> {
    if args.use_existing_dataset {
        let control = StorageControl::builder()
            .with_credentials(credentials.clone())
            .build()
            .await?;
        return existing(args, control).await;
    }
    let client = Storage::builder()
        .with_credentials(credentials.clone())
        .build()
        .await?;
    let data = bytes::Bytes::from_owner((32..128_u8).cycle().take(SIZE).collect::<Vec<_>>());
    let objects = futures::future::join_all(
        (0..args.task_count).map(|task| create(task, data.clone(), args, &client)),
    )
    .await
    .into_iter()
    .collect::<anyhow::Result<Vec<_>>>()?;
    tracing::info!("create(DONE)");
    Ok(objects)
}

/// Finds all the objects in `args.bucket_name` that are large enough to run
/// the experiment.
async fn existing(args: &Args, control: StorageControl) -> anyhow::Result<Vec<Object>> {
    use google_cloud_gax::paginator::ItemPaginator;
    let mut objects = Vec::new();
    let mut list = control
        .list_objects()
        .set_parent(args.full_bucket_name())
        .by_item();
    while let Some(item) = list.next().await.transpose()? {
        if (item.size as usize) < SIZE {
            continue;
        }
        objects.push(item);
    }
    Ok(objects)
}

async fn create(
    task: usize,
    data: bytes::Bytes,
    args: &Args,
    client: &Storage,
) -> anyhow::Result<Object> {
    tokio::time::sleep(args.dataset_rampup_period * (task as u32)).await;
    if task % 128 == 0 {
        tracing::info!("create({})", task);
    }
    let name = random_object_name();
    let object = client
        .write_object(args.full_bucket_name(), name, data)
        .send_unbuffered()
        .await?;
    Ok(object)
}
