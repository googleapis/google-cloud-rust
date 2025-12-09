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
use anyhow::Result;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::{Object, compose_object_request::SourceObject};
use rand::{Rng, distr::Uniform};

/// Populates a bucket with a dataset for the benchmark.
///
/// This function finds objects that are large enough to run the experiments
/// described in `args`.
///
/// If `args.use_existing_dataset` is false this creates a new object to run the
/// experiment. The object is populated with random data.
pub async fn populate(args: &Args, credentials: Credentials) -> Result<Vec<String>> {
    let control = StorageControl::builder()
        .with_credentials(credentials.clone())
        .build()
        .await?;

    if args.use_existing_dataset {
        existing(args, control).await
    } else {
        let client = Storage::builder()
            .with_credentials(credentials.clone())
            .build()
            .await?;
        create(args, client, control).await
    }
}

/// Finds all the objects in `args.bucket_name` that are large enough to run
/// the experiment described by the `args` size parameters.
async fn existing(args: &Args, control: StorageControl) -> Result<Vec<String>> {
    use google_cloud_gax::paginator::ItemPaginator;
    let target_size = args.min_range_count * args.range_size.max();
    let mut objects = Vec::new();
    let mut list = control
        .list_objects()
        .set_parent(format!("projects/_/buckets/{}", args.bucket_name))
        .by_item();
    while let Some(item) = list.next().await.transpose()? {
        if (item.size as u64) < target_size {
            continue;
        }
        objects.push(item.name);
    }
    Ok(objects)
}

/// Creates a new object in `args.bucket_name` large enough to run the
/// experiment described by the `args` size parameters.
async fn create(args: &Args, client: Storage, control: StorageControl) -> Result<Vec<String>> {
    const BLOCK_SIZE: usize = 1024 * 1024;
    tracing::info!("generating random data");
    let data = bytes::Bytes::from_owner(
        rand::rng()
            .sample_iter(Uniform::new_inclusive(u8::MIN, u8::MAX)?)
            .take(BLOCK_SIZE)
            .collect::<Vec<_>>(),
    );
    tracing::info!("random data ready");

    let bucket_name = format!("projects/_/buckets/{}", &args.bucket_name);
    let mut block = client
        .write_object(&bucket_name, random_object_name(), data)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    let target_size = args.min_range_count * args.range_size.max();
    while (block.size as u64) < target_size {
        let count = target_size.div_ceil(block.size as u64);
        let source = block;
        block = control
            .compose_object()
            .set_if_generation_match(0)
            .set_destination(
                Object::new()
                    .set_bucket(&bucket_name)
                    .set_name(random_object_name()),
            )
            .set_source_objects((0..count).take(32).map(|_| {
                SourceObject::new()
                    .set_name(&source.name)
                    .set_generation(source.generation)
            }))
            .with_idempotency(true)
            .send()
            .await?;
        control
            .delete_object()
            .set_bucket(&bucket_name)
            .set_object(source.name)
            .set_generation(source.generation)
            .with_idempotency(true)
            .send()
            .await?;
    }

    Ok(vec![block.name])
}
