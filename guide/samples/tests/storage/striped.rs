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

// [START rust_storage_striped_all] ANCHOR: all
use std::sync::Arc;

use google_cloud_gax::options::RequestOptionsBuilder;
// [START rust_storage_striped_seed_function] ANCHOR: seed-function
use google_cloud_storage::client::Storage;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::Object;

async fn seed(client: Storage, control: StorageControl, bucket_name: &str) -> anyhow::Result<()> {
    // [END rust_storage_striped_seed_function] ANCHOR_END: seed-function
    // [START rust_storage_striped_seed_use] ANCHOR: seed-use
    use google_cloud_storage::model::compose_object_request::SourceObject;
    // [END rust_storage_striped_seed_use] ANCHOR_END: seed-use

    // [START rust_storage_striped_create_1mib] ANCHOR: create-1MiB
    let buffer = String::from_iter(('a'..='z').cycle().take(1024 * 1024));
    let seed = client
        .write_object(bucket_name, "1MiB.txt", bytes::Bytes::from_owner(buffer))
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    println!(
        "Uploaded object {}, size={}KiB",
        seed.name,
        seed.size / 1024
    );
    // [END rust_storage_striped_create_1mib] ANCHOR_END: create-1MiB

    // [START rust_storage_striped_compose_32] ANCHOR: compose-32
    let seed_32 = control
        .compose_object()
        .set_destination(Object::new().set_bucket(bucket_name).set_name("32MiB.txt"))
        .set_source_objects((0..32).map(|_| {
            SourceObject::new()
                .set_name(&seed.name)
                .set_generation(seed.generation)
        }))
        .set_if_generation_match(0)
        .with_idempotency(true)
        .send()
        .await?;
    println!(
        "Created object {}, size={}MiB",
        seed.name,
        seed.size / (1024 * 1024)
    );
    // [END rust_storage_striped_compose_32] ANCHOR_END: compose-32

    // [START rust_storage_striped_compose_1024] ANCHOR: compose-1024
    let seed_1024 = control
        .compose_object()
        .set_destination(Object::new().set_bucket(bucket_name).set_name("1GiB.txt"))
        .set_source_objects((0..32).map(|_| {
            SourceObject::new()
                .set_name(&seed_32.name)
                .set_generation(seed_32.generation)
        }))
        .set_if_generation_match(0)
        .with_idempotency(true)
        .send()
        .await?;
    println!(
        "Created object {}, size={}MiB",
        seed.name,
        seed.size / (1024 * 1024)
    );
    // [END rust_storage_striped_compose_1024] ANCHOR_END: compose-1024

    // [START rust_storage_striped_compose_gib] ANCHOR: compose-GiB
    for s in [2, 4, 8, 16, 32] {
        let name = format!("{s}GiB.txt");
        let target = control
            .compose_object()
            .set_destination(Object::new().set_bucket(bucket_name).set_name(&name))
            .set_source_objects((0..s).map(|_| {
                SourceObject::new()
                    .set_name(&seed_1024.name)
                    .set_generation(seed_1024.generation)
            }))
            .set_if_generation_match(0)
            .with_idempotency(true)
            .send()
            .await?;
        println!(
            "Created object {} size={} MiB",
            target.name,
            target.size / (1024 * 1024)
        );
    }
    // [END rust_storage_striped_compose_gib] ANCHOR_END: compose-GiB

    // [START rust_storage_striped_seed_function_end] ANCHOR: seed-function-end
    Ok(())
}
// [END rust_storage_striped_seed_function_end] ANCHOR_END: seed-function-end

// [START rust_storage_striped_download_function] ANCHOR: download-function
async fn download(
    client: Storage,
    control: StorageControl,
    bucket_name: &str,
    object_name: &str,
    stripe_size: usize,
    destination: &str,
) -> anyhow::Result<()> {
    // [END rust_storage_striped_download_function] ANCHOR_END: download-function
    // [START rust_storage_striped_get_metadata] ANCHOR: get-metadata
    let metadata = control
        .get_object()
        .set_bucket(bucket_name)
        .set_object(object_name)
        .send()
        .await?;
    // [END rust_storage_striped_get_metadata] ANCHOR_END: get-metadata

    // [START rust_storage_striped_create_destination] ANCHOR: create-destination
    let file = Arc::new(std::fs::File::create(destination)?);
    file.set_len(metadata.size as u64)?;
    // [END rust_storage_striped_create_destination] ANCHOR_END: create-destination
    let start = std::time::Instant::now();

    // [START rust_storage_striped_compute_stripes] ANCHOR: compute-stripes
    let size = metadata.size as u64;
    let limit = stripe_size as u64;
    let count = size / limit;
    let mut stripes = (0..count)
        .map(|i| write_stripe(client.clone(), &file, i * limit, limit, &metadata))
        .collect::<Vec<_>>();
    if !size.is_multiple_of(limit) {
        stripes.push(write_stripe(
            client.clone(),
            &file,
            count * limit,
            limit,
            &metadata,
        ))
    }
    // [END rust_storage_striped_compute_stripes] ANCHOR_END: compute-stripes

    // [START rust_storage_striped_run_stripes] ANCHOR: run-stripes
    futures::future::join_all(stripes)
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    // [END rust_storage_striped_run_stripes] ANCHOR_END: run-stripes

    let elapsed = start.elapsed();
    let mib = metadata.size as f64 / (1024.0 * 1024.0);
    let bw = mib / elapsed.as_secs_f64();
    println!(
        "Completed {mib:.2} MiB download in {elapsed:?}, using {count} stripes, effective bandwidth = {bw:.2} MiB/s"
    );

    // [START rust_storage_striped_download_function_end] ANCHOR: download-function-end
    Ok(())
}
// [END rust_storage_striped_download_function_end] ANCHOR_END: download-function-end

// [START rust_storage_striped_write_stripe_function] ANCHOR: write-stripe-function
async fn write_stripe(
    client: Storage,
    file: &Arc<std::fs::File>,
    offset: u64,
    limit: u64,
    metadata: &Object,
) -> anyhow::Result<()> {
    use google_cloud_storage::model_ext::ReadRange;
    // [END rust_storage_striped_write_stripe_function] ANCHOR_END: write-stripe-function
    // [START rust_storage_striped_write_stripe_reader] ANCHOR: write-stripe-reader
    let mut reader = client
        .read_object(&metadata.bucket, &metadata.name)
        // [END rust_storage_striped_write_stripe_reader] ANCHOR_END: write-stripe-reader
        // [START rust_storage_striped_write_stripe_reader_generation] ANCHOR: write-stripe-reader-generation
        .set_generation(metadata.generation)
        // [END rust_storage_striped_write_stripe_reader_generation] ANCHOR_END: write-stripe-reader-generation
        // [START rust_storage_striped_write_stripe_reader_range] ANCHOR: write-stripe-reader-range
        .set_read_range(ReadRange::segment(offset, limit))
        // [END rust_storage_striped_write_stripe_reader_range] ANCHOR_END: write-stripe-reader-range
        // [START rust_storage_striped_write_stripe_reader] ANCHOR: write-stripe-reader
        .send()
        .await?;
    // [END rust_storage_striped_write_stripe_reader] ANCHOR_END: write-stripe-reader
    // [START rust_storage_striped_write_stripe_loop] ANCHOR: write-stripe-loop
    let mut current_pos = offset;
    while let Some(b) = reader.next().await.transpose()? {
        let chunk_len = b.len() as u64;
        let handle = file.clone();
        #[cfg(unix)]
        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            handle.write_all_at(&b, current_pos)
        })
        .await??;
        #[cfg(windows)]
        tokio::task::spawn_blocking(move || {
            use std::os::windows::fs::FileExt;
            handle.seek_write(&b, current_pos)
        })
        .await??;
        current_pos += chunk_len;
    }
    // [END rust_storage_striped_write_stripe_loop] ANCHOR_END: write-stripe-loop
    // [START rust_storage_striped_write_stripe_function_end] ANCHOR: write-stripe-function-end
    Ok(())
}
// [END rust_storage_striped_write_stripe_function_end] ANCHOR_END: write-stripe-function-end
// [END rust_storage_striped_all] ANCHOR_END: all

pub async fn test(bucket_name: &str, destination: &str) -> anyhow::Result<()> {
    const MB: usize = 1024 * 1024;
    let client = Storage::builder().build().await?;
    let control = StorageControl::builder().build().await?;
    seed(client.clone(), control.clone(), bucket_name).await?;
    download(
        client.clone(),
        control.clone(),
        bucket_name,
        "32MiB.txt",
        32 * MB,
        destination,
    )
    .await?;
    download(
        client.clone(),
        control.clone(),
        bucket_name,
        "32MiB.txt",
        MB,
        destination,
    )
    .await?;
    // These only work on Linux because they use /dev/shm.
    #[cfg(all(target_os = "linux", feature = "run-large-downloads"))]
    {
        let destination = "/dev/shm/output";
        download(
            client.clone(),
            control.clone(),
            bucket_name,
            "32GiB.txt",
            512 * MB,
            destination,
        )
        .await?;
        download(
            client.clone(),
            control.clone(),
            bucket_name,
            "32GiB.txt",
            256 * MB,
            destination,
        )
        .await?;
        download(
            client.clone(),
            control.clone(),
            bucket_name,
            "1GiB.txt",
            1024 * MB,
            destination,
        )
        .await?;
        download(
            client.clone(),
            control.clone(),
            bucket_name,
            "1GiB.txt",
            512 * MB,
            destination,
        )
        .await?;
        download(
            client.clone(),
            control.clone(),
            bucket_name,
            "4GiB.txt",
            512 * MB,
            destination,
        )
        .await?;
        download(
            client.clone(),
            control.clone(),
            bucket_name,
            "8GiB.txt",
            512 * MB,
            destination,
        )
        .await?;
        download(
            client.clone(),
            control.clone(),
            bucket_name,
            "16GiB.txt",
            512 * MB,
            destination,
        )
        .await?;
    }
    Ok(())
}
